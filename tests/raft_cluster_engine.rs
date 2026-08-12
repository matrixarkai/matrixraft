// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    InstallSnapshotRequest, RaftCluster, RaftConfig, RaftConfigError, RaftLearnerAutoPromoteState,
    RaftMembershipOperation, RaftSnapshot, RustRaftAdminCommand, RustRaftAppendEntriesRequest,
    RustRaftAppendEntriesResponse, RustRaftApplySnapshotFence, RustRaftConfig, RustRaftConsensus,
    RustRaftError, RustRaftInstallSnapshotResponse, RustRaftLogEntry, RustRaftLogId,
    RustRaftMessage, RustRaftPeer, RustRaftPeerProgressState, RustRaftProposeOptions,
    RustRaftReadIndexRequest, RustRaftReplicaRole, RustRaftRole, RustRaftSnapshotChunk,
    RustRaftSnapshotMeta, RustRaftSnapshotState, RustRaftStepResult, RustRaftVoteRequest,
    RustRaftVoteResponse,
};

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn three_node_cluster() -> RaftCluster {
    RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid cluster")
}

fn five_node_cluster() -> RaftCluster {
    RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Voter),
            peer(5, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid five-node cluster")
}

#[test]
fn raft_config_validates_timing_and_capacity() {
    let mut config = RustRaftConfig::default();
    config.heartbeat_interval_ms = config.election_timeout_ms;

    assert_eq!(
        config.validate(),
        Err(RaftConfigError::HeartbeatNotLessThanElection {
            heartbeat_interval_ms: 1_000,
            election_timeout_ms: 1_000,
        })
    );

    config = RustRaftConfig::default();
    config.max_payload_bytes = 0;
    assert_eq!(config.validate(), Err(RaftConfigError::ZeroMaxPayloadBytes));

    config = RustRaftConfig::default();
    config.max_log_buffer_bytes = 0;
    assert_eq!(
        config.validate(),
        Err(RaftConfigError::ZeroMaxLogBufferBytes)
    );
}

#[test]
fn cluster_start_campaigns_and_tracks_leader_term() {
    let mut cluster = three_node_cluster();

    cluster.start().expect("cluster starts");
    assert_eq!(cluster.leader_id(), Some(1));

    let leader_status = cluster.status(1).expect("leader status");
    assert_eq!(leader_status.role, RustRaftRole::Leader);
    assert_eq!(leader_status.term, 1);

    cluster.campaign(2, false).expect("campaign to node 2");
    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(cluster.status(2).expect("new leader status").term, 2);
}

#[test]
fn initial_campaign_appends_noop_like_matrixraft() {
    let mut cluster = three_node_cluster();

    cluster.start().expect("cluster starts");
    let leader = cluster.leader_id().expect("leader");
    let leader_record = cluster
        .wal_record_for(leader)
        .expect("leader wal after start");

    assert_eq!(leader_record.entries.len(), 1);
    assert_eq!(
        leader_record.entries[0].log_id,
        RustRaftLogId { term: 1, index: 1 }
    );
    assert_eq!(leader_record.entries[0].payload, b"no-op".to_vec());
    assert_eq!(
        cluster.status(leader).expect("leader status").commit_index,
        0
    );
}

#[test]
fn campaign_on_current_leader_is_noop_like_matrixraft_admin_election() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"base".to_vec()).expect("base append");
    let leader = cluster.leader_id().expect("leader");
    let before = cluster.status(leader).expect("leader status");
    let before_log_len = cluster
        .wal_record_for(leader)
        .expect("leader wal before campaign")
        .entries
        .len();

    cluster
        .campaign(leader, true)
        .expect("campaign on current leader is ignored");

    let after = cluster
        .status(leader)
        .expect("leader status after campaign");
    assert_eq!(after.term, before.term);
    assert_eq!(after.last_log_index, before.last_log_index);
    assert_eq!(
        cluster
            .wal_record_for(leader)
            .expect("leader wal after campaign")
            .entries
            .len(),
        before_log_len
    );
    assert_eq!(cluster.leader_id(), Some(leader));
}

#[test]
fn prohibits_election_blocks_normal_campaign_but_allows_forced_campaign() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.set_prohibits_election(true);
    assert!(cluster.prohibits_election());

    let vote = cluster.pre_vote(2).expect("pre-vote");
    assert!(!vote.vote_granted);
    assert_eq!(vote.reason, "election_prohibited");
    assert!(cluster
        .campaign(2, false)
        .expect_err("normal campaign blocked")
        .to_string()
        .contains("election is prohibited"));

    cluster.campaign(2, true).expect("forced campaign");
    assert_eq!(cluster.leader_id(), Some(2));
}

#[test]
fn prohibits_election_does_not_reject_remote_votes_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"base".to_vec()).expect("base append");
    cluster.set_follower_lease_valid(false);
    cluster.set_leader_lease_valid(false);
    cluster.set_prohibits_election(true);

    let pre_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: true,
                force: false,
            },
        )
        .expect("remote pre-vote is handled");
    assert!(pre_vote.vote_granted);
    assert_eq!(pre_vote.reason, "pre_vote_granted");
    assert_eq!(cluster.status(2).expect("node 2 status").term, 1);

    let vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("remote vote is handled");
    assert!(vote.vote_granted);
    assert_eq!(vote.reason, "vote_granted");
    assert_eq!(cluster.leader_id(), None);
    assert_eq!(cluster.status(2).expect("node 2 status").term, 2);
}

#[test]
fn timeout_now_campaigns_only_followers_like_baseline_raft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.set_prohibits_election(true);

    let follower_timeout = cluster.timeout_now(1, 2).expect("timeout-now follower");
    assert!(follower_timeout.campaigned);
    assert_eq!(follower_timeout.reason, "timeout_now_campaign");
    assert_eq!(cluster.leader_id(), Some(2));

    let mut learner = peer(4, RustRaftReplicaRole::Learner);
    learner.auto_promote = false;
    cluster.add_learner(learner).expect("add learner");
    let learner_term = cluster.status(4).expect("learner status").term;
    let learner_timeout = cluster.timeout_now(2, 4).expect("timeout-now learner");
    assert!(!learner_timeout.campaigned);
    assert_eq!(learner_timeout.reason, "timeout_now_ignored_Learner");
    let learner_status = cluster.status(4).expect("learner status after timeout");
    assert_eq!(learner_status.role, RustRaftRole::Learner);
    assert_eq!(learner_status.term, learner_term);

    cluster
        .add_witness(peer(5, RustRaftReplicaRole::Witness))
        .expect("add witness");
    let witness_term = cluster.status(5).expect("witness status").term;
    let witness_timeout = cluster.timeout_now(2, 5).expect("timeout-now witness");
    assert!(!witness_timeout.campaigned);
    assert_eq!(witness_timeout.reason, "timeout_now_ignored_Witness");
    assert_eq!(
        cluster
            .status(5)
            .expect("witness status after timeout")
            .term,
        witness_term
    );
}

#[test]
fn added_peer_catches_up_immediately_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"before-add".to_vec()).expect("propose");

    cluster
        .add_learner(peer(4, RustRaftReplicaRole::Learner))
        .expect("add learner");
    let learner = cluster.status(4).expect("learner status");
    assert_eq!(learner.last_log_index, 2);
    assert_eq!(learner.commit_index, 2);
    assert_eq!(
        cluster
            .peer_pipeline_status(4)
            .expect("learner pipeline")
            .match_index,
        2
    );

    cluster
        .add_witness(peer(5, RustRaftReplicaRole::Witness))
        .expect("add witness");
    let witness = cluster.status(5).expect("witness status");
    assert_eq!(witness.last_log_index, 2);
    assert_eq!(witness.commit_index, 2);
    assert_eq!(
        cluster
            .peer_pipeline_status(5)
            .expect("witness pipeline")
            .match_index,
        2
    );
}

#[test]
fn commit_quorum_counts_stored_match_from_unhealthy_peer_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    let leader = cluster.leader_id().expect("leader");
    let term = cluster.status(leader).expect("leader status").term;

    for target in [leader, 2] {
        cluster
            .append_entries_to(
                target,
                RustRaftAppendEntriesRequest {
                    group_id: 7,
                    term,
                    leader_id: leader,
                    prev_log_id: None,
                    entries: vec![RustRaftLogEntry {
                        log_id: RustRaftLogId { term, index: 1 },
                        payload: b"stored-quorum".to_vec(),
                        is_command: true,
                    }],
                    leader_commit: 0,
                    lease_epoch: 0,
                },
            )
            .expect("seed stored match");
    }

    cluster
        .set_node_healthy(2, false)
        .expect("peer 2 goes offline");
    cluster.set_ignore_witness(true);

    assert_eq!(
        cluster.status(leader).expect("leader status").commit_index,
        1
    );
    assert_eq!(
        cluster.status(2).expect("offline peer status").commit_index,
        0
    );
}

#[test]
fn propose_replicates_to_quorum_and_advances_commit_and_apply() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let log_id = cluster.propose(b"set a=1".to_vec()).expect("propose");
    assert_eq!(log_id, RustRaftLogId { term: 1, index: 2 });

    for node_id in [1, 2, 3] {
        let status = cluster.status(node_id).expect("node status");
        assert_eq!(status.commit_index, 2);
        assert_eq!(status.applied_index, 2);
        assert_eq!(status.last_log_index, 2);
    }
}

#[test]
fn propose_preserves_command_entry_flag_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Witness),
        ],
    )
    .expect("cluster with witness");
    cluster.start().expect("cluster starts");

    cluster
        .propose_with_options(
            b"opaque-entry".to_vec(),
            RustRaftProposeOptions {
                expected_term: Some(1),
                is_command: false,
                ..Default::default()
            },
        )
        .expect("non-command propose");

    for node_id in [1, 2, 3] {
        let record = cluster.wal_record_for(node_id).expect("node wal");
        assert_eq!(record.entries.len(), 2);
        assert_eq!(record.entries[0].payload, b"no-op".to_vec());
        assert_eq!(
            record.entries[1].log_id,
            RustRaftLogId { term: 1, index: 2 }
        );
        assert!(!record.entries[1].is_command);
        assert_eq!(record.entries[1].payload, b"opaque-entry".to_vec());
    }

    let witness_record = cluster.wal_record_for(4).expect("witness wal");
    assert_eq!(witness_record.entries.len(), 2);
    assert!(!witness_record.entries[1].is_command);
    assert!(witness_record.entries[1].payload.is_empty());
    assert_eq!(cluster.status(4).expect("witness status").last_log_index, 2);
}

#[test]
fn membership_proposal_downgrades_second_unapplied_change_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Witness),
        ],
    )
    .expect("cluster with witness");
    cluster.start().expect("cluster starts");

    let first = cluster
        .propose_with_options(
            b"add-peer-5".to_vec(),
            RustRaftProposeOptions {
                is_membership_change: true,
                ..Default::default()
            },
        )
        .expect("first membership proposal");
    assert_eq!(first.index, 2);
    assert_eq!(cluster.pending_membership_change_index(), Some(2));
    cluster
        .mark_apply_task_inflight(1, 2)
        .expect("first membership apply remains inflight");

    let second = cluster
        .propose_with_options(
            b"add-peer-6".to_vec(),
            RustRaftProposeOptions {
                is_membership_change: true,
                ..Default::default()
            },
        )
        .expect("second membership proposal is downgraded to normal");
    assert_eq!(second.index, 3);
    assert_eq!(cluster.pending_membership_change_index(), Some(2));

    let witness_record = cluster.wal_record_for(4).expect("witness wal");
    assert_eq!(witness_record.entries.len(), 3);
    assert_eq!(witness_record.entries[1].payload, b"add-peer-5".to_vec());
    assert!(
        !witness_record.entries[1].is_command,
        "config changes are not user commands"
    );
    assert!(
        witness_record.entries[2].payload.is_empty(),
        "witnesses do not retain downgraded command payloads"
    );
    assert!(
        !witness_record.entries[2].is_command,
        "witnesses store downgraded membership proposals as metadata"
    );

    cluster
        .submit_apply_result(1, 2, false)
        .expect("first membership apply completes");
    cluster.mark_membership_change_applied(2);
    let third = cluster
        .propose_with_options(
            b"add-peer-7".to_vec(),
            RustRaftProposeOptions {
                is_membership_change: true,
                ..Default::default()
            },
        )
        .expect("new membership proposal after safe apply");
    assert_eq!(third.index, 4);
    assert_eq!(cluster.pending_membership_change_index(), Some(4));
}

#[test]
fn witness_append_entries_store_command_entries_as_metadata_without_payload() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Witness),
        ],
    )
    .expect("cluster with witness");
    cluster.start().expect("cluster starts");

    let response = cluster
        .append_entries_to(
            4,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 2 },
                    payload: b"user-command".to_vec(),
                    is_command: true,
                }],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("append command to witness");
    assert!(response.success);
    assert_eq!(response.match_index, 2);

    let witness_record = cluster.wal_record_for(4).expect("witness wal");
    assert_eq!(witness_record.entries.len(), 2);
    assert_eq!(
        witness_record.entries[1].log_id,
        RustRaftLogId { term: 1, index: 2 }
    );
    assert!(!witness_record.entries[1].is_command);
    assert!(witness_record.entries[1].payload.is_empty());
}

#[test]
fn membership_proposal_ignores_stale_expected_term_like_matrixraft_config_change() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let stale_normal = cluster
        .propose_with_options(
            b"normal-stale-term".to_vec(),
            RustRaftProposeOptions {
                expected_term: Some(0),
                ..Default::default()
            },
        )
        .expect_err("normal proposal still checks expected term");
    assert!(stale_normal
        .to_string()
        .contains("expected term 0 does not match current term 1"));

    let membership = cluster
        .propose_with_options(
            b"config-change-stale-term".to_vec(),
            RustRaftProposeOptions {
                expected_term: Some(0),
                is_membership_change: true,
                ..Default::default()
            },
        )
        .expect("membership proposal stamps current term like MatrixRaft");
    assert_eq!(membership, RustRaftLogId { term: 1, index: 2 });
    assert_eq!(cluster.pending_membership_change_index(), Some(2));

    let leader_record = cluster.wal_record_for(1).expect("leader wal");
    assert_eq!(leader_record.entries.len(), 2);
    assert_eq!(leader_record.entries[1].log_id, membership);
    assert!(!leader_record.entries[1].is_command);
}

#[test]
fn rejected_apply_result_blocks_apply_until_snapshot_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    cluster.propose(b"first".to_vec()).expect("first propose");
    let stale_committed_snapshot = cluster.install_snapshot_to(
        2,
        RaftSnapshot {
            group_id: 7,
            meta: RustRaftSnapshotMeta {
                snapshot_id: "already-committed".to_string(),
                last_log_id: RustRaftLogId { term: 1, index: 2 },
                membership: vec![1, 2, 3],
                members: Vec::new(),
            },
            payload: b"stale-committed-snapshot".to_vec(),
        },
        RustRaftApplySnapshotFence {
            applied_index: 2,
            commit_index: 2,
            installed_snapshot_index: 2,
            first_retained_log_index: 3,
        },
    );
    assert!(stale_committed_snapshot
        .expect_err("snapshot at committed index is stale")
        .to_string()
        .contains("not newer than committed index 2"));

    cluster
        .submit_apply_result(2, 2, true)
        .expect("rejected follower apply");
    assert_eq!(
        cluster.rejected_apply_index(2).expect("rejected index"),
        Some(3)
    );
    let require_snapshot = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("heartbeat carries snapshot requirement");
    assert_eq!(require_snapshot.require_snapshot, Some(3));

    let stale_snapshot = cluster.install_snapshot_to(
        2,
        RaftSnapshot {
            group_id: 7,
            meta: RustRaftSnapshotMeta {
                snapshot_id: "apply-recovery-too-old".to_string(),
                last_log_id: RustRaftLogId { term: 1, index: 2 },
                membership: vec![1, 2, 3],
                members: Vec::new(),
            },
            payload: b"stale-snapshot".to_vec(),
        },
        RustRaftApplySnapshotFence {
            applied_index: 2,
            commit_index: 2,
            installed_snapshot_index: 2,
            first_retained_log_index: 3,
        },
    );
    assert!(stale_snapshot
        .expect_err("stale rejected-apply recovery snapshot")
        .to_string()
        .contains("below rejected apply index 3"));
    assert_eq!(
        cluster.rejected_apply_index(2).expect("rejected index"),
        Some(3)
    );

    cluster.propose(b"second".to_vec()).expect("second propose");
    let blocked = cluster.status(2).expect("blocked follower status");
    assert_eq!(blocked.commit_index, 3);
    assert_eq!(blocked.applied_index, 2);

    cluster
        .install_snapshot_to(
            2,
            RaftSnapshot {
                group_id: 7,
                meta: RustRaftSnapshotMeta {
                    snapshot_id: "apply-recovery-2".to_string(),
                    last_log_id: RustRaftLogId { term: 1, index: 3 },
                    membership: vec![1, 2, 3],
                    members: Vec::new(),
                },
                payload: b"snapshot".to_vec(),
            },
            RustRaftApplySnapshotFence {
                applied_index: 3,
                commit_index: 3,
                installed_snapshot_index: 3,
                first_retained_log_index: 4,
            },
        )
        .expect("snapshot clears rejected apply");
    assert_eq!(
        cluster.rejected_apply_index(2).expect("rejected index"),
        None
    );
    assert_eq!(
        cluster.status(2).expect("recovered follower").applied_index,
        3
    );

    let mut unapplied_record = cluster.wal_record_for(2).expect("follower wal");
    unapplied_record.apply_snapshot_fence.applied_index = 2;
    unapplied_record.apply_snapshot_fence.commit_index = 3;
    cluster
        .restore_wal_record(unapplied_record)
        .expect("restore follower with pending apply");

    let pending_apply_snapshot = cluster.install_snapshot_to(
        2,
        RaftSnapshot {
            group_id: 7,
            meta: RustRaftSnapshotMeta {
                snapshot_id: "blocked-by-unapplied-entries".to_string(),
                last_log_id: RustRaftLogId { term: 1, index: 4 },
                membership: vec![1, 2, 3],
                members: Vec::new(),
            },
            payload: b"snapshot".to_vec(),
        },
        RustRaftApplySnapshotFence {
            applied_index: 4,
            commit_index: 4,
            installed_snapshot_index: 4,
            first_retained_log_index: 5,
        },
    );
    assert!(pending_apply_snapshot
        .expect_err("snapshot waits for pending apply")
        .to_string()
        .contains("unapplied entries 2..3"));

    cluster
        .mark_apply_task_inflight(2, 3)
        .expect("pending entry is dispatched to apply");
    cluster
        .submit_apply_result(2, 3, false)
        .expect("pending apply drains");
    cluster
        .install_snapshot_to(
            2,
            RaftSnapshot {
                group_id: 7,
                meta: RustRaftSnapshotMeta {
                    snapshot_id: "loaded-after-apply-drains".to_string(),
                    last_log_id: RustRaftLogId { term: 1, index: 4 },
                    membership: vec![1, 2, 3],
                    members: Vec::new(),
                },
                payload: b"snapshot".to_vec(),
            },
            RustRaftApplySnapshotFence {
                applied_index: 4,
                commit_index: 4,
                installed_snapshot_index: 4,
                first_retained_log_index: 5,
            },
        )
        .expect("snapshot loads after committed entries apply");
    assert_eq!(
        cluster
            .status(2)
            .expect("snapshot-loaded follower")
            .applied_index,
        4
    );

    cluster.propose(b"fourth".to_vec()).expect("fourth propose");
    cluster
        .mark_apply_task_inflight(2, 5)
        .expect("follower apply task is inflight");
    assert_eq!(cluster.safety_applied_index(2).expect("safe apply"), 4);
    let inflight_apply_snapshot = cluster.install_snapshot_to(
        2,
        RaftSnapshot {
            group_id: 7,
            meta: RustRaftSnapshotMeta {
                snapshot_id: "blocked-by-inflight-apply".to_string(),
                last_log_id: RustRaftLogId { term: 1, index: 6 },
                membership: vec![1, 2, 3],
                members: Vec::new(),
            },
            payload: b"snapshot".to_vec(),
        },
        RustRaftApplySnapshotFence {
            applied_index: 6,
            commit_index: 6,
            installed_snapshot_index: 6,
            first_retained_log_index: 7,
        },
    );
    assert!(inflight_apply_snapshot
        .expect_err("snapshot waits for inflight apply")
        .to_string()
        .contains("inflight apply tasks 4..5"));

    cluster
        .submit_apply_result(2, 5, false)
        .expect("inflight apply completes");
    cluster
        .install_snapshot_to(
            2,
            RaftSnapshot {
                group_id: 7,
                meta: RustRaftSnapshotMeta {
                    snapshot_id: "loaded-after-inflight-apply".to_string(),
                    last_log_id: RustRaftLogId { term: 1, index: 6 },
                    membership: vec![1, 2, 3],
                    members: Vec::new(),
                },
                payload: b"snapshot".to_vec(),
            },
            RustRaftApplySnapshotFence {
                applied_index: 6,
                commit_index: 6,
                installed_snapshot_index: 6,
                first_retained_log_index: 7,
            },
        )
        .expect("snapshot loads after inflight apply completes");
}

#[test]
fn snapshot_install_resets_membership_and_preserves_receiver_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .propose(b"before-snapshot".to_vec())
        .expect("propose");
    cluster
        .set_node_healthy(2, false)
        .expect("isolate receiver");
    cluster
        .propose(b"after-reconfig".to_vec())
        .expect("propose");
    cluster.set_node_healthy(2, true).expect("heal receiver");

    cluster
        .install_snapshot_to(
            2,
            RaftSnapshot {
                group_id: 7,
                meta: RustRaftSnapshotMeta {
                    snapshot_id: "membership-reset".to_string(),
                    last_log_id: RustRaftLogId { term: 1, index: 3 },
                    membership: vec![1, 4, 5],
                    members: vec![
                        peer(4, RustRaftReplicaRole::Learner),
                        peer(5, RustRaftReplicaRole::Witness),
                    ],
                },
                payload: b"snapshot-with-membership".to_vec(),
            },
            RustRaftApplySnapshotFence {
                applied_index: 3,
                commit_index: 3,
                installed_snapshot_index: 3,
                first_retained_log_index: 4,
            },
        )
        .expect("snapshot install resets membership");

    let membership = cluster.membership();
    assert_eq!(membership.voters, vec![1, 2]);
    assert_eq!(membership.learners, vec![4]);
    assert_eq!(membership.witnesses, vec![5]);
    assert!(!cluster.node_ids().contains(&3));
    assert_eq!(
        cluster.status(4).expect("learner from snapshot").role,
        RustRaftRole::Learner
    );
    assert!(cluster.status(5).is_ok());
    assert_eq!(
        cluster
            .status(2)
            .expect("receiver preserved")
            .last_log_index,
        3
    );
}

#[test]
fn stale_snapshot_rpc_is_acknowledged_without_install_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"committed".to_vec()).expect("propose");

    let committed = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "already-committed-rpc".to_string(),
                        last_log_id: RustRaftLogId { term: 1, index: 2 },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"stale".to_vec(),
                    done: true,
                },
            },
        )
        .expect("stale committed snapshot is acknowledged");
    assert!(committed.accepted);
    assert_eq!(committed.committed_index, 2);
    assert_eq!(committed.reason, "stale_snapshot_ignored");
    assert_eq!(cluster.status(2).expect("status").last_snapshot_index, 0);

    cluster
        .submit_apply_result(2, 2, true)
        .expect("follower needs recovery snapshot");
    let below_recovery_floor = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "below-recovery-floor-rpc".to_string(),
                        last_log_id: RustRaftLogId { term: 1, index: 2 },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"stale".to_vec(),
                    done: true,
                },
            },
        )
        .expect("snapshot below rejected apply floor is acknowledged");
    assert!(below_recovery_floor.accepted);
    assert_eq!(below_recovery_floor.reason, "stale_snapshot_ignored");
    assert_eq!(
        cluster.rejected_apply_index(2).expect("rejected index"),
        Some(3)
    );
}

#[test]
fn snapshot_rpc_waits_for_apply_drain_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"first".to_vec()).expect("first propose");
    cluster.propose(b"second".to_vec()).expect("second propose");

    let mut unapplied_record = cluster.wal_record_for(2).expect("follower wal");
    unapplied_record.apply_snapshot_fence.applied_index = 1;
    unapplied_record.apply_snapshot_fence.commit_index = 2;
    cluster
        .restore_wal_record(unapplied_record)
        .expect("restore follower with pending apply");

    let pending = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "pending-unapplied-rpc".to_string(),
                        last_log_id: RustRaftLogId { term: 1, index: 3 },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"pending-snapshot".to_vec(),
                    done: true,
                },
            },
        )
        .expect("fresh snapshot waits behind apply drain");
    assert!(pending.accepted);
    assert_eq!(pending.reason, "snapshot_pending_apply");
    assert_eq!(
        cluster
            .status(2)
            .expect("pending status")
            .last_snapshot_index,
        0
    );

    cluster
        .mark_apply_task_inflight(2, 2)
        .expect("dispatch pending apply");
    cluster
        .submit_apply_result(2, 2, false)
        .expect("apply result loads pending snapshot");
    let recovered = cluster.status(2).expect("snapshot-loaded status");
    assert_eq!(recovered.last_snapshot_index, 3);
    assert_eq!(recovered.applied_index, 3);
}

#[test]
fn compaction_waits_for_safe_apply_index_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"first".to_vec()).expect("first propose");
    cluster.propose(b"second".to_vec()).expect("second propose");

    let inflight = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ApplyTaskInflight {
                node_id: 2,
                applied_index: 3,
            },
        })
        .expect("follower apply task inflight through admin step");
    assert_eq!(inflight, RustRaftStepResult::Handled);
    assert_eq!(cluster.safety_applied_index(2).expect("safe apply"), 2);

    let compacted = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CompactLogsThrough { log_index: 3 },
        })
        .expect("compact through admin step");
    let RustRaftStepResult::CompactedLogs(released) = compacted else {
        panic!("unexpected compaction step response: {compacted:?}");
    };
    assert!(released > 0);
    let inflight_record = cluster.wal_record_for(2).expect("follower wal");
    assert!(
        inflight_record
            .entries
            .iter()
            .any(|entry| entry.log_id.index == 3),
        "inflight apply entry must stay retained until safe-applied"
    );

    cluster
        .submit_apply_result(2, 3, false)
        .expect("inflight apply completes");
    cluster.compact_logs_through(3);
    let compacted_record = cluster
        .wal_record_for(2)
        .expect("follower wal after compact");
    assert!(
        compacted_record
            .entries
            .iter()
            .all(|entry| entry.log_id.index > 3),
        "entry can be compacted after safe apply catches up"
    );

    let mut leader_cluster = three_node_cluster();
    leader_cluster.start().expect("leader cluster starts");
    let leader = leader_cluster.leader_id().expect("leader");
    leader_cluster
        .set_node_healthy(2, false)
        .expect("make follower miss replication");
    leader_cluster
        .propose(b"leader-retains-for-online-lagging-peer".to_vec())
        .expect("propose while follower is offline");
    leader_cluster
        .propose(b"leader-retains-boundary-entry".to_vec())
        .expect("second propose while follower is offline");
    leader_cluster
        .set_node_healthy(2, true)
        .expect("follower is online but lagging");
    assert_eq!(
        leader_cluster
            .min_replicated_index(leader)
            .expect("min replicated"),
        1
    );
    leader_cluster.compact_logs_through(1);
    let leader_record = leader_cluster
        .wal_record_for(leader)
        .expect("leader wal before lag catch-up");
    assert!(
        leader_record
            .entries
            .iter()
            .any(|entry| entry.log_id.index == 2),
        "online lagging follower keeps leader log retained"
    );
    leader_cluster
        .catch_up_peer(2)
        .expect("catch up lagging follower");
    leader_cluster
        .catch_up_peer(3)
        .expect("catch up second lagging follower");
    leader_cluster
        .submit_apply_result(leader, 3, false)
        .expect("leader safe apply catches up");
    assert_eq!(
        leader_cluster
            .min_replicated_index(leader)
            .expect("min replicated after catch-up"),
        3
    );
    assert!(leader_cluster.release_memory().expect("release memory"));
    let compacted_leader_record = leader_cluster
        .wal_record_for(leader)
        .expect("leader wal after lag catch-up");
    assert!(
        compacted_leader_record
            .entries
            .iter()
            .all(|entry| entry.log_id.index > 2),
        "leader can compact entries before the replicated/apply boundary"
    );
    assert!(
        compacted_leader_record
            .entries
            .iter()
            .any(|entry| entry.log_id.index == 3),
        "release-memory retains the replicated/apply boundary entry like MatrixRaft"
    );
}

#[test]
fn busy_propose_releases_safe_memory_like_matrixraft() {
    let config = RaftConfig {
        max_log_buffer_bytes: 9,
        ..Default::default()
    };
    let mut cluster = RaftCluster::new(
        7,
        config,
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    let leader = cluster.leader_id().expect("leader");

    cluster.propose(b"abc".to_vec()).expect("first propose");
    assert!(!cluster.is_busy());
    cluster.propose(b"def".to_vec()).expect("second propose");
    assert!(!cluster.is_busy());
    cluster.propose(b"ghi".to_vec()).expect("third propose");
    assert!(cluster.is_busy());

    let fourth = cluster
        .propose(b"jkl".to_vec())
        .expect("busy propose releases safe memory before append");
    assert_eq!(fourth.index, 5);
    let record = cluster.wal_record_for(leader).expect("leader wal");
    assert!(
        record.entries.iter().all(|entry| entry.log_id.index > 3),
        "busy propose released compactable entry before appending"
    );
    assert!(
        record.entries.iter().any(|entry| entry.log_id.index == 4),
        "release keeps the MatrixRaft boundary entry"
    );
    assert!(
        record.entries.iter().any(|entry| entry.log_id.index == 5),
        "new proposal is appended after release"
    );
}

#[test]
fn high_watermark_propose_releases_safe_memory_before_hard_busy_like_matrixraft() {
    let config = RaftConfig {
        max_log_buffer_bytes: 9,
        ..Default::default()
    };
    let mut cluster = RaftCluster::new(
        7,
        config,
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    let leader = cluster.leader_id().expect("leader");

    cluster.propose(b"abc".to_vec()).expect("first propose");
    assert!(cluster.should_release_memory());
    assert!(!cluster.is_busy());

    let second = cluster
        .propose(b"def".to_vec())
        .expect("high-watermark propose releases memory before append");
    assert_eq!(second.index, 3);
    let record = cluster.wal_record_for(leader).expect("leader wal");
    assert!(
        record.entries.iter().all(|entry| entry.log_id.index > 1),
        "90 percent high watermark released compactable entries before hard busy"
    );
    assert!(
        record.entries.iter().any(|entry| entry.log_id.index == 2),
        "release keeps the replicated/apply boundary entry like MatrixRaft"
    );
    assert!(
        record.entries.iter().any(|entry| entry.log_id.index == 3),
        "new proposal is appended after high-watermark release"
    );
}

#[test]
fn local_stabled_result_does_not_commit_without_quorum_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.set_node_healthy(2, false).expect("isolate node 2");
    cluster.set_node_healthy(3, false).expect("isolate node 3");

    let proposed = cluster
        .propose(b"leader-only-durable".to_vec())
        .expect("leader accepts local proposal");
    assert_eq!(proposed.index, 2);
    assert_eq!(cluster.status(1).expect("leader status").commit_index, 1);

    assert!(cluster
        .submit_stabled_result(Some(1), Some(2), 0)
        .expect("local flush result is handled"));

    assert_eq!(
        cluster
            .status(1)
            .expect("leader status after local stable")
            .commit_index,
        1,
        "MatrixRaft updates local stabled match but still requires quorum to commit"
    );
    assert_eq!(cluster.status(2).expect("node 2 status").commit_index, 0);
    assert_eq!(cluster.status(3).expect("node 3 status").commit_index, 0);

    cluster.set_node_healthy(2, true).expect("heal node 2");
    cluster.catch_up_peer(2).expect("replicate to quorum peer");
    cluster
        .handle_append_entries_response(
            1,
            2,
            RustRaftAppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 2,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("follower append response advances quorum commit");
    assert_eq!(cluster.status(1).expect("leader committed").commit_index, 2);
    assert_eq!(
        cluster.status(2).expect("follower committed").commit_index,
        2
    );
}

#[test]
fn busy_follower_append_caps_batch_like_matrixraft() {
    let config = RaftConfig {
        max_log_buffer_bytes: 1,
        ..Default::default()
    };
    let mut cluster = RaftCluster::new(
        7,
        config,
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 1 },
                    payload: b"seed".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("seed append response");
    assert!(response.success);
    assert_eq!(cluster.status(2).expect("node 2 status").last_log_index, 1);

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                entries: vec![
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 2 },
                        payload: b"first".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 3 },
                        payload: b"second".to_vec(),
                        is_command: true,
                    },
                ],
                leader_commit: 3,
                lease_epoch: 0,
            },
        )
        .expect("busy batch append response");

    assert!(response.success);
    assert_eq!(response.match_index, 2);
    let follower = cluster.status(2).expect("node 2 status");
    assert_eq!(follower.last_log_index, 2);
    assert_eq!(follower.commit_index, 2);
}

#[test]
fn leader_apply_rejection_transfers_to_closest_follower_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .propose(b"leader-apply-reject".to_vec())
        .expect("propose");

    let old_leader = cluster.leader_id().expect("leader");
    cluster
        .submit_apply_result(old_leader, 2, true)
        .expect("leader rejected apply");

    assert_ne!(cluster.leader_id(), Some(old_leader));
    assert!(cluster.leader_id().is_some());
    assert_eq!(
        cluster
            .rejected_apply_index(old_leader)
            .expect("rejected index"),
        Some(3)
    );
    assert_eq!(
        cluster.status(old_leader).expect("old leader").role,
        RustRaftRole::Follower
    );
}

#[test]
fn append_entries_updates_follower_commit_and_rejects_missing_prev_log() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 8 }),
                entries: vec![],
                leader_commit: 8,
                lease_epoch: 0,
            },
        )
        .expect("append response");
    assert!(!response.success);

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 1 },
                    payload: b"x".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("append response");
    assert!(response.success);
    assert_eq!(cluster.status(2).expect("node 2 status").applied_index, 1);

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 99, index: 1 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 2 },
                    payload: b"bad-prev-term".to_vec(),
                    is_command: true,
                }],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("term mismatch response");
    assert!(!response.success);
    assert_eq!(response.rejection_hint, Some(2));
    assert_eq!(response.rejected_index, Some(2));
    assert_eq!(cluster.status(2).expect("node 2 status").last_log_index, 1);

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 2 },
                    payload: b"advance-floor".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("second append response");
    assert!(response.success);
    assert_eq!(cluster.status(2).expect("node 2 status").commit_index, 1);

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("heartbeat commit response");
    assert!(response.success);
    assert_eq!(response.match_index, 2);
    assert_eq!(cluster.status(2).expect("node 2 status").commit_index, 2);

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 4 },
                    payload: b"gap-entry".to_vec(),
                    is_command: true,
                }],
                leader_commit: 4,
                lease_epoch: 0,
            },
        )
        .expect("gap append response");
    assert!(!response.success);
    assert_eq!(response.rejection_hint, Some(3));
    assert_eq!(cluster.status(2).expect("node 2 status").last_log_index, 2);

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 99, index: 1 }),
                entries: vec![],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("old committed prev response");
    assert!(response.success);
    assert_eq!(response.match_index, 2);
    assert_eq!(response.rejection_hint, None);

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 1 },
                    payload: b"below-commit-floor".to_vec(),
                    is_command: true,
                }],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("implicit zero prev below committed response");
    assert!(response.success);
    assert_eq!(response.match_index, 2);
    assert_eq!(cluster.status(2).expect("node 2 status").last_log_index, 2);

    cluster.campaign(2, true).expect("make node 2 leader");
    assert_eq!(cluster.leader_id(), Some(2));
    let node_2_leader_term = cluster.status(2).expect("node 2 leader status").term;
    let stale_term_response = cluster
        .append_entries_to(
            3,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: node_2_leader_term.saturating_sub(1),
                leader_id: 1,
                prev_log_id: None,
                entries: vec![],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("stale-term append response");
    assert!(!stale_term_response.success);
    assert_eq!(stale_term_response.term, node_2_leader_term);
    assert_eq!(cluster.leader_id(), Some(2));

    let same_term_claim = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: node_2_leader_term,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId {
                    term: node_2_leader_term,
                    index: 99,
                }),
                entries: vec![],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("same-term leader claim response");
    assert!(!same_term_claim.success);
    assert_eq!(same_term_claim.term, node_2_leader_term);
    assert_eq!(cluster.leader_id(), Some(1));
    assert_eq!(
        cluster.status(2).expect("node 2 status after claim").role,
        RustRaftRole::Follower
    );

    cluster.campaign(2, true).expect("make node 2 leader again");
    assert_eq!(cluster.leader_id(), Some(2));
    let high_term = cluster.status(2).expect("node 2 leader status").term + 1;
    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: high_term,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId {
                    term: high_term,
                    index: 99,
                }),
                entries: vec![],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("high-term missing prev response");
    assert!(!response.success);
    assert_eq!(response.term, high_term);
    assert_eq!(cluster.leader_id(), Some(1));
    assert_eq!(
        cluster.status(1).expect("node 1 status").role,
        RustRaftRole::Leader
    );
    let node_2_status = cluster.status(2).expect("node 2 status");
    assert_eq!(node_2_status.term, high_term);
    assert_eq!(node_2_status.role, RustRaftRole::Follower);
}

#[test]
fn append_entries_caps_commit_to_matched_boundary_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 1 },
                    payload: b"cap-commit".to_vec(),
                    is_command: true,
                }],
                leader_commit: 8,
                lease_epoch: 0,
            },
        )
        .expect("append response");

    assert!(response.success);
    assert_eq!(response.match_index, 1);
    assert_eq!(cluster.status(2).expect("node 2 status").commit_index, 1);
}

#[test]
fn append_entries_preserves_matching_suffix_like_baseline_raft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 1 },
                        payload: b"one".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 2 },
                        payload: b"two".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 3 },
                        payload: b"three".to_vec(),
                        is_command: true,
                    },
                ],
                leader_commit: 0,
                lease_epoch: 0,
            },
        )
        .expect("seed follower log");

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 2 },
                    payload: b"two-again".to_vec(),
                    is_command: true,
                }],
                leader_commit: 0,
                lease_epoch: 0,
            },
        )
        .expect("matching append response");

    assert!(response.success);
    let record = cluster.wal_record_for(2).expect("follower wal");
    let indexes: Vec<_> = record
        .entries
        .iter()
        .map(|entry| entry.log_id.index)
        .collect();
    assert_eq!(indexes, vec![1, 2, 3]);
}

#[test]
fn append_entries_reorder_queue_drains_adjacent_batches_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let base = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 1 },
                    payload: b"one".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("base append");
    assert!(base.success);

    let out_of_order = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 3 },
                    payload: b"three".to_vec(),
                    is_command: true,
                }],
                leader_commit: 3,
                lease_epoch: 0,
            },
        )
        .expect("out-of-order append");
    assert!(!out_of_order.success);
    assert_eq!(cluster.status(2).expect("node 2 status").last_log_index, 1);

    let gap_fill = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 2 },
                    payload: b"two".to_vec(),
                    is_command: true,
                }],
                leader_commit: 3,
                lease_epoch: 0,
            },
        )
        .expect("gap-fill append");
    assert!(gap_fill.success);
    let follower = cluster.status(2).expect("node 2 status");
    assert_eq!(follower.last_log_index, 3);
    assert_eq!(follower.commit_index, 3);
}

#[test]
fn append_entries_reorder_queue_is_cleared_on_leader_change_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let base = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 1 },
                    payload: b"one".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("base append");
    assert!(base.success);

    let queued = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 3 },
                    payload: b"stale-three".to_vec(),
                    is_command: true,
                }],
                leader_commit: 3,
                lease_epoch: 0,
            },
        )
        .expect("out-of-order append");
    assert!(!queued.success);

    let new_leader_append = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 3,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 2 },
                    payload: b"new-leader-two".to_vec(),
                    is_command: true,
                }],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("new leader append");
    assert!(new_leader_append.success);
    let follower = cluster.status(2).expect("node 2 status");
    assert_eq!(follower.last_log_index, 2);
    assert_eq!(follower.commit_index, 2);
}

#[test]
fn append_entries_conflict_hint_skips_local_term_run_like_baseline_raft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 2,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 1 },
                        payload: b"one".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 2, index: 2 },
                        payload: b"two".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 2, index: 3 },
                        payload: b"three".to_vec(),
                        is_command: true,
                    },
                ],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("seed follower log");

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 2,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 9, index: 3 }),
                entries: vec![],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("conflict response");

    assert!(!response.success);
    assert_eq!(response.rejection_hint, Some(2));
    assert_eq!(response.rejected_index, Some(4));
}

#[test]
fn duplicate_append_response_reports_packet_match_range_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 2,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 1 },
                        payload: b"one".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 2, index: 2 },
                        payload: b"two".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 2, index: 3 },
                        payload: b"three".to_vec(),
                        is_command: true,
                    },
                ],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("seed follower log");

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 2,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 2, index: 2 },
                    payload: b"two".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("duplicate append response");

    assert!(response.success);
    assert_eq!(response.match_index, 2);
    assert_eq!(cluster.status(2).expect("node 2 status").last_log_index, 3);
}

#[test]
fn conflict_truncation_clamps_read_index_current_term_floor_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 1 },
                        payload: b"one".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 2 },
                        payload: b"two".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 3 },
                        payload: b"three".to_vec(),
                        is_command: true,
                    },
                ],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("seed uncommitted tail");
    cluster.campaign(2, true).expect("make node 2 leader");
    let term = cluster.status(2).expect("leader status").term;

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term,
                leader_id: 2,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term, index: 2 },
                    payload: b"replacement-two".to_vec(),
                    is_command: true,
                }],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("conflicting append");
    assert!(response.success);

    let read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 2,
            min_commit_index: 2,
            allow_lease_read: false,
        })
        .expect("read index");
    assert!(read.safe);
    assert_eq!(read.read_index, 2);
}

#[test]
fn append_entries_conflict_truncation_resets_pending_membership_change_like_baseline_raft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 5,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 1 },
                        payload: b"one".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 2 },
                        payload: b"two".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 3 },
                        payload: b"three".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 2, index: 4 },
                        payload: b"four".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 3, index: 5 },
                        payload: b"membership-change".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 5, index: 6 },
                        payload: b"six".to_vec(),
                        is_command: true,
                    },
                ],
                leader_commit: 4,
                lease_epoch: 0,
            },
        )
        .expect("seed follower log");
    cluster
        .begin_pending_membership_change(5)
        .expect("pending membership change");

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 10,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 2, index: 4 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 6, index: 5 },
                    payload: b"replacement".to_vec(),
                    is_command: true,
                }],
                leader_commit: 4,
                lease_epoch: 0,
            },
        )
        .expect("conflicting append");

    assert!(response.success);
    assert_eq!(cluster.pending_membership_change_index(), None);
}

#[test]
fn append_entries_conflict_truncation_restores_surviving_membership_change_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 5,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 1 },
                        payload: b"one".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 2 },
                        payload: b"two".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 3 },
                        payload: b"three".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 2, index: 4 },
                        payload: b"four".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 3, index: 5 },
                        payload: b"membership-five".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 4, index: 6 },
                        payload: b"membership-six".to_vec(),
                        is_command: true,
                    },
                ],
                leader_commit: 4,
                lease_epoch: 0,
            },
        )
        .expect("seed follower log");
    cluster
        .begin_pending_membership_change(5)
        .expect("track first membership change");
    cluster.mark_membership_change_applied(5);
    cluster
        .begin_pending_membership_change(6)
        .expect("track second membership change");

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 10,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 3, index: 5 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 10, index: 6 },
                    payload: b"replacement-six".to_vec(),
                    is_command: true,
                }],
                leader_commit: 4,
                lease_epoch: 0,
            },
        )
        .expect("conflicting append");

    assert!(response.success);
    assert_eq!(cluster.pending_membership_change_index(), Some(5));
}

#[test]
fn append_entries_rejects_second_unapplied_membership_change_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .begin_pending_membership_change(5)
        .expect("pending membership change");

    let rejected = cluster
        .append_entries_with_membership_change_indexes_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 2,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 2, index: 1 },
                    payload: b"second-membership-change".to_vec(),
                    is_command: true,
                }],
                leader_commit: 0,
                lease_epoch: 0,
            },
            &[1],
        )
        .expect("append response");
    assert!(!rejected.success);
    assert_eq!(cluster.status(2).expect("node 2 status").last_log_index, 0);

    let partial = cluster
        .append_entries_with_membership_change_indexes_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 2,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 2, index: 1 },
                        payload: b"normal-before-config".to_vec(),
                        is_command: true,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 2, index: 2 },
                        payload: b"blocked-membership-change".to_vec(),
                        is_command: true,
                    },
                ],
                leader_commit: 0,
                lease_epoch: 0,
            },
            &[2],
        )
        .expect("append response");
    assert!(partial.success);
    let record = cluster.wal_record_for(2).expect("follower wal");
    assert_eq!(record.entries.len(), 1);
    assert_eq!(record.entries[0].payload, b"normal-before-config".to_vec());
    assert_eq!(cluster.pending_membership_change_index(), Some(5));

    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    for index in 1..=5 {
        cluster
            .propose(format!("seed-{index}").into_bytes())
            .expect("seed proposal");
    }
    cluster
        .begin_pending_membership_change(6)
        .expect("pending membership change at dispatched index");
    cluster
        .mark_apply_task_inflight(2, 6)
        .expect("membership apply is still inflight");

    let inflight_rejected = cluster
        .append_entries_with_membership_change_indexes_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 2,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 6 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 2, index: 7 },
                    payload: b"second-config-while-inflight".to_vec(),
                    is_command: true,
                }],
                leader_commit: 6,
                lease_epoch: 0,
            },
            &[7],
        )
        .expect("append response");
    assert!(!inflight_rejected.success);
    assert_eq!(cluster.status(2).expect("node 2 status").last_log_index, 6);

    cluster
        .submit_apply_result(2, 6, false)
        .expect("membership apply completes");
    let accepted_after_safe_apply = cluster
        .append_entries_with_membership_change_indexes_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 2,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 6 }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 2, index: 7 },
                    payload: b"second-config-after-safe-apply".to_vec(),
                    is_command: true,
                }],
                leader_commit: 6,
                lease_epoch: 0,
            },
            &[7],
        )
        .expect("append response after safe apply");
    assert!(accepted_after_safe_apply.success);
    assert_eq!(cluster.pending_membership_change_index(), Some(7));
}

#[test]
fn witness_preserves_membership_change_payload_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Witness),
        ],
    )
    .expect("cluster with witness");
    cluster.start().expect("cluster starts");

    let response = cluster
        .append_entries_with_membership_change_indexes_to(
            4,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 1 },
                        payload: b"ordinary-meta".to_vec(),
                        is_command: false,
                    },
                    RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 2 },
                        payload: b"add-node-5".to_vec(),
                        is_command: false,
                    },
                ],
                leader_commit: 0,
                lease_epoch: 0,
            },
            &[2],
        )
        .expect("append membership change to witness");
    assert!(response.success);

    let record = cluster.wal_record_for(4).expect("witness wal");
    assert_eq!(record.entries.len(), 2);
    assert!(record.entries[0].payload.is_empty());
    assert_eq!(record.entries[1].payload, b"add-node-5".to_vec());
}

#[test]
fn append_entries_does_not_mutate_log_while_snapshot_is_installing() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"base".to_vec()).expect("base append");
    cluster
        .begin_snapshot_install_from(2, "recv-2-5", 5, 2)
        .expect("begin snapshot receive");

    let before = cluster.status(2).expect("node 2 status before");
    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: before.term + 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId {
                        term: before.term + 1,
                        index: before.last_log_index + 1,
                    },
                    payload: b"blocked-by-snapshot".to_vec(),
                    is_command: true,
                }],
                leader_commit: before.commit_index + 1,
                lease_epoch: 0,
            },
        )
        .expect("append response");

    assert!(response.success);
    assert_eq!(response.match_index, before.commit_index);
    let after = cluster.status(2).expect("node 2 status after");
    assert_eq!(after.term, before.term + 1);
    assert_eq!(after.last_log_index, before.last_log_index);
    assert_eq!(after.commit_index, before.commit_index);
}

#[test]
fn vote_and_pre_vote_require_higher_term_and_fresh_log() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"base".to_vec()).expect("base append");

    let observed_candidate = cluster
        .vote_to(
            1,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: true,
                force: false,
            },
        )
        .expect("leader observes pre-vote requester");
    assert!(!observed_candidate.vote_granted);
    assert_eq!(
        cluster.status(3).expect("observed candidate").role,
        RustRaftRole::PreCandidate
    );

    let stale_pre_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 1,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: true,
                force: false,
            },
        )
        .expect("stale pre-vote response");
    assert!(!stale_pre_vote.vote_granted);
    assert_eq!(stale_pre_vote.reason, "stale_pre_vote_term");

    let stale_log_pre_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 0, index: 99 }),
                pre_vote: true,
                force: false,
            },
        )
        .expect("stale-log pre-vote response");
    assert!(!stale_log_pre_vote.vote_granted);
    assert_eq!(stale_log_pre_vote.reason, "candidate_log_stale");
    assert_eq!(stale_log_pre_vote.term, 1);

    let stale_log_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 1,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 0, index: 99 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("stale-log vote response");
    assert!(!stale_log_vote.vote_granted);
    assert_eq!(stale_log_vote.reason, "known_leader");

    let known_leader_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 1,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("known-leader vote response");
    assert!(!known_leader_vote.vote_granted);
    assert_eq!(known_leader_vote.reason, "known_leader");
    assert_eq!(cluster.leader_id(), Some(1));

    let high_term_stale_log_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 0, index: 99 }),
                pre_vote: false,
                force: true,
            },
        )
        .expect("high-term stale-log vote response");
    assert!(!high_term_stale_log_vote.vote_granted);
    assert_eq!(high_term_stale_log_vote.reason, "candidate_log_stale");
    assert_eq!(high_term_stale_log_vote.term, 2);
    assert_eq!(cluster.leader_id(), None);
    assert_eq!(cluster.status(2).expect("node 2 status").term, 2);

    cluster.set_follower_lease_valid(true);
    let in_lease_pre_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 3,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: true,
                force: false,
            },
        )
        .expect("in-lease pre-vote response");
    assert!(!in_lease_pre_vote.vote_granted);
    assert_eq!(in_lease_pre_vote.reason, "in_lease");
    assert_eq!(in_lease_pre_vote.term, 2);
    assert_eq!(cluster.status(2).expect("node 2 status").term, 2);

    let in_lease_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 4,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("in-lease vote response");
    assert!(!in_lease_vote.vote_granted);
    assert_eq!(in_lease_vote.reason, "in_lease");
    assert_eq!(in_lease_vote.term, 2);
    assert_eq!(cluster.status(2).expect("node 2 status").term, 2);

    assert!(cluster.follower_lease_valid());
    let expired = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::TickFollowerLease {
                elapsed_ms: cluster.config.leader_lease_ms,
            },
        })
        .expect("tick follower lease through admin step");
    assert_eq!(expired, RustRaftStepResult::FollowerLeaseExpired(true));
    assert!(!cluster.follower_lease_valid());
    let expired_lease_pre_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 3,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: true,
                force: false,
            },
        )
        .expect("expired-lease pre-vote response");
    assert!(expired_lease_pre_vote.vote_granted);

    let received = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReceiveFollowerLease { epoch: 10 },
        })
        .expect("receive follower lease through admin step");
    assert_eq!(received, RustRaftStepResult::FollowerLeaseReceived(true));
    let stale = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReceiveFollowerLease { epoch: 9 },
        })
        .expect("reject stale follower lease through admin step");
    assert_eq!(stale, RustRaftStepResult::FollowerLeaseReceived(false));
    let expired = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::TickFollowerLease {
                elapsed_ms: cluster.config.leader_lease_ms,
            },
        })
        .expect("expire follower lease through admin step");
    assert_eq!(expired, RustRaftStepResult::FollowerLeaseExpired(true));
    assert!(!cluster.follower_lease_valid());
    let stale = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReceiveFollowerLease { epoch: 9 },
        })
        .expect("reject stale expired follower lease through admin step");
    assert_eq!(stale, RustRaftStepResult::FollowerLeaseReceived(false));
    let received = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReceiveFollowerLease { epoch: 11 },
        })
        .expect("receive newer follower lease through admin step");
    assert_eq!(received, RustRaftStepResult::FollowerLeaseReceived(true));
    assert!(cluster.follower_lease_valid());

    cluster.set_follower_lease_valid(true);
    let forced_pre_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 3,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: true,
                force: true,
            },
        )
        .expect("forced pre-vote response");
    assert!(forced_pre_vote.vote_granted);

    let higher_term_forced_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 3,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: false,
                force: true,
            },
        )
        .expect("higher-term forced vote response");
    assert!(higher_term_forced_vote.vote_granted);
    assert_eq!(higher_term_forced_vote.reason, "vote_granted");
    assert_eq!(cluster.leader_id(), None);
}

#[test]
fn vote_requests_do_not_require_local_live_quorum_like_matrixraft() {
    let mut cluster = five_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.set_leader_lease_valid(false);
    cluster.set_follower_lease_valid(false);
    cluster.set_node_healthy(1, false).expect("isolate leader");
    cluster.set_node_healthy(4, false).expect("mark peer down");
    cluster.set_node_healthy(5, false).expect("mark peer down");

    let pre_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: true,
                force: false,
            },
        )
        .expect("pre-vote without locally observed quorum");
    assert!(pre_vote.vote_granted);
    assert_eq!(pre_vote.reason, "pre_vote_granted");

    let vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("vote without locally observed quorum");
    assert!(vote.vote_granted);
    assert_eq!(vote.reason, "vote_granted");
    assert_eq!(cluster.status(2).expect("voter status").term, 2);
    assert_eq!(cluster.leader_id(), None);
}

#[test]
fn startup_follower_lease_blocks_pre_vote_until_carried_duration_expires_like_matrixraft() {
    let config = RaftConfig {
        last_follower_lease_ms: 25,
        ..Default::default()
    };
    let mut cluster = RaftCluster::new(
        7,
        config,
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");

    let blocked = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 1,
                candidate_id: 3,
                last_log_id: None,
                pre_vote: true,
                force: false,
            },
        )
        .expect("startup lease pre-vote response");
    assert!(!blocked.vote_granted);
    assert_eq!(blocked.reason, "in_lease");

    assert!(!cluster.tick_follower_lease(24));
    assert!(cluster.follower_lease_valid());
    assert!(cluster.tick_follower_lease(1));
    assert!(!cluster.follower_lease_valid());

    let granted = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 1,
                candidate_id: 3,
                last_log_id: None,
                pre_vote: true,
                force: false,
            },
        )
        .expect("expired startup lease pre-vote response");
    assert!(granted.vote_granted);
}

#[test]
fn learners_do_not_vote_but_witnesses_do_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"base".to_vec()).expect("base append");

    cluster
        .add_learner(peer(4, RustRaftReplicaRole::Learner))
        .expect("add learner");
    cluster
        .add_witness(peer(5, RustRaftReplicaRole::Witness))
        .expect("add witness");

    let learner_pre_vote = cluster
        .vote_to(
            4,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: true,
                force: true,
            },
        )
        .expect("learner pre-vote response");
    assert!(!learner_pre_vote.vote_granted);
    assert_eq!(learner_pre_vote.reason, "target_cannot_vote");

    let learner_vote = cluster
        .vote_to(
            4,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: false,
                force: true,
            },
        )
        .expect("learner vote response");
    assert!(!learner_vote.vote_granted);
    assert_eq!(learner_vote.reason, "target_cannot_vote");

    let witness_pre_vote = cluster
        .vote_to(
            5,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: true,
                force: true,
            },
        )
        .expect("witness pre-vote response");
    assert!(witness_pre_vote.vote_granted);
    assert_eq!(witness_pre_vote.reason, "pre_vote_granted");
}

#[test]
fn removed_peer_cannot_collect_votes_or_step_down_leader_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"before-remove".to_vec()).expect("propose");
    let leader_term = cluster.status(1).expect("leader status").term;

    cluster.remove_peer(3).expect("remove voter");

    let removed_pre_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: leader_term + 1,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: true,
                force: true,
            },
        )
        .expect("removed candidate pre-vote response");
    assert!(!removed_pre_vote.vote_granted);
    assert_eq!(removed_pre_vote.reason, "candidate_not_member");

    let removed_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: leader_term + 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: false,
                force: true,
            },
        )
        .expect("removed candidate vote response");
    assert!(!removed_vote.vote_granted);
    assert_eq!(removed_vote.reason, "candidate_not_member");
    assert_eq!(cluster.leader_id(), Some(1));
    assert_eq!(cluster.status(1).expect("leader status").term, leader_term);

    let removed_append = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: leader_term + 3,
                leader_id: 3,
                prev_log_id: None,
                entries: vec![],
                leader_commit: 0,
                lease_epoch: 0,
            },
        )
        .expect("removed leader append response");
    assert!(!removed_append.success);
    assert_eq!(cluster.leader_id(), Some(1));
    assert_eq!(cluster.status(1).expect("leader status").term, leader_term);

    let removed_snapshot = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 7,
                term: leader_term + 4,
                leader_id: 3,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "removed-leader-snapshot".to_string(),
                        last_log_id: RustRaftLogId {
                            term: leader_term + 4,
                            index: 8,
                        },
                        membership: vec![1, 2],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"removed-leader".to_vec(),
                    done: true,
                },
            },
        )
        .expect("removed leader snapshot response");
    assert!(!removed_snapshot.accepted);
    assert_eq!(removed_snapshot.reason, "leader_not_member");
    assert_eq!(cluster.leader_id(), Some(1));
    assert_eq!(cluster.status(1).expect("leader status").term, leader_term);
}

#[test]
fn removing_peer_drops_stale_vote_responses_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("four voter cluster");
    cluster.start().expect("cluster starts");

    cluster
        .handle_vote_response_from(
            2,
            1,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: true,
                reason: "pre_vote_granted".to_string(),
            },
            true,
        )
        .expect("first pre-vote grant");
    cluster
        .handle_vote_response_from(
            2,
            3,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: true,
                reason: "pre_vote_granted".to_string(),
            },
            true,
        )
        .expect("pre-vote quorum starts real vote");
    assert_eq!(
        cluster.status(2).expect("candidate status").role,
        RustRaftRole::Candidate
    );

    cluster
        .handle_vote_response_from(
            2,
            4,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: false,
                reason: "removed_peer_rejected".to_string(),
            },
            false,
        )
        .expect("record rejection before peer removal");
    cluster.remove_peer(4).expect("remove rejecting peer");

    cluster
        .handle_vote_response_from(
            2,
            3,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: false,
                reason: "one_remaining_voter_rejected".to_string(),
            },
            false,
        )
        .expect("single retained rejection does not include removed peer");
    assert_eq!(
        cluster.status(2).expect("candidate status").role,
        RustRaftRole::Candidate
    );
}

#[test]
fn stopped_leader_cannot_drive_append_or_snapshot_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"before-stop".to_vec()).expect("propose");
    let leader_term = cluster.status(1).expect("leader status").term;

    cluster.set_node_healthy(1, false).expect("stop leader");
    assert_eq!(cluster.leader_id(), Some(1));

    let stopped_append = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: leader_term + 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("stopped leader append response");
    assert!(!stopped_append.success);
    assert_eq!(cluster.leader_id(), Some(1));
    assert_eq!(cluster.status(2).expect("target status").term, leader_term);

    let stopped_snapshot = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 7,
                term: leader_term + 2,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "stopped-leader-snapshot".to_string(),
                        last_log_id: RustRaftLogId {
                            term: leader_term + 2,
                            index: 8,
                        },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"stopped-leader".to_vec(),
                    done: true,
                },
            },
        )
        .expect("stopped leader snapshot response");
    assert!(!stopped_snapshot.accepted);
    assert_eq!(stopped_snapshot.reason, "leader_unavailable");
    assert_eq!(cluster.leader_id(), Some(1));
    assert_eq!(
        cluster
            .status(2)
            .expect("target status")
            .last_snapshot_index,
        0
    );
}

#[test]
fn remove_and_readd_peer_drops_partial_snapshot_install_state_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .add_peer(peer(4, RustRaftReplicaRole::Voter))
        .expect("add peer 4");
    let meta = RustRaftSnapshotMeta {
        snapshot_id: "snap-readd-4".to_string(),
        last_log_id: RustRaftLogId { term: 1, index: 5 },
        membership: vec![1, 2, 3, 4],
        members: Vec::new(),
    };

    let first = cluster
        .install_snapshot_chunk_to(
            4,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: meta.clone(),
                    offset: 0,
                    data: b"part-a".to_vec(),
                    done: false,
                },
            },
        )
        .expect("first snapshot chunk");
    assert_eq!(first.reason, "snapshot_chunk_accepted");

    cluster.remove_peer(4).expect("remove peer 4");
    cluster
        .add_peer(peer(4, RustRaftReplicaRole::Voter))
        .expect("re-add peer 4");

    let stale_tail = cluster
        .install_snapshot_chunk_to(
            4,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: meta.clone(),
                    offset: 6,
                    data: b"part-b".to_vec(),
                    done: true,
                },
            },
        )
        .expect_err("re-added peer does not continue old partial snapshot");
    assert!(stale_tail.to_string().contains("arrived before offset 0"));

    let fresh = cluster
        .install_snapshot_chunk_to(
            4,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta,
                    offset: 0,
                    data: b"fresh-state".to_vec(),
                    done: true,
                },
            },
        )
        .expect("fresh snapshot after re-add");
    assert_eq!(fresh.reason, "snapshot_installed");
    assert_eq!(cluster.status(4).expect("status").last_snapshot_index, 5);
}

#[test]
fn remove_and_readd_peer_drops_pending_snapshot_install_state_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .add_peer(peer(4, RustRaftReplicaRole::Voter))
        .expect("add peer 4");
    cluster.propose(b"first".to_vec()).expect("first propose");
    cluster.propose(b"second".to_vec()).expect("second propose");

    let mut unapplied_record = cluster.wal_record_for(4).expect("peer 4 wal");
    unapplied_record.apply_snapshot_fence.applied_index = 1;
    unapplied_record.apply_snapshot_fence.commit_index = 2;
    cluster
        .restore_wal_record(unapplied_record)
        .expect("restore peer 4 with pending apply");

    let pending = cluster
        .install_snapshot_chunk_to(
            4,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "pending-before-readd-4".to_string(),
                        last_log_id: RustRaftLogId { term: 1, index: 3 },
                        membership: vec![1, 2, 3, 4],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"pending-before-readd".to_vec(),
                    done: true,
                },
            },
        )
        .expect("completed snapshot waits for apply");
    assert_eq!(pending.reason, "snapshot_pending_apply");
    assert_eq!(
        cluster
            .status(4)
            .expect("pending status")
            .last_snapshot_index,
        0
    );

    cluster.remove_peer(4).expect("remove peer 4");
    cluster
        .add_peer(peer(4, RustRaftReplicaRole::Voter))
        .expect("re-add peer 4");
    let readded = cluster.status(4).expect("re-added status");
    assert_eq!(readded.last_snapshot_index, 0);
    let readded_applied = readded.applied_index;

    cluster
        .submit_apply_result(4, readded_applied, false)
        .expect("apply result does not install removed pending snapshot");
    assert_eq!(
        cluster
            .status(4)
            .expect("status after apply result")
            .last_snapshot_index,
        0
    );
}

#[test]
fn remove_and_readd_peer_drops_reordered_append_state_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"one".to_vec()).expect("base proposal");
    let leader_term = cluster
        .status(cluster.leader_id().expect("leader"))
        .expect("leader status")
        .term;

    let queued = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: leader_term,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId {
                    term: leader_term,
                    index: 3,
                }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId {
                        term: leader_term,
                        index: 4,
                    },
                    payload: b"stale-four".to_vec(),
                    is_command: true,
                }],
                leader_commit: 4,
                lease_epoch: 0,
            },
        )
        .expect("out-of-order append");
    assert!(!queued.success);

    cluster.remove_peer(2).expect("remove peer 2");
    cluster
        .add_peer(peer(2, RustRaftReplicaRole::Voter))
        .expect("re-add peer 2");

    let gap_fill = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: leader_term,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId {
                    term: leader_term,
                    index: 1,
                }),
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId {
                        term: leader_term,
                        index: 2,
                    },
                    payload: b"two-after-readd".to_vec(),
                    is_command: true,
                }],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("gap-fill append after re-add");
    assert!(gap_fill.success);
    let status = cluster.status(2).expect("peer 2 status");
    assert_eq!(status.last_log_index, 2);
    assert_eq!(status.commit_index, 2);
}

#[test]
fn snapshot_install_in_progress_ignores_new_snapshot_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    let first_meta = RustRaftSnapshotMeta {
        snapshot_id: "snap-first".to_string(),
        last_log_id: RustRaftLogId { term: 1, index: 8 },
        membership: vec![1, 2, 3],
        members: Vec::new(),
    };
    let second_meta = RustRaftSnapshotMeta {
        snapshot_id: "snap-second".to_string(),
        last_log_id: RustRaftLogId { term: 1, index: 9 },
        membership: vec![1, 2, 3],
        members: Vec::new(),
    };

    let first = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: first_meta.clone(),
                    offset: 0,
                    data: b"first-".to_vec(),
                    done: false,
                },
            },
        )
        .expect("begin first snapshot");
    assert_eq!(first.reason, "snapshot_chunk_accepted");

    let ignored = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: second_meta,
                    offset: 0,
                    data: b"second".to_vec(),
                    done: false,
                },
            },
        )
        .expect("second snapshot is ignored while first receives");
    assert!(ignored.accepted);
    assert_eq!(ignored.next_offset, 6);
    assert_eq!(ignored.reason, "snapshot_install_ignored_while_receiving");

    let finish = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: first_meta,
                    offset: 6,
                    data: b"done".to_vec(),
                    done: true,
                },
            },
        )
        .expect("finish first snapshot");
    assert_eq!(finish.reason, "snapshot_installed");
    assert_eq!(cluster.status(2).expect("status").last_snapshot_index, 8);
}

#[test]
fn high_term_vote_responses_step_down_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(2, true).expect("make node 2 leader");
    let leader_term = cluster.status(2).expect("node 2 status").term;

    cluster
        .handle_vote_response(
            2,
            RustRaftVoteResponse {
                term: leader_term + 1,
                vote_granted: true,
                reason: "pre_vote_granted".to_string(),
            },
            true,
        )
        .expect("granted pre-vote response");
    assert_eq!(cluster.status(2).expect("node 2 status").term, leader_term);
    assert_eq!(cluster.leader_id(), Some(2));

    cluster
        .handle_vote_response(
            2,
            RustRaftVoteResponse {
                term: leader_term + 2,
                vote_granted: false,
                reason: "pre_vote_rejected".to_string(),
            },
            true,
        )
        .expect("rejected pre-vote response");
    assert_eq!(
        cluster.status(2).expect("node 2 status").term,
        leader_term + 2
    );
    assert_eq!(
        cluster.status(2).expect("node 2 status").role,
        RustRaftRole::Follower
    );
    assert_eq!(cluster.leader_id(), None);

    cluster.campaign(2, true).expect("make node 2 leader again");
    let leader_term = cluster.status(2).expect("node 2 status").term;
    cluster
        .handle_vote_response(
            2,
            RustRaftVoteResponse {
                term: leader_term + 1,
                vote_granted: true,
                reason: "vote_granted".to_string(),
            },
            false,
        )
        .expect("vote response");
    assert_eq!(
        cluster.status(2).expect("node 2 status").term,
        leader_term + 1
    );
    assert_eq!(
        cluster.status(2).expect("node 2 status").role,
        RustRaftRole::Follower
    );
    assert_eq!(cluster.leader_id(), None);
}

#[test]
fn vote_response_quorum_promotes_candidate_without_extra_term_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let self_vote = cluster
        .vote_to(
            2,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 2,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: false,
                force: true,
            },
        )
        .expect("candidate self vote");
    assert!(self_vote.vote_granted);
    assert_eq!(cluster.leader_id(), None);
    assert_eq!(cluster.status(2).expect("candidate status").term, 2);

    cluster
        .handle_vote_response_from(
            2,
            1,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: true,
                reason: "vote_granted".to_string(),
            },
            false,
        )
        .expect("remote vote response reaches quorum");

    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(cluster.status(2).expect("leader status").term, 2);
    assert_eq!(
        cluster.status(2).expect("leader status").role,
        RustRaftRole::Leader
    );
}

#[test]
fn pre_vote_quorum_starts_real_vote_before_leader_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    cluster
        .handle_vote_response_from(
            2,
            1,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: true,
                reason: "pre_vote_granted".to_string(),
            },
            true,
        )
        .expect("pre-vote quorum starts real vote");

    assert_eq!(cluster.leader_id(), None);
    assert_eq!(cluster.status(2).expect("candidate status").term, 2);
    assert_eq!(
        cluster.status(2).expect("candidate status").role,
        RustRaftRole::Candidate
    );

    cluster
        .handle_vote_response_from(
            2,
            1,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: true,
                reason: "vote_granted".to_string(),
            },
            false,
        )
        .expect("real vote quorum promotes leader");

    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(
        cluster.status(2).expect("leader status").role,
        RustRaftRole::Leader
    );
}

#[test]
fn ignore_witness_drops_stale_witness_vote_responses_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Voter),
            peer(5, RustRaftReplicaRole::Witness),
        ],
    )
    .expect("cluster with witness");
    cluster.start().expect("cluster starts");

    cluster
        .handle_vote_response_from(
            2,
            1,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: true,
                reason: "pre_vote_granted".to_string(),
            },
            true,
        )
        .expect("pre-vote quorum starts real vote");
    cluster
        .handle_vote_response_from(
            2,
            3,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: true,
                reason: "pre_vote_granted".to_string(),
            },
            true,
        )
        .expect("pre-vote quorum starts real vote");
    assert_eq!(
        cluster.status(2).expect("candidate status").role,
        RustRaftRole::Candidate
    );

    cluster
        .handle_vote_response_from(
            2,
            5,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: false,
                reason: "witness_rejected".to_string(),
            },
            false,
        )
        .expect("record witness rejection before policy change");
    cluster.set_ignore_witness(true);

    cluster
        .handle_vote_response_from(
            2,
            4,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: false,
                reason: "voter_rejected".to_string(),
            },
            false,
        )
        .expect("one voter rejection does not include stale witness rejection");

    assert_eq!(
        cluster.status(2).expect("candidate status").role,
        RustRaftRole::Candidate
    );
    assert_eq!(cluster.leader_id(), None);
}

#[test]
fn ignore_witness_keeps_matrixraft_rejection_threshold() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Witness),
        ],
    )
    .expect("cluster with witness");
    cluster.start().expect("cluster starts");
    cluster.set_ignore_witness(true);

    cluster
        .handle_vote_response_from(
            2,
            1,
            RustRaftVoteResponse {
                term: 1,
                vote_granted: true,
                reason: "pre_vote_granted".to_string(),
            },
            true,
        )
        .expect("pre-vote quorum starts real vote");
    assert_eq!(
        cluster.status(2).expect("candidate status").role,
        RustRaftRole::Candidate
    );

    cluster
        .handle_vote_response_from(
            2,
            1,
            RustRaftVoteResponse {
                term: 2,
                vote_granted: false,
                reason: "voter_rejected".to_string(),
            },
            false,
        )
        .expect("single voter rejection does not beat MatrixRaft threshold");

    assert_eq!(
        cluster.status(2).expect("candidate status").role,
        RustRaftRole::Candidate
    );
    assert_eq!(cluster.leader_id(), None);
}

#[test]
fn high_term_append_entries_responses_step_down_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(2, true).expect("make node 2 leader");
    let leader_term = cluster.status(2).expect("node 2 status").term;

    cluster
        .handle_append_entries_response(
            2,
            1,
            RustRaftAppendEntriesResponse {
                term: leader_term,
                success: true,
                match_index: 1,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("same-term append response");
    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(cluster.status(2).expect("node 2 status").term, leader_term);

    cluster
        .handle_append_entries_response(
            2,
            1,
            RustRaftAppendEntriesResponse {
                term: leader_term + 1,
                success: false,
                match_index: 1,
                rejection_hint: Some(1),
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("high-term append response");

    assert_eq!(cluster.leader_id(), None);
    assert_eq!(
        cluster.status(2).expect("node 2 status").term,
        leader_term + 1
    );
    assert_eq!(
        cluster.status(2).expect("node 2 status").role,
        RustRaftRole::Follower
    );
}

#[test]
fn heartbeat_append_response_carries_snapshot_progress_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RustRaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    let leader_term = cluster.status(1).expect("leader status").term;

    cluster
        .begin_snapshot_install_from(2, "snap-20", 20, 4)
        .expect("follower starts snapshot install");
    let receiving = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: leader_term,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("heartbeat response");
    assert_eq!(receiving.snapshot_state, RustRaftSnapshotState::Receiving);
    cluster
        .rollback_snapshot_install_from(2)
        .expect("clear follower install state");

    cluster
        .begin_snapshot_send_to(2, "snap-20", 20, 4)
        .expect("leader sends snapshot");
    cluster
        .handle_append_entries_response(
            1,
            2,
            RustRaftAppendEntriesResponse {
                term: leader_term,
                success: true,
                match_index: 1,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::Receiving,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("receiving progress keeps send active");
    assert!(
        cluster
            .peer_pipeline_status(2)
            .expect("pipeline")
            .snapshot_sending
    );

    for _ in 0..11 {
        cluster
            .handle_append_entries_response(
                1,
                2,
                RustRaftAppendEntriesResponse {
                    term: leader_term,
                    success: true,
                    match_index: 1,
                    rejection_hint: None,
                    rejected_index: None,
                    require_snapshot: None,
                    snapshot_state: RustRaftSnapshotState::None,
                    lease_confirmation_epoch: 0,
                    lease_duration_ms: 0,
                },
            )
            .expect("non-receiving heartbeat response");
    }
    let status = cluster.peer_pipeline_status(2).expect("pipeline");
    assert!(!status.snapshot_sending);
    assert_eq!(status.snapshot_send_timeouts, 1);
}

#[test]
fn leader_transition_resets_peer_pipelines_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    let last_before_campaign = cluster.status(1).expect("leader status").last_log_index;

    cluster
        .begin_snapshot_send_to(2, "stale-snapshot", 10, 3)
        .expect("start stale snapshot send");
    assert!(
        cluster
            .peer_pipeline_status(2)
            .expect("peer 2 pipeline")
            .snapshot_sending
    );

    cluster.campaign(2, true).expect("make node 2 leader");

    let former_snapshot_target = cluster.peer_pipeline_status(2).expect("node 2 pipeline");
    assert!(!former_snapshot_target.snapshot_sending);
    assert_eq!(
        former_snapshot_target.progress_state,
        RustRaftPeerProgressState::Replicate
    );
    assert_eq!(former_snapshot_target.match_index, last_before_campaign);
    assert_eq!(former_snapshot_target.next_index, last_before_campaign + 1);
    assert_eq!(former_snapshot_target.inflight_entries, 0);

    let follower = cluster.peer_pipeline_status(1).expect("node 1 pipeline");
    assert_eq!(follower.progress_state, RustRaftPeerProgressState::Probe);
    assert_eq!(follower.match_index, 0);
    assert_eq!(follower.next_index, last_before_campaign + 1);
    assert_eq!(follower.inflight_entries, 0);
    assert!(!follower.snapshot_sending);
}

#[test]
fn high_term_snapshot_request_updates_leader_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(2, true).expect("make node 2 leader");
    let leader_term = cluster.status(2).expect("node 2 status").term;

    let response = cluster
        .install_snapshot_chunk_to(
            3,
            InstallSnapshotRequest {
                group_id: 7,
                term: leader_term + 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "high-term-snapshot-leader".to_string(),
                        last_log_id: RustRaftLogId {
                            term: leader_term + 1,
                            index: 2,
                        },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"partial".to_vec(),
                    done: false,
                },
            },
        )
        .expect("high-term snapshot chunk");

    assert!(response.accepted);
    assert_eq!(response.term, leader_term + 1);
    assert_eq!(cluster.leader_id(), Some(1));
    assert_eq!(
        cluster.status(2).expect("old leader status").role,
        RustRaftRole::Follower
    );
    assert_eq!(
        cluster.status(1).expect("snapshot leader status").role,
        RustRaftRole::Leader
    );
}

#[test]
fn stale_term_snapshot_request_is_rejected_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(2, true).expect("make node 2 leader");
    let leader_term = cluster.status(2).expect("node 2 status").term;

    let response = cluster
        .install_snapshot_chunk_to(
            3,
            InstallSnapshotRequest {
                group_id: 7,
                term: leader_term - 1,
                leader_id: 2,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "stale-term-snapshot".to_string(),
                        last_log_id: RustRaftLogId {
                            term: leader_term - 1,
                            index: 4,
                        },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"stale-term".to_vec(),
                    done: true,
                },
            },
        )
        .expect("stale-term snapshot response");

    assert!(!response.accepted);
    assert_eq!(response.reason, "stale_term");
    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(
        cluster
            .status(3)
            .expect("target status")
            .last_snapshot_index,
        0
    );
}

#[test]
fn high_term_snapshot_responses_step_down_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(2, true).expect("make node 2 leader");
    let leader_term = cluster.status(2).expect("node 2 status").term;

    cluster
        .handle_install_snapshot_response(
            2,
            1,
            RustRaftInstallSnapshotResponse {
                term: leader_term,
                accepted: true,
                next_offset: 0,
                committed_index: 0,
                reason: "snapshot_installed".to_string(),
            },
        )
        .expect("same-term snapshot response");
    assert_eq!(cluster.leader_id(), Some(2));

    cluster
        .handle_install_snapshot_response(
            2,
            1,
            RustRaftInstallSnapshotResponse {
                term: leader_term + 1,
                accepted: false,
                next_offset: 0,
                committed_index: 0,
                reason: "higher_term".to_string(),
            },
        )
        .expect("high-term snapshot response");

    assert_eq!(cluster.leader_id(), None);
    assert_eq!(
        cluster.status(2).expect("node 2 status").term,
        leader_term + 1
    );
    assert_eq!(
        cluster.status(2).expect("node 2 status").role,
        RustRaftRole::Follower
    );
}

#[test]
fn rejected_snapshot_response_triggers_fresh_snapshot_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    let leader_term = cluster.status(1).expect("leader status").term;

    cluster
        .begin_snapshot_send_to(2, "stale-snapshot-finish", 2, 1)
        .expect("begin snapshot send");
    cluster
        .handle_install_snapshot_response(
            1,
            2,
            RustRaftInstallSnapshotResponse {
                term: leader_term,
                accepted: false,
                next_offset: 0,
                committed_index: 0,
                reason: "snapshot_rejected".to_string(),
            },
        )
        .expect("same-term rejected snapshot finish");

    let pipeline = cluster.peer_pipeline_status(2).expect("peer pipeline");
    assert!(!pipeline.snapshot_sending);
    assert_eq!(pipeline.snapshot_chunk_retry_count, 1);
    assert!(cluster.snapshot_trigger_status().in_progress);
}

#[test]
fn stale_snapshot_ready_does_not_clear_active_trigger_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let trigger = cluster.trigger_snapshot().expect("trigger snapshot");
    cluster
        .handle_snapshot_ready("older-snapshot-ready", true)
        .expect("stale snapshot ready is ignored");

    let status = cluster.snapshot_trigger_status();
    assert!(status.in_progress);
    assert_eq!(
        status.snapshot_id.as_deref(),
        Some(trigger.snapshot_id.as_str())
    );

    cluster
        .handle_snapshot_ready(&trigger.snapshot_id, true)
        .expect("current snapshot ready completes trigger");
    assert!(!cluster.snapshot_trigger_status().in_progress);
}

#[test]
fn same_term_snapshot_response_finishes_send_and_resumes_catchup_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    let leader_term = cluster.status(1).expect("leader status").term;

    cluster
        .begin_snapshot_send_to(2, "snap-finish-2", 2, 1)
        .expect("begin snapshot send");
    cluster
        .handle_install_snapshot_response(
            1,
            2,
            RustRaftInstallSnapshotResponse {
                term: leader_term,
                accepted: true,
                next_offset: 0,
                committed_index: 2,
                reason: "snapshot_installed".to_string(),
            },
        )
        .expect("same-term snapshot finish");

    let pipeline = cluster.peer_pipeline_status(2).expect("peer pipeline");
    assert!(!pipeline.snapshot_sending);
    assert_eq!(
        pipeline.acked_snapshot_index,
        pipeline.required_snapshot_index
    );
    assert!(pipeline.match_index >= 2);
    assert!(pipeline.next_index >= 3);
}

#[test]
fn stopped_peer_responses_do_not_step_down_leader_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    let leader = cluster.leader_id().expect("leader");
    let leader_term = cluster.status(leader).expect("leader status").term;

    cluster.set_node_healthy(2, false).expect("stop peer 2");

    cluster
        .handle_vote_response_from(
            leader,
            2,
            RustRaftVoteResponse {
                term: leader_term + 1,
                vote_granted: false,
                reason: "stopped_peer".to_string(),
            },
            false,
        )
        .expect("stopped peer vote response ignored");
    assert_eq!(cluster.leader_id(), Some(leader));
    assert_eq!(
        cluster.status(leader).expect("leader status").term,
        leader_term
    );

    cluster
        .handle_append_entries_response(
            leader,
            2,
            RustRaftAppendEntriesResponse {
                term: leader_term + 2,
                success: false,
                match_index: 0,
                rejection_hint: Some(0),
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("stopped peer append response ignored");
    assert_eq!(cluster.leader_id(), Some(leader));
    assert_eq!(
        cluster.status(leader).expect("leader status").term,
        leader_term
    );

    cluster
        .handle_install_snapshot_response(
            leader,
            2,
            RustRaftInstallSnapshotResponse {
                term: leader_term + 3,
                accepted: false,
                next_offset: 0,
                committed_index: 0,
                reason: "stopped_peer".to_string(),
            },
        )
        .expect("stopped peer snapshot response ignored");
    assert_eq!(cluster.leader_id(), Some(leader));
    assert_eq!(
        cluster.status(leader).expect("leader status").term,
        leader_term
    );
}

#[test]
fn read_index_and_lease_read_follow_leader_lease_and_apply_floor() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let initial_read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 0,
            allow_lease_read: true,
        })
        .expect("initial read index");
    assert!(!initial_read.lease_read);
    assert!(!cluster.lease_read_eligible(1, 0).expect("lease eligible"));

    cluster.propose(b"set a=1".to_vec()).expect("propose");

    cluster
        .mark_apply_task_inflight(1, 2)
        .expect("leader apply is inflight");
    let inflight_read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 2,
            allow_lease_read: true,
        })
        .expect("read waits for safe apply");
    assert!(!inflight_read.safe);
    assert_eq!(inflight_read.reason, "applied_index_behind_min_commit");
    cluster
        .submit_apply_result(1, 2, false)
        .expect("leader apply completes");

    let lease = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 2,
            allow_lease_read: true,
        })
        .expect("read index");
    assert!(lease.safe);
    assert!(lease.lease_read);

    cluster.set_leader_lease_valid(false);
    let read_index = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 2,
            allow_lease_read: true,
        })
        .expect("read index");
    assert!(read_index.safe);
    assert!(!read_index.lease_read);
    assert_eq!(read_index.reason, "read_index");

    assert!(!cluster.receive_leader_lease_confirmation(1, 100));
    assert!(!cluster.receive_leader_lease_confirmation(2, 0));
    assert!(!cluster.lease_read_eligible(1, 1).expect("lease eligible"));
    assert!(cluster.receive_leader_lease_confirmation(2, 10));
    assert!(cluster.lease_read_eligible(1, 1).expect("lease eligible"));
    cluster.expire_leader_lease();
    assert!(!cluster.receive_leader_lease_confirmation(2, 9));
    assert!(!cluster.lease_read_eligible(1, 1).expect("lease eligible"));
    assert!(cluster.receive_leader_lease_confirmation(2, 11));
    assert!(cluster.lease_read_eligible(1, 1).expect("lease eligible"));

    let unsafe_read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 3,
            allow_lease_read: true,
        })
        .expect("read index");
    assert!(!unsafe_read.safe);
}

#[test]
fn append_entries_piggybacks_lease_epoch_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    cluster
        .propose(b"current-term-read-floor".to_vec())
        .expect("current-term entry");
    let term = cluster.status(1).expect("leader status").term;
    cluster.set_leader_lease_valid(false);
    assert!(!cluster
        .lease_read_eligible(1, 1)
        .expect("lease starts invalid"));

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![],
                leader_commit: 0,
                lease_epoch: 77,
            },
        )
        .expect("append entries heartbeat");
    assert_eq!(response.lease_confirmation_epoch, 77);
    assert_eq!(response.lease_duration_ms, cluster.config.leader_lease_ms);

    cluster
        .handle_append_entries_response(1, 2, response)
        .expect("leader records lease response");
    assert!(cluster
        .lease_read_eligible(1, 1)
        .expect("lease restored by follower confirmation"));
}

#[test]
fn leader_lease_confirmation_duration_bounds_quorum_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    cluster
        .propose(b"current-term-read-floor".to_vec())
        .expect("current-term entry");
    cluster.set_leader_lease_valid(false);

    let confirmed = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReceiveLeaderLeaseConfirmation {
                node_id: 2,
                confirmation_epoch: 90,
                duration_ms: Some(5),
            },
        })
        .expect("leader lease confirmation through admin step");
    assert_eq!(confirmed, RustRaftStepResult::LeaderLeaseConfirmed(true));
    assert!(cluster
        .lease_read_eligible(1, 1)
        .expect("short confirmation restores quorum lease"));
    let still_valid = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::TickLeaderLease { elapsed_ms: 4 },
        })
        .expect("tick leader lease through admin step");
    assert_eq!(still_valid, RustRaftStepResult::LeaderLeaseExpired(false));
    assert!(cluster
        .lease_read_eligible(1, 1)
        .expect("confirmation still in duration"));
    let expired = cluster
        .step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::TickLeaderLease { elapsed_ms: 1 },
        })
        .expect("expire leader lease through admin step");
    assert_eq!(expired, RustRaftStepResult::LeaderLeaseExpired(true));
    assert!(!cluster
        .lease_read_eligible(1, 1)
        .expect("expired confirmation no longer counts"));
}

#[test]
fn reduced_follower_lease_confirmation_does_not_rewind_lease_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    cluster
        .propose(b"current-term-read-floor".to_vec())
        .expect("current-term entry");
    cluster.set_leader_lease_valid(false);

    assert!(cluster.receive_leader_lease_confirmation_with_duration(2, 90, 100));
    assert!(cluster
        .lease_read_eligible(1, 1)
        .expect("full follower lease restores quorum lease"));
    assert!(!cluster.tick_leader_lease(50));
    assert!(cluster.receive_leader_lease_confirmation_with_duration(2, 91, 30));
    assert!(!cluster.tick_leader_lease(49));
    assert!(cluster
        .lease_read_eligible(1, 1)
        .expect("shorter newer confirmation must not rewind lease"));
    assert!(cluster.tick_leader_lease(1));
    assert!(!cluster
        .lease_read_eligible(1, 1)
        .expect("original follower lease eventually expires"));
}

#[test]
fn legacy_append_entries_response_renews_leader_lease_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    cluster
        .propose(b"current-term-read-floor".to_vec())
        .expect("current-term entry");
    let term = cluster.status(1).expect("leader status").term;
    cluster.set_leader_lease_valid(false);

    cluster
        .handle_append_entries_response(
            1,
            2,
            RustRaftAppendEntriesResponse {
                term,
                success: true,
                match_index: 0,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("legacy append response");

    assert!(cluster
        .lease_read_eligible(1, 1)
        .expect("legacy response renews leader lease"));
}

#[test]
fn ignore_witness_preserves_voter_backed_leader_lease_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Witness),
        ],
    )
    .expect("cluster with witness");
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    cluster
        .propose(b"current-term-read-floor".to_vec())
        .expect("current-term entry");
    cluster.set_leader_lease_valid(true);
    assert!(cluster
        .lease_read_eligible(1, 1)
        .expect("live quorum restores leader lease"));

    cluster.set_ignore_witness(true);

    assert!(cluster.ignore_witness());
    assert!(cluster
        .lease_read_eligible(1, 1)
        .expect("ignore witness does not reset voter-backed lease"));
}

#[test]
fn append_entries_response_marks_peer_healthy_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    let term = cluster.status(1).expect("leader status").term;

    cluster.set_node_healthy(2, false).expect("mark peer down");
    assert!(
        !cluster
            .status(1)
            .expect("leader status")
            .peers
            .iter()
            .find(|peer| peer.node_id == 2)
            .expect("peer 2 status")
            .healthy
    );

    cluster
        .handle_append_entries_response(
            1,
            2,
            RustRaftAppendEntriesResponse {
                term,
                success: true,
                match_index: 0,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("append response from peer");

    assert!(
        cluster
            .status(1)
            .expect("leader status")
            .peers
            .iter()
            .find(|peer| peer.node_id == 2)
            .expect("peer 2 status")
            .healthy
    );
}

#[test]
fn added_caught_up_auto_promote_learner_becomes_voter_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    cluster.propose(b"entry".to_vec()).expect("propose");

    let mut learner = peer(4, RustRaftReplicaRole::Learner);
    learner.auto_promote = true;
    cluster.add_learner(learner).expect("add learner");

    let membership = cluster.membership();
    assert!(membership.voters.contains(&4));
    assert!(!membership.learners.contains(&4));
    assert_eq!(
        cluster.status(4).expect("promoted learner status").role,
        RustRaftRole::Follower
    );
}

#[test]
fn auto_promote_learner_waits_for_pending_membership_change_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            {
                let mut learner = peer(4, RustRaftReplicaRole::Learner);
                learner.auto_promote = true;
                learner
            },
        ],
    )
    .expect("cluster with learner");
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    cluster
        .begin_pending_membership_change(5)
        .expect("first membership change is pending");

    cluster
        .propose(b"entry".to_vec())
        .expect("replicate to auto-promote learner");
    assert!(cluster.membership().learners.contains(&4));
    assert!(!cluster.membership().voters.contains(&4));

    let blocked = cluster
        .auto_promote_learner(4)
        .expect("manual auto promote observes pending fence");
    assert!(!blocked.promoted);
    assert_eq!(blocked.state_before, RaftLearnerAutoPromoteState::Promoting);
    assert_eq!(blocked.state_after, RaftLearnerAutoPromoteState::Promoting);
    assert_eq!(blocked.reason, "membership_change_pending");

    cluster.mark_membership_change_applied(5);
    let promoted = cluster
        .auto_promote_learner(4)
        .expect("auto promote after pending change applies");
    assert!(promoted.promoted);
    assert!(cluster.membership().voters.contains(&4));
}

#[test]
fn auto_promote_learner_uses_leader_noop_as_first_matched_log_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            {
                let mut learner = peer(4, RustRaftReplicaRole::Learner);
                learner.auto_promote = true;
                learner
            },
        ],
    )
    .expect("cluster with auto learner");
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");

    let promoted_from_noop = cluster
        .auto_promote_learner(4)
        .expect("leader no-op is the learner's first matched log");
    assert!(promoted_from_noop.promoted);
    assert_eq!(
        promoted_from_noop.state_before,
        RaftLearnerAutoPromoteState::Stop
    );
    assert_eq!(
        promoted_from_noop.state_after,
        RaftLearnerAutoPromoteState::Promoted
    );
    assert_eq!(promoted_from_noop.reason, "learner_promoted");
    assert!(cluster.membership().voters.contains(&4));
}

#[test]
fn auto_promote_learner_waits_one_check_turn_when_lagging_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            {
                let mut learner = peer(4, RustRaftReplicaRole::Learner);
                learner.auto_promote = true;
                learner
            },
        ],
    )
    .expect("cluster with auto learner");
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    cluster
        .install_snapshot_to(
            4,
            RaftSnapshot {
                group_id: 7,
                meta: RustRaftSnapshotMeta {
                    snapshot_id: "learner-baseline".to_string(),
                    last_log_id: RustRaftLogId { term: 1, index: 1 },
                    membership: vec![1, 2, 3, 4],
                    members: Vec::new(),
                },
                payload: b"baseline".to_vec(),
            },
            RustRaftApplySnapshotFence {
                applied_index: 1,
                commit_index: 1,
                installed_snapshot_index: 1,
                first_retained_log_index: 2,
            },
        )
        .expect("baseline learner snapshot");
    assert_eq!(cluster.status(4).expect("learner status").last_log_index, 1);

    cluster.set_node_healthy(4, false).expect("isolate learner");
    cluster
        .propose(b"learner-missed".to_vec())
        .expect("entry while learner is isolated");
    cluster.set_node_healthy(4, true).expect("heal learner");

    let checking = cluster
        .auto_promote_learner(4)
        .expect("lagging learner starts check turn");
    assert!(!checking.promoted);
    assert_eq!(checking.state_before, RaftLearnerAutoPromoteState::Stop);
    assert_eq!(checking.state_after, RaftLearnerAutoPromoteState::Check);
    assert_eq!(checking.reason, "learner_check_turn_started");
    assert!(cluster.membership().learners.contains(&4));
    assert_eq!(
        cluster.status(4).expect("learner caught up").last_log_index,
        2
    );

    let promoted = cluster
        .auto_promote_learner(4)
        .expect("checked learner promotes on next turn");
    assert!(promoted.promoted);
    assert!(cluster.membership().voters.contains(&4));
}

#[test]
fn read_index_waits_for_first_current_term_entry_after_leader_change() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"term-one".to_vec()).expect("propose");

    cluster.campaign(2, true).expect("new leader");
    let leader_wal = cluster.wal_record_for(2).expect("leader WAL");
    let noop = leader_wal.entries.last().expect("leader no-op");
    assert_eq!(noop.log_id, RustRaftLogId { term: 2, index: 3 });
    assert_eq!(noop.payload, b"no-op".to_vec());
    assert_eq!(cluster.status(2).expect("leader status").commit_index, 2);

    let read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 2,
            min_commit_index: 1,
            allow_lease_read: false,
        })
        .expect("read index");

    assert!(!read.safe);
    assert_eq!(read.read_index, 3);
    assert_eq!(read.reason, "applied_index_behind_read_index");
}

#[test]
fn read_index_rejects_until_first_current_term_entry_is_applied() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 0,
            allow_lease_read: false,
        })
        .expect("read index");

    assert!(!read.safe);
    assert_eq!(read.read_index, 1);
    assert_eq!(read.reason, "applied_index_behind_read_index");
}

#[test]
fn read_index_rejects_follower_requester_like_baseline_raft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .propose(b"leader-only-read-index".to_vec())
        .expect("propose");

    let read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 2,
            min_commit_index: 1,
            allow_lease_read: false,
        })
        .expect("read index");

    assert!(!read.safe);
    assert_eq!(read.reason, "not_leader");
    assert_eq!(read.read_index, 2);
}

#[test]
fn read_path_report_tracks_quorum_lease_fence_and_bounded_stale() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"set a=1".to_vec()).expect("propose");

    cluster.set_leader_lease_valid(false);
    let stale_lease = cluster
        .read_path_report(
            RustRaftReadIndexRequest {
                group_id: 7,
                requester_id: 1,
                min_commit_index: 1,
                allow_lease_read: true,
            },
            0,
        )
        .expect("read path report");
    assert!(stale_lease.safe);
    assert!(!stale_lease.lease_read);
    assert!(stale_lease.stale_leader_rejected);
    assert!(stale_lease.quorum.reached);
    assert_eq!(stale_lease.quorum.required, 2);
    assert!(!stale_lease.lease_read_eligibility.eligible);
    assert_eq!(
        stale_lease.lease_read_eligibility.reason,
        "stale_leader_lease"
    );

    let fenced = cluster
        .read_path_report(
            RustRaftReadIndexRequest {
                group_id: 7,
                requester_id: 1,
                min_commit_index: 3,
                allow_lease_read: false,
            },
            0,
        )
        .expect("fenced read path");
    assert!(!fenced.safe);
    assert!(!fenced.applied_index_fence.passed);
    assert_eq!(
        fenced.applied_index_fence.reason,
        "applied_index_behind_min_commit"
    );

    cluster
        .set_node_healthy(3, false)
        .expect("mark follower down");
    cluster
        .propose(b"set a=2".to_vec())
        .expect("propose while follower down");
    cluster.set_node_healthy(3, true).expect("heal follower");

    let bounded = cluster
        .read_path_report(
            RustRaftReadIndexRequest {
                group_id: 7,
                requester_id: 3,
                min_commit_index: 1,
                allow_lease_read: false,
            },
            1,
        )
        .expect("bounded stale report");
    assert!(bounded.safe);
    let bounded_stale = bounded.bounded_stale.expect("bounded stale");
    assert_eq!(bounded_stale.lag, 1);
    assert!(bounded_stale.allowed);

    let too_stale = cluster
        .read_path_report(
            RustRaftReadIndexRequest {
                group_id: 7,
                requester_id: 3,
                min_commit_index: 1,
                allow_lease_read: false,
            },
            0,
        )
        .expect("stale follower report");
    assert!(!too_stale.safe);
    assert_eq!(too_stale.reason, "replica_lagging");
    assert!(!too_stale.bounded_stale.expect("bounded stale").allowed);
}

#[test]
fn lost_quorum_leader_steps_down_after_lease_expires_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .propose(b"lease-before-quorum-loss".to_vec())
        .expect("propose");
    let leader = cluster.leader_id().expect("leader");
    cluster.set_leader_lease_valid(true);
    assert!(cluster.lease_read_eligible(leader, 0).expect("lease read"));

    cluster.set_node_healthy(2, false).expect("mark peer down");
    cluster.set_node_healthy(3, false).expect("mark peer down");
    assert!(!cluster.step_down_leader_if_lost_quorum());
    assert_eq!(cluster.leader_id(), Some(leader));

    let lease = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: leader,
            min_commit_index: 0,
            allow_lease_read: true,
        })
        .expect("lease read during quorum loss");
    assert!(lease.safe);
    assert!(lease.lease_read);
    assert_eq!(lease.reason, "lease_read");

    assert!(cluster.tick_leader_lease(cluster.config.leader_lease_ms));
    let no_quorum = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: leader,
            min_commit_index: 0,
            allow_lease_read: true,
        })
        .expect("read after lease expiry");
    assert!(!no_quorum.safe);
    assert_eq!(no_quorum.reason, "no_live_quorum");
    assert!(cluster.step_down_leader_if_lost_quorum());
    assert_eq!(cluster.leader_id(), None);
}

#[test]
fn liveness_timeout_preserves_confirmed_leader_lease_until_duration_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .propose(b"lease-before-liveness-timeout".to_vec())
        .expect("propose");
    let leader = cluster.leader_id().expect("leader");
    cluster.set_leader_lease_valid(true);
    assert!(cluster.lease_read_eligible(leader, 0).expect("lease read"));

    let timed_out = cluster.tick_peer_liveness(cluster.config.election_timeout_ms + 1);
    assert_eq!(timed_out, vec![2, 3]);
    assert!(!cluster.step_down_leader_if_lost_quorum());

    let lease = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: leader,
            min_commit_index: 0,
            allow_lease_read: true,
        })
        .expect("lease read after liveness timeout");
    assert!(lease.safe);
    assert!(lease.lease_read);
    assert_eq!(lease.reason, "lease_read");

    assert!(cluster.tick_leader_lease(cluster.config.leader_lease_ms));
    assert!(cluster.step_down_leader_if_lost_quorum());
}

#[test]
fn read_index_rejects_when_live_quorum_is_lost() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"set a=1".to_vec()).expect("propose");
    cluster.set_node_healthy(2, false).expect("mark peer down");
    cluster.set_node_healthy(3, false).expect("mark peer down");
    cluster.set_leader_lease_valid(false);

    let read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 1,
            allow_lease_read: true,
        })
        .expect("read index");
    assert!(!read.safe);
    assert!(!read.lease_read);
    assert_eq!(read.reason, "no_live_quorum");
}

#[test]
fn ignore_witness_recomputes_quorum_and_commit_index() {
    let mut dynamic_witness_cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Witness),
        ],
    )
    .expect("cluster with witness");
    dynamic_witness_cluster.start().expect("cluster starts");
    dynamic_witness_cluster
        .set_node_healthy(2, false)
        .expect("normal voter down");
    dynamic_witness_cluster
        .propose(b"witness-backed-entry".to_vec())
        .expect("proposal replicated to witness");
    assert_eq!(
        dynamic_witness_cluster
            .status(1)
            .expect("leader before witness count")
            .commit_index,
        1
    );
    assert!(!dynamic_witness_cluster.count_witness_in_commit_quorum());
    assert!(dynamic_witness_cluster.renew_leader_lease_from_live_quorum());
    assert!(!dynamic_witness_cluster.count_witness_in_commit_quorum());

    dynamic_witness_cluster.tick_leader_lease(dynamic_witness_cluster.config.leader_lease_ms);
    assert!(dynamic_witness_cluster.count_witness_in_commit_quorum());
    assert_eq!(
        dynamic_witness_cluster
            .status(1)
            .expect("leader after witness count")
            .commit_index,
        2
    );

    dynamic_witness_cluster
        .set_node_healthy(2, true)
        .expect("normal voter recovers");
    dynamic_witness_cluster.tick_leader_lease(dynamic_witness_cluster.config.leader_lease_ms);
    assert!(!dynamic_witness_cluster.count_witness_in_commit_quorum());

    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Witness),
        ],
    )
    .expect("cluster with witness");
    cluster.start().expect("cluster starts");
    cluster.set_node_healthy(3, false).expect("node 3 down");

    cluster.propose(b"set a=1".to_vec()).expect("propose");
    assert_eq!(cluster.status(1).expect("leader status").commit_index, 2);
    let witness_status = cluster.status(4).expect("witness status");
    assert_eq!(witness_status.last_log_index, 2);
    assert_eq!(witness_status.applied_index, 2);
    assert_eq!(witness_status.role, RustRaftRole::Follower);
    assert_eq!(
        cluster
            .wal_record_for(4)
            .expect("witness wal")
            .entries
            .len(),
        2
    );
    assert!(!cluster.ignore_witness());

    let counted = cluster.witness_quorum_report([1, 2, 4]);
    assert_eq!(counted.required, 3);
    assert_eq!(counted.acknowledged, 3);
    assert!(counted.reached);

    cluster.set_ignore_witness(true);
    assert!(cluster.ignore_witness());
    assert_eq!(cluster.status(1).expect("leader status").commit_index, 2);

    cluster.set_node_healthy(4, false).expect("witness down");
    let pre_vote = cluster
        .vote_to(
            1,
            RustRaftVoteRequest {
                group_id: 7,
                term: 2,
                candidate_id: 2,
                last_log_id: Some(RustRaftLogId { term: 1, index: 2 }),
                pre_vote: true,
                force: true,
            },
        )
        .expect("pre-vote with ignored witness down");
    assert!(pre_vote.vote_granted);

    let ignored = cluster.witness_quorum_report([1, 4]);
    assert_eq!(ignored.required, 2);
    assert_eq!(ignored.acknowledged, 1);
    assert!(!ignored.reached);
    let read_quorum = cluster.read_quorum_report();
    assert!(read_quorum.reached);
    assert!(read_quorum.live_witnesses.is_empty());
}

#[test]
fn removing_down_voter_recomputes_commit_like_matrixraft() {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
            peer(4, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("cluster with four voters");
    cluster.start().expect("cluster starts");
    cluster.set_node_healthy(3, false).expect("node 3 down");
    cluster.set_node_healthy(4, false).expect("node 4 down");

    cluster
        .propose(b"pending-with-two-acks".to_vec())
        .expect("propose");
    assert_eq!(cluster.status(1).expect("leader status").commit_index, 1);
    assert_eq!(
        cluster.status(2).expect("follower status").last_log_index,
        2
    );

    cluster.remove_peer(4).expect("remove down voter");
    assert_eq!(cluster.status(1).expect("leader status").commit_index, 2);
    assert_eq!(cluster.status(2).expect("follower status").commit_index, 2);
    assert!(!cluster.membership().voters.contains(&4));
}

#[test]
fn removing_current_leader_transfers_to_closest_follower_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .propose(b"before-leader-remove".to_vec())
        .expect("propose");
    assert_eq!(cluster.leader_id(), Some(1));

    cluster.remove_peer(1).expect("remove current leader");

    assert!(!cluster.membership().voters.contains(&1));
    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(
        cluster.status(2).expect("new leader status").role,
        RustRaftRole::Leader
    );
    let read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 2,
            min_commit_index: 2,
            allow_lease_read: false,
        })
        .expect("new leader read index");
    assert!(!read.safe);
    assert_eq!(read.read_index, 3);
    assert_eq!(read.reason, "applied_index_behind_read_index");
}

#[test]
fn leader_transfer_requires_a_caught_up_voter() {
    let mut no_leader = three_node_cluster();
    assert_eq!(no_leader.transfer_leader(2), Err(RustRaftError::NoLeader));

    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"set a=1".to_vec()).expect("propose");
    let initial_leader = cluster.leader_id().expect("leader");
    let initial_term = cluster.status(initial_leader).expect("leader status").term;

    cluster
        .transfer_leader(initial_leader)
        .expect("self transfer is no-op");
    assert_eq!(cluster.leader_id(), Some(initial_leader));
    assert_eq!(
        cluster.status(initial_leader).expect("leader status").term,
        initial_term
    );

    cluster.transfer_leader(2).expect("transfer leader");
    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(
        cluster.status(2).expect("node 2 status").role,
        RustRaftRole::Leader
    );

    let leader_after_valid_transfer = cluster.leader_id();
    cluster
        .transfer_leader(99)
        .expect("unknown transferee is ignored like MatrixRaft");
    assert_eq!(cluster.leader_id(), leader_after_valid_transfer);

    let mut learner = peer(4, RustRaftReplicaRole::Learner);
    learner.auto_promote = false;
    cluster.add_learner(learner).expect("add learner");
    cluster
        .transfer_leader(4)
        .expect("learner transferee is ignored like MatrixRaft");
    assert_eq!(cluster.leader_id(), leader_after_valid_transfer);

    cluster
        .add_witness(peer(5, RustRaftReplicaRole::Witness))
        .expect("add witness");
    cluster
        .transfer_leader(5)
        .expect("witness transferee is ignored like MatrixRaft");
    assert_eq!(cluster.leader_id(), leader_after_valid_transfer);
}

#[test]
fn promote_rejects_witness_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .add_witness(peer(5, RustRaftReplicaRole::Witness))
        .expect("add witness");

    let err = cluster
        .promote_peer(5)
        .expect_err("direct witness promotion is invalid");
    assert!(err.to_string().contains("node 5 is not a learner"));
    assert!(cluster.membership().witnesses.contains(&5));

    let err = cluster
        .apply_committed_membership_operation(RaftMembershipOperation::Promote(5))
        .expect_err("committed witness promotion is invalid");
    assert!(err.to_string().contains("node 5 is not a learner"));
    assert!(cluster.membership().witnesses.contains(&5));
}

#[test]
fn step_down_transfers_to_closest_healthy_follower() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"set a=1".to_vec()).expect("propose");
    let initial_leader = cluster.leader_id().expect("leader");

    assert_eq!(cluster.closest_follower(), Some(2));
    let transferee = cluster.step_down(None).expect("step down");
    assert_eq!(transferee, Some(2));
    assert_ne!(cluster.leader_id(), Some(initial_leader));
    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(cluster.closest_follower(), Some(3));

    cluster.set_node_healthy(1, false).expect("node 1 down");
    cluster.set_node_healthy(3, false).expect("node 3 down");
    assert!(cluster.step_down(None).is_err());
    assert!(cluster
        .resign_leader("operator_resign")
        .expect("resign without transfer target"));
    assert_eq!(cluster.leader_id(), None);
    assert_eq!(
        cluster.status(2).expect("resigned leader status").role,
        RustRaftRole::Follower
    );
    assert!(!cluster
        .resign_leader("already_resigned")
        .expect("second resign is ignored"));
}

#[test]
fn leader_transfer_tracks_in_progress_duplicate_abort_and_timeout() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"set a=1".to_vec()).expect("propose");

    let first = cluster
        .begin_leader_transfer(2)
        .expect("begin transfer")
        .expect("transfer state");
    assert_eq!(first.transferee_id, 2);
    assert_eq!(first.reason, "transfer_ready");
    assert_eq!(
        cluster
            .cluster_status_report()
            .expect("cluster status")
            .leader_transfer
            .expect("status transfer")
            .transferee_id,
        2
    );

    let duplicate = cluster
        .begin_leader_transfer(2)
        .expect("duplicate transfer")
        .expect("duplicate state");
    assert_eq!(duplicate.transferee_id, 2);
    assert_eq!(duplicate.duplicate_requests, 1);
    assert_eq!(duplicate.reason, "duplicate_transfer_in_progress");

    let replacement = cluster
        .begin_leader_transfer(3)
        .expect("replace transfer")
        .expect("replacement state");
    assert_eq!(replacement.transferee_id, 3);
    assert_eq!(replacement.aborted_transfers, 1);

    cluster.campaign(2, true).expect("campaign non-transferee");
    assert!(cluster.leader_transfer_state().is_none());

    cluster.campaign(1, true).expect("restore leader");
    cluster
        .begin_leader_transfer(2)
        .expect("begin transfer to candidate")
        .expect("candidate transfer state");
    cluster.campaign(2, true).expect("campaign transferee");
    assert!(cluster.leader_transfer_state().is_none());

    cluster
        .begin_leader_transfer(3)
        .expect("begin timed transfer")
        .expect("timed transfer state");
    while cluster.leader_transfer_state().is_some() {
        cluster.tick_leader_transfer();
    }
    assert!(cluster.leader_transfer_state().is_none());
}

#[test]
fn leader_transfer_waits_for_transferee_catchup_before_campaign() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.set_node_healthy(2, false).expect("isolate node 2");
    cluster.propose(b"set a=1".to_vec()).expect("propose");
    let initial_leader = cluster.leader_id().expect("leader");

    cluster.set_node_healthy(2, true).expect("heal node 2");
    cluster.transfer_leader(2).expect("start transfer");
    assert_eq!(cluster.leader_id(), Some(initial_leader));
    assert_eq!(
        cluster
            .leader_transfer_state()
            .expect("transfer waits")
            .reason,
        "waiting_for_transferee_catchup"
    );
    assert!(!cluster
        .try_complete_leader_transfer()
        .expect("not caught up yet"));
    assert_eq!(cluster.leader_id(), Some(initial_leader));

    let leader_tail_before_transfer_proposal = cluster
        .status(initial_leader)
        .expect("leader status before transfer proposal")
        .last_log_index;
    let proposed_during_transfer = cluster
        .propose(b"set b=2".to_vec())
        .expect("proposal continues while transfer is in progress like MatrixRaft");
    assert!(proposed_during_transfer.index > leader_tail_before_transfer_proposal);
    let mut completed = false;
    for _ in 0..3 {
        cluster.catch_up_peer(2).expect("catch up node 2");
        if cluster
            .try_complete_leader_transfer()
            .expect("caught up transfer completion attempt")
        {
            completed = true;
            break;
        }
    }
    assert!(completed);
    assert_eq!(cluster.leader_id(), Some(2));
    assert!(cluster.leader_transfer_state().is_none());
}

#[test]
fn leader_transfer_to_stopped_follower_times_out_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .propose(b"before-transfer".to_vec())
        .expect("propose");
    let initial_leader = cluster.leader_id().expect("leader");

    cluster.set_node_healthy(2, false).expect("stop follower");
    cluster.transfer_leader(2).expect("begin transfer");
    let transfer = cluster
        .leader_transfer_state()
        .expect("stopped follower transfer state");
    assert_eq!(transfer.transferee_id, 2);
    assert_eq!(transfer.reason, "waiting_for_transferee_available");
    let proposed_during_transfer = cluster
        .propose(b"proposal-while-transfer-is-pending".to_vec())
        .expect("MatrixRaft keeps accepting proposals during leader transfer");
    assert_eq!(proposed_during_transfer.index, 3);
    assert!(!cluster
        .try_complete_leader_transfer()
        .expect("stopped follower does not campaign"));

    while cluster.leader_transfer_state().is_some() {
        cluster.tick_leader_transfer();
    }
    assert_eq!(cluster.leader_id(), Some(initial_leader));
    assert!(cluster.leader_transfer_state().is_none());
}

#[test]
fn append_response_completes_caught_up_leader_transfer_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.set_node_healthy(2, false).expect("isolate node 2");
    cluster.propose(b"transfer-gap".to_vec()).expect("propose");
    let leader = cluster.leader_id().expect("leader");
    let leader_last_index = cluster
        .status(leader)
        .expect("leader status")
        .last_log_index;

    cluster.set_node_healthy(2, true).expect("heal node 2");
    cluster.transfer_leader(2).expect("start transfer");
    assert_eq!(
        cluster
            .leader_transfer_state()
            .expect("transfer waits")
            .reason,
        "waiting_for_transferee_catchup"
    );

    cluster
        .handle_append_entries_response(
            leader,
            2,
            RustRaftAppendEntriesResponse {
                term: cluster.status(leader).expect("leader status").term,
                success: true,
                match_index: leader_last_index,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("caught-up append response completes transfer");

    assert_eq!(cluster.leader_id(), Some(2));
    assert!(cluster.leader_transfer_state().is_none());
}

#[test]
fn heartbeat_skips_snapshot_transfer_peer_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");

    cluster
        .begin_snapshot_send_to(2, "snap-heartbeat-skip", 5, 1)
        .expect("start snapshot send");

    assert_eq!(
        cluster.broadcast_heartbeat().expect("heartbeat sent"),
        1,
        "normal heartbeat append should skip peer in snapshot transfer"
    );
    assert!(
        cluster
            .peer_pipeline_status(2)
            .expect("peer 2 pipeline")
            .snapshot_sending
    );
    assert_eq!(
        cluster.status(3).expect("peer 3 status").role,
        RustRaftRole::Follower
    );
}

#[test]
fn heartbeat_refreshes_liveness_and_missing_heartbeats_timeout_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let timed_out = cluster.tick_peer_liveness(900);
    assert!(timed_out.is_empty());
    assert_eq!(
        cluster.broadcast_heartbeat().expect("heartbeat sent"),
        2,
        "leader heartbeats both remote voters"
    );

    let timed_out = cluster.tick_peer_liveness(900);
    assert!(timed_out.is_empty());
    assert!(cluster
        .status(1)
        .expect("leader status")
        .peers
        .iter()
        .all(|peer| peer.healthy));

    let timed_out = cluster.tick_peer_liveness(1_001);
    assert_eq!(timed_out, vec![2, 3]);
    let leader_status = cluster.status(1).expect("leader status");
    assert!(leader_status.peers.iter().all(|peer| !peer.healthy));
    assert!(
        cluster
            .peer_pipeline_status(2)
            .expect("peer 2 pipeline")
            .offline_timeout_reached
    );
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("peer 2 pipeline")
            .offline_timeout_rejections,
        1
    );

    assert_eq!(
        cluster.broadcast_heartbeat().expect("heartbeat sent"),
        2,
        "leader probes liveness-timed-out peers like MatrixRaft heartbeat broadcast"
    );
    let leader_status = cluster.status(1).expect("leader status");
    assert!(leader_status.peers.iter().all(|peer| peer.healthy));
    assert!(
        !cluster
            .peer_pipeline_status(2)
            .expect("peer 2 pipeline")
            .offline_timeout_reached
    );
}

#[test]
fn heartbeat_response_resumes_paused_peer_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");

    let next_index = cluster
        .peer_pipeline_status(2)
        .expect("peer 2 pipeline")
        .next_index;
    let term = cluster.status(1).expect("leader status").term;
    cluster
        .handle_append_entries_response(
            1,
            2,
            RustRaftAppendEntriesResponse {
                term,
                success: false,
                match_index: 0,
                rejection_hint: Some(next_index),
                rejected_index: Some(next_index),
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("same-index rejection pauses peer");
    assert!(
        cluster
            .peer_pipeline_status(2)
            .expect("peer 2 pipeline")
            .paused
    );

    cluster.broadcast_heartbeat().expect("heartbeat broadcast");

    assert!(
        !cluster
            .peer_pipeline_status(2)
            .expect("peer 2 pipeline")
            .paused
    );
}

#[test]
fn append_rejection_retries_catchup_immediately_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.campaign(1, true).expect("make node 1 leader");
    let term = cluster.status(1).expect("leader status").term;

    cluster
        .propose(b"already-matched".to_vec())
        .expect("base entry");
    assert_eq!(cluster.status(2).expect("peer 2 status").last_log_index, 2);

    cluster
        .set_node_healthy(2, false)
        .expect("partition peer 2");
    cluster
        .propose(b"queued-one".to_vec())
        .expect("first entry");
    cluster
        .propose(b"queued-two".to_vec())
        .expect("second entry");
    assert_eq!(cluster.status(2).expect("peer 2 status").last_log_index, 2);

    cluster.set_node_healthy(2, true).expect("heal peer 2");
    cluster
        .handle_append_entries_response(
            1,
            2,
            RustRaftAppendEntriesResponse {
                term,
                success: false,
                match_index: 0,
                rejection_hint: Some(1),
                rejected_index: Some(2),
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("normal append rejection triggers retry");

    assert_eq!(cluster.status(2).expect("peer 2 status").last_log_index, 4);
    let pipeline = cluster.peer_pipeline_status(2).expect("peer 2 pipeline");
    assert!(!pipeline.paused);
    assert_eq!(pipeline.match_index, 4);
}

#[test]
fn required_snapshot_above_leader_snapshot_triggers_new_snapshot_like_matrixraft() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .propose(b"needs-new-snapshot".to_vec())
        .expect("propose");
    cluster
        .propose(b"snapshot-covers-required-index".to_vec())
        .expect("second propose");
    let leader = cluster.leader_id().expect("leader");
    let term = cluster.status(leader).expect("leader status").term;

    let rejected = cluster.handle_append_entries_response(
        leader,
        2,
        RustRaftAppendEntriesResponse {
            term,
            success: false,
            match_index: 1,
            rejection_hint: Some(1),
            rejected_index: None,
            require_snapshot: Some(2),
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        },
    );
    rejected.expect("append rejection is handled as progress");

    let pipeline = cluster.peer_pipeline_status(2).expect("peer 2 pipeline");
    assert_eq!(pipeline.required_snapshot_index, 2);
    assert!(!pipeline.snapshot_sending);
    let trigger = cluster.snapshot_trigger_status();
    assert!(trigger.in_progress);
    let snapshot_id = trigger.snapshot_id.expect("snapshot id");

    cluster
        .handle_snapshot_ready(&snapshot_id, true)
        .expect("ready snapshot is published");

    let pipeline = cluster.peer_pipeline_status(2).expect("peer 2 pipeline");
    assert!(pipeline.snapshot_sending);
    assert_eq!(pipeline.snapshot_install_total_chunks, 1);
    assert!(!cluster.snapshot_trigger_status().in_progress);
}

#[test]
fn raft_cluster_implements_consensus_trait_surface() {
    let mut cluster = three_node_cluster();
    RustRaftConsensus::start(&mut cluster).expect("trait start");
    let log_id = RustRaftConsensus::propose(&mut cluster, b"x".to_vec(), Default::default())
        .expect("trait propose");
    assert_eq!(log_id.index, 2);

    let read = RustRaftConsensus::read_index(&cluster, 1).expect("trait read index");
    assert!(read.safe);
    assert_eq!(read.read_index, 2);
}
