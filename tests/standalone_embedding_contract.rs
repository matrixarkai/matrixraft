// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    metrics::matrixraft_metric_names, readiness::matrixraft_standalone_readiness_report,
    snapshot::PersistentRaftSnapshotStore, AppendEntriesRequest, InstallSnapshotRequest,
    PersistentRaftSnapshotStoreOptions, PersistentRaftWal, PersistentRaftWalOptions, RaftCluster,
    RaftConfig, RaftMembershipExecutor, RaftMembershipOperation, RaftNodeRuntime,
    RaftNodeRuntimeState, RaftSnapshot, RaftSnapshotLifecycle, RaftSnapshotLifecycleConfig,
    ReadIndexRequest, RustRaftApplySnapshotFence, RustRaftHardState,
    RustRaftInstallSnapshotResponse, RustRaftLogEntry, RustRaftLogId, RustRaftMembership,
    RustRaftNodeOptions, RustRaftPeer, RustRaftReadIndexRequest, RustRaftReplicaRole,
    RustRaftSnapshotMeta, RustRaftWalRecord, VoteRequest,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-standalone-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 31_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 32_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn cluster() -> RaftCluster {
    RaftCluster::new(
        909,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("cluster")
}

fn snapshot(index: u64, membership: Vec<u64>) -> RaftSnapshot {
    RaftSnapshot {
        group_id: 909,
        meta: RustRaftSnapshotMeta {
            snapshot_id: format!("standalone-snapshot-{index}"),
            last_log_id: RustRaftLogId { term: 1, index },
            membership,
            members: Vec::new(),
        },
        payload: format!("checkpoint-through-{index}").into_bytes(),
    }
}

fn tail_entry(index: u64) -> RustRaftLogEntry {
    RustRaftLogEntry {
        log_id: RustRaftLogId { term: 1, index },
        payload: format!("tail-{index}").into_bytes(),
        is_command: true,
    }
}

fn wal_record(index: u64) -> RustRaftWalRecord {
    RustRaftWalRecord {
        group_id: 909,
        node_id: 1,
        hard_state: RustRaftHardState {
            current_term: 1,
            voted_for: Some(1),
            committed: Some(RustRaftLogId { term: 1, index }),
        },
        membership: RustRaftMembership {
            group_id: 909,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: vec![tail_entry(index)],
        installed_snapshot: None,
        apply_snapshot_fence: RustRaftApplySnapshotFence {
            applied_index: index,
            commit_index: index,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    }
}

#[test]
fn pass_1_non_temporalstore_app_can_drive_node_lifecycle() {
    let wal_dir = temp_dir("runtime-wal");
    let snapshot_dir = temp_dir("runtime-snapshot");
    let mut runtime = RaftNodeRuntime::create(RustRaftNodeOptions {
        group_id: 909,
        node_id: 1,
        raft_addr: "127.0.0.1:31001".to_string(),
        snapshot_addr: "127.0.0.1:32001".to_string(),
        wal_dir: wal_dir.display().to_string(),
        snapshot_dir: snapshot_dir.display().to_string(),
        role: RustRaftReplicaRole::Voter,
        config: RaftConfig {
            heartbeat_interval_ms: 5,
            election_timeout_ms: 20,
            leader_lease_ms: 10,
            ..Default::default()
        },
        peers: vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    })
    .expect("create runtime");

    runtime.start().expect("start");
    assert_eq!(
        runtime.status().expect("status").state,
        RaftNodeRuntimeState::Running
    );
    assert_eq!(
        runtime
            .propose(b"standalone-write".to_vec())
            .expect("propose")
            .index,
        2
    );
    let read = runtime.read_index(1).expect("read index");
    assert!(read.safe);
    assert!(read.lease_read);
    let snapshot = runtime.trigger_snapshot().expect("trigger snapshot");
    assert_eq!(snapshot.last_log_id.index, 2);
    assert!(runtime.pre_vote().expect("pre vote").vote_granted);
    runtime.campaign(false).expect("campaign");
    runtime.transfer_leader(2).expect("transfer");
    assert_eq!(
        runtime
            .status()
            .expect("status after transfer")
            .timer_status
            .leader_transfer_executions,
        1
    );

    runtime.restart().expect("restart");
    let restarted = runtime.status().expect("restarted status");
    assert_eq!(restarted.state, RaftNodeRuntimeState::Running);
    assert_eq!(restarted.restart_count, 1);
    runtime.stop().expect("stop");
    assert_eq!(
        runtime.status().expect("stopped status").state,
        RaftNodeRuntimeState::Stopped
    );
    runtime.shutdown().expect("shutdown");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Shutdown);

    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}

#[test]
fn pass_2_non_temporalstore_app_gets_replication_and_read_safety() {
    let mut cluster = cluster();
    cluster.start().expect("start");
    for index in 1..=3 {
        cluster
            .propose(format!("replicated-{index}").into_bytes())
            .expect("propose");
    }

    let peer_two = cluster.peer_pipeline_status(2).expect("peer 2 pipeline");
    assert!(peer_two.append_requests > 0);
    assert_eq!(peer_two.match_index, 4);
    assert_eq!(peer_two.follower_lag, 0);
    assert!(cluster.lease_read_eligible(1, 4).expect("lease read"));

    cluster
        .set_node_healthy(2, false)
        .expect("mark follower down");
    cluster
        .set_node_healthy(3, false)
        .expect("mark follower down");
    cluster.set_leader_lease_valid(false);
    let no_quorum = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 909,
            requester_id: 1,
            min_commit_index: 4,
            allow_lease_read: true,
        })
        .expect("read index without quorum");
    assert!(!no_quorum.safe);
    assert_eq!(no_quorum.reason, "no_live_quorum");

    cluster.set_node_healthy(2, true).expect("restore quorum");
    cluster.set_node_healthy(3, true).expect("restore quorum");
    let stale_lease = cluster
        .read_path_report(
            RustRaftReadIndexRequest {
                group_id: 909,
                requester_id: 1,
                min_commit_index: 4,
                allow_lease_read: true,
            },
            0,
        )
        .expect("read path report");
    assert!(stale_lease.safe);
    assert!(stale_lease.stale_leader_rejected);
    assert!(!stale_lease.lease_read);
    assert_eq!(stale_lease.reason, "stale_leader_lease");
}

#[test]
fn pass_3_non_temporalstore_app_gets_membership_executor_workflow() {
    let mut cluster = cluster();
    cluster.start().expect("start");
    for index in 1..=3 {
        cluster
            .propose(format!("membership-{index}").into_bytes())
            .expect("propose");
    }

    let mut executor = RaftMembershipExecutor::new();
    executor
        .execute(
            &mut cluster,
            RaftMembershipOperation::AddLearner(peer(4, RustRaftReplicaRole::Learner)),
        )
        .expect("add learner");
    let blockers = executor.validate(&cluster, &RaftMembershipOperation::Promote(4));
    assert!(blockers.is_empty());
    executor
        .execute(&mut cluster, RaftMembershipOperation::Promote(4))
        .expect("promotion catches up learner");
    assert!(cluster.membership().voters.contains(&4));

    let failed = executor.execute_all_with_rollback(
        &mut cluster,
        vec![
            RaftMembershipOperation::AddWitness(peer(5, RustRaftReplicaRole::Witness)),
            RaftMembershipOperation::Remove(99),
        ],
    );
    assert!(failed.is_err());
    assert!(!cluster.membership().witnesses.contains(&5));

    let reports = executor
        .execute_all(
            &mut cluster,
            vec![
                RaftMembershipOperation::AddWitness(peer(5, RustRaftReplicaRole::Witness)),
                RaftMembershipOperation::TransferLeader(2),
                RaftMembershipOperation::Remove(1),
            ],
        )
        .expect("membership workflow");
    assert!(reports.iter().all(|report| report.success));
    assert!(reports
        .iter()
        .any(|report| report.joint_consensus.is_some()));
    let membership = cluster.membership();
    assert!(membership.voters.contains(&4));
    assert!(membership.witnesses.contains(&5));
    assert!(!membership.voters.contains(&1));
    assert_eq!(cluster.leader_id(), Some(2));
}

#[test]
fn pass_4_non_temporalstore_app_gets_wal_snapshot_recovery_and_fences() {
    let wal_dir = temp_dir("wal");
    let wal_options = PersistentRaftWalOptions {
        dir: wal_dir.clone(),
        max_records_per_segment: 2,
        max_segment_bytes: 4096,
        min_keep_segments: 1,
        fsync_on_append: true,
    };
    {
        let mut wal = PersistentRaftWal::open(wal_options.clone()).expect("open wal");
        wal.append(wal_record(1)).expect("append 1");
        wal.append(wal_record(2)).expect("append 2");
        wal.append(wal_record(3)).expect("append 3");
        wal.corrupt_tail_for_test().expect("corrupt tail");
    }
    let mut reopened = PersistentRaftWal::open(wal_options).expect("reopen wal");
    let recovery = reopened.recover().expect("recover wal");
    assert!(recovery.truncated_corrupt_tail);
    assert_eq!(recovery.surviving_records, 3);
    assert_eq!(
        recovery
            .recovered
            .expect("recovered record")
            .hard_state
            .committed
            .expect("commit")
            .index,
        3
    );

    let snapshot_dir = temp_dir("snapshot");
    let store = PersistentRaftSnapshotStore::open(PersistentRaftSnapshotStoreOptions {
        dir: snapshot_dir.clone(),
        chunk_size: 4,
    })
    .expect("snapshot store");
    let snap = snapshot(3, vec![1, 2, 3]);
    store.save_checkpoint(&snap).expect("save checkpoint");
    let chunks = store
        .checkpoint_chunks("standalone-snapshot-3")
        .expect("checkpoint chunks");
    assert!(chunks.len() > 1);

    let mut sender = RaftSnapshotLifecycle::new(RaftSnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 4,
        max_retry_attempts: 2,
    })
    .expect("sender lifecycle");
    let mut receiver = RaftSnapshotLifecycle::new(Default::default()).expect("receiver lifecycle");
    sender.begin_send(&snap, 1, 1).expect("begin send");
    while sender.status().sending {
        for request in sender.poll_send_requests().expect("poll send") {
            let installed = receiver.install_request(request).expect("install chunk");
            sender
                .record_send_response(&RustRaftInstallSnapshotResponse {
                    term: 1,
                    accepted: true,
                    next_offset: receiver.status().received_chunks,
                    committed_index: 0,
                    reason: "accepted".to_string(),
                })
                .expect("ack send");
            if let Some(installed) = installed {
                assert_eq!(installed, snap);
            }
        }
    }
    assert!(sender.status().completed);
    assert!(receiver.status().completed);

    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}

#[test]
fn pass_5_standalone_status_api_covers_all_embedding_capabilities() {
    let report = matrixraft_standalone_readiness_report();
    let capability_ids = report
        .capabilities
        .iter()
        .map(|capability| capability.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        capability_ids,
        vec![
            "node_lifecycle",
            "replication",
            "election_pre_vote",
            "membership",
            "wal_recovery",
            "snapshots",
            "read_index_lease_read",
            "status_metrics_readiness",
        ]
    );
    assert!(report.standalone);
    assert!(report.evidence.len() >= capability_ids.len());
    assert!(report.missing.is_empty());
    assert!(matrixraft_metric_names()
        .append_latency_ms
        .starts_with("rustraft_"));

    let _append = std::mem::size_of::<AppendEntriesRequest>();
    let _vote = std::mem::size_of::<VoteRequest>();
    let _snapshot = std::mem::size_of::<InstallSnapshotRequest>();
    let _read = std::mem::size_of::<ReadIndexRequest>();
}
