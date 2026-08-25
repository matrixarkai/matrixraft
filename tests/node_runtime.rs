// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, RaftAdminCommand,
    RaftMembershipOperation, RaftNodeRuntime, RaftNodeRuntimeState, RaftSnapshot, ReadIndexRequest,
    RustRaftApplySnapshotFence, RustRaftConfig, RustRaftLogId, RustRaftMessage,
    RustRaftNodeOptions, RustRaftPeer, RustRaftProposeOptions, RustRaftReplicaRole,
    RustRaftRequestTimer, RustRaftSnapshotChunk, RustRaftSnapshotMeta, RustRaftSnapshotState,
    RustRaftStepResult, RustRaftStorageApplyFence, RustRaftTickBackpressure, VoteRequest,
    MATRIXRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS,
};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 12_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 13_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn auto_promote_peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        auto_promote: true,
        ..peer(node_id)
    }
}

fn temp_runtime_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-node-runtime-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn node_options() -> RustRaftNodeOptions {
    node_options_in(temp_runtime_dir("default"))
}

fn node_options_in(base_dir: PathBuf) -> RustRaftNodeOptions {
    RustRaftNodeOptions {
        group_id: 77,
        node_id: 1,
        raft_addr: "127.0.0.1:12001".to_string(),
        snapshot_addr: "127.0.0.1:13001".to_string(),
        wal_dir: base_dir.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base_dir.join("snapshot").to_string_lossy().into_owned(),
        role: RustRaftReplicaRole::Voter,
        config: RustRaftConfig::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

fn timer_node_options() -> RustRaftNodeOptions {
    let mut options = node_options_in(temp_runtime_dir("timer"));
    options.config.heartbeat_interval_ms = 10;
    options.config.election_timeout_ms = 50;
    options.config.leader_lease_ms = 20;
    options
}

#[test]
fn node_runtime_lifecycle_drives_background_cluster() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Created);

    runtime.start().expect("start runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Running);
    let status = runtime.status().expect("status");
    assert!(status.worker_running);
    assert_eq!(status.node_id, 1);
    assert_eq!(status.group_id, 77);
    assert_eq!(
        status.cluster_status.expect("cluster status").leader_id,
        Some(1)
    );

    let log_id = runtime
        .propose(b"write through worker".to_vec())
        .expect("propose");
    assert_eq!(log_id.index, 2);
    let read = runtime.read_index(2).expect("read index");
    assert!(read.safe);
    assert_eq!(read.read_index, 2);
    assert!(read.lease_read);

    let stale_lease = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::SetLeaderLeaseValid { valid: false },
        })
        .expect("stale leader lease through runtime step");
    assert_eq!(stale_lease, RustRaftStepResult::Handled);
    let read = runtime.read_index(2).expect("read index without lease");
    assert!(read.safe);
    assert!(!read.lease_read);
    assert_eq!(read.reason, "read_index");

    runtime.stop().expect("stop runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Stopped);
    assert!(runtime.propose(b"stopped".to_vec()).is_err());

    runtime.restart().expect("restart runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Running);
    assert_eq!(runtime.restart_count(), 1);
    assert_eq!(runtime.propose(b"after restart".to_vec()).unwrap().index, 3);

    runtime.shutdown().expect("shutdown runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Shutdown);
    assert!(runtime.read_index(1).is_err());
}

#[test]
fn node_runtime_rejects_stale_expected_term_proposals_like_baseline_raft() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let stale = runtime
        .propose_with_options(
            b"stale-term".to_vec(),
            RustRaftProposeOptions {
                expected_term: Some(0),
                is_command: true,
                ..Default::default()
            },
        )
        .expect_err("stale expected term is rejected");
    assert!(stale
        .to_string()
        .contains("expected term 0 does not match current term 1"));

    let accepted = runtime
        .propose_with_options(
            b"current-term".to_vec(),
            RustRaftProposeOptions {
                expected_term: Some(1),
                is_command: true,
                ..Default::default()
            },
        )
        .expect("current expected term is accepted");
    assert_eq!(accepted, RustRaftLogId { term: 1, index: 2 });
    runtime.shutdown().expect("shutdown runtime");
}

#[test]
fn node_runtime_read_index_rejects_without_live_quorum() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"write".to_vec()).expect("propose");
    let node_2_down = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::SetNodeHealthy {
                node_id: 2,
                healthy: false,
            },
        })
        .expect("mark node 2 down through runtime step");
    assert_eq!(node_2_down, RustRaftStepResult::Handled);
    let node_3_down = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::SetNodeHealthy {
                node_id: 3,
                healthy: false,
            },
        })
        .expect("mark node 3 down through runtime step");
    assert_eq!(node_3_down, RustRaftStepResult::Handled);
    runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::SetLeaderLeaseValid { valid: false },
        })
        .expect("invalidate leader lease");

    let read = runtime.read_index(2).expect("read index");
    assert!(!read.safe);
    assert!(!read.lease_read);
    assert_eq!(read.reason, "no_live_quorum");
}

#[test]
fn node_runtime_handles_read_index_rpc_request() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .propose(b"read-index-rpc".to_vec())
        .expect("propose");

    let read = runtime
        .read_index_request(ReadIndexRequest {
            group_id: 77,
            requester_id: 2,
            min_commit_index: 2,
            allow_lease_read: false,
        })
        .expect("read-index request through runtime");
    assert!(!read.safe);
    assert_eq!(read.read_index, 2);
    assert!(!read.lease_read);
    assert_eq!(read.reason, "not_leader");

    let bounded = runtime
        .bounded_stale_read_index(2, 0)
        .expect("bounded-stale read through runtime");
    assert!(bounded.safe);
    assert_eq!(bounded.read_index, 2);

    let bad_group = runtime
        .read_index_request(ReadIndexRequest {
            group_id: 0,
            requester_id: 2,
            min_commit_index: 1,
            allow_lease_read: false,
        })
        .expect_err("bad group is rejected");
    assert!(bad_group.to_string().contains("group id mismatch"));
}

#[test]
fn node_runtime_expires_and_renews_leader_lease() {
    let mut options = node_options();
    options.config.heartbeat_interval_ms = 5;
    options.config.election_timeout_ms = 50;
    options.config.leader_lease_ms = 10;
    let mut runtime = RaftNodeRuntime::create(options).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"lease-start".to_vec()).expect("propose");
    assert!(runtime.read_index(1).expect("initial lease").lease_read);

    std::thread::sleep(std::time::Duration::from_millis(25));

    let expired = runtime.status().expect("status");
    assert!(!expired.timer_status.leader_lease_valid);
    assert!(expired.timer_status.leader_lease_elapsed_ms >= 10);
    let read = runtime.read_index(1).expect("read after lease expiry");
    assert!(read.safe);
    assert!(!read.lease_read);

    runtime
        .propose(b"lease-renew".to_vec())
        .expect("renew lease");
    let renewed = runtime.status().expect("renewed status");
    assert!(renewed.timer_status.leader_lease_valid);
    assert_eq!(renewed.timer_status.leader_lease_elapsed_ms, 0);
    assert!(runtime.read_index(2).expect("renewed lease").lease_read);
}

#[test]
fn node_runtime_steps_down_leader_after_lost_quorum() {
    let mut options = node_options();
    options.config.heartbeat_interval_ms = 5;
    options.config.election_timeout_ms = 50;
    options.config.leader_lease_ms = 10;
    let mut runtime = RaftNodeRuntime::create(options).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .propose(b"before-quorum-loss".to_vec())
        .expect("propose");

    runtime
        .set_node_healthy(2, false)
        .expect("isolate follower 2");
    runtime
        .set_node_healthy(3, false)
        .expect("isolate follower 3");
    let mut status = runtime.status().expect("status after quorum loss");
    for _ in 0..10 {
        if status
            .cluster_status
            .as_ref()
            .and_then(|cluster| cluster.leader_id)
            .is_none()
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        status = runtime.status().expect("status after quorum loss");
    }
    let cluster = status.cluster_status.expect("cluster status");
    assert_eq!(cluster.leader_id, None);
    assert!(status
        .fatal_blocker_report
        .blockers
        .iter()
        .any(|blocker| blocker.id == "lost_quorum_step_down"));
    assert!(runtime.propose(b"after-quorum-loss".to_vec()).is_err());
}

#[test]
fn node_runtime_election_timeout_campaigns_after_stale_leader() {
    let mut options = node_options();
    options.node_id = 2;
    options.config.enable_lease_read = false;
    options.config.heartbeat_interval_ms = 5;
    options.config.election_timeout_ms = 20;
    options.config.leader_lease_ms = 5;
    let mut runtime = RaftNodeRuntime::create(options).expect("create runtime");
    runtime.start().expect("start runtime");

    std::thread::sleep(Duration::from_millis(35));
    let live_leader = runtime.status().expect("live leader status");
    assert_eq!(
        live_leader
            .cluster_status
            .expect("live cluster status")
            .leader_id,
        Some(1)
    );

    runtime
        .set_node_healthy(1, false)
        .expect("stale remembered leader");
    std::thread::sleep(Duration::from_millis(80));

    let stale_leader = runtime.status().expect("stale leader status");
    let cluster = stale_leader.cluster_status.expect("stale cluster status");
    assert_eq!(cluster.leader_id, Some(2));
    assert!(stale_leader.timer_status.pre_vote_executions >= 1);
    assert!(stale_leader.timer_status.campaign_executions >= 1);
}

#[test]
fn node_runtime_transfers_leader_on_fatal_event() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .propose(b"before-leader-fatal".to_vec())
        .expect("propose before fatal");

    let status = runtime.status().expect("status before fatal");
    let leader_id = status.cluster_status.expect("cluster status").leader_id;
    assert_eq!(leader_id, Some(1));

    let transferee = runtime
        .fire_fatal_event(1, "fsm_apply_fatal")
        .expect("fire fatal event");
    assert_eq!(transferee, Some(2));

    let status = runtime.status().expect("status after fatal transfer");
    let cluster = status.cluster_status.expect("cluster status");
    assert_eq!(cluster.leader_id, Some(2));
    assert!(status
        .fatal_blocker_report
        .blockers
        .iter()
        .any(|blocker| blocker.id == "fatal_event:1:fsm_apply_fatal"));
    assert!(runtime.propose(b"old-leader-rejects".to_vec()).is_err());
}

#[test]
fn node_runtime_supports_transfer_and_campaign_lifecycle_commands() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    runtime.transfer_leader(2).expect("transfer leader");
    let status = runtime.status().expect("status");
    assert_eq!(
        status.cluster_status.expect("cluster status").leader_id,
        Some(2)
    );

    runtime.campaign(true).expect("campaign local node");
    let status = runtime.status().expect("status");
    assert_eq!(
        status.cluster_status.expect("cluster status").leader_id,
        Some(1)
    );
}

#[test]
fn node_runtime_runs_heartbeat_election_timer_loop_and_peer_state_machine() {
    let mut runtime = RaftNodeRuntime::create(timer_node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let mut status = runtime.status().expect("status");
    for _ in 0..10 {
        if status.timer_status.heartbeat_ticks > 0 && status.timer_status.election_ticks > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
        status = runtime.status().expect("status");
    }
    assert!(status.timer_status.heartbeat_ticks > 0);
    assert!(status.timer_status.election_ticks > 0);
    assert_eq!(status.timer_status.heartbeat_interval_ms, 10);
    assert_eq!(status.timer_status.election_timeout_ms, 50);
    assert_eq!(status.peer_runtime.len(), 3);
    assert!(!status
        .peer_runtime
        .iter()
        .any(|peer| peer.transfer_leader_target));
    assert!(status.fatal_blocker_report.ready);
}

#[test]
fn node_runtime_executes_prevote_and_reports_blockers_from_runtime_tasks() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let vote = runtime
        .step(RustRaftMessage::PreVote { candidate_id: 1 })
        .expect("pre-vote through runtime step");
    let RustRaftStepResult::PreVote(vote) = vote else {
        panic!("unexpected pre-vote step response: {vote:?}");
    };
    assert!(vote.vote_granted);
    assert_eq!(vote.reason, "pre_vote_granted");

    runtime
        .set_node_healthy(2, false)
        .expect("mark node 2 down");
    runtime
        .set_node_healthy(3, false)
        .expect("mark node 3 down");
    runtime
        .transfer_leader(99)
        .expect("unknown transferee is ignored");
    assert!(runtime.step_down(None).is_err());

    let status = runtime.status().expect("status");
    assert!(status.timer_status.pre_vote_executions >= 1);
    assert!(status.timer_status.leader_transfer_executions >= 1);
    assert!(!status.fatal_blocker_report.ready);
    assert!(!status.fatal_blocker_report.blockers.is_empty());
}

#[test]
fn node_runtime_can_prohibit_election_until_forced_campaign() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let prohibit = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::ProhibitsElection { prohibits: true },
        })
        .expect("prohibit election through runtime step");
    assert_eq!(prohibit, RustRaftStepResult::Handled);
    let vote = runtime.pre_vote().expect("pre-vote");
    assert!(!vote.vote_granted);
    assert_eq!(vote.reason, "election_prohibited");
    assert!(runtime.campaign(false).is_err());

    runtime.campaign(true).expect("forced campaign");
}

#[test]
fn node_runtime_handles_timeout_now_like_baseline_raft() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    let prohibit = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::ProhibitsElection { prohibits: true },
        })
        .expect("prohibit normal elections through runtime step");
    assert_eq!(prohibit, RustRaftStepResult::Handled);

    let follower_timeout = runtime.timeout_now(1, 2).expect("timeout-now follower");
    assert!(follower_timeout.campaigned);
    assert_eq!(follower_timeout.reason, "timeout_now_campaign");
    assert_eq!(
        runtime
            .status()
            .expect("status after follower timeout-now")
            .cluster_status
            .expect("cluster status")
            .leader_id,
        Some(2)
    );

    let mut learner = peer(4);
    learner.role = RustRaftReplicaRole::Learner;
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddLearner(learner))
        .expect("add learner");
    let learner_timeout = runtime.timeout_now(2, 4).expect("timeout-now learner");
    assert!(!learner_timeout.campaigned);
    assert_eq!(learner_timeout.reason, "timeout_now_ignored_Learner");
    assert_eq!(learner_timeout.term, 0);

    let mut witness = peer(5);
    witness.role = RustRaftReplicaRole::Witness;
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddWitness(witness))
        .expect("add witness");
    let witness_timeout = runtime.timeout_now(2, 5).expect("timeout-now witness");
    assert!(!witness_timeout.campaigned);
    assert_eq!(witness_timeout.reason, "timeout_now_ignored_Witness");
    assert_eq!(witness_timeout.term, 0);
    assert_eq!(
        runtime
            .status()
            .expect("status after ignored timeout-now")
            .cluster_status
            .expect("cluster status")
            .leader_id,
        Some(2)
    );
}

#[test]
fn node_runtime_step_down_transfers_to_selected_follower() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let transferee = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::StepDown { transferee: None },
        })
        .expect("step down through runtime step");
    let RustRaftStepResult::StepDown(transferee) = transferee else {
        panic!("unexpected step-down response: {transferee:?}");
    };
    assert_eq!(transferee, Some(2));
    let status = runtime.status().expect("status");
    assert_eq!(
        status.cluster_status.expect("cluster status").leader_id,
        Some(2)
    );
    assert!(status.timer_status.leader_transfer_executions >= 1);
}

#[test]
fn node_runtime_resigns_leader_without_transfer_target() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.set_node_healthy(2, false).expect("stop follower 2");
    runtime.set_node_healthy(3, false).expect("stop follower 3");

    assert!(runtime
        .step_down(None)
        .expect_err("transfer-style step-down needs a target")
        .to_string()
        .contains("no healthy follower"));
    let resigned = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::Resign {
                reason: "operator_resign".to_string(),
            },
        })
        .expect("resign leader through runtime step");
    assert_eq!(resigned, RustRaftStepResult::LeaderResigned(true));
    let status = runtime.status().expect("status after resign");
    assert_eq!(
        status.cluster_status.expect("cluster status").leader_id,
        None
    );
    let second_resign = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::Resign {
                reason: "already_resigned".to_string(),
            },
        })
        .expect("second resign is ignored");
    assert_eq!(second_resign, RustRaftStepResult::LeaderResigned(false));
}

#[test]
fn node_runtime_rejects_propose_after_local_node_steps_down() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    assert_eq!(
        runtime.propose(b"before-transfer".to_vec()).unwrap().index,
        2
    );

    let transferee = runtime.step_down(Some(2)).expect("step down");
    assert_eq!(transferee, Some(2));

    let err = runtime
        .propose(b"after-transfer".to_vec())
        .expect_err("follower runtime must reject writes");
    assert!(err.to_string().contains("not the leader"));
}

#[test]
fn node_runtime_rejects_read_index_after_local_node_steps_down() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"before-transfer".to_vec()).unwrap();
    assert!(runtime.read_index(1).expect("leader read-index").safe);

    let transferee = runtime.step_down(Some(2)).expect("step down");
    assert_eq!(transferee, Some(2));

    let err = runtime
        .read_index(1)
        .expect_err("follower runtime must reject leader read-index");
    assert!(err.to_string().contains("not the leader"));
}

#[test]
fn node_runtime_allows_explicit_bounded_stale_read_after_step_down() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"before-transfer".to_vec()).unwrap();

    let transferee = runtime.step_down(Some(2)).expect("step down");
    assert_eq!(transferee, Some(2));

    let read = runtime
        .bounded_stale_read_index(1, 0)
        .expect("bounded-stale follower read");
    assert!(read.safe);
    assert!(!read.lease_read);
    assert_eq!(read.reason, "read_index_quorum");
    assert_eq!(read.read_index, 2);
    assert!(
        read.bounded_stale
            .as_ref()
            .expect("bounded-stale report")
            .allowed
    );
}

#[test]
fn node_runtime_executes_membership_operations() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let learner = runtime
        .step(RustRaftMessage::Membership {
            operation: RaftMembershipOperation::AddLearner(peer(4)),
        })
        .expect("add learner through runtime step");
    let RustRaftStepResult::Membership(learner) = learner else {
        panic!("unexpected learner membership step response: {learner:?}");
    };
    assert!(learner.success);
    assert!(learner.after.learners.contains(&4));

    let witness = runtime
        .step(RustRaftMessage::Membership {
            operation: RaftMembershipOperation::AddWitness(peer(5)),
        })
        .expect("add witness through runtime step");
    let RustRaftStepResult::Membership(witness) = witness else {
        panic!("unexpected witness membership step response: {witness:?}");
    };
    assert!(witness.success);
    assert!(witness.after.witnesses.contains(&5));

    let removed = runtime
        .step(RustRaftMessage::Membership {
            operation: RaftMembershipOperation::Remove(3),
        })
        .expect("remove peer through runtime step");
    let RustRaftStepResult::Membership(removed) = removed else {
        panic!("unexpected remove membership step response: {removed:?}");
    };
    assert!(removed.success);
    assert!(!removed.after.voters.contains(&3));

    let status = runtime.status().expect("runtime status");
    assert!(status
        .peer_runtime
        .iter()
        .any(|peer| peer.node_id == 4 && peer.replica_role == RustRaftReplicaRole::Learner));
    assert!(status
        .peer_runtime
        .iter()
        .any(|peer| peer.node_id == 5 && peer.replica_role == RustRaftReplicaRole::Witness));
    assert!(!status.peer_runtime.iter().any(|peer| peer.node_id == 3));
}

#[test]
fn node_runtime_rolls_back_membership_workflow() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .propose(b"membership-workflow".to_vec())
        .expect("write");
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddLearner(peer(4)))
        .expect("add learner");
    runtime.catch_up_peer(4).expect("catch up learner");
    runtime
        .execute_membership_operation(RaftMembershipOperation::Promote(4))
        .expect("promote peer");

    let status_before = runtime.status().expect("status before rollback");
    let mut voters_before = status_before
        .peer_runtime
        .iter()
        .filter(|node| node.replica_role == RustRaftReplicaRole::Voter)
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    voters_before.sort_unstable();

    runtime
        .execute_membership_workflow_with_rollback(vec![
            RaftMembershipOperation::AddWitness(peer(5)),
            RaftMembershipOperation::Remove(99),
        ])
        .expect_err("workflow should roll back");
    let reports = runtime
        .membership_execution_reports()
        .expect("membership reports");
    let rollback_report = reports.last().expect("rollback report");
    assert!(rollback_report.rolled_back);
    assert!(rollback_report.reason.contains("rolled_back"));
    assert!(!rollback_report.success);

    let status_after = runtime.status().expect("status after rollback");
    let cluster = status_after
        .cluster_status
        .as_ref()
        .expect("cluster status");
    assert_eq!(cluster.leader_id, Some(1));
    let mut voters_after = status_after
        .peer_runtime
        .iter()
        .filter(|node| node.replica_role == RustRaftReplicaRole::Voter)
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    voters_after.sort_unstable();
    assert_eq!(voters_after, voters_before);
    assert!(status_after
        .fatal_blocker_report
        .blockers
        .iter()
        .any(|blocker| blocker.id.contains("membership_workflow_with_rollback")));
}

#[test]
fn node_runtime_reports_witness_quorum_policy() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddWitness(peer(5)))
        .expect("add witness");

    let quorum = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::WitnessQuorum {
                acknowledgements: vec![1, 2, 5],
            },
        })
        .expect("witness quorum through runtime step");
    let RustRaftStepResult::WitnessQuorum(quorum) = quorum else {
        panic!("unexpected witness quorum response: {quorum:?}");
    };
    assert_eq!(quorum.required, 3);
    assert_eq!(quorum.acknowledged, 3);
    assert!(quorum.reached);
    assert_eq!(quorum.witnesses, vec![5]);

    let witness = runtime.peer_pipeline_status(5).expect("witness status");
    assert_eq!(witness.witness_quorum_required, 3);
    assert_eq!(witness.witness_quorum_acked, 3);
    assert!(witness.witness_quorum_reached);

    let ignore_witness = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::IgnoreWitness { ignore: true },
        })
        .expect("ignore witness through runtime step");
    assert_eq!(ignore_witness, RustRaftStepResult::Handled);
    let voter_quorum = runtime
        .witness_quorum_report([1, 2])
        .expect("voter quorum with witness ignored");
    assert_eq!(voter_quorum.required, 2);
    assert_eq!(voter_quorum.acknowledged, 2);
    assert!(voter_quorum.reached);
}

#[test]
fn node_runtime_installs_snapshot_with_apply_fence() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddLearner(peer(4)))
        .expect("add learner");
    runtime.propose(b"before-snapshot".to_vec()).expect("write");

    runtime
        .install_snapshot_to(
            4,
            RaftSnapshot {
                group_id: 77,
                meta: RustRaftSnapshotMeta {
                    snapshot_id: "runtime-install-2".to_string(),
                    last_log_id: RustRaftLogId { term: 1, index: 3 },
                    membership: vec![1, 2, 3, 4],
                    members: Vec::new(),
                },
                payload: b"snapshot-payload".to_vec(),
            },
            RustRaftApplySnapshotFence {
                applied_index: 3,
                commit_index: 3,
                installed_snapshot_index: 3,
                first_retained_log_index: 4,
            },
        )
        .expect("install snapshot through runtime");

    let status = runtime.status().expect("runtime status");
    let target = status
        .cluster_status
        .expect("cluster status")
        .nodes
        .into_iter()
        .find(|node| node.node_id == 4)
        .expect("learner status");
    assert_eq!(target.last_snapshot_index, 3);
    assert_eq!(target.applied_index, 3);
    assert_eq!(target.commit_index, 3);
}

#[test]
fn node_runtime_installs_snapshot_chunk_through_runtime() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddLearner(peer(4)))
        .expect("add learner");
    runtime.propose(b"before-chunk".to_vec()).expect("write");

    let meta = RustRaftSnapshotMeta {
        snapshot_id: "runtime-chunk-3".to_string(),
        last_log_id: RustRaftLogId { term: 1, index: 3 },
        membership: vec![1, 2, 3, 4],
        members: Vec::new(),
    };
    let accepted = runtime
        .install_snapshot_chunk_to(
            4,
            InstallSnapshotRequest {
                group_id: 77,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: meta.clone(),
                    offset: 0,
                    data: b"partial".to_vec(),
                    done: false,
                },
            },
        )
        .expect("accept partial chunk");
    assert!(accepted.accepted);
    assert_eq!(accepted.next_offset, 7);
    assert_eq!(accepted.reason, "snapshot_chunk_accepted");

    let installed = runtime
        .install_snapshot_chunk_to(
            4,
            InstallSnapshotRequest {
                group_id: 77,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta,
                    offset: 0,
                    data: b"snapshot-payload".to_vec(),
                    done: true,
                },
            },
        )
        .expect("install final chunk");
    assert!(installed.accepted);
    assert_eq!(installed.reason, "snapshot_installed");

    let target = runtime
        .status()
        .expect("runtime status")
        .cluster_status
        .expect("cluster status")
        .nodes
        .into_iter()
        .find(|node| node.node_id == 4)
        .expect("learner status");
    assert_eq!(target.last_snapshot_index, 3);
    assert_eq!(target.applied_index, 3);
}

#[test]
fn node_runtime_catches_up_added_learner() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"before-learner".to_vec()).expect("write");
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddLearner(peer(4)))
        .expect("add learner");

    let report = runtime
        .step(RustRaftMessage::CatchUpPeer { peer_id: 4 })
        .expect("catch up learner through runtime step");
    let RustRaftStepResult::CatchUpPeer(report) = report else {
        panic!("unexpected catch-up step response: {report:?}");
    };
    assert!(report.caught_up);
    assert_eq!(report.learner_match_index_before, 2);
    assert_eq!(report.learner_match_index_after, 2);
    assert_eq!(report.leader_commit_index, 2);

    let status = runtime.status().expect("runtime status");
    let learner = status
        .peer_runtime
        .iter()
        .find(|peer| peer.node_id == 4)
        .expect("learner runtime state");
    assert_eq!(learner.matched, 2);
    assert_eq!(learner.lag, 0);
}

#[test]
fn node_runtime_auto_promotes_caught_up_learner() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .propose(b"before-auto-promote".to_vec())
        .expect("write");
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddLearner(auto_promote_peer(4)))
        .expect("add auto-promote learner");

    let report = runtime
        .step(RustRaftMessage::AutoPromoteLearner { learner_id: 4 })
        .expect("auto promote through runtime step");
    let RustRaftStepResult::AutoPromoteLearner(report) = report else {
        panic!("unexpected auto-promote step response: {report:?}");
    };
    assert!(report.auto_promote);
    assert!(report.promoted);
    assert_eq!(report.reason, "learner_promoted");

    let status = runtime.status().expect("runtime status");
    assert!(status
        .peer_runtime
        .iter()
        .any(|peer| peer.node_id == 4 && peer.replica_role == RustRaftReplicaRole::Voter));
}

#[test]
fn node_runtime_reports_peer_pipeline_status() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"pipeline-lag".to_vec()).expect("write");

    let pipeline = runtime.peer_pipeline_status(2).expect("pipeline status");
    assert_eq!(pipeline.peer_id, 2);
    assert_eq!(pipeline.match_index, 2);
    assert!(pipeline.append_requests > 0);
    assert!(pipeline.append_accepted > 0);
    let network_error = runtime
        .step(RustRaftMessage::NetworkError { peer_id: 2 })
        .expect("record peer network error through runtime step");
    assert_eq!(network_error, RustRaftStepResult::Handled);
    let recovered = runtime
        .peer_pipeline_status(2)
        .expect("recovered pipeline status");
    assert_eq!(recovered.next_index, recovered.match_index + 1);
}

#[test]
fn node_runtime_reports_all_peer_pipeline_statuses() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"pipeline-all".to_vec()).expect("write");
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddLearner(peer(4)))
        .expect("add learner");
    runtime
        .execute_membership_operation(RaftMembershipOperation::AddWitness(peer(5)))
        .expect("add witness");

    let statuses = runtime
        .peer_pipeline_statuses()
        .expect("all pipeline statuses");
    assert!(statuses.iter().any(|peer| peer.peer_id == 2));
    assert!(statuses.iter().any(|peer| peer.peer_id == 3));
    assert!(statuses.iter().any(|peer| peer.peer_id == 4));
    assert!(statuses.iter().any(|peer| peer.peer_id == 5));
    assert!(statuses
        .iter()
        .any(|peer| peer.peer_id == 2 && peer.match_index == 2 && peer.append_accepted > 0));
    assert_eq!(statuses.len(), 4);
}

#[test]
fn node_runtime_steps_batch_of_raft_messages() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let single = runtime
        .step(RustRaftMessage::AppendEntries {
            target: 3,
            request: AppendEntriesRequest {
                group_id: 77,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![matrixraft::RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 1 },
                    payload: b"single-step".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        })
        .expect("single step through runtime");
    assert!(matches!(
        single,
        RustRaftStepResult::AppendEntries(response) if response.success && response.match_index == 1
    ));

    let responses = runtime
        .step_batch(vec![
            RustRaftMessage::AppendEntries {
                target: 2,
                request: AppendEntriesRequest {
                    group_id: 77,
                    term: 1,
                    leader_id: 1,
                    prev_log_id: None,
                    entries: vec![matrixraft::RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 1, index: 1 },
                        payload: b"replicated".to_vec(),
                        is_command: true,
                    }],
                    leader_commit: 1,
                    lease_epoch: 0,
                },
            },
            RustRaftMessage::Vote {
                target: 2,
                request: VoteRequest {
                    group_id: 77,
                    term: 2,
                    candidate_id: 3,
                    last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                    pre_vote: true,
                    force: true,
                },
            },
        ])
        .expect("step batch through runtime");
    assert_eq!(responses.len(), 2);
    assert!(matches!(
        &responses[0],
        RustRaftStepResult::AppendEntries(response) if response.success && response.match_index == 1
    ));
    assert!(matches!(
        &responses[1],
        RustRaftStepResult::Vote(response) if response.vote_granted
    ));

    let proposed = runtime
        .step(RustRaftMessage::Propose {
            payload: b"step-membership-1".to_vec(),
            options: RustRaftProposeOptions {
                is_membership_change: true,
                ..Default::default()
            },
        })
        .expect("step proposal through runtime");
    assert!(matches!(
        proposed,
        RustRaftStepResult::Proposed(RustRaftLogId { index: 2, .. })
    ));

    let downgraded = runtime
        .step(RustRaftMessage::Propose {
            payload: b"step-membership-2".to_vec(),
            options: RustRaftProposeOptions {
                is_membership_change: true,
                ..Default::default()
            },
        })
        .expect("step second membership proposal through runtime");
    assert!(matches!(
        downgraded,
        RustRaftStepResult::Proposed(RustRaftLogId { index: 3, .. })
    ));

    let apply_result = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::ApplyResult {
                node_id: 2,
                applied_index: 3,
                rejected: false,
            },
        })
        .expect("step apply-result admin command");
    assert_eq!(apply_result, RustRaftStepResult::Handled);

    let handled = runtime
        .step(RustRaftMessage::AppendEntriesResponse {
            local_node_id: 1,
            peer_id: 2,
            response: AppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 3,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: RustRaftSnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        })
        .expect("step append response through runtime");
    assert_eq!(handled, RustRaftStepResult::Handled);

    let network_error = runtime
        .step(RustRaftMessage::NetworkError { peer_id: 2 })
        .expect("step network-error message through runtime");
    assert_eq!(network_error, RustRaftStepResult::Handled);

    runtime
        .begin_snapshot_send_to(2, "step-snapshot-finish", 3, 1)
        .expect("begin snapshot send");
    let snapshot_finish = runtime
        .step(RustRaftMessage::SnapshotFinish {
            peer_id: 2,
            accepted: true,
            committed_index: 3,
        })
        .expect("step snapshot-finish message through runtime");
    assert_eq!(snapshot_finish, RustRaftStepResult::Handled);

    let follower = runtime
        .status()
        .expect("runtime status")
        .cluster_status
        .expect("cluster status")
        .nodes
        .into_iter()
        .find(|node| node.node_id == 2)
        .expect("follower status");
    assert_eq!(follower.last_log_index, 3);
    assert_eq!(follower.commit_index, 3);
    assert_eq!(follower.applied_index, 3);

    let transferred = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::TransferLeader { target: 2 },
        })
        .expect("step transfer-leader admin command");
    assert_eq!(transferred, RustRaftStepResult::Handled);
    assert_eq!(
        runtime
            .status()
            .expect("runtime status after transfer")
            .cluster_status
            .expect("cluster status after transfer")
            .leader_id,
        Some(2)
    );

    let snapshot = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::TriggerSnapshot,
        })
        .expect("step trigger-snapshot admin command");
    let snapshot_id = match snapshot {
        RustRaftStepResult::SnapshotTriggered(meta) => meta.snapshot_id,
        other => panic!("unexpected snapshot admin response: {other:?}"),
    };
    let snapshot_applied = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::SnapshotApplied { snapshot_id },
        })
        .expect("step snapshot-applied admin command");
    assert_eq!(snapshot_applied, RustRaftStepResult::Handled);
}

#[test]
fn node_runtime_handles_vote_rpc() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let pre_vote = runtime
        .vote_to(
            2,
            VoteRequest {
                group_id: 77,
                term: 2,
                candidate_id: 2,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: true,
                force: false,
            },
        )
        .expect("pre-vote through runtime");
    assert!(pre_vote.vote_granted);
    assert_eq!(pre_vote.reason, "pre_vote_granted");

    let vote = runtime
        .vote_to(
            2,
            VoteRequest {
                group_id: 77,
                term: 2,
                candidate_id: 2,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("vote through runtime");
    assert!(vote.vote_granted);
    assert_eq!(vote.reason, "vote_granted");

    let rejected = runtime
        .vote_to(
            2,
            VoteRequest {
                group_id: 77,
                term: 2,
                candidate_id: 3,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("second vote through runtime");
    assert!(!rejected.vote_granted);
    assert_eq!(rejected.reason, "already_voted");
}

#[test]
fn node_runtime_tracks_and_expires_reorder_queue() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let queued_append = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::ReceiveOutOfOrderAppend {
                peer_id: 2,
                entry: matrixraft::RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 3 },
                    payload: b"future".to_vec(),
                    is_command: true,
                },
            },
        })
        .expect("queue out-of-order append through runtime step");
    assert_eq!(queued_append, RustRaftStepResult::Handled);

    let queued = runtime.peer_pipeline_status(2).expect("pipeline status");
    assert_eq!(queued.reorder_queue_depth, 1);

    let dropped = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::ExpirePeerReorderQueue { peer_id: 2 },
        })
        .expect("expire reorder queue through runtime step");
    assert_eq!(dropped, RustRaftStepResult::CompactedLogs(1));

    let expired = runtime.peer_pipeline_status(2).expect("expired status");
    assert_eq!(expired.reorder_queue_depth, 0);
    assert_eq!(expired.reorder_entry_timeouts, 1);
    assert_eq!(expired.reorder_dropped_packages, 1);
}

#[test]
fn tick_backpressure_caps_pending_ticks() {
    let mut ticks = RustRaftTickBackpressure::new(2);

    let first = ticks.admit_tick();
    assert!(first.accepted);
    assert_eq!(first.pending_ticks, 1);
    assert_eq!(first.reason, "tick_admitted");

    let second = ticks.admit_tick();
    assert!(second.accepted);
    assert_eq!(second.pending_ticks, 2);

    let rejected = ticks.admit_tick();
    assert!(!rejected.accepted);
    assert_eq!(rejected.pending_ticks, 2);
    assert_eq!(rejected.rejected_ticks, 1);
    assert_eq!(rejected.reason, "pending_tick_limit_reached");
    assert_eq!(ticks.accepted_ticks, 2);
    assert_eq!(ticks.rejected_ticks, 1);

    assert!(ticks.complete_tick());
    assert_eq!(ticks.pending_ticks, 1);
    assert_eq!(ticks.completed_ticks, 1);

    let admitted_after_drain = ticks.admit_tick();
    assert!(admitted_after_drain.accepted);
    assert_eq!(admitted_after_drain.pending_ticks, 2);
    assert_eq!(ticks.accepted_ticks, 3);
}

#[test]
fn tick_backpressure_handles_empty_completion_and_reset() {
    let mut ticks = RustRaftTickBackpressure::new(0);
    assert_eq!(ticks.max_pending_ticks, 1);
    assert!(!ticks.complete_tick());

    assert!(ticks.admit_tick().accepted);
    assert!(!ticks.admit_tick().accepted);
    ticks.reset();

    assert_eq!(ticks.pending_ticks, 0);
    assert_eq!(ticks.accepted_ticks, 1);
    assert_eq!(ticks.rejected_ticks, 1);
    assert!(ticks.admit_tick().accepted);
}

#[test]
fn request_timer_watch_cancel_and_notify() {
    let mut timer = RustRaftRequestTimer::new();
    assert_eq!(
        timer.next_timeout_ms(100),
        MATRIXRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS
    );

    assert!(timer.watch(1, 1, 0, 100).is_none());
    assert_eq!(timer.len(), 1);
    assert_eq!(timer.timed_len(), 0);
    assert_eq!(
        timer.next_timeout_ms(100),
        MATRIXRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS
    );

    assert!(timer.watch(1, 2, 110, 100).is_none());
    assert_eq!(timer.timed_len(), 1);
    assert_eq!(timer.next_timeout_ms(100), 10);

    let canceled = timer.cancel(1, 2).expect("cancel timed task");
    assert_eq!(canceled.node_id, 1);
    assert_eq!(canceled.request_id, 2);
    assert_eq!(canceled.deadline_ms, 110);
    assert_eq!(timer.timed_len(), 0);

    let notified = timer.notify(1, 1).expect("notify untimed task");
    assert_eq!(notified.request_id, 1);
    assert!(timer.is_empty());
}

#[test]
fn request_timer_lapses_with_limit_and_removes_node_tasks() {
    let mut timer = RustRaftRequestTimer::new();
    timer.watch(1, 1, 90, 80);
    timer.watch(1, 2, 95, 80);
    timer.watch(2, 3, 120, 100);

    let first = timer.lapsed(100, 1);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].request_id, 1);
    assert_eq!(timer.len(), 2);
    assert_eq!(timer.next_timeout_ms(100), 0);

    let second = timer.lapsed(100, 100);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].request_id, 2);
    assert_eq!(timer.next_timeout_ms(100), 20);

    let removed = timer.remove_node_tasks(2);
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].node_id, 2);
    assert!(timer.is_empty());
    assert_eq!(
        timer.next_timeout_ms(100),
        MATRIXRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS
    );
}

#[test]
fn request_timer_replaces_existing_handler_timeout_index() {
    let mut timer = RustRaftRequestTimer::new();
    assert!(timer.watch(1, 1, 100, 90).is_none());

    let replaced = timer.watch(1, 1, 150, 95).expect("replace task");
    assert_eq!(replaced.deadline_ms, 100);
    assert_eq!(timer.len(), 1);
    assert_eq!(timer.timed_len(), 1);
    assert_eq!(timer.next_timeout_ms(100), 50);

    let lapsed = timer.lapsed(125, 10);
    assert!(lapsed.is_empty());

    let lapsed = timer.lapsed(151, 10);
    assert_eq!(lapsed.len(), 1);
    assert_eq!(lapsed[0].deadline_ms, 150);
    assert!(timer.is_empty());
}

#[test]
fn node_runtime_tracks_snapshot_transfer_progress() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let begin_send = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::BeginSnapshotSend {
                peer_id: 2,
                snapshot_id: "runtime-send-5".to_string(),
                snapshot_index: 5,
                total_chunks: 2,
            },
        })
        .expect("begin snapshot send through runtime step");
    assert_eq!(begin_send, RustRaftStepResult::Handled);
    let sent_chunk = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::RecordSnapshotChunkSent {
                peer_id: 2,
                bytes: 128,
            },
        })
        .expect("record sent chunk through runtime step");
    assert_eq!(sent_chunk, RustRaftStepResult::Handled);
    let retry = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::RetrySnapshotChunk { peer_id: 2 },
        })
        .expect("retry snapshot chunk through runtime step");
    assert_eq!(retry, RustRaftStepResult::Handled);
    let retrying = runtime.peer_pipeline_status(2).expect("retry status");
    assert_eq!(retrying.snapshot_chunk_retry_count, 1);
    assert_eq!(retrying.snapshot_install_progress_per_mille, 0);
    let ack = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::AcknowledgeSnapshotChunk { peer_id: 2 },
        })
        .expect("ack first chunk through runtime step");
    assert_eq!(ack, RustRaftStepResult::Handled);

    let sending = runtime.peer_pipeline_status(2).expect("sender status");
    assert!(sending.snapshot_sending);
    assert_eq!(sending.snapshot_install_progress_per_mille, 500);

    let progress = runtime
        .step(RustRaftMessage::SnapshotProgress {
            peer_id: 2,
            remote_receiving: true,
            elapsed_since_last_receiving_ms: 500,
            send_timeout_ms: 100,
        })
        .expect("step snapshot progress");
    assert_eq!(progress, RustRaftStepResult::Handled);
    assert!(
        runtime
            .peer_pipeline_status(2)
            .expect("progress status")
            .snapshot_sending
    );

    let timed_out = runtime
        .step(RustRaftMessage::SnapshotProgress {
            peer_id: 2,
            remote_receiving: false,
            elapsed_since_last_receiving_ms: 101,
            send_timeout_ms: 100,
        })
        .expect("step snapshot timeout progress");
    assert_eq!(timed_out, RustRaftStepResult::Handled);
    assert!(
        !runtime
            .peer_pipeline_status(2)
            .expect("timeout status")
            .snapshot_sending
    );

    runtime
        .begin_snapshot_send_to(2, "runtime-send-5-retry", 5, 1)
        .expect("restart snapshot send after timeout");

    runtime
        .acknowledge_snapshot_chunk_to(2)
        .expect("ack final chunk");
    let sent = runtime.peer_pipeline_status(2).expect("sent status");
    assert!(!sent.snapshot_sending);
    assert_eq!(sent.snapshot_installed_index, 5);
    assert_eq!(sent.snapshot_install_progress_per_mille, 1000);

    let begin_install = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::BeginSnapshotInstall {
                peer_id: 3,
                snapshot_id: "runtime-recv-7".to_string(),
                snapshot_index: 7,
                total_chunks: 2,
            },
        })
        .expect("begin snapshot receive through runtime step");
    assert_eq!(begin_install, RustRaftStepResult::Handled);
    let first_chunk = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::ReceiveSnapshotChunk {
                peer_id: 3,
                bytes: 64,
                done: false,
            },
        })
        .expect("receive first chunk through runtime step");
    assert_eq!(first_chunk, RustRaftStepResult::Handled);
    let receiving = runtime.peer_pipeline_status(3).expect("receiver status");
    assert!(receiving.snapshot_installing);
    assert_eq!(receiving.snapshot_install_progress_per_mille, 500);

    let final_chunk = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::ReceiveSnapshotChunk {
                peer_id: 3,
                bytes: 64,
                done: true,
            },
        })
        .expect("receive final chunk through runtime step");
    assert_eq!(final_chunk, RustRaftStepResult::Handled);
    let received = runtime.peer_pipeline_status(3).expect("received status");
    assert!(!received.snapshot_installing);
    assert_eq!(received.snapshot_installed_index, 7);
    assert_eq!(received.snapshot_install_progress_per_mille, 1000);

    runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::BeginSnapshotInstall {
                peer_id: 3,
                snapshot_id: "runtime-rollback-9".to_string(),
                snapshot_index: 9,
                total_chunks: 3,
            },
        })
        .expect("begin rollback snapshot receive through runtime step");
    runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::ReceiveSnapshotChunk {
                peer_id: 3,
                bytes: 32,
                done: false,
            },
        })
        .expect("receive rollback chunk through runtime step");
    let rollback = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::RollbackSnapshotInstall { peer_id: 3 },
        })
        .expect("rollback snapshot install through runtime step");
    assert_eq!(rollback, RustRaftStepResult::Handled);
    let rolled_back = runtime.peer_pipeline_status(3).expect("rollback status");
    assert!(!rolled_back.snapshot_installing);
    assert_eq!(rolled_back.snapshot_install_rolled_back, 1);
}

#[test]
fn node_runtime_compacts_logs_through_runtime() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .propose(b"compact-one".to_vec())
        .expect("first write");
    runtime
        .propose(b"compact-two".to_vec())
        .expect("second write");

    let released = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::ReleaseMemory,
        })
        .expect("release memory through runtime step");
    assert_eq!(released, RustRaftStepResult::ReleasedMemory(true));

    let read = runtime.read_index(3).expect("read after compaction");
    assert!(read.safe);
    assert_eq!(read.read_index, 3);
    assert_eq!(
        runtime
            .status()
            .expect("status")
            .cluster_status
            .expect("cluster status")
            .nodes
            .into_iter()
            .find(|node| node.node_id == 1)
            .expect("local node status")
            .last_log_index,
        3
    );
}

#[test]
fn node_runtime_compacts_wal_with_storage_fence() {
    let base_dir = temp_runtime_dir("wal-fenced-compaction");
    let mut options = node_options_in(base_dir.clone());
    options.config.max_segment_bytes = 1;
    options.config.min_keep_segment_num = 1;
    let mut runtime = RaftNodeRuntime::create(options).expect("create runtime");
    runtime.start().expect("start runtime");
    for index in 1..=5 {
        assert_eq!(
            runtime
                .propose(format!("fenced-compaction-{index}").into_bytes())
                .expect("write")
                .index,
            index + 1
        );
    }
    assert!(
        runtime
            .wal_lifecycle_status()
            .expect("WAL status before compaction")
            .segment_count
            > 1
    );

    let blocked = match runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::CompactLogsWithStorageFence {
                log_index: 4,
                fence: RustRaftStorageApplyFence {
                    group_id: 77,
                    node_id: 1,
                    committed_index: 6,
                    applied_index: 6,
                    durable_applied_index: 3,
                    storage_flushed_index: 6,
                    installed_snapshot_index: 0,
                    first_retained_log_index: 1,
                },
            },
        })
        .expect("blocked compaction step")
    {
        RustRaftStepResult::FencedCompaction(report) => report,
        other => panic!("unexpected compaction step result: {other:?}"),
    };
    assert!(!blocked.fence_valid);
    assert_eq!(blocked.released_segments, 0);
    assert!(blocked.blocker.expect("blocker").contains("behind"));

    let released = match runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::CompactLogsWithStorageFence {
                log_index: 4,
                fence: RustRaftStorageApplyFence {
                    group_id: 77,
                    node_id: 1,
                    committed_index: 6,
                    applied_index: 6,
                    durable_applied_index: 5,
                    storage_flushed_index: 5,
                    installed_snapshot_index: 0,
                    first_retained_log_index: 1,
                },
            },
        })
        .expect("safe compaction step")
    {
        RustRaftStepResult::FencedCompaction(report) => report,
        other => panic!("unexpected compaction step result: {other:?}"),
    };
    assert!(released.fence_valid);
    assert!(released.released_segments > 0);
    assert_eq!(released.retained_range.last_log_index, 6);

    let status = runtime
        .wal_lifecycle_status()
        .expect("WAL status after compaction");
    assert!(status.released_segment_count > 0);
    assert_eq!(status.last_log_index, 6);
    let read = runtime.read_index(6).expect("read after fenced compaction");
    assert!(read.safe);
    assert_eq!(read.read_index, 6);
    runtime.shutdown().expect("shutdown runtime");

    let _ = fs::remove_dir_all(base_dir);
}

#[test]
fn node_runtime_checkpoints_snapshot_through_runtime() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .propose(b"checkpoint-me".to_vec())
        .expect("write before checkpoint");

    let snapshot = match runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::CheckpointSnapshot {
                target: 1,
                snapshot_id: "runtime-checkpoint-1".to_string(),
            },
        })
        .expect("checkpoint snapshot through runtime step")
    {
        RustRaftStepResult::CheckpointedSnapshot(snapshot) => snapshot,
        other => panic!("unexpected checkpoint step result: {other:?}"),
    };

    assert_eq!(snapshot.group_id, 77);
    assert_eq!(snapshot.meta.snapshot_id, "runtime-checkpoint-1");
    assert_eq!(snapshot.meta.last_log_id.index, 2);
    assert!(snapshot.meta.membership.contains(&1));
    assert!(snapshot.meta.membership.contains(&2));
    assert!(snapshot.meta.membership.contains(&3));
    assert!(!snapshot.payload.is_empty());
}

#[test]
fn node_runtime_times_out_lagging_leader_transfer() {
    let mut options = node_options();
    options.config.heartbeat_interval_ms = 5;
    options.config.election_timeout_ms = 10;
    options.config.leader_lease_ms = 5;
    let mut runtime = RaftNodeRuntime::create(options).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .set_node_healthy(2, false)
        .expect("make transferee miss append");
    runtime.propose(b"transfer-gap".to_vec()).expect("propose");
    runtime.transfer_leader(2).expect("begin transfer");

    let pending = runtime.status().expect("status");
    assert_eq!(
        pending
            .cluster_status
            .as_ref()
            .and_then(|status| status.leader_transfer.as_ref())
            .map(|transfer| transfer.reason.as_str()),
        Some("waiting_for_transferee_available")
    );

    std::thread::sleep(std::time::Duration::from_millis(25));

    let status = runtime.status().expect("status");
    assert!(status
        .cluster_status
        .as_ref()
        .and_then(|cluster| cluster.leader_transfer.as_ref())
        .is_none());
    assert!(status
        .fatal_blocker_report
        .blockers
        .iter()
        .any(|blocker| blocker.id == "leader_transfer_timeout"));
}

#[test]
fn node_runtime_completes_and_aborts_leader_transfer_lifecycle() {
    let mut completing = RaftNodeRuntime::create(node_options()).expect("create runtime");
    completing.start().expect("start runtime");
    completing
        .set_node_healthy(2, false)
        .expect("make transferee miss append");
    completing
        .propose(b"transfer-gap".to_vec())
        .expect("propose");
    completing
        .set_node_healthy(2, true)
        .expect("restore transferee health");
    completing.transfer_leader(2).expect("begin transfer");

    let pending = completing
        .leader_transfer_state()
        .expect("query pending leader transfer")
        .expect("leader transfer pending");
    assert_eq!(pending.transferee_id, 2);
    assert_eq!(pending.reason, "waiting_for_transferee_catchup");

    completing.catch_up_peer(2).expect("catch up transferee");
    let complete_transfer = completing
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::CompleteLeaderTransfer,
        })
        .expect("complete leader transfer through runtime step");
    assert_eq!(
        complete_transfer,
        RustRaftStepResult::LeaderTransferCompleted(true)
    );
    assert!(completing
        .leader_transfer_state()
        .expect("query completed leader transfer")
        .is_none());
    assert_eq!(
        completing
            .status()
            .expect("status")
            .cluster_status
            .as_ref()
            .and_then(|status| status.leader_id),
        Some(2)
    );
    let no_transfer_left = completing
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::CompleteLeaderTransfer,
        })
        .expect("no transfer left to complete through runtime step");
    assert_eq!(
        no_transfer_left,
        RustRaftStepResult::LeaderTransferCompleted(false)
    );

    let mut aborting = RaftNodeRuntime::create(node_options()).expect("create runtime");
    aborting.start().expect("start runtime");
    aborting
        .set_node_healthy(2, false)
        .expect("make transferee miss append");
    aborting.propose(b"abort-gap".to_vec()).expect("propose");
    aborting
        .set_node_healthy(2, true)
        .expect("restore transferee health");
    aborting.transfer_leader(2).expect("begin transfer");
    let abort_transfer = aborting
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::AbortLeaderTransfer {
                reason: "operator_abort".to_string(),
            },
        })
        .expect("abort transfer through runtime step");
    assert_eq!(
        abort_transfer,
        RustRaftStepResult::LeaderTransferAborted(true)
    );
    assert!(aborting
        .leader_transfer_state()
        .expect("query aborted transfer")
        .is_none());
    assert_eq!(
        aborting
            .status()
            .expect("status")
            .cluster_status
            .as_ref()
            .and_then(|status| status.leader_id),
        Some(1)
    );
    let no_transfer_left = aborting
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::AbortLeaderTransfer {
                reason: "operator_abort".to_string(),
            },
        })
        .expect("no transfer left to abort through runtime step");
    assert_eq!(
        no_transfer_left,
        RustRaftStepResult::LeaderTransferAborted(false)
    );

    let mut removing = RaftNodeRuntime::create(node_options()).expect("create runtime");
    removing.start().expect("start runtime");
    removing
        .set_node_healthy(2, false)
        .expect("make transferee miss append");
    removing
        .propose(b"remove-transfer-gap".to_vec())
        .expect("propose");
    removing
        .set_node_healthy(2, true)
        .expect("restore transferee health");
    removing.transfer_leader(2).expect("begin transfer");
    assert!(removing
        .leader_transfer_state()
        .expect("query removing transfer")
        .is_some());
    removing
        .execute_membership_operation(RaftMembershipOperation::Remove(2))
        .expect("remove transferee");
    assert!(removing
        .leader_transfer_state()
        .expect("query removed transfer")
        .is_none());
}

#[test]
fn node_runtime_catches_up_recovered_follower_on_heartbeat() {
    let mut options = node_options();
    options.config.heartbeat_interval_ms = 5;
    options.config.election_timeout_ms = 20;
    options.config.leader_lease_ms = 5;
    let mut runtime = RaftNodeRuntime::create(options).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .set_node_healthy(2, false)
        .expect("make follower miss append");
    runtime.propose(b"catch-up-me".to_vec()).expect("propose");

    let lagging = runtime.status().expect("status");
    assert!(lagging
        .peer_runtime
        .iter()
        .any(|peer| peer.node_id == 2 && peer.lag > 0));

    runtime.set_node_healthy(2, true).expect("restore follower");
    std::thread::sleep(std::time::Duration::from_millis(20));

    let caught_up = runtime.status().expect("status");
    let peer = caught_up
        .peer_runtime
        .iter()
        .find(|peer| peer.node_id == 2)
        .expect("peer 2 status");
    assert_eq!(peer.lag, 0);
    assert_eq!(peer.matched, 2);
}

#[test]
fn node_runtime_partitions_and_heals_peer_with_catchup_report() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    let partition = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::PartitionPeer { peer_id: 2 },
        })
        .expect("partition peer through runtime step");
    assert_eq!(partition, RustRaftStepResult::Handled);
    runtime
        .propose(b"partitioned-write".to_vec())
        .expect("propose");

    let partitioned = runtime.status().expect("partitioned status");
    let peer = partitioned
        .peer_runtime
        .iter()
        .find(|peer| peer.node_id == 2)
        .expect("peer 2 status");
    assert!(!peer.healthy);
    assert!(peer.lag > 0);

    let catchup = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::HealPeer { peer_id: 2 },
        })
        .expect("heal peer through runtime step");
    let RustRaftStepResult::CatchUpPeer(catchup) = catchup else {
        panic!("unexpected heal peer step response: {catchup:?}");
    };
    assert_eq!(catchup.learner_id, 2);
    assert!(catchup.caught_up);
    assert_eq!(catchup.learner_match_index_after, 2);
    assert_eq!(catchup.reason, "healed_peer_caught_up");

    let healed = runtime.status().expect("healed status");
    let peer = healed
        .peer_runtime
        .iter()
        .find(|peer| peer.node_id == 2)
        .expect("peer 2 status");
    assert!(peer.healthy);
    assert_eq!(peer.lag, 0);
    assert_eq!(peer.matched, 2);

    let read = runtime
        .bounded_stale_read_index(2, 0)
        .expect("bounded stale read after heal catchup");
    assert!(read.safe);
    assert_eq!(read.read_index, 2);
}

#[test]
fn node_runtime_triggers_snapshot_metadata() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"snapshot-me".to_vec()).expect("propose");

    let snapshot = runtime.trigger_snapshot().expect("trigger snapshot");
    assert_eq!(snapshot.last_log_id.index, 2);
    assert!(snapshot.membership.contains(&1));
    assert!(snapshot.snapshot_id.ends_with("-2"));
    let status = runtime.status().expect("status");
    assert!(status.snapshot_trigger_status.in_progress);
    assert_eq!(
        status.snapshot_trigger_status.snapshot_id.as_deref(),
        Some(snapshot.snapshot_id.as_str())
    );
    assert_eq!(status.snapshot_trigger_status.duplicate_requests, 0);
    assert_eq!(status.snapshot_trigger_status.elapsed_ticks, 0);
    assert!(status.snapshot_trigger_status.timeout_ticks > 0);

    let duplicate = runtime
        .trigger_snapshot()
        .expect("duplicate trigger returns in-progress snapshot");
    assert_eq!(duplicate, snapshot);
    assert_eq!(
        runtime
            .status()
            .expect("status")
            .snapshot_trigger_status
            .duplicate_requests,
        1
    );

    let ready = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::SnapshotReady {
                snapshot_id: snapshot.snapshot_id.clone(),
                success: false,
            },
        })
        .expect("step snapshot-ready admin command");
    assert_eq!(ready, RustRaftStepResult::Handled);
    assert!(
        !runtime
            .status()
            .expect("status")
            .snapshot_trigger_status
            .in_progress
    );
    runtime
        .trigger_snapshot()
        .expect("trigger again after completion");
    let stale_ready = runtime
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::SnapshotReady {
                snapshot_id: "stale-ready-callback".to_string(),
                success: true,
            },
        })
        .expect("stale snapshot-ready callback is ignored");
    assert_eq!(stale_ready, RustRaftStepResult::Handled);
    assert!(
        runtime
            .status()
            .expect("status")
            .snapshot_trigger_status
            .in_progress
    );
}

#[test]
fn node_runtime_reports_stale_snapshot_trigger_timeout() {
    let mut options = node_options();
    options.config.heartbeat_interval_ms = 5;
    options.config.election_timeout_ms = 10;
    options.config.leader_lease_ms = 5;
    let mut runtime = RaftNodeRuntime::create(options).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"slow-snapshot".to_vec()).expect("propose");
    runtime.trigger_snapshot().expect("trigger snapshot");

    std::thread::sleep(std::time::Duration::from_millis(25));

    let status = runtime.status().expect("status");
    assert!(status.snapshot_trigger_status.in_progress);
    assert!(status.snapshot_trigger_status.elapsed_ticks >= 2);
    assert!(status.snapshot_trigger_status.timed_out);
    assert!(status
        .fatal_blocker_report
        .blockers
        .iter()
        .any(|blocker| blocker.id.starts_with("snapshot_trigger_timeout:")));
}

#[test]
fn node_runtime_shutdown_is_idempotent() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.shutdown().expect("shutdown runtime");
    runtime.shutdown().expect("second shutdown is ok");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Shutdown);
}

#[test]
fn node_runtime_recovers_committed_index_from_persistent_wal() {
    let base_dir = temp_runtime_dir("wal-recovery");
    let options = node_options_in(base_dir.clone());
    {
        let mut runtime = RaftNodeRuntime::create(options.clone()).expect("create runtime");
        runtime.start().expect("start runtime");
        assert_eq!(runtime.propose(b"one".to_vec()).expect("first").index, 2);
        assert_eq!(runtime.propose(b"two".to_vec()).expect("second").index, 3);
        let wal_status = runtime
            .wal_lifecycle_status()
            .expect("query WAL lifecycle status");
        assert_eq!(wal_status.last_log_index, 3);
        assert!(wal_status.total_records >= 2);
        assert_eq!(
            runtime
                .status()
                .expect("runtime status")
                .wal_lifecycle_status
                .expect("status includes WAL lifecycle")
                .last_log_index,
            3
        );
        runtime.shutdown().expect("shutdown");
    }

    let mut recovered = RaftNodeRuntime::create(options).expect("recreate runtime");
    recovered.start().expect("start recovered runtime");
    let recovery = recovered
        .wal_recovery_report()
        .expect("query WAL recovery report")
        .expect("startup recovery report");
    assert!(recovery.recovered.is_some());
    assert_eq!(recovery.surviving_records, 2);
    assert_eq!(recovery.removed_records, 0);
    let recovered_status = recovered.status().expect("recovered status");
    assert_eq!(
        recovered_status
            .wal_recovery_report
            .expect("status includes recovery report")
            .surviving_records,
        2
    );
    assert_eq!(
        recovered_status
            .wal_lifecycle_status
            .expect("status includes WAL lifecycle")
            .last_log_index,
        3
    );
    let read = recovered.read_index(3).expect("read recovered index");
    assert!(!read.safe);
    assert_eq!(read.read_index, 4);
    assert_eq!(read.reason, "applied_index_behind_read_index");
    assert_eq!(
        recovered
            .propose(b"three".to_vec())
            .expect("post recovery write")
            .index,
        5
    );
    recovered.shutdown().expect("shutdown recovered");

    let _ = fs::remove_dir_all(base_dir);
}
