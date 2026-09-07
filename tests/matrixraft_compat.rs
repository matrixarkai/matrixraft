// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    Config, LearnerAutoPromoteState, MatrixRaftAttribute, MatrixRaftConfState, MatrixRaftNode,
    MatrixRaftNodeId, MatrixRaftProposeOptions, MatrixRaftReadIndexMode,
    MatrixRaftReadIndexOptions, NodeOptions, NodeRuntimeState, Peer, RaftError, ReplicaRole,
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
        "matrixraft-matrixraft-compat-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 41_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 42_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn node_id(peer_id: u64) -> MatrixRaftNodeId {
    MatrixRaftNodeId {
        peer_id,
        raft_addr: format!("127.0.0.1:{}", 41_000 + peer_id),
        snapshot_addr: format!("127.0.0.1:{}", 42_000 + peer_id),
    }
}

fn assert_invalid_request_contains(result: Result<(), RaftError>, expected: &str) {
    match result {
        Err(RaftError::InvalidRequest(message)) => assert!(
            message.contains(expected),
            "expected invalid request containing {expected:?}, got {message:?}"
        ),
        other => panic!("expected invalid request containing {expected:?}, got {other:?}"),
    }
}

fn assert_propose_invalid_request_contains(
    result: Result<matrixraft::LogId, RaftError>,
    expected: &str,
) {
    match result {
        Err(RaftError::InvalidRequest(message)) => assert!(
            message.contains(expected),
            "expected invalid request containing {expected:?}, got {message:?}"
        ),
        other => panic!("expected invalid request containing {expected:?}, got {other:?}"),
    }
}

#[test]
fn matrixraft_facade_exposes_step_down_and_resign_admin_shape() {
    let wal_dir = temp_dir("step-down-wal");
    let snapshot_dir = temp_dir("step-down-snapshot");
    let options = NodeOptions {
        group_id: 504,
        node_id: 1,
        raft_addr: "127.0.0.1:41001".to_string(),
        snapshot_addr: "127.0.0.1:42001".to_string(),
        wal_dir: wal_dir.display().to_string(),
        snapshot_dir: snapshot_dir.display().to_string(),
        role: ReplicaRole::Voter,
        config: Config {
            heartbeat_interval_ms: 5,
            election_timeout_ms: 20,
            leader_lease_ms: 10,
            ..Default::default()
        },
        peers: vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    };

    let mut node = MatrixRaftNode::create(options, 1).expect("create node");
    node.start(1).expect("start");
    let step_down = node.step_down(Some(2)).expect("step down to peer 2");
    assert!(step_down.stepped_down);
    assert_eq!(step_down.requested_transferee_id, Some(2));
    assert_eq!(step_down.transferee_id, Some(2));
    assert_eq!(
        step_down
            .transferee_node
            .as_ref()
            .expect("transferee node")
            .peer_id,
        2
    );
    assert_eq!(node.leader().expect("leader after step down"), Some(2));

    let resign = node
        .resign_leader("operator_resign")
        .expect("resign leader");
    assert!(resign.resigned);
    assert_eq!(resign.reason, "operator_resign");
    assert_eq!(resign.leader_before, Some(2));
    assert_eq!(resign.leader_after, None);

    node.shutdown().expect("shutdown");
    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}

#[test]
fn matrixraft_facade_exposes_snapshot_peer_lifecycle_shape() {
    let wal_dir = temp_dir("snapshot-peer-wal");
    let snapshot_dir = temp_dir("snapshot-peer-snapshot");
    let options = NodeOptions {
        group_id: 505,
        node_id: 1,
        raft_addr: "127.0.0.1:41001".to_string(),
        snapshot_addr: "127.0.0.1:42001".to_string(),
        wal_dir: wal_dir.display().to_string(),
        snapshot_dir: snapshot_dir.display().to_string(),
        role: ReplicaRole::Voter,
        config: Config {
            heartbeat_interval_ms: 5,
            election_timeout_ms: 20,
            leader_lease_ms: 10,
            ..Default::default()
        },
        peers: vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    };

    let mut node = MatrixRaftNode::create(options, 1).expect("create node");
    node.start(1).expect("start");
    let sending = node
        .begin_snapshot_send_to(2, "matrixraft-send-12", 12, 2)
        .expect("begin snapshot send");
    assert_eq!(sending.peer_id, 2);
    assert!(sending.status.snapshot_sending);
    assert_eq!(sending.status.snapshot_install_total_chunks, 2);
    let sent = node
        .record_snapshot_chunk_sent_to(2, 8)
        .expect("record sent chunk");
    assert!(sent.status.snapshot_sending);
    assert_eq!(sent.status.snapshot_send_attempts, 1);
    let retry = node
        .retry_snapshot_chunk_to(2)
        .expect("retry snapshot chunk");
    assert_eq!(retry.status.snapshot_chunk_retry_count, 1);
    let canceled = node
        .cancel_snapshot_send_to(2)
        .expect("cancel snapshot send");
    assert!(!canceled.status.snapshot_sending);

    let installing = node
        .begin_snapshot_install_from(2, "matrixraft-install-13", 13, 2)
        .expect("begin snapshot install");
    assert!(installing.status.snapshot_installing);
    let receiving = node
        .receive_snapshot_chunk_from(2, 8, false)
        .expect("receive snapshot chunk");
    assert!(receiving.status.snapshot_installing);
    assert_eq!(receiving.status.snapshot_install_progress_per_mille, 500);
    let rolled_back = node
        .rollback_snapshot_install_from(2)
        .expect("rollback snapshot install");
    assert!(!rolled_back.status.snapshot_installing);
    assert_eq!(rolled_back.status.snapshot_install_rolled_back, 1);

    node.shutdown().expect("shutdown");
    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}

#[test]
fn matrixraft_facade_exposes_node_lifecycle_and_admin_shape() {
    let wal_dir = temp_dir("wal");
    let snapshot_dir = temp_dir("snapshot");
    let options = NodeOptions {
        group_id: 501,
        node_id: 1,
        raft_addr: "127.0.0.1:41001".to_string(),
        snapshot_addr: "127.0.0.1:42001".to_string(),
        wal_dir: wal_dir.display().to_string(),
        snapshot_dir: snapshot_dir.display().to_string(),
        role: ReplicaRole::Voter,
        // Intervals far longer than this test suppress the runtime's automatic
        // tick, which fires from the timeout arm of its command loop. With a
        // 5ms heartbeat, a tick could land between `set_leader_lease_valid(false)`
        // and the status read that checks it took effect, renewing the lease and
        // failing the assertion -- roughly once in eighty runs under 2x CPU
        // load. This test drives every state change it asserts on, so it does
        // not need the automatic tick at all.
        config: Config {
            heartbeat_interval_ms: 10_000,
            election_timeout_ms: 20_000,
            leader_lease_ms: 5_000,
            ..Default::default()
        },
        peers: vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    };

    let mut node = MatrixRaftNode::create(options, 7).expect("create matrixraft node");
    assert_eq!(node.start_index(), 7);
    node.start(9).expect("start");
    assert_eq!(node.start_index(), 9);
    assert_eq!(
        node.get_local_status().expect("local status").state,
        NodeRuntimeState::Running
    );

    assert_eq!(node.leader().expect("leader"), Some(1));
    assert_eq!(node.leader_node().expect("leader node").unwrap().peer_id, 1);
    node.set_leader_lease_valid(true).expect("set lease");
    let status = node.get_status().expect("leased status");
    assert!(node.in_lease(Some(status.term)).expect("lease"));
    assert!(!node.in_lease(Some(status.term + 1)).expect("lease term"));
    assert!(status.leader_lease_valid);
    node.set_leader_lease_valid(false).expect("expire lease");
    let expired_status = node.get_status().expect("expired status");
    assert!(!expired_status.leader_lease_valid);
    assert!(!node.in_lease(Some(status.term)).expect("expired lease"));
    node.set_leader_lease_valid(true).expect("restore lease");
    assert!(node.in_lease(Some(status.term)).expect("restored lease"));

    let log_id = node
        .propose_with_options(
            MatrixRaftProposeOptions {
                with_term: Some(status.term),
                is_command: true,
            },
            b"compat-write".to_vec(),
        )
        .expect("propose with options");
    assert!(log_id.index > 0);
    let legacy_read = node.read_index(log_id.index).expect("read index");
    assert!(legacy_read.safe);
    assert!(legacy_read.lease_read);
    let lease_read = node
        .lease_read_index(log_id.index)
        .expect("lease read index");
    assert!(lease_read.safe);
    assert!(lease_read.lease_read);
    assert_eq!(lease_read.reason, "lease_read");
    let quorum_read = node
        .quorum_read_index(log_id.index)
        .expect("quorum read index");
    assert!(quorum_read.safe);
    assert!(!quorum_read.lease_read);
    assert_eq!(quorum_read.reason, "read_index");
    let explicit_quorum = node
        .read_index_with_options(MatrixRaftReadIndexOptions {
            min_commit_index: log_id.index,
            mode: MatrixRaftReadIndexMode::QuorumRead,
        })
        .expect("explicit quorum read");
    assert!(explicit_quorum.safe);
    assert!(!explicit_quorum.lease_read);

    // A node inside a follower lease refuses to campaign:
    // `InvalidRequest("follower is still in leader lease")`. That lease used to
    // be left to lapse on its own, so this line depended on how many automatic
    // ticks the runtime had managed to fire -- and the automatic tick only
    // fires when the command channel has been idle for a heartbeat interval, so
    // a test issuing commands back to back starves it. Under 2x CPU load this
    // failed about once in twenty runs. Expire the lease explicitly instead.
    node.tick_follower_lease(1_000)
        .expect("expire follower lease before campaign");
    node.campaign().expect("campaign");
    node.forced_campaign().expect("forced campaign");
    node.transfer_leader(2).expect("transfer leader");
    let ready_snapshot = node.async_snapshot().expect("async snapshot ready");
    assert!(ready_snapshot.last_log_id.index > 0);
    node.async_snapshot_ready(&ready_snapshot.snapshot_id, true)
        .expect("snapshot ready");
    let applied_snapshot = node.async_snapshot().expect("async snapshot applied");
    node.async_snapshot_applied(&applied_snapshot.snapshot_id)
        .expect("snapshot applied");

    let learner = node.add_learner(node_id(4), false).expect("add learner");
    assert!(learner.success);
    assert_eq!(node.resolve_address(4).expect("resolve learner").peer_id, 4);
    let promoted = node
        .promote_after_catch_up(4)
        .expect("promote learner after catch-up");
    assert_eq!(promoted.learner_id, 4);
    assert!(promoted.catch_up.caught_up);
    assert!(promoted.promoted);
    assert!(promoted.membership.success);
    assert!(!node
        .get_status()
        .expect("status")
        .membership
        .learners
        .contains(&4));
    let auto_learner = node
        .add_learner(node_id(6), true)
        .expect("add auto-promote learner");
    assert!(auto_learner.success);
    let auto_promoted = node.auto_promote_learner(6).expect("auto-promote learner");
    assert_eq!(auto_promoted.learner_id, 6);
    assert!(auto_promoted.auto_promote);
    assert!(auto_promoted.promoted);
    assert_eq!(auto_promoted.state_after, LearnerAutoPromoteState::Promoted);
    assert_eq!(auto_promoted.reason, "learner_promoted");
    assert_eq!(
        node.get_membership_members()
            .expect("membership members")
            .iter()
            .find(|member| member.id == 6)
            .expect("auto-promoted member")
            .conf_state,
        MatrixRaftConfState::Voter
    );
    let witness = node.add_witness(node_id(5)).expect("add witness");
    assert!(witness.success);
    let removed = node.remove_node_with_report(5).expect("remove witness");
    assert!(removed.removed);
    assert_eq!(removed.removed_id, 5);
    assert_eq!(
        removed.removed_node.expect("removed node address").peer_id,
        5
    );
    assert_eq!(
        removed.removed_conf_state,
        Some(MatrixRaftConfState::Witness)
    );
    assert!(removed.membership.success);

    node.alter_attribute(MatrixRaftAttribute::IgnoreWitness, true)
        .expect("ignore witness");
    node.alter_attribute(MatrixRaftAttribute::ProhibitsElection, true)
        .expect("prohibits election");
    assert!(node.get_fatal_events().expect("fatal events").is_empty());
    let _ = node
        .fire_fatal_event(1, "disk_failure")
        .expect("fire fatal event");
    let fatal_events = node.get_fatal_events().expect("fatal events");
    assert_eq!(fatal_events.len(), 1);
    assert_eq!(fatal_events[0].node_id, Some(1));
    assert_eq!(fatal_events[0].reason, "disk_failure");
    assert_eq!(fatal_events[0].raw_id, "fatal_event:1:disk_failure");
    assert_eq!(node.get_fatal_blockers().expect("fatal blockers").len(), 1);

    node.restart(true).expect("restart");
    assert!(node.recover_fsm_from_snapshot());
    assert_eq!(
        node.get_local_status().expect("restarted").state,
        NodeRuntimeState::Running
    );
    node.stop().expect("stop");
    node.shutdown().expect("shutdown");

    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}

#[test]
fn matrixraft_facade_enforces_witness_and_learner_semantic_edges() {
    let wal_dir = temp_dir("semantic-wal");
    let snapshot_dir = temp_dir("semantic-snapshot");
    let options = NodeOptions {
        group_id: 502,
        node_id: 1,
        raft_addr: "127.0.0.1:41001".to_string(),
        snapshot_addr: "127.0.0.1:42001".to_string(),
        wal_dir: wal_dir.display().to_string(),
        snapshot_dir: snapshot_dir.display().to_string(),
        role: ReplicaRole::Voter,
        config: Config {
            heartbeat_interval_ms: 5,
            election_timeout_ms: 20,
            leader_lease_ms: 10,
            ..Default::default()
        },
        peers: vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Learner),
            peer(4, ReplicaRole::Witness),
        ],
    };

    let mut node = MatrixRaftNode::create(options, 1).expect("create node");
    node.start(1).expect("start");
    let membership = node.get_membership_members().expect("membership members");
    assert_eq!(
        membership
            .iter()
            .find(|member| member.id == 3)
            .expect("learner member")
            .conf_state,
        MatrixRaftConfState::Learner
    );
    assert_eq!(
        membership
            .iter()
            .find(|member| member.id == 4)
            .expect("witness member")
            .conf_state,
        MatrixRaftConfState::Witness
    );
    let flat_membership = node.get_membership().expect("flat membership");
    assert!(flat_membership.iter().any(|member| member.peer_id == 3));
    assert!(flat_membership.iter().any(|member| member.peer_id == 4));
    node.transfer_leader(2).expect("voter transfer target");
    assert_invalid_request_contains(node.transfer_leader(3), "must be voter");
    assert_invalid_request_contains(node.transfer_leader(4), "must be voter");

    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);

    let witness_wal_dir = temp_dir("witness-wal");
    let witness_snapshot_dir = temp_dir("witness-snapshot");
    let witness_options = NodeOptions {
        group_id: 503,
        node_id: 4,
        raft_addr: "127.0.0.1:41004".to_string(),
        snapshot_addr: "127.0.0.1:42004".to_string(),
        wal_dir: witness_wal_dir.display().to_string(),
        snapshot_dir: witness_snapshot_dir.display().to_string(),
        role: ReplicaRole::Witness,
        config: Config {
            heartbeat_interval_ms: 5,
            election_timeout_ms: 20,
            leader_lease_ms: 10,
            ..Default::default()
        },
        peers: vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
            peer(4, ReplicaRole::Witness),
        ],
    };

    let mut witness_node = MatrixRaftNode::create(witness_options, 1).expect("create witness node");
    witness_node.start(1).expect("start witness");
    assert_propose_invalid_request_contains(
        witness_node.propose_with_options(
            MatrixRaftProposeOptions {
                with_term: None,
                is_command: true,
            },
            b"normal-command".to_vec(),
        ),
        "witness node ignores normal command proposals",
    );

    let _ = fs::remove_dir_all(witness_wal_dir);
    let _ = fs::remove_dir_all(witness_snapshot_dir);
}
