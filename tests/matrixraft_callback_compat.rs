// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    Config, MatrixRaftAsyncOperation, MatrixRaftAsyncResult, MatrixRaftCallbackScheduler,
    MatrixRaftConfState, MatrixRaftNode, MatrixRaftNodeId, MatrixRaftProposeOptions, NodeOptions,
    NodeRuntimeState, Peer, ReplicaRole,
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
        "matrixraft-matrixraft-callback-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 52_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 53_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn node_id(peer_id: u64) -> MatrixRaftNodeId {
    MatrixRaftNodeId {
        peer_id,
        raft_addr: format!("127.0.0.1:{}", 52_000 + peer_id),
        snapshot_addr: format!("127.0.0.1:{}", 53_000 + peer_id),
    }
}

fn assert_callback(
    returned: &MatrixRaftAsyncResult,
    callback: &MatrixRaftAsyncResult,
    operation: MatrixRaftAsyncOperation,
) {
    assert_eq!(returned, callback);
    assert_eq!(returned.operation, operation);
    assert!(returned.ok);
    assert!(!returned.timed_out);
}

#[test]
fn matrixraft_callback_scheduler_exposes_scheduled_timeout_and_cancellation_shape() {
    let mut scheduler = MatrixRaftCallbackScheduler::new();
    assert!(scheduler.is_empty());
    assert!(scheduler
        .schedule(1, 10, MatrixRaftAsyncOperation::Propose, 100, 50)
        .is_none());
    assert_eq!(scheduler.len(), 1);
    assert_eq!(scheduler.timed_len(), 1);
    assert_eq!(scheduler.next_timeout_ms(100), 50);

    let replaced = scheduler
        .schedule(1, 10, MatrixRaftAsyncOperation::ReadIndex, 110, 70)
        .expect("replaced scheduled callback");
    assert_eq!(replaced.operation, MatrixRaftAsyncOperation::Propose);
    assert_eq!(replaced.task.request_id, 10);
    assert_eq!(scheduler.next_timeout_ms(110), 70);

    let completed = scheduler.complete(1, 10).expect("complete callback");
    assert_eq!(completed.operation, MatrixRaftAsyncOperation::ReadIndex);
    let completed_result = completed.completed_result();
    assert!(completed_result.ok);
    assert!(!completed_result.timed_out);
    assert_eq!(completed_result.node_id, Some(1));
    assert_eq!(completed_result.request_id, Some(10));
    assert_eq!(completed_result.deadline_ms, Some(180));
    assert!(scheduler.is_empty());

    scheduler.schedule(2, 20, MatrixRaftAsyncOperation::TransferLeader, 200, 25);
    scheduler.schedule(2, 21, MatrixRaftAsyncOperation::Campaign, 200, 0);
    assert_eq!(scheduler.timed_len(), 1);
    let timeouts = scheduler.lapsed(226, 10);
    assert_eq!(timeouts.len(), 1);
    assert_eq!(
        timeouts[0].operation,
        MatrixRaftAsyncOperation::TransferLeader
    );
    assert!(timeouts[0].timed_out);
    assert_eq!(timeouts[0].node_id, Some(2));
    assert_eq!(timeouts[0].request_id, Some(20));
    assert_eq!(timeouts[0].deadline_ms, Some(225));

    let canceled = scheduler.cancel(2, 21).expect("cancel untimed callback");
    assert_eq!(canceled.operation, MatrixRaftAsyncOperation::Campaign);
    assert_eq!(canceled.task.deadline_ms, 0);
    assert!(scheduler.is_empty());
}

#[test]
fn matrixraft_callback_facade_invokes_step_down_and_resign_admin_callbacks() {
    let wal_dir = temp_dir("admin-wal");
    let snapshot_dir = temp_dir("admin-snapshot");
    let options = NodeOptions {
        group_id: 902,
        node_id: 1,
        raft_addr: "127.0.0.1:52001".to_string(),
        snapshot_addr: "127.0.0.1:53001".to_string(),
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

    let mut step_down_callback = None;
    let step_down =
        node.step_down_callback(Some(2), |result| step_down_callback = Some(result), 1_000);
    assert_callback(
        &step_down,
        step_down_callback.as_ref().expect("step-down callback"),
        MatrixRaftAsyncOperation::StepDown,
    );
    let step_down_report = step_down.step_down.as_ref().expect("step-down report");
    assert_eq!(step_down_report.requested_transferee_id, Some(2));
    assert_eq!(step_down_report.transferee_id, Some(2));
    assert_eq!(
        step_down_report
            .transferee_node
            .as_ref()
            .expect("transferee node")
            .peer_id,
        2
    );

    let mut resign_callback = None;
    let resign = node.resign_leader_callback(
        "operator_resign",
        |result| resign_callback = Some(result),
        1_000,
    );
    assert_callback(
        &resign,
        resign_callback.as_ref().expect("resign callback"),
        MatrixRaftAsyncOperation::ResignLeader,
    );
    let resign_report = resign.resign.as_ref().expect("resign report");
    assert_eq!(resign_report.reason, "operator_resign");
    assert_eq!(resign_report.leader_before, Some(2));
    assert_eq!(resign_report.leader_after, None);
    assert!(resign_report.resigned);

    node.shutdown().expect("shutdown");
    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}

#[test]
fn matrixraft_callback_facade_invokes_timeout_shaped_callbacks_for_node_operations() {
    let wal_dir = temp_dir("wal");
    let snapshot_dir = temp_dir("snapshot");
    let options = NodeOptions {
        group_id: 901,
        node_id: 1,
        raft_addr: "127.0.0.1:52001".to_string(),
        snapshot_addr: "127.0.0.1:53001".to_string(),
        wal_dir: wal_dir.display().to_string(),
        snapshot_dir: snapshot_dir.display().to_string(),
        role: ReplicaRole::Voter,
        // A 20ms election timeout meant that when the runtime thread was
        // starved, the node churned through elections between the operations
        // below and an operation could come back not-ok -- `assert!(returned.ok)`
        // failed about 13 runs in 60 under 2x CPU load. Intervals longer than
        // the test suppress the automatic tick, so leadership stays put. This
        // test is about callback delivery, not about election timing.
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

    let mut node = MatrixRaftNode::create(options, 1).expect("create node");
    node.start(1).expect("start");
    assert_eq!(
        node.get_local_status().expect("status").state,
        NodeRuntimeState::Running
    );

    let mut propose_callback = None;
    let propose = node.propose_with_options_callback(
        MatrixRaftProposeOptions {
            with_term: None,
            is_command: true,
        },
        b"callback-write".to_vec(),
        |result| propose_callback = Some(result),
        1_000,
    );
    assert_callback(
        &propose,
        propose_callback.as_ref().expect("propose callback"),
        MatrixRaftAsyncOperation::Propose,
    );
    assert!(propose.log_id.is_some());
    assert_eq!(propose.node_id, Some(1));
    assert_eq!(propose.request_id, Some(1));
    assert!(propose.deadline_ms.is_some());
    assert_eq!(node.callback_scheduler_len(), 0);

    let mut read_callback = None;
    let read = node.read_index_callback(|result| read_callback = Some(result), 1_000);
    assert_callback(
        &read,
        read_callback.as_ref().expect("read callback"),
        MatrixRaftAsyncOperation::ReadIndex,
    );
    assert!(read.read_index.as_ref().expect("read index").safe);
    assert!(read.read_index.as_ref().expect("read index").lease_read);
    assert_eq!(read.node_id, Some(1));
    assert_eq!(read.request_id, Some(2));
    assert!(read.deadline_ms >= propose.deadline_ms);

    let mut quorum_read_callback = None;
    let quorum_read =
        node.quorum_read_index_callback(1, |result| quorum_read_callback = Some(result), 1_000);
    assert_callback(
        &quorum_read,
        quorum_read_callback.as_ref().expect("quorum read callback"),
        MatrixRaftAsyncOperation::ReadIndex,
    );
    let quorum_response = quorum_read.read_index.as_ref().expect("quorum read index");
    assert!(quorum_response.safe);
    assert!(!quorum_response.lease_read);

    let mut learner_callback = None;
    let learner = node.add_learner_callback(
        node_id(4),
        false,
        |result| learner_callback = Some(result),
        1_000,
    );
    assert_callback(
        &learner,
        learner_callback.as_ref().expect("learner callback"),
        MatrixRaftAsyncOperation::AddLearner,
    );
    assert!(learner.membership.as_ref().expect("membership").success);

    let mut promote_callback = None;
    let promoted =
        node.promote_callback(node_id(4), |result| promote_callback = Some(result), 1_000);
    assert_callback(
        &promoted,
        promote_callback.as_ref().expect("promote callback"),
        MatrixRaftAsyncOperation::Promote,
    );

    node.add_learner(node_id(6), true)
        .expect("add auto-promote learner");
    let mut auto_promote_callback = None;
    let auto_promoted =
        node.auto_promote_learner_callback(6, |result| auto_promote_callback = Some(result), 1_000);
    assert_callback(
        &auto_promoted,
        auto_promote_callback
            .as_ref()
            .expect("auto-promote callback"),
        MatrixRaftAsyncOperation::AutoPromoteLearner,
    );
    assert!(
        auto_promoted
            .auto_promote
            .as_ref()
            .expect("auto-promote report")
            .promoted
    );

    let mut witness_callback = None;
    let witness =
        node.add_witness_callback(node_id(5), |result| witness_callback = Some(result), 1_000);
    assert_callback(
        &witness,
        witness_callback.as_ref().expect("witness callback"),
        MatrixRaftAsyncOperation::AddWitness,
    );

    let mut remove_callback = None;
    let removed =
        node.remove_node_callback(node_id(5), |result| remove_callback = Some(result), 1_000);
    assert_callback(
        &removed,
        remove_callback.as_ref().expect("remove callback"),
        MatrixRaftAsyncOperation::RemoveNode,
    );
    let remove_report = removed.remove.as_ref().expect("remove report");
    assert_eq!(remove_report.removed_id, 5);
    assert_eq!(
        remove_report
            .removed_node
            .as_ref()
            .expect("removed node")
            .peer_id,
        5
    );
    assert_eq!(
        remove_report.removed_conf_state,
        Some(MatrixRaftConfState::Witness)
    );
    assert!(removed.membership.as_ref().expect("membership").success);

    let mut campaign_callback = None;
    let campaign = node.campaign_callback(|result| campaign_callback = Some(result), 1_000);
    assert_callback(
        &campaign,
        campaign_callback.as_ref().expect("campaign callback"),
        MatrixRaftAsyncOperation::Campaign,
    );

    let mut forced_callback = None;
    let forced = node.forced_campaign_callback(|result| forced_callback = Some(result), 1_000);
    assert_callback(
        &forced,
        forced_callback.as_ref().expect("forced callback"),
        MatrixRaftAsyncOperation::ForcedCampaign,
    );

    let mut timeout_now_callback = None;
    let timeout_now =
        node.timeout_now_callback(1, 2, |result| timeout_now_callback = Some(result), 1_000);
    assert_callback(
        &timeout_now,
        timeout_now_callback.as_ref().expect("timeout-now callback"),
        MatrixRaftAsyncOperation::TimeoutNow,
    );
    let timeout_now_response = timeout_now
        .timeout_now
        .as_ref()
        .expect("timeout-now response");
    assert_eq!(timeout_now_response.from, 1);
    assert_eq!(timeout_now_response.node_id, 2);
    assert!(timeout_now_response.campaigned);
    assert_eq!(timeout_now_response.reason, "timeout_now_campaign");

    let mut transfer_callback = None;
    let transfer =
        node.transfer_leader_callback(3, |result| transfer_callback = Some(result), 1_000);
    assert_callback(
        &transfer,
        transfer_callback.as_ref().expect("transfer callback"),
        MatrixRaftAsyncOperation::TransferLeader,
    );
    let transfer_report = transfer.transfer_leader.as_ref().expect("transfer report");
    assert_eq!(transfer_report.transferee_id, 3);
    assert_eq!(
        transfer_report
            .transferee_node
            .as_ref()
            .expect("transferee node")
            .peer_id,
        3
    );
    assert!(transfer_report.transferred);

    let mut snapshot_callback = None;
    let snapshot = node.async_snapshot_callback(|result| snapshot_callback = Some(result));
    assert_callback(
        &snapshot,
        snapshot_callback.as_ref().expect("snapshot callback"),
        MatrixRaftAsyncOperation::AsyncSnapshot,
    );
    assert!(snapshot.snapshot.is_some());

    node.shutdown().expect("shutdown");
    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}
