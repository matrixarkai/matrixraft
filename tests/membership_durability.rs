// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A removed peer stays removed across a restart.
//!
//! Reporting a membership change as applied is a promise, and it was the third
//! one this engine made without writing a record. `MembershipExecutor` has no
//! WAL at all -- the word does not appear in the file -- and
//! `runtime_step_message` handed `Message::Membership` straight to it.
//!
//! Measured before the fix:
//!
//! ```text
//! before: nodes=[1, 2, 3]
//! remove(3) -> success: true, after: voters [1, 2], joint_quorum_reached: true
//! after remove: nodes=[1, 2]
//! after restart: nodes=[1, 2, 3]     <-- node 3 is back
//! recovered voters = None            <-- no record was written
//! ```
//!
//! A removed peer that rejoins the quorum breaks the intersection the
//! configuration change exists to preserve: two quorums drawn from the two
//! configurations can then commit without overlapping.
//!
//! The sharper part is that recovery was already right. It deliberately prefers
//! the record's membership over the configured peers, because a restart that
//! took the configured list resurrected removed peers -- which is the same
//! defect from the other side. The record simply never received the new
//! membership, so recovery faithfully restored a stale one.
//!
//! **Both membership paths.** The single step and the rollback workflow each
//! apply changes, and the workflow applies several at once -- one record covers
//! it, because a record describes the membership as it now stands.

use matrixraft::{
    Config, MembershipOperation, Message, NodeOptions, NodeRuntime, Peer, ReplicaRole,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 251;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 51_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 51_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn base_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-membership-dur-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn options(base: &Path) -> NodeOptions {
    NodeOptions {
        group_id: GROUP_ID,
        node_id: 1,
        raft_addr: "127.0.0.1:51001".to_string(),
        snapshot_addr: "127.0.0.1:51501".to_string(),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config: Config::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

/// The node ids the group reports, sorted.
fn node_ids(runtime: &NodeRuntime) -> Vec<u64> {
    let status = runtime.status().expect("status");
    let mut ids: Vec<u64> = status
        .cluster_status
        .as_ref()
        .map(|cluster| cluster.nodes.iter().map(|node| node.node_id).collect())
        .unwrap_or_default();
    ids.sort_unstable();
    ids
}

#[test]
fn a_removed_peer_does_not_come_back_on_restart() {
    let base = base_dir("removed");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create the group");
        runtime.start().expect("start it");
        assert_eq!(
            node_ids(&runtime),
            vec![1, 2, 3],
            "the premise: three peers to start with"
        );
        runtime
            .step(Message::Membership {
                operation: MembershipOperation::Remove(3),
            })
            .expect("the removal is applied");
        assert_eq!(
            node_ids(&runtime),
            vec![1, 2],
            "and it took effect in memory"
        );
        runtime.shutdown().expect("shut it down");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create the group");
    restarted.start().expect("re-start it");
    assert_eq!(
        node_ids(&restarted),
        vec![1, 2],
        "a peer removed by a committed change must not rejoin the quorum when \
         the node restarts, whatever the configured peer list says"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn the_record_carries_the_new_membership() {
    // Ties the behaviour above to the record it came from, so the test cannot
    // pass by the group happening to re-derive the same answer.
    let base = base_dir("record");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        runtime
            .step(Message::Membership {
                operation: MembershipOperation::Remove(3),
            })
            .expect("applied");
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    let report = restarted
        .wal_recovery_report()
        .expect("a recovery report")
        .expect("the group recovered something");
    let voters = report
        .recovered
        .as_ref()
        .map(|record| record.membership.voters.clone())
        .expect("a recovered record");
    assert_eq!(
        voters,
        vec![1, 2],
        "the record has to carry the membership as it now stands: {report:?}"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn an_added_learner_survives_a_restart_too() {
    // The other direction, so the fix cannot be "write a record when a peer is
    // removed". An addition is as much a committed change as a removal.
    let base = base_dir("added");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        let mut learner = peer(4);
        learner.role = ReplicaRole::Learner;
        runtime
            .step(Message::Membership {
                operation: MembershipOperation::AddLearner(learner),
            })
            .expect("the learner is added");
        assert_eq!(node_ids(&runtime), vec![1, 2, 3, 4]);
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    assert_eq!(
        node_ids(&restarted),
        vec![1, 2, 3, 4],
        "an added peer has to survive as well as a removed one"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_group_whose_membership_never_changed_writes_nothing() {
    // The positive control. A node that wrote a record on every step, or on
    // start, would satisfy the tests above whatever the membership path did.
    let base = base_dir("unchanged");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    let surviving = restarted
        .wal_recovery_report()
        .expect("a recovery report")
        .map(|report| report.surviving_records)
        .unwrap_or(0);
    assert_eq!(
        surviving, 0,
        "so the records above came from the membership change and not from \
         starting up"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_rollback_workflow_persists_what_it_applied() {
    // The workflow is a second entry point with its own condition, so fixing
    // only the single step would leave an operator driving the workflow with a
    // membership that evaporates.
    let base = base_dir("workflow");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        let reports = runtime
            .execute_membership_workflow_with_rollback(vec![MembershipOperation::Remove(3)])
            .expect("the workflow runs");
        assert!(
            reports.iter().any(|report| report.success),
            "the premise: the workflow applied something: {reports:?}"
        );
        assert_eq!(node_ids(&runtime), vec![1, 2]);
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    assert_eq!(
        node_ids(&restarted),
        vec![1, 2],
        "a workflow's change has to be as durable as a single step's"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
