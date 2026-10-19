// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A command entry waits for the committed index that covers it to be durable.
//!
//! A command entry is the shape a host uses for an operation it must be able
//! to replay on restart. Replay is driven by the committed index, so applying
//! a command before that index is durable means a crash leaves the node
//! restarting with a lower committed index and **applying the command twice**.
//! For a non-idempotent command that is a corruption the state machine cannot
//! see coming.
//!
//! `is_command` existed on every entry and was read only to classify entries
//! for reporting. The comparable engine reaches the same conclusion the same
//! way -- a committed command entry sets `should_delay_apply` alongside
//! `should_flush_meta`, holding the apply task until the committed index has
//! been written.

use matrixraft::{
    matrixraft_fsm_entry_kind, Config, FsmApplyEntryKind, LogEntry, LogId, Peer, ProposeOptions,
    RaftCluster, RaftError, ReplicaRole,
};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 81_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 82_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started() -> (RaftCluster, u64) {
    let mut cluster =
        RaftCluster::new(7, Config::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    let leader = cluster.leader_id().expect("leader");
    (cluster, leader)
}

fn propose_plain(cluster: &mut RaftCluster, payload: &[u8]) -> u64 {
    cluster
        .propose_with_options(
            payload.to_vec(),
            ProposeOptions {
                expected_term: None,
                is_command: false,
                is_membership_change: false,
            },
        )
        .expect("propose a non-command entry")
        .index
}

fn propose_command(cluster: &mut RaftCluster, payload: &[u8]) -> u64 {
    cluster
        .propose(payload.to_vec())
        .expect("propose a command entry")
        .index
}

#[test]
fn a_command_entry_is_held_back_until_its_commit_index_is_durable() {
    let (mut cluster, leader) = started();
    let plain = propose_plain(&mut cluster, b"not a command");
    let command = propose_command(&mut cluster, b"a command");
    let after = propose_command(&mut cluster, b"another command");
    let commit_index = cluster.status(leader).expect("status").commit_index;
    assert!(
        commit_index >= after,
        "all three should be committed, commit={commit_index} last={after}"
    );

    // Nothing durable: the plain entry is still eligible, the command is not.
    let nothing = cluster
        .apply_eligible_through(leader, 0)
        .expect("eligibility");
    assert!(nothing.is_held_back());
    assert_eq!(
        nothing.held_back_at,
        Some(command),
        "the first command entry is what holds it"
    );
    assert_eq!(
        nothing.eligible_through,
        command - 1,
        "everything before the command stays eligible, so a plain entry is not \
         stalled by a command behind it"
    );
    assert!(
        nothing.eligible_through >= plain,
        "the plain entry at {plain} should be applicable"
    );

    // Durable up to the command: it becomes eligible, and the next command
    // takes over as the thing holding the line.
    let partway = cluster
        .apply_eligible_through(leader, command)
        .expect("eligibility");
    assert_eq!(partway.held_back_at, Some(after));
    assert_eq!(partway.eligible_through, after - 1);

    // Durable through the end: nothing is held back.
    let all = cluster
        .apply_eligible_through(leader, commit_index)
        .expect("eligibility");
    assert!(!all.is_held_back());
    assert_eq!(all.eligible_through, commit_index);
    assert_eq!(all.commit_index, commit_index);
    assert_eq!(all.durable_commit_index, commit_index);
}

#[test]
fn a_log_without_commands_is_never_held_back() {
    // The cost of this for a host that writes no command entries: none. It
    // gets the commit index every time, whatever it says is durable.
    let (mut cluster, leader) = started();
    propose_plain(&mut cluster, b"one");
    propose_plain(&mut cluster, b"two");
    let commit_index = cluster.status(leader).expect("status").commit_index;

    for durable in [0, 1, commit_index] {
        let eligibility = cluster
            .apply_eligible_through(leader, durable)
            .expect("eligibility");
        assert!(
            !eligibility.is_held_back(),
            "a log of plain entries must not hold back at durable={durable}"
        );
        assert_eq!(eligibility.eligible_through, commit_index);
    }
}

#[test]
fn the_fenced_mark_refuses_to_apply_past_what_is_durable() {
    let (mut cluster, leader) = started();
    propose_plain(&mut cluster, b"not a command");
    let command = propose_command(&mut cluster, b"a command");
    let commit_index = cluster.status(leader).expect("status").commit_index;

    // Applying through the command with nothing durable is refused, and the
    // message says which entry and what it needs.
    let refused = cluster.mark_apply_task_inflight_with_fence(leader, commit_index, 0);
    let message = match refused {
        Err(RaftError::InvalidRequest(message)) => message,
        other => panic!("applying past a command should be refused, got {other:?}"),
    };
    assert!(
        message.contains(&command.to_string()),
        "the refusal should name the command entry, got {message}"
    );

    // Applying only up to the eligible index is **not** reachable in this
    // model, and the reason is worth pinning rather than working around: a
    // `RaftCluster` holds every replica and advances its own applied index, so
    // the pre-existing validation refuses a lower one.
    let eligible = cluster
        .apply_eligible_through(leader, 0)
        .expect("eligibility")
        .eligible_through;
    let below = cluster.mark_apply_task_inflight_with_fence(leader, eligible, 0);
    match below {
        Err(RaftError::InvalidRequest(message)) => assert!(
            message.contains("below safety applied index"),
            "expected the pre-existing lower bound, got {message}"
        ),
        other => panic!("expected the model's own applied index to refuse, got {other:?}"),
    }

    // Which is the honest shape of this: the fence is for a host driving its
    // own apply, where the applied index is the host's to advance. What it
    // adds here is the upper bound -- and once the commit index is durable,
    // that bound is gone.
    cluster
        .mark_apply_task_inflight_with_fence(leader, commit_index, commit_index)
        .expect("a durable commit index releases the command");
}

#[test]
fn the_unchecked_mark_is_unchanged() {
    // A host whose commands are idempotent, or which writes none, pays
    // nothing: the original call still applies whatever the commit index
    // allows, with no fence and no durability argument.
    let (mut cluster, leader) = started();
    propose_command(&mut cluster, b"a command");
    let commit_index = cluster.status(leader).expect("status").commit_index;
    cluster
        .mark_apply_task_inflight(leader, commit_index)
        .expect("the unchecked call still applies through the commit index");
}

#[test]
fn a_leader_no_op_is_not_a_command() {
    // Found while building the fence above: a leader's election no-op was
    // written with `is_command: true`, so the fence held the apply at index 1
    // of every started cluster -- and `FsmApplyEntryKind::NoOp` was a variant
    // the actual no-op never reached.
    //
    // A no-op carries nothing a host has to replay, so it needs no replay
    // protection and should not look like a command to a state machine.
    // Changing it left all 753 other tests passing, which is the evidence that
    // nothing depended on the old classification.
    let no_op = LogEntry {
        log_id: LogId { term: 1, index: 1 },
        payload: b"no-op".to_vec(),
        is_command: false,
    };
    let command = LogEntry {
        log_id: LogId { term: 1, index: 2 },
        payload: b"a command".to_vec(),
        is_command: true,
    };
    assert_eq!(matrixraft_fsm_entry_kind(&no_op), FsmApplyEntryKind::NoOp);
    assert_eq!(matrixraft_fsm_entry_kind(&command), FsmApplyEntryKind::Data);

    // And the one a started cluster actually writes: with nothing durable, a
    // cluster that has only campaigned holds back nothing, because its single
    // entry is the no-op.
    let (cluster, leader) = started();
    let eligibility = cluster
        .apply_eligible_through(leader, 0)
        .expect("eligibility");
    assert!(
        !eligibility.is_held_back(),
        "a freshly started cluster has only its no-op, which is not a command"
    );
}
