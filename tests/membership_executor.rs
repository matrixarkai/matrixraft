// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    AdminCommand, ApplySnapshotFence, Config, LogId, MembershipExecutor, MembershipOperation,
    Message, Peer, RaftCluster, RaftSnapshot, ReplicaRole, SnapshotMetadata, StepResult,
};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 18_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 19_000 + node_id),
        role,
        auto_promote: false,
    }
}

#[test]
fn membership_executor_runs_full_runtime_workflow() {
    let mut cluster = RaftCluster::new(
        66,
        Default::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"a".to_vec()).expect("write");

    let mut executor = MembershipExecutor::new();
    executor
        .execute(
            &mut cluster,
            MembershipOperation::AddLearner(peer(4, ReplicaRole::Voter)),
        )
        .expect("add learner");
    assert!(cluster.membership().learners.contains(&4));

    let promoted = executor
        .execute(
            &mut cluster,
            MembershipOperation::AddVoter(peer(4, ReplicaRole::Voter)),
        )
        .expect("add-voter config change promotes existing learner");
    assert!(promoted.success);
    assert!(promoted.joint_consensus.is_some());
    let promoted_commit = promoted
        .joint_consensus_commit
        .as_ref()
        .expect("joint commit evidence");
    assert_eq!(promoted_commit.old_quorum_size, 2);
    assert_eq!(promoted_commit.new_quorum_size, 3);
    assert!(promoted_commit.old_majority_acked);
    assert!(promoted_commit.new_majority_acked);
    assert!(promoted_commit.joint_quorum_reached);

    let reports = executor
        .execute_all(
            &mut cluster,
            vec![
                MembershipOperation::AddWitness(peer(5, ReplicaRole::Voter)),
                MembershipOperation::TransferLeader(4),
                MembershipOperation::Remove(2),
            ],
        )
        .expect("execute workflow");

    assert_eq!(reports.len(), 3);
    assert!(reports.iter().all(|report| report.success));
    assert_eq!(cluster.leader_id(), Some(4));
    let membership = cluster.membership();
    assert!(membership.voters.contains(&4));
    assert!(membership.witnesses.contains(&5));
    assert!(!membership.voters.contains(&2));
    assert_eq!(executor.reports().len(), 5);
}

#[test]
fn membership_executor_validates_reports_joint_changes_and_rolls_back() {
    let mut cluster = RaftCluster::new(
        67,
        Default::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"a".to_vec()).expect("write");

    let mut executor = MembershipExecutor::new();
    let add_voter = executor
        .execute(
            &mut cluster,
            MembershipOperation::AddVoter(peer(4, ReplicaRole::Learner)),
        )
        .expect("add voter");
    assert!(add_voter.validation_passed);
    assert!(add_voter.success);
    assert!(add_voter.joint_consensus.is_some());
    assert!(
        add_voter
            .joint_consensus_commit
            .as_ref()
            .expect("add voter joint commit evidence")
            .joint_quorum_reached
    );
    assert!(cluster.membership().voters.contains(&4));

    let remove_leader = executor
        .execute(&mut cluster, MembershipOperation::Remove(1))
        .expect("leader removal transfers to closest follower");
    assert!(remove_leader.validation_passed);
    assert!(remove_leader.success);
    let remove_commit = remove_leader
        .joint_consensus_commit
        .as_ref()
        .expect("remove voter joint commit evidence");
    assert_eq!(remove_commit.old_quorum_size, 3);
    assert_eq!(remove_commit.new_quorum_size, 2);
    assert!(remove_commit.joint_quorum_reached);
    assert_eq!(remove_leader.leader_before, Some(1));
    assert_eq!(remove_leader.leader_after, Some(2));
    assert!(!cluster.membership().voters.contains(&1));
    assert_eq!(cluster.leader_id(), Some(2));

    let voters_before = cluster.membership().voters;
    let rollback = executor.execute_all_with_rollback(
        &mut cluster,
        vec![
            MembershipOperation::AddWitness(peer(5, ReplicaRole::Witness)),
            MembershipOperation::Remove(99),
        ],
    );
    assert!(rollback.is_err());
    assert_eq!(cluster.membership().voters, voters_before);
    assert_eq!(cluster.leader_id(), Some(2));
    let rollback_report = executor.reports().last().expect("rollback report");
    assert!(rollback_report.rolled_back);
    assert!(rollback_report.reason.contains("rolled_back"));
}

#[test]
fn committed_membership_apply_is_idempotent_like_baseline_raft_replay() {
    let mut cluster = RaftCluster::new(
        68,
        Default::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");

    assert!(cluster
        .add_peer(peer(2, ReplicaRole::Voter))
        .expect_err("strict operator add rejects duplicate")
        .to_string()
        .contains("duplicate raft node id"));

    assert!(!cluster
        .apply_committed_membership_operation(MembershipOperation::AddVoter(peer(
            2,
            ReplicaRole::Voter,
        )))
        .expect("duplicate committed add is skipped"));

    cluster
        .remove_peer(99)
        .expect("missing remove is skipped like MatrixRaft");
    assert!(!cluster
        .apply_committed_membership_operation(MembershipOperation::Remove(99))
        .expect("duplicate committed remove is skipped"));

    assert!(cluster
        .apply_committed_membership_operation(MembershipOperation::AddLearner(peer(
            4,
            ReplicaRole::Voter,
        )))
        .expect("committed learner add"));
    assert!(cluster.membership().learners.contains(&4));

    let mut executor = MembershipExecutor::new();
    let add_node_promote = executor
        .execute(
            &mut cluster,
            MembershipOperation::AddNode(peer(4, ReplicaRole::Voter)),
        )
        .expect("add-node config change promotes existing learner");
    assert!(add_node_promote.success);
    assert!(add_node_promote.joint_consensus.is_some());
    assert!(cluster.membership().voters.contains(&4));
    assert!(!cluster.membership().learners.contains(&4));

    assert!(cluster
        .apply_committed_membership_operation(MembershipOperation::AddLearner(peer(
            5,
            ReplicaRole::Voter,
        )))
        .expect("second committed learner add"));

    assert!(cluster
        .apply_committed_membership_operation(MembershipOperation::AddVoter(peer(
            5,
            ReplicaRole::Learner,
        )))
        .expect("committed add voter promotes existing learner"));
    let membership = cluster.membership();
    assert!(membership.voters.contains(&5));
    assert!(!membership.learners.contains(&5));

    assert!(!cluster
        .apply_committed_membership_operation(MembershipOperation::Remove(99))
        .expect("missing committed remove is skipped"));
}

#[test]
fn pending_membership_change_fence_matches_baseline_raft_config_change_rule() {
    let mut cluster = RaftCluster::new(
        69,
        Default::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");

    cluster
        .begin_pending_membership_change(5)
        .expect("first pending membership change");
    assert_eq!(cluster.pending_membership_change_index(), Some(5));
    assert!(cluster
        .begin_pending_membership_change(6)
        .expect_err("second unapplied membership change is rejected")
        .to_string()
        .contains("pending_membership_change_index:5"));

    cluster.mark_membership_change_applied(5);
    assert_eq!(cluster.pending_membership_change_index(), None);
    cluster
        .begin_pending_membership_change(6)
        .expect("new change after apply");
    assert_eq!(cluster.pending_membership_change_index(), Some(6));

    cluster.reset_pending_membership_change_after_truncation(4, 0);
    assert_eq!(cluster.pending_membership_change_index(), None);
}

#[test]
fn saving_membership_change_blocks_next_config_until_stabled() {
    let mut cluster = RaftCluster::new(
        70,
        Default::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");

    cluster
        .begin_saving_membership_change(5)
        .expect("start saving first config change");
    assert_eq!(cluster.pending_membership_change_index(), Some(5));
    assert_eq!(cluster.saving_membership_change_index(), Some(5));

    assert!(cluster
        .begin_pending_membership_change(6)
        .expect_err("second config waits for metadata stability")
        .to_string()
        .contains("saving_membership_change_index:5"));

    cluster.mark_membership_change_applied(5);
    assert_eq!(
        cluster.pending_membership_change_index(),
        Some(5),
        "apply alone must not clear an unstabled membership change"
    );
    assert!(cluster
        .begin_pending_membership_change(6)
        .expect_err("still blocked until stabled")
        .to_string()
        .contains("saving_membership_change_index:5"));

    let stabled = cluster
        .step(Message::Admin {
            command: AdminCommand::StabledResult {
                first_index: None,
                last_index: None,
                stabled_membership_change_index: 5,
            },
        })
        .expect("stable first config change through step");
    assert_eq!(stabled, StepResult::Handled);
    assert_eq!(cluster.saving_membership_change_index(), None);
    assert_eq!(cluster.stabled_membership_change_index(), 5);
    assert_eq!(cluster.pending_membership_change_index(), None);
    cluster
        .begin_pending_membership_change(6)
        .expect("new config allowed after metadata is stabled");
    assert_eq!(cluster.pending_membership_change_index(), Some(6));
}

#[test]
fn snapshot_applied_clears_covered_membership_change() {
    let mut cluster = RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("cluster starts");

    cluster
        .begin_saving_membership_change(5)
        .expect("membership change enters saving state");
    assert_eq!(cluster.pending_membership_change_index(), Some(5));
    assert_eq!(cluster.saving_membership_change_index(), Some(5));

    cluster
        .install_snapshot_to(
            2,
            RaftSnapshot {
                group_id: 7,
                meta: SnapshotMetadata {
                    snapshot_id: "membership-floor-5".to_string(),
                    last_log_id: LogId { term: 1, index: 5 },
                    membership: vec![1, 2, 3],
                    members: Vec::new(),
                },
                payload: b"membership snapshot".to_vec(),
            },
            ApplySnapshotFence {
                applied_index: 5,
                commit_index: 5,
                installed_snapshot_index: 5,
                first_retained_log_index: 6,
            },
        )
        .expect("snapshot applies membership floor");

    assert_eq!(cluster.pending_membership_change_index(), None);
    assert_eq!(cluster.saving_membership_change_index(), None);
    assert_eq!(cluster.stabled_membership_change_index(), 5);
    cluster
        .begin_pending_membership_change(6)
        .expect("new membership change is allowed after snapshot floor");
    assert_eq!(cluster.pending_membership_change_index(), Some(6));
}
