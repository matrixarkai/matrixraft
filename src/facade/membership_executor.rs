// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// membership operation executor and validation helpers.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MembershipOperation {
    AddNode(Peer),
    AddVoter(Peer),
    AddLearner(Peer),
    AddWitness(Peer),
    Promote(NodeId),
    Remove(NodeId),
    TransferLeader(NodeId),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipExecutionReport {
    pub operation: MembershipOperation,
    pub before: Membership,
    pub after: Membership,
    pub leader_before: Option<NodeId>,
    pub leader_after: Option<NodeId>,
    pub success: bool,
    pub reason: String,
    #[serde(default)]
    pub validation_passed: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
    #[serde(default)]
    pub rolled_back: bool,
    #[serde(default)]
    pub joint_consensus: Option<JointConsensusMembership>,
    #[serde(default)]
    pub joint_consensus_commit: Option<JointConsensusCommitEvidence>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipExecutor {
    reports: Vec<MembershipExecutionReport>,
}

impl MembershipExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn execute(
        &mut self,
        cluster: &mut RaftCluster,
        operation: MembershipOperation,
    ) -> Result<MembershipExecutionReport, RaftError> {
        self.execute_inner(cluster, operation, false)
    }

    fn execute_inner(
        &mut self,
        cluster: &mut RaftCluster,
        operation: MembershipOperation,
        rollback_on_failure: bool,
    ) -> Result<MembershipExecutionReport, RaftError> {
        let cluster_before = rollback_on_failure.then(|| cluster.clone());
        let before = cluster.membership();
        let leader_before = cluster.leader_id();
        let blockers = self.validate(cluster, &operation);
        let validation_passed = blockers.is_empty();
        let mut applied = false;
        let result = if !validation_passed {
            Err(RaftError::InvalidRequest(blockers.join("; ")))
        } else {
            applied = true;
            match operation.clone() {
                MembershipOperation::AddNode(peer) => cluster
                    .apply_committed_membership_operation(MembershipOperation::AddNode(peer))
                    .map(|_| ()),
                MembershipOperation::AddVoter(mut peer) => {
                    peer.role = ReplicaRole::Voter;
                    cluster
                        .apply_committed_membership_operation(MembershipOperation::AddVoter(
                            peer,
                        ))
                        .map(|_| ())
                }
                MembershipOperation::AddLearner(peer) => cluster.add_learner(peer),
                MembershipOperation::AddWitness(peer) => cluster.add_witness(peer),
                MembershipOperation::Promote(node_id) => {
                    match cluster.learner_catch_up_loop(node_id) {
                        Ok(catchup) if catchup.caught_up => match cluster.catchup_report(node_id) {
                            Ok(catchup) if catchup.promotable => cluster.promote_peer(node_id),
                            Ok(catchup) => Err(RaftError::InvalidRequest(format!(
                                "node {} cannot be promoted: {}",
                                node_id, catchup.reason
                            ))),
                            Err(error) => Err(error),
                        },
                        Ok(catchup) => Err(RaftError::InvalidRequest(format!(
                            "node {} cannot be promoted: {}",
                            node_id, catchup.reason
                        ))),
                        Err(error) => Err(error),
                    }
                }
                MembershipOperation::Remove(node_id) => cluster.remove_peer(node_id),
                MembershipOperation::TransferLeader(node_id) => {
                    cluster.transfer_leader(node_id)
                }
            }
        };
        let success = result.is_ok();
        let mut rolled_back = false;
        if !success && applied && rollback_on_failure {
            if let Some(cluster_before) = cluster_before {
                *cluster = cluster_before;
                rolled_back = true;
            }
        }
        let reason = match &result {
            Ok(()) => "membership_operation_applied".to_string(),
            Err(error) if rolled_back => format!("{error}; rolled_back"),
            Err(error) => error.to_string(),
        };
        let after = cluster.membership();
        let joint_consensus = membership_joint_if_voters_changed(&before, &after);
        let joint_consensus_commit = joint_consensus
            .as_ref()
            .filter(|_| success)
            .map(|joint| joint.commit_evidence(joint_acknowledgements(&before, &after)));
        let report = MembershipExecutionReport {
            operation,
            joint_consensus,
            joint_consensus_commit,
            before,
            after,
            leader_before,
            leader_after: cluster.leader_id(),
            success,
            reason,
            validation_passed,
            blockers,
            rolled_back,
        };
        self.reports.push(report.clone());
        result.map(|_| report)
    }

    pub fn execute_all<I>(
        &mut self,
        cluster: &mut RaftCluster,
        operations: I,
    ) -> Result<Vec<MembershipExecutionReport>, RaftError>
    where
        I: IntoIterator<Item = MembershipOperation>,
    {
        let mut reports = Vec::new();
        for operation in operations {
            reports.push(self.execute(cluster, operation)?);
        }
        Ok(reports)
    }

    pub fn execute_all_with_rollback<I>(
        &mut self,
        cluster: &mut RaftCluster,
        operations: I,
    ) -> Result<Vec<MembershipExecutionReport>, RaftError>
    where
        I: IntoIterator<Item = MembershipOperation>,
    {
        let workflow_before = cluster.clone();
        let mut reports = Vec::new();
        for operation in operations {
            match self.execute_inner(cluster, operation, true) {
                Ok(report) => reports.push(report),
                Err(error) => {
                    *cluster = workflow_before;
                    if let Some(report) = self.reports.last_mut() {
                        if !report.success {
                            report.rolled_back = true;
                            if !report.reason.contains("rolled_back") {
                                report.reason = format!("{}; rolled_back", report.reason);
                            }
                        }
                    }
                    return Err(error);
                }
            }
        }
        Ok(reports)
    }

    pub fn validate(
        &self,
        cluster: &RaftCluster,
        operation: &MembershipOperation,
    ) -> Vec<String> {
        let membership = cluster.membership();
        let mut blockers = Vec::new();
        match operation {
            MembershipOperation::AddNode(peer) => {
                if !(peer.role == ReplicaRole::Voter
                    && membership.learners.contains(&peer.node_id))
                {
                    validate_peer_absent(&membership, peer.node_id, &mut blockers);
                }
            }
            MembershipOperation::AddVoter(peer) => {
                if !membership.learners.contains(&peer.node_id) {
                    validate_peer_absent(&membership, peer.node_id, &mut blockers);
                }
            }
            MembershipOperation::AddLearner(peer) => {
                validate_peer_absent(&membership, peer.node_id, &mut blockers);
            }
            MembershipOperation::AddWitness(peer) => {
                validate_peer_absent(&membership, peer.node_id, &mut blockers);
            }
            MembershipOperation::Promote(node_id) => {
                if !membership.learners.contains(node_id) {
                    blockers.push(format!("node_{node_id}_is_not_learner"));
                }
            }
            MembershipOperation::Remove(node_id) => {
                if !membership.voters.contains(node_id)
                    && !membership.learners.contains(node_id)
                    && !membership.witnesses.contains(node_id)
                {
                    blockers.push(format!("node_{node_id}_not_member"));
                }
                if membership.voters.contains(node_id) && membership.voters.len() <= 1 {
                    blockers.push("cannot_remove_last_voter".to_string());
                }
                if cluster.leader_id() == Some(*node_id) && cluster.closest_follower().is_none() {
                    blockers.push("cannot_remove_current_leader_without_transfer".to_string());
                }
            }
            MembershipOperation::TransferLeader(node_id) => match cluster.nodes.get(node_id) {
                Some(node) if !node.replica_role.can_be_leader() => {
                    blockers.push(format!("node_{node_id}_cannot_be_leader"));
                }
                Some(node) if node.match_index() < cluster.commit_index => {
                    blockers.push(format!("node_{node_id}_is_lagging"));
                }
                Some(_) => {}
                None => blockers.push(format!("node_{node_id}_not_found")),
            },
        }
        blockers
    }

    pub fn reports(&self) -> &[MembershipExecutionReport] {
        &self.reports
    }
}

fn validate_peer_absent(
    membership: &Membership,
    node_id: NodeId,
    blockers: &mut Vec<String>,
) {
    if membership.voters.contains(&node_id)
        || membership.learners.contains(&node_id)
        || membership.witnesses.contains(&node_id)
    {
        blockers.push(format!("node_{node_id}_already_member"));
    }
}

fn membership_joint_if_voters_changed(
    before: &Membership,
    after: &Membership,
) -> Option<JointConsensusMembership> {
    (before.voters != after.voters).then(|| JointConsensusMembership {
        old_voters: before.voters.clone(),
        new_voters: after.voters.clone(),
    })
}

fn joint_acknowledgements(
    before: &Membership,
    after: &Membership,
) -> Vec<NodeId> {
    before
        .voters
        .iter()
        .chain(after.voters.iter())
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}
