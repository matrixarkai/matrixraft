// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// membership operation executor and validation helpers.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftMembershipOperation {
    AddNode(RustRaftPeer),
    AddVoter(RustRaftPeer),
    AddLearner(RustRaftPeer),
    AddWitness(RustRaftPeer),
    Promote(RustRaftNodeId),
    Remove(RustRaftNodeId),
    TransferLeader(RustRaftNodeId),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipExecutionReport {
    pub operation: RaftMembershipOperation,
    pub before: RaftMembership,
    pub after: RaftMembership,
    pub leader_before: Option<RustRaftNodeId>,
    pub leader_after: Option<RustRaftNodeId>,
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
    pub joint_consensus_commit: Option<RustRaftJointConsensusCommitEvidence>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipExecutor {
    reports: Vec<RaftMembershipExecutionReport>,
}

impl RaftMembershipExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn execute(
        &mut self,
        cluster: &mut RaftCluster,
        operation: RaftMembershipOperation,
    ) -> Result<RaftMembershipExecutionReport, RaftError> {
        self.execute_inner(cluster, operation, false)
    }

    fn execute_inner(
        &mut self,
        cluster: &mut RaftCluster,
        operation: RaftMembershipOperation,
        rollback_on_failure: bool,
    ) -> Result<RaftMembershipExecutionReport, RaftError> {
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
                RaftMembershipOperation::AddNode(peer) => cluster
                    .apply_committed_membership_operation(RaftMembershipOperation::AddNode(peer))
                    .map(|_| ()),
                RaftMembershipOperation::AddVoter(mut peer) => {
                    peer.role = RustRaftReplicaRole::Voter;
                    cluster
                        .apply_committed_membership_operation(RaftMembershipOperation::AddVoter(
                            peer,
                        ))
                        .map(|_| ())
                }
                RaftMembershipOperation::AddLearner(peer) => cluster.add_learner(peer),
                RaftMembershipOperation::AddWitness(peer) => cluster.add_witness(peer),
                RaftMembershipOperation::Promote(node_id) => {
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
                RaftMembershipOperation::Remove(node_id) => cluster.remove_peer(node_id),
                RaftMembershipOperation::TransferLeader(node_id) => {
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
        let report = RaftMembershipExecutionReport {
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
    ) -> Result<Vec<RaftMembershipExecutionReport>, RaftError>
    where
        I: IntoIterator<Item = RaftMembershipOperation>,
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
    ) -> Result<Vec<RaftMembershipExecutionReport>, RaftError>
    where
        I: IntoIterator<Item = RaftMembershipOperation>,
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
        operation: &RaftMembershipOperation,
    ) -> Vec<String> {
        let membership = cluster.membership();
        let mut blockers = Vec::new();
        match operation {
            RaftMembershipOperation::AddNode(peer) => {
                if !(peer.role == RustRaftReplicaRole::Voter
                    && membership.learners.contains(&peer.node_id))
                {
                    validate_peer_absent(&membership, peer.node_id, &mut blockers);
                }
            }
            RaftMembershipOperation::AddVoter(peer) => {
                if !membership.learners.contains(&peer.node_id) {
                    validate_peer_absent(&membership, peer.node_id, &mut blockers);
                }
            }
            RaftMembershipOperation::AddLearner(peer) => {
                validate_peer_absent(&membership, peer.node_id, &mut blockers);
            }
            RaftMembershipOperation::AddWitness(peer) => {
                validate_peer_absent(&membership, peer.node_id, &mut blockers);
            }
            RaftMembershipOperation::Promote(node_id) => {
                if !membership.learners.contains(node_id) {
                    blockers.push(format!("node_{node_id}_is_not_learner"));
                }
            }
            RaftMembershipOperation::Remove(node_id) => {
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
            RaftMembershipOperation::TransferLeader(node_id) => match cluster.nodes.get(node_id) {
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

    pub fn reports(&self) -> &[RaftMembershipExecutionReport] {
        &self.reports
    }
}

fn validate_peer_absent(
    membership: &RaftMembership,
    node_id: RustRaftNodeId,
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
    before: &RaftMembership,
    after: &RaftMembership,
) -> Option<JointConsensusMembership> {
    (before.voters != after.voters).then(|| JointConsensusMembership {
        old_voters: before.voters.clone(),
        new_voters: after.voters.clone(),
    })
}

fn joint_acknowledgements(
    before: &RaftMembership,
    after: &RaftMembership,
) -> Vec<RustRaftNodeId> {
    before
        .voters
        .iter()
        .chain(after.voters.iter())
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}
