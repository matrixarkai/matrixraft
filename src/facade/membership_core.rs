// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// core membership roles, peers, learners, and joint membership helpers.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftMembershipScope {
    Metaserver,
    DataNode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftMembershipTransitionKind {
    Failover,
    ScaleUp,
    ScaleDown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipTransitionEvidence {
    pub scope: RustRaftMembershipScope,
    pub transition: RustRaftMembershipTransitionKind,
    #[serde(default)]
    pub before_voters: Vec<u64>,
    #[serde(default)]
    pub after_voters: Vec<u64>,
    #[serde(default)]
    pub before_learners: Vec<u64>,
    #[serde(default)]
    pub after_learners: Vec<u64>,
    pub leader_before: Option<u64>,
    pub leader_after: Option<u64>,
    #[serde(default)]
    pub failed_or_removed_nodes: Vec<u64>,
    #[serde(default)]
    pub added_nodes: Vec<u64>,
    #[serde(default)]
    pub caught_up_nodes: Vec<u64>,
    pub commit_index_before: u64,
    pub commit_index_after: u64,
    pub applied_index_after: u64,
    pub joint_consensus_used: bool,
    pub old_majority_preserved: bool,
    pub new_majority_reached: bool,
    #[serde(default)]
    pub joint_old_quorum_size: usize,
    #[serde(default)]
    pub joint_new_quorum_size: usize,
    #[serde(default)]
    pub joint_acknowledged_voters: Vec<u64>,
    #[serde(default)]
    pub joint_old_majority_acked: bool,
    #[serde(default)]
    pub joint_new_majority_acked: bool,
    pub stale_leader_rejected: bool,
    pub read_index_validated_after: bool,
    pub write_validated_after: bool,
    pub snapshot_floor_preserved: bool,
    pub secondary_replication_visible: bool,
    #[serde(default)]
    pub scheduler_generation_advanced: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipTransitionDecision {
    pub scope: RustRaftMembershipScope,
    pub transition: RustRaftMembershipTransitionKind,
    pub ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipReadinessReport {
    pub ready: bool,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub decisions: Vec<RustRaftMembershipTransitionDecision>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftRole {
    Leader,
    Follower,
    Candidate,
    PreCandidate,
    Learner,
}

pub type RustRaftNodeId = u64;
pub type RustRaftGroupId = u64;
pub type RustRaftTerm = u64;
pub type RustRaftLogIndex = u64;
pub type RustRaftSnapshotId = String;
pub type RustRaftPayload = Vec<u8>;
pub type EntryPayload = RustRaftPayload;
pub type RustRaftSnapshotPayload = Vec<u8>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLogId {
    pub term: RustRaftTerm,
    pub index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericLogEntry<P = RustRaftPayload> {
    pub log_id: RustRaftLogId,
    pub payload: P,
    #[serde(default)]
    pub is_command: bool,
}

pub type RustRaftLogEntry = RustRaftGenericLogEntry<RustRaftPayload>;
pub type RaftLogEntry<P = EntryPayload> = RustRaftGenericLogEntry<P>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftHardState {
    pub current_term: RustRaftTerm,
    pub voted_for: Option<RustRaftNodeId>,
    pub committed: Option<RustRaftLogId>,
}

pub type RaftHardState = RustRaftHardState;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum RustRaftReplicaRole {
    #[default]
    Voter,
    Learner,
    Witness,
}

impl RustRaftReplicaRole {
    pub fn participates_in_quorum(self) -> bool {
        matches!(self, Self::Voter | Self::Witness)
    }

    pub fn can_serve_data(self) -> bool {
        matches!(self, Self::Voter | Self::Learner)
    }

    pub fn can_be_leader(self) -> bool {
        matches!(self, Self::Voter)
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPeer {
    pub node_id: RustRaftNodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub role: RustRaftReplicaRole,
    #[serde(default)]
    pub auto_promote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembership {
    pub group_id: RustRaftGroupId,
    pub voters: Vec<RustRaftNodeId>,
    #[serde(default)]
    pub learners: Vec<RustRaftNodeId>,
    #[serde(default)]
    pub witnesses: Vec<RustRaftNodeId>,
    #[serde(default)]
    pub epoch: u64,
}

pub type RaftMembership = RustRaftMembership;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftLearnerCatchUpReport {
    pub learner_id: RustRaftNodeId,
    pub learner_match_index: RustRaftLogIndex,
    pub leader_commit_index: RustRaftLogIndex,
    pub caught_up: bool,
    pub lag: RustRaftLogIndex,
    pub promotable: bool,
    pub reason: String,
}

impl RustRaftMembership {
    pub fn quorum_size(&self) -> usize {
        self.quorum_size_with_witness_policy(false)
    }

    pub fn quorum_size_with_witness_policy(&self, ignore_witness: bool) -> usize {
        if ignore_witness {
            return self.voters.len() / 2 + 1;
        }
        let participants = self.voters.len() + self.witnesses.len();
        participants / 2 + 1
    }

    pub fn quorum_reached<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = RustRaftNodeId>,
    {
        self.quorum_reached_with_witness_policy(acknowledgements, false)
    }

    pub fn quorum_reached_with_witness_policy<I>(
        &self,
        acknowledgements: I,
        ignore_witness: bool,
    ) -> bool
    where
        I: IntoIterator<Item = RustRaftNodeId>,
    {
        let acknowledgements: Vec<_> = acknowledgements.into_iter().collect();
        let votes = self
            .voters
            .iter()
            .chain(
                (!ignore_witness)
                    .then_some(&self.witnesses)
                    .into_iter()
                    .flatten(),
            )
            .filter(|node_id| acknowledgements.contains(node_id))
            .count();
        votes >= self.quorum_size_with_witness_policy(ignore_witness)
    }

    pub fn add_learner(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        self.ensure_absent(node_id)?;
        self.learners.push(node_id);
        self.epoch += 1;
        Ok(())
    }

    pub fn add_witness(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        self.ensure_absent(node_id)?;
        self.witnesses.push(node_id);
        self.epoch += 1;
        Ok(())
    }

    pub fn promote_learner(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        let position = self
            .learners
            .iter()
            .position(|learner| *learner == node_id)
            .ok_or_else(|| {
                RaftError::InvalidRequest(format!("node {} is not a learner", node_id))
            })?;
        self.learners.remove(position);
        self.voters.push(node_id);
        self.epoch += 1;
        Ok(())
    }

    pub fn remove_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        let removed = remove_node(&mut self.voters, node_id)
            || remove_node(&mut self.learners, node_id)
            || remove_node(&mut self.witnesses, node_id);
        if !removed {
            return Err(RaftError::NodeNotFound(node_id));
        }
        self.epoch += 1;
        Ok(())
    }

    pub fn catchup_report(
        &self,
        learner_id: RustRaftNodeId,
        learner_match_index: RustRaftLogIndex,
        leader_commit_index: RustRaftLogIndex,
    ) -> RaftLearnerCatchUpReport {
        let lag = leader_commit_index.saturating_sub(learner_match_index);
        let is_learner = self.learners.contains(&learner_id);
        let caught_up = is_learner && learner_match_index >= leader_commit_index;
        RaftLearnerCatchUpReport {
            learner_id,
            learner_match_index,
            leader_commit_index,
            caught_up,
            lag,
            promotable: caught_up,
            reason: if !is_learner {
                "node_is_not_learner".to_string()
            } else if caught_up {
                "learner_caught_up".to_string()
            } else {
                "learner_lagging".to_string()
            },
        }
    }

    fn ensure_absent(&self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        if self.voters.contains(&node_id)
            || self.learners.contains(&node_id)
            || self.witnesses.contains(&node_id)
        {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is already a member",
                node_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftJointMembership {
    pub old_voters: Vec<RustRaftNodeId>,
    pub new_voters: Vec<RustRaftNodeId>,
}

pub type JointConsensusMembership = RustRaftJointMembership;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftJointConsensusCommitEvidence {
    pub old_quorum_size: usize,
    pub new_quorum_size: usize,
    pub acknowledged_voters: Vec<RustRaftNodeId>,
    pub old_majority_acked: bool,
    pub new_majority_acked: bool,
    pub joint_quorum_reached: bool,
}

impl RustRaftJointMembership {
    pub fn old_quorum_size(&self) -> usize {
        self.old_voters.len() / 2 + 1
    }

    pub fn new_quorum_size(&self) -> usize {
        self.new_voters.len() / 2 + 1
    }

    pub fn quorum_reached<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = RustRaftNodeId>,
    {
        self.commit_evidence(acknowledgements).joint_quorum_reached
    }

    pub fn commit_evidence<I>(&self, acknowledgements: I) -> RustRaftJointConsensusCommitEvidence
    where
        I: IntoIterator<Item = RustRaftNodeId>,
    {
        let acknowledgements: Vec<_> = acknowledgements.into_iter().collect();
        let old_votes = self
            .old_voters
            .iter()
            .filter(|node_id| acknowledgements.contains(node_id))
            .count();
        let new_votes = self
            .new_voters
            .iter()
            .filter(|node_id| acknowledgements.contains(node_id))
            .count();
        let old_majority_acked = old_votes >= self.old_quorum_size();
        let new_majority_acked = new_votes >= self.new_quorum_size();
        RustRaftJointConsensusCommitEvidence {
            old_quorum_size: self.old_quorum_size(),
            new_quorum_size: self.new_quorum_size(),
            acknowledged_voters: acknowledgements,
            old_majority_acked,
            new_majority_acked,
            joint_quorum_reached: old_majority_acked && new_majority_acked,
        }
    }
}

fn remove_node(nodes: &mut Vec<RustRaftNodeId>, node_id: RustRaftNodeId) -> bool {
    if let Some(position) = nodes.iter().position(|existing| *existing == node_id) {
        nodes.remove(position);
        true
    } else {
        false
    }
}
