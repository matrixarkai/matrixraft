// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// core membership roles, peers, learners, and joint membership helpers.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MembershipScope {
    Metaserver,
    DataNode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MembershipTransitionKind {
    Failover,
    ScaleUp,
    ScaleDown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipTransitionEvidence {
    pub scope: MembershipScope,
    pub transition: MembershipTransitionKind,
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
pub struct MembershipTransitionDecision {
    pub scope: MembershipScope,
    pub transition: MembershipTransitionKind,
    pub ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipReadinessReport {
    pub ready: bool,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub decisions: Vec<MembershipTransitionDecision>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StateRole {
    Leader,
    Follower,
    Candidate,
    PreCandidate,
    Learner,
}

pub type NodeId = u64;
pub type GroupId = u64;
pub type Term = u64;
pub type LogIndex = u64;
pub type SnapshotId = String;
pub type Payload = Vec<u8>;
pub type EntryPayload = Payload;
pub type SnapshotPayload = Vec<u8>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogId {
    pub term: Term,
    pub index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenericLogEntry<P = Payload> {
    pub log_id: LogId,
    pub payload: P,
    #[serde(default)]
    pub is_command: bool,
}

pub type LogEntry = GenericLogEntry<Payload>;
pub type RaftLogEntry<P = EntryPayload> = GenericLogEntry<P>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub committed: Option<LogId>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ReplicaRole {
    #[default]
    Voter,
    Learner,
    Witness,
}

impl ReplicaRole {
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
pub struct Peer {
    pub node_id: NodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub role: ReplicaRole,
    #[serde(default)]
    pub auto_promote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Membership {
    pub group_id: GroupId,
    pub voters: Vec<NodeId>,
    #[serde(default)]
    pub learners: Vec<NodeId>,
    #[serde(default)]
    pub witnesses: Vec<NodeId>,
    #[serde(default)]
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LearnerCatchUpReport {
    pub learner_id: NodeId,
    pub learner_match_index: LogIndex,
    pub leader_commit_index: LogIndex,
    pub caught_up: bool,
    pub lag: LogIndex,
    pub promotable: bool,
    pub reason: String,
}

impl Membership {
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
        I: IntoIterator<Item = NodeId>,
    {
        self.quorum_reached_with_witness_policy(acknowledgements, false)
    }

    pub fn quorum_reached_with_witness_policy<I>(
        &self,
        acknowledgements: I,
        ignore_witness: bool,
    ) -> bool
    where
        I: IntoIterator<Item = NodeId>,
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

    pub fn add_learner(&mut self, node_id: NodeId) -> Result<(), RaftError> {
        self.ensure_absent(node_id)?;
        self.learners.push(node_id);
        self.epoch += 1;
        Ok(())
    }

    pub fn add_witness(&mut self, node_id: NodeId) -> Result<(), RaftError> {
        self.ensure_absent(node_id)?;
        self.witnesses.push(node_id);
        self.epoch += 1;
        Ok(())
    }

    pub fn promote_learner(&mut self, node_id: NodeId) -> Result<(), RaftError> {
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

    pub fn remove_peer(&mut self, node_id: NodeId) -> Result<(), RaftError> {
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
        learner_id: NodeId,
        learner_match_index: LogIndex,
        leader_commit_index: LogIndex,
    ) -> LearnerCatchUpReport {
        let lag = leader_commit_index.saturating_sub(learner_match_index);
        let is_learner = self.learners.contains(&learner_id);
        let caught_up = is_learner && learner_match_index >= leader_commit_index;
        LearnerCatchUpReport {
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

    fn ensure_absent(&self, node_id: NodeId) -> Result<(), RaftError> {
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
pub struct JointMembership {
    pub old_voters: Vec<NodeId>,
    pub new_voters: Vec<NodeId>,
}

pub type JointConsensusMembership = JointMembership;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JointConsensusCommitEvidence {
    pub old_quorum_size: usize,
    pub new_quorum_size: usize,
    pub acknowledged_voters: Vec<NodeId>,
    pub old_majority_acked: bool,
    pub new_majority_acked: bool,
    pub joint_quorum_reached: bool,
}

impl JointMembership {
    pub fn old_quorum_size(&self) -> usize {
        self.old_voters.len() / 2 + 1
    }

    pub fn new_quorum_size(&self) -> usize {
        self.new_voters.len() / 2 + 1
    }

    pub fn quorum_reached<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = NodeId>,
    {
        self.commit_evidence(acknowledgements).joint_quorum_reached
    }

    pub fn commit_evidence<I>(&self, acknowledgements: I) -> JointConsensusCommitEvidence
    where
        I: IntoIterator<Item = NodeId>,
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
        JointConsensusCommitEvidence {
            old_quorum_size: self.old_quorum_size(),
            new_quorum_size: self.new_quorum_size(),
            acknowledged_voters: acknowledgements,
            old_majority_acked,
            new_majority_acked,
            joint_quorum_reached: old_majority_acked && new_majority_acked,
        }
    }
}

fn remove_node(nodes: &mut Vec<NodeId>, node_id: NodeId) -> bool {
    if let Some(position) = nodes.iter().position(|existing| *existing == node_id) {
        nodes.remove(position);
        true
    } else {
        false
    }
}
