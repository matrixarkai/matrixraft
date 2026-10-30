// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Explicit opt-in configuration changes for durable peers.
use super::*;
use raft_core::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};

/// One membership operation. Acceptance is not a commit acknowledgment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipChange {
    AddLearner(u64),
    Promote(u64),
    Remove(u64),
    LeaveJoint,
}
/// Committed membership, including both majorities during a joint transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipState {
    pub voters: Vec<u64>,
    pub outgoing_voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub next_learners: Vec<u64>,
}
fn sorted(values: &[u64]) -> Vec<u64> {
    let mut values = values.to_vec();
    values.sort_unstable();
    values
}
pub(super) fn validate_conf(conf: &ConfState, permitted: &[u64]) -> Result<(), RaftError> {
    let sets: Vec<BTreeSet<u64>> = [
        &conf.voters,
        &conf.voters_outgoing,
        &conf.learners,
        &conf.learners_next,
    ]
    .into_iter()
    .map(|values| values.iter().copied().collect())
    .collect();
    for (values, set) in [
        &conf.voters,
        &conf.voters_outgoing,
        &conf.learners,
        &conf.learners_next,
    ]
    .into_iter()
    .zip(&sets)
    {
        if values.len() != set.len() || values.iter().any(|id| !permitted.contains(id)) {
            return Err(invalid("invalid configuration members"));
        }
    }
    if sets[0].is_empty()
        || sets[0].len() > 7
        || sets[1].len() > 7
        || !sets[0].is_disjoint(&sets[2])
        || !sets[1].is_disjoint(&sets[2])
        || !sets[0].is_disjoint(&sets[3])
        || !sets[2].is_disjoint(&sets[3])
        || !sets[3].is_subset(&sets[1])
        || (sets[1].is_empty() && (!sets[3].is_empty() || conf.auto_leave))
        || conf.auto_leave
    {
        return Err(invalid("invalid joint configuration"));
    }
    Ok(())
}
pub(super) fn decode_change(entry: &Entry, permitted: &[u64]) -> Result<ConfChangeV2, RaftError> {
    if permitted.is_empty()
        || entry.entry_type != EntryType::EntryConfChangeV2 as i32
        || entry.data.len() > 1024
    {
        return Err(invalid("configuration entry requires membership support"));
    }
    let change = ConfChangeV2::decode(entry.data.as_slice()).map_err(invalid)?;
    if change.encode_to_vec() != entry.data || !change.context.is_empty() || change.changes.len() > 1
        || !matches!(change.transition, x if x == ConfChangeTransition::Auto as i32 || x == ConfChangeTransition::Explicit as i32)
        || change.changes.iter().any(|c| !permitted.contains(&c.node_id) || !matches!(c.change_type,
            x if x == ConfChangeType::AddNode as i32 || x == ConfChangeType::AddLearnerNode as i32 || x == ConfChangeType::RemoveNode as i32))
    {
        return Err(invalid("invalid configuration entry"));
    }
    Ok(change)
}
impl DurablePeer {
    /// Open a membership-enabled peer. The permitted inventory is immutable and
    /// includes bootstrap voters and every identity that may later join.
    /// A non-bootstrap identity starts without a vote and must be added by a leader.
    pub fn open_with_membership(
        options: PeerOptions,
        mut permitted: Vec<u64>,
    ) -> Result<Self, RaftError> {
        permitted.sort_unstable();
        if permitted.is_empty()
            || permitted.len() > 64
            || permitted.contains(&0)
            || permitted.windows(2).any(|w| w[0] == w[1])
            || !permitted.contains(&options.node_id)
            || options.voters.iter().any(|id| !permitted.contains(id))
        {
            return Err(invalid("invalid permitted peer inventory"));
        }
        Self::open_inner(options, permitted)
    }
    /// A compacted image predates the committed configuration. Capture a fresh
    /// application checkpoint so newly added learners can restore that membership.
    pub fn membership_checkpoint_needed(&self) -> bool {
        if self.store.permitted.is_empty() {
            return false;
        }
        let current = self
            .store
            .initial_state()
            .expect("memory configuration")
            .conf_state;
        self.store
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|snapshot| snapshot.get_metadata().get_conf_state() != &current)
    }
    pub fn membership(&self) -> MembershipState {
        let conf = self
            .store
            .initial_state()
            .expect("memory configuration")
            .conf_state;
        MembershipState {
            voters: sorted(&conf.voters),
            outgoing_voters: sorted(&conf.voters_outgoing),
            learners: sorted(&conf.learners),
            next_learners: sorted(&conf.learners_next),
        }
    }
    /// Propose one change. Promote only a learner caught up to the leader's commit.
    /// Voter changes enter explicit joint consensus; call LeaveJoint after commit.
    pub fn propose_membership(&mut self, operation: MembershipChange) -> Result<LogId, RaftError> {
        self.healthy()?;
        if self.store.permitted.is_empty() {
            return Err(invalid("membership changes are disabled"));
        }
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.node_id()));
        }
        if self.raw.has_ready()
            || self.restore_on_open.is_some()
            || self.raw.raft.pending_conf_index > self.applied_index()
        {
            return Err(invalid(
                "membership change requires settled committed state",
            ));
        }
        let state = self.membership();
        let joint = !state.outgoing_voters.is_empty();
        let mut change = ConfChangeV2::default();
        if operation == MembershipChange::LeaveJoint {
            if !joint {
                return Err(invalid("configuration is not joint"));
            }
        } else {
            if joint {
                return Err(invalid("leave the committed joint configuration first"));
            }
            let (node_id, kind, joint_change) = match operation {
                MembershipChange::AddLearner(id) => {
                    if state.voters.contains(&id) || state.learners.contains(&id) {
                        return Err(invalid("member already exists"));
                    }
                    (id, ConfChangeType::AddLearnerNode, false)
                }
                MembershipChange::Promote(id) => {
                    if !state.learners.contains(&id)
                        || state.voters.len() >= 7
                        || self
                            .raw
                            .raft
                            .prs()
                            .get(id)
                            .is_none_or(|p| p.matched < self.commit_index())
                    {
                        return Err(invalid(
                            "promotion requires a caught-up learner and voter capacity",
                        ));
                    }
                    (id, ConfChangeType::AddNode, true)
                }
                MembershipChange::Remove(id) => {
                    let voter = state.voters.contains(&id);
                    if (!voter && !state.learners.contains(&id))
                        || (voter && state.voters.len() == 1)
                    {
                        return Err(invalid("cannot remove this member"));
                    }
                    (id, ConfChangeType::RemoveNode, voter)
                }
                MembershipChange::LeaveJoint => unreachable!(),
            };
            if !self.store.permitted.contains(&node_id) {
                return Err(invalid("peer is outside the permitted inventory"));
            }
            change.changes.push(ConfChangeSingle {
                node_id,
                change_type: kind as i32,
            });
            if joint_change {
                change.transition = ConfChangeTransition::Explicit as i32;
            }
        }
        let before = self.raw.raft.raft_log.last_index();
        self.raw
            .propose_conf_change(Vec::new(), change)
            .map_err(invalid)?;
        let index = self.raw.raft.raft_log.last_index();
        if index == before {
            return Err(invalid("configuration proposal was not admitted"));
        }
        Ok(LogId {
            term: self.term(),
            index,
        })
    }
    pub(super) fn apply_committed(
        &mut self,
        entry: Entry,
        apply: &mut impl FnMut(LogId, &[u8]) -> Result<(), RaftError>,
    ) -> Result<(), RaftError> {
        if entry.entry_type == EntryType::EntryNormal as i32 {
            return apply(
                LogId {
                    term: entry.term,
                    index: entry.index,
                },
                &entry.data,
            );
        }
        let change = decode_change(&entry, &self.store.permitted)?;
        let conf = self.raw.apply_conf_change(&change).map_err(invalid)?;
        validate_conf(&conf, &self.store.permitted)?;
        let local_voter =
            conf.voters.contains(&self.node_id()) || conf.voters_outgoing.contains(&self.node_id());
        self.store.wl().set_conf_state(conf);
        if !local_voter && self.raw.raft.state != StateRole::Follower {
            self.raw.raft.become_follower(self.term(), 0);
        }
        // A caller must start a new read under the committed configuration.
        self.pending_read = None;
        Ok(())
    }
}
