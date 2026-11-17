// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Explicit opt-in configuration changes for durable peers.
use super::*;
use raft_core::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};

/// One membership operation. Acceptance is not a commit acknowledgment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipChange {
    AddLearner(u64),
    /// Commit a learner whose promotion is scheduled after catch-up.
    AddLearnerAuto(u64),
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
    /// The leader will propose the exit after applying this joint configuration.
    pub auto_leave: bool,
    /// Committed learners whose catch-up will schedule automatic promotion.
    pub automatic_learners: Vec<u64>,
}
const AUTO_LEARNER: &[u8] = b"matrixraft-automatic-learner-v1";

#[derive(Clone, PartialEq, prost::Message)]
struct PromotionPolicy {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, repeated, packed = "true", tag = "2")]
    targets: Vec<u64>,
}
pub(super) fn encode_promotions(targets: &BTreeSet<u64>) -> Vec<u8> {
    if targets.is_empty() {
        return Vec::new();
    }
    PromotionPolicy {
        version: 1,
        targets: targets.iter().copied().collect(),
    }
    .encode_to_vec()
}
pub(super) fn decode_promotions(
    bytes: &[u8],
    conf: &ConfState,
) -> Result<BTreeSet<u64>, RaftError> {
    if bytes.is_empty() {
        return Ok(BTreeSet::new());
    }
    let policy = PromotionPolicy::decode(bytes).map_err(invalid)?;
    if policy.version != 1
        || policy.targets.is_empty()
        || policy.targets.len() > 64
        || policy.targets.windows(2).any(|w| w[0] >= w[1])
        || policy.targets.iter().any(|id| !conf.learners.contains(id))
        || policy.encode_to_vec() != bytes
    {
        return Err(invalid("invalid automatic learner policy"));
    }
    Ok(policy.targets.into_iter().collect())
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
    let automatic_learner = change.context == AUTO_LEARNER
        && change.changes.len() == 1
        && change.transition == ConfChangeTransition::Auto as i32
        && change.changes[0].change_type == ConfChangeType::AddLearnerNode as i32;
    if change.encode_to_vec() != entry.data || (!change.context.is_empty() && !automatic_learner) || change.changes.len() > 1
        || !matches!(change.transition, x if x == ConfChangeTransition::Auto as i32 || x == ConfChangeTransition::Implicit as i32 || x == ConfChangeTransition::Explicit as i32)
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
        permitted: Vec<u64>,
    ) -> Result<Self, RaftError> {
        Self::open_with_membership_and_tuning(options, permitted, PeerTuning::default())
    }
    pub fn open_with_membership_and_tuning(
        options: PeerOptions,
        mut permitted: Vec<u64>,
        tuning: PeerTuning,
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
        Self::open_inner_with_tuning(options, permitted, tuning)
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
        let policy_changed = *self.store.checkpoint_promotions.lock().unwrap()
            != encode_promotions(&self.automatic_learners);
        self.store
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|snapshot| {
                snapshot.get_metadata().get_conf_state() != &current || policy_changed
            })
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
            auto_leave: conf.auto_leave,
            automatic_learners: self.automatic_learners.iter().copied().collect(),
        }
    }
    /// Propose one change. Promote only a learner caught up to the leader's commit.
    /// Voter changes enter explicit joint consensus; call LeaveJoint after commit.
    /// AddLearnerAuto schedules its later promotion with an automatic joint exit.
    pub fn propose_membership(&mut self, operation: MembershipChange) -> Result<LogId, RaftError> {
        self.propose_membership_inner(operation, false)
    }
    /// Propose a change whose joint exit is generated by the elected leader.
    /// Both majorities still govern the exit. The returned position identifies
    /// the initial change, not completion; continue ticking, Ready processing,
    /// and packet delivery. Wait for that position to apply and verify the
    /// requested membership with an empty outgoing voter set.
    /// Upgrade all peers before enabling automatic transitions.
    pub fn propose_membership_auto(
        &mut self,
        operation: MembershipChange,
    ) -> Result<LogId, RaftError> {
        self.propose_membership_inner(operation, true)
    }
    fn propose_membership_inner(
        &mut self,
        operation: MembershipChange,
        automatic: bool,
    ) -> Result<LogId, RaftError> {
        self.healthy()?;
        if self.store.permitted.is_empty() {
            return Err(invalid("membership changes are disabled"));
        }
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.node_id()));
        }
        if self.store.cache.enabled()
            && self
                .store
                .cache
                .busy(self.store.memory.rl().hard_state().commit)
        {
            return Err(invalid("durable log cache is waiting for commitment"));
        }
        if self.consensus_work_ready()
            || self.pending_restore.is_some()
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
                MembershipChange::AddLearner(id) | MembershipChange::AddLearnerAuto(id) => {
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
            if matches!(operation, MembershipChange::AddLearnerAuto(_)) {
                change.context = AUTO_LEARNER.to_vec();
            }
            if joint_change {
                change.transition = if automatic {
                    ConfChangeTransition::Implicit
                } else {
                    ConfChangeTransition::Explicit
                } as i32;
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
        let unstable = self.raw.raft.raft_log.unstable_entries();
        self.store.cache.admit(&unstable[unstable.len() - 1..]);
        Ok(LogId {
            term: self.term(),
            index,
        })
    }
    pub(super) fn apply_committed_membership(&mut self, entry: &Entry) -> Result<(), RaftError> {
        let change = decode_change(entry, &self.store.permitted)?;
        let conf = self.raw.apply_conf_change(&change).map_err(invalid)?;
        validate_conf(&conf, &self.store.permitted)?;
        if change.context == AUTO_LEARNER {
            self.automatic_learners.insert(change.changes[0].node_id);
        }
        self.automatic_learners
            .retain(|id| conf.learners.contains(id));
        let local_voter =
            conf.voters.contains(&self.node_id()) || conf.voters_outgoing.contains(&self.node_id());
        self.store.wl().set_conf_state(conf);
        self.append_reorder.clear();
        if !local_voter && self.raw.raft.state != StateRole::Follower {
            self.raw.raft.become_follower(self.term(), 0);
        }
        let progress = self.raw.raft.prs();
        self.stream
            .retain_send_targets(|to| progress.get(to).is_some());
        self.snapshot_flights
            .retain(|to, _| progress.get(*to).is_some());
        // A caller must start a new read under the committed configuration.
        while let Some((&token, _)) = self.pending_reads.first_key_value() {
            self.cancel_read(token)?;
        }
        Ok(())
    }

    pub(super) fn automatic_promotion_candidate(&self) -> Option<u64> {
        if self.automatic_learners.is_empty()
            || !self.is_leader()
            || self.recovering_membership
            || self.consensus_work_ready()
            || self.applied_index() < self.commit_index()
            || self.raw.raft.pending_conf_index > self.applied_index()
            || self.raw.raft.lead_transferee.is_some()
        {
            return None;
        }
        if self.store.cache.enabled()
            && self
                .store
                .cache
                .busy(self.store.memory.rl().hard_state().commit)
        {
            return None;
        }
        let state = self.membership();
        if !state.outgoing_voters.is_empty() || state.voters.len() >= 7 {
            return None;
        }
        self.automatic_learners.iter().copied().find(|id| {
            if self.raw.raft.prs().get(*id).is_none_or(|progress| {
                progress.matched < self.commit_index() || progress.matched == 0
            }) {
                return false;
            }
            let outstanding = self.raw.raft.uncommitted_size() as u64;
            if outstanding == 0 {
                return true;
            }
            let bytes = ConfChangeV2 {
                transition: ConfChangeTransition::Implicit as i32,
                changes: vec![ConfChangeSingle {
                    node_id: *id,
                    change_type: ConfChangeType::AddNode as i32,
                }],
                ..Default::default()
            }
            .encoded_len() as u64;
            outstanding.saturating_add(bytes) <= self.uncommitted_limit
        })
    }
    pub(super) fn schedule_automatic_promotion(&mut self) -> Result<(), RaftError> {
        if let Some(id) = self.automatic_promotion_candidate() {
            self.propose_membership_auto(MembershipChange::Promote(id))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_learner_policy_requires_canonical_committed_learners() {
        let conf = ConfState {
            voters: vec![1, 2, 3],
            learners: vec![4, 5],
            ..Default::default()
        };
        let targets = BTreeSet::from([4, 5]);
        let bytes = encode_promotions(&targets);
        assert_eq!(decode_promotions(&bytes, &conf).unwrap(), targets);
        assert!(encode_promotions(&BTreeSet::new()).is_empty());
        assert!(decode_promotions(&[], &conf).unwrap().is_empty());
        for (version, targets) in [
            (0, vec![4]),
            (2, vec![4]),
            (1, vec![]),
            (1, vec![1]),
            (1, vec![6]),
            (1, vec![4, 4]),
            (1, vec![5, 4]),
        ] {
            assert!(decode_promotions(
                &PromotionPolicy { version, targets }.encode_to_vec(),
                &conf
            )
            .is_err());
        }
        let mut extra = bytes;
        extra.extend_from_slice(&[24, 1]);
        assert!(decode_promotions(&extra, &conf).is_err());
    }

    #[test]
    fn automatic_learner_intent_is_valid_only_on_one_learner_addition() {
        let mut change = ConfChangeV2 {
            context: AUTO_LEARNER.to_vec(),
            changes: vec![ConfChangeSingle {
                node_id: 4,
                change_type: ConfChangeType::AddLearnerNode as i32,
            }],
            ..Default::default()
        };
        let decode = |change: &ConfChangeV2| {
            decode_change(
                &Entry {
                    entry_type: EntryType::EntryConfChangeV2 as i32,
                    data: change.encode_to_vec(),
                    ..Default::default()
                },
                &[1, 2, 3, 4],
            )
        };
        assert!(decode(&change).is_ok());
        change.transition = ConfChangeTransition::Implicit as i32;
        assert!(decode(&change).is_err());
        change.transition = ConfChangeTransition::Auto as i32;
        change.changes[0].change_type = ConfChangeType::AddNode as i32;
        assert!(decode(&change).is_err());
        change.changes.clear();
        assert!(decode(&change).is_err());
    }

    #[test]
    fn invalid_snapshot_policies_cannot_advance_terms_under_receive_pressure() {
        let dir = tempfile::tempdir().unwrap();
        let options = PeerOptions {
            group_id: 71,
            node_id: 2,
            voters: vec![1, 2, 3],
            wal_path: dir.path().join("peer.wal"),
        };
        let mut peer = DurablePeer::open_with_membership(options, vec![1, 2, 3, 4]).unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let quota = Arc::new(Mutex::new(crate::ByteQuotaLimiter::with_available(
            1024 * 1024,
            0,
        )));
        peer.set_checkpoint_transfer_limiters(None, Some(quota))
            .unwrap();
        let mut message = Message {
            from: 1,
            to: 2,
            term: 2,
            msg_type: MessageType::MsgSnapshot as i32,
            context: encode_promotions(&BTreeSet::from([1])),
            snapshot: Some(Snapshot {
                data: vec![42],
                metadata: Some(SnapshotMetadata {
                    index: 1,
                    term: 1,
                    conf_state: Some(ConfState {
                        voters: vec![1, 2, 3],
                        learners: vec![4],
                        ..Default::default()
                    }),
                }),
            }),
            ..Default::default()
        };
        let term = peer.term();
        for version in [1, 4] {
            let packet = encode_envelope(version, 71, &message).unwrap();
            assert!(peer.step(&packet).is_err());
            assert_eq!(peer.term(), term);
            assert!(peer.incoming_promotions.is_none());
        }
        message.context = encode_promotions(&BTreeSet::from([4]));
        assert!(peer
            .step(&encode_envelope(1, 71, &message).unwrap())
            .is_err());
        assert_eq!(peer.term(), term);
        peer.step(&encode_envelope(4, 71, &message).unwrap())
            .unwrap();
        assert_eq!(peer.term(), 2);
        assert_eq!(peer.commit_index(), 0);
        assert!(peer.incoming_promotions.is_none());
        assert!(peer.automatic_learners.is_empty());
    }

    #[test]
    fn ignored_snapshots_cannot_replace_an_accepted_promotion_policy() {
        for graceful in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let options = PeerOptions {
                group_id: 71,
                node_id: 2,
                voters: vec![1, 2, 3],
                wal_path: dir.path().join("peer.wal"),
            };
            let mut peer =
                DurablePeer::open_with_membership(options.clone(), vec![1, 2, 3, 4, 5]).unwrap();
            peer.process_ready(|_, _| Ok(())).unwrap();
            let message = Message {
                from: 1,
                to: 2,
                term: 4,
                msg_type: MessageType::MsgSnapshot as i32,
                context: encode_promotions(&BTreeSet::from([4])),
                snapshot: Some(Snapshot {
                    data: vec![42],
                    metadata: Some(SnapshotMetadata {
                        index: 10,
                        term: 3,
                        conf_state: Some(ConfState {
                            voters: vec![1, 2, 3],
                            learners: vec![4, 5],
                            ..Default::default()
                        }),
                    }),
                }),
                ..Default::default()
            };
            peer.step(&encode_envelope(4, 71, &message).unwrap())
                .unwrap();
            let mut stale = message.clone();
            stale.term = 3;
            stale.mut_snapshot().mut_metadata().index = 11;
            stale.context = encode_promotions(&BTreeSet::from([5]));
            peer.step(&encode_envelope(4, 71, &stale).unwrap()).unwrap();
            if graceful {
                peer.begin_shutdown().unwrap();
            }
            peer.process_ready_with_snapshot(
                |_, _| Ok(()),
                |id, bytes| {
                    assert_eq!(id.index, 10);
                    assert_eq!(bytes, &[42]);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(peer.membership().automatic_learners, vec![4]);
            drop(peer);
            let mut recovered =
                DurablePeer::open_with_membership(options, vec![1, 2, 3, 4, 5]).unwrap();
            recovered
                .process_ready_with_snapshot(
                    |_, _| Ok(()),
                    |_, bytes| {
                        assert_eq!(bytes, &[42]);
                        Ok(())
                    },
                )
                .unwrap();
            assert_eq!(recovered.membership().automatic_learners, vec![4]);
        }
    }

    #[test]
    fn automatic_exit_metadata_requires_a_valid_joint_configuration() {
        let mut conf = ConfState {
            voters: vec![1, 2, 3, 4],
            voters_outgoing: vec![1, 2, 3],
            auto_leave: true,
            ..Default::default()
        };
        assert!(validate_conf(&conf, &[1, 2, 3, 4]).is_ok());
        assert!(validate_conf(&conf, &[1, 2, 3]).is_err());
        conf.voters_outgoing.clear();
        assert!(validate_conf(&conf, &[1, 2, 3, 4]).is_err());
        conf.auto_leave = false;
        assert!(validate_conf(&conf, &[1, 2, 3, 4]).is_ok());
    }

    #[test]
    fn automatic_transition_entries_keep_inventory_and_context_validation() {
        let mut change = ConfChangeV2 {
            transition: ConfChangeTransition::Implicit as i32,
            changes: vec![ConfChangeSingle {
                node_id: 4,
                change_type: ConfChangeType::AddNode as i32,
            }],
            ..Default::default()
        };
        let entry = |change: &ConfChangeV2| Entry {
            entry_type: EntryType::EntryConfChangeV2 as i32,
            data: change.encode_to_vec(),
            ..Default::default()
        };
        assert!(decode_change(&entry(&change), &[1, 2, 3, 4]).is_ok());
        assert!(decode_change(&entry(&change), &[1, 2, 3]).is_err());
        assert!(decode_change(&entry(&change), &[]).is_err());
        change.context = vec![1];
        assert!(decode_change(&entry(&change), &[1, 2, 3, 4]).is_err());
        change.context.clear();
        change.transition = 99;
        assert!(decode_change(&entry(&change), &[1, 2, 3, 4]).is_err());
    }
}
