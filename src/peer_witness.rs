// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

pub(super) struct CommitPolicy {
    witnesses: BTreeSet<u64>,
    leader_term: Option<u64>,
    count_witnesses: bool,
    voters: raft_core::JointConfig,
    incoming: Vec<u64>,
    outgoing: Vec<u64>,
    positions: Vec<(u64, u64)>,
    groups: Vec<(u64, u64)>,
}

impl CommitPolicy {
    pub(super) fn contains(&self, id: u64) -> bool {
        self.witnesses.contains(&id)
    }
    pub(super) fn from_witnesses(witnesses: BTreeSet<u64>) -> Option<Box<Self>> {
        (!witnesses.is_empty()).then(|| {
            Box::new(Self {
                witnesses,
                leader_term: None,
                count_witnesses: false,
                voters: raft_core::JointConfig::default(),
                incoming: Vec::new(),
                outgoing: Vec::new(),
                positions: Vec::new(),
                groups: Vec::new(),
            })
        })
    }

    fn synchronize_term(&mut self, core: &raft_core::Raft<PeerStorage>) {
        let term = (core.state == StateRole::Leader).then_some(core.term);
        if self.leader_term != term {
            self.leader_term = term;
            self.count_witnesses = false;
        }
    }

    fn synchronize_voters(&mut self, core: &raft_core::Raft<PeerStorage>) {
        let current = core.prs().conf().voters();
        if &self.voters != current {
            let state = core.prs().conf().to_conf_state();
            self.incoming = state.voters;
            self.outgoing = state.voters_outgoing;
            self.voters.clone_from(current);
        }
    }

    fn recent_majority(
        &self,
        core: &raft_core::Raft<PeerStorage>,
        voters: &[u64],
        data_only: bool,
    ) -> bool {
        voters.is_empty()
            || voters
                .iter()
                .filter(|id| {
                    (!data_only || !self.witnesses.contains(id))
                        && (**id == core.id
                            || core.prs().get(**id).is_some_and(|p| p.recent_active))
                })
                .count()
                > voters.len() / 2
    }

    fn before_tick(&mut self, core: &raft_core::Raft<PeerStorage>) {
        self.synchronize_term(core);
        if self.leader_term.is_none()
            || core.election_elapsed.saturating_add(1) < core.election_timeout()
        {
            return;
        }
        // The core consumes recent_active at this same quorum-clock boundary.
        self.synchronize_voters(core);
        let data_active = self.recent_majority(core, &self.incoming, true)
            && self.recent_majority(core, &self.outgoing, true);
        if data_active {
            self.count_witnesses = false;
        } else if self.recent_majority(core, &self.incoming, false)
            && self.recent_majority(core, &self.outgoing, false)
        {
            self.count_witnesses = true;
        }
    }

    fn boundary(
        witnesses: &BTreeSet<u64>,
        positions: &mut Vec<(u64, u64)>,
        core: &raft_core::Raft<PeerStorage>,
        voters: &[u64],
    ) -> Option<(u64, u64)> {
        if voters.is_empty() {
            return Some((u64::MAX, 0));
        }
        positions.clear();
        positions.extend(
            voters
                .iter()
                .filter(|id| !witnesses.contains(id))
                .map(|&id| (core.prs().get(id).map_or(0, |p| p.matched), id)),
        );
        positions.sort_unstable_by(|a, b| b.cmp(a));
        positions.get(voters.len() / 2).copied()
    }

    fn settle(&mut self, core: &mut raft_core::Raft<PeerStorage>) {
        self.synchronize_term(core);
        self.synchronize_voters(core);
        core.enable_group_commit(true);
        core.clear_commit_group();
        if core.state != StateRole::Leader {
            return;
        }
        let incoming = Self::boundary(&self.witnesses, &mut self.positions, core, &self.incoming);
        let outgoing = Self::boundary(&self.witnesses, &mut self.positions, core, &self.outgoing);
        if incoming.is_none() || outgoing.is_none() {
            self.count_witnesses = true;
        }
        let boundary = if self.count_witnesses {
            None
        } else {
            incoming.zip(outgoing).map(|(a, b)| {
                // An empty outgoing half has no boundary identity, even when
                // incoming positions reach the maximum representable index.
                if a.0 <= b.0 {
                    a.1
                } else {
                    b.1
                }
            })
        };
        self.groups.clear();
        self.groups.extend(
            core.prs()
                .iter()
                .map(|(&id, _)| (id, if boundary == Some(id) { 2 } else { 1 })),
        );
        core.assign_commit_groups(&self.groups);
        // Between inputs all groups stay zero: the core can commit only a
        // unanimous prefix until the next policy settlement. No matched index
        // or acknowledgment is fabricated, projected, or rolled back.
        core.clear_commit_group();
    }
}

impl DurablePeer {
    pub(super) fn settle_witness_commit(&mut self) {
        if let Some(policy) = &mut self.witness_commit {
            policy.settle(&mut self.raw.raft);
        }
    }

    pub(super) fn tick_witness_commit(&mut self) {
        if let Some(policy) = &mut self.witness_commit {
            policy.before_tick(&self.raw.raft);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peers(
        ids: &[u64],
        permitted: &[u64],
        witnesses: &[u64],
    ) -> (tempfile::TempDir, Vec<DurablePeer>) {
        let directory = tempfile::tempdir().unwrap();
        let peers = permitted
            .iter()
            .map(|&id| {
                let mut peer = DurablePeer::open_with_membership(
                    PeerOptions {
                        group_id: 73,
                        node_id: id,
                        voters: ids.to_vec(),
                        wal_path: directory.path().join(format!("{id}.wal")),
                    },
                    permitted.to_vec(),
                )
                .unwrap();
                // These tests exercise only commit selection; all peers still
                // replicate full entries and run real elections and Ready I/O.
                peer.witness_commit =
                    CommitPolicy::from_witnesses(witnesses.iter().copied().collect());
                peer.settle_witness_commit();
                peer
            })
            .collect();
        (directory, peers)
    }

    fn drive(peers: &mut [DurablePeer], blocked: Option<u64>) {
        for _ in 0..128 {
            let mut packets = Vec::new();
            for peer in &mut *peers {
                for packet in peer.process_ready(|_, _| Ok(())).unwrap() {
                    packets.push((peer.node_id(), packet));
                }
                assert!(peer
                    .raw
                    .raft
                    .prs()
                    .iter()
                    .all(|(_, p)| p.commit_group_id == 0));
                if let Some(policy) = &peer.witness_commit {
                    assert_eq!(&policy.voters, peer.raw.raft.prs().conf().voters());
                }
            }
            if packets.is_empty() && peers.iter().all(|p| !p.has_ready()) {
                return;
            }
            for (from, packet) in packets {
                if blocked == Some(from) || blocked == Some(packet.to) {
                    continue;
                }
                if let Some(peer) = peers.iter_mut().find(|p| p.node_id() == packet.to) {
                    peer.step(&packet.bytes).unwrap();
                    assert!(peer
                        .raw
                        .raft
                        .prs()
                        .iter()
                        .all(|(_, p)| p.commit_group_id == 0));
                }
            }
        }
        panic!("commit policy peers did not settle");
    }

    fn tick_leader(peers: &mut [DurablePeer], blocked: Option<u64>, count: usize) {
        for _ in 0..count {
            peers[0].tick().unwrap();
            drive(peers, blocked);
        }
    }

    #[test]
    fn default_has_no_policy_or_group_commit() {
        let directory = tempfile::tempdir().unwrap();
        let mut peer = DurablePeer::open(PeerOptions {
            group_id: 73,
            node_id: 1,
            voters: vec![1],
            wal_path: directory.path().join("peer.wal"),
        })
        .unwrap();
        assert!(peer.witness_commit.is_none());
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert!(peer.is_leader());
        assert!(!peer.raw.raft.group_commit());
    }

    #[test]
    fn real_responses_prefer_data_then_enable_and_disable_witness_counting() {
        let (_directory, mut peers) = peers(&[1, 2, 3], &[1, 2, 3], &[3]);
        peers[0].campaign().unwrap();
        drive(&mut peers, None);
        assert!(peers[0].is_leader());
        let buffers = |peer: &DurablePeer| {
            let policy = peer.witness_commit.as_ref().unwrap();
            (
                policy.incoming.as_ptr(),
                policy.outgoing.as_ptr(),
                policy.positions.as_ptr(),
                policy.groups.as_ptr(),
            )
        };
        let original_buffers = buffers(&peers[0]);
        let before = peers[0].commit_index();
        let proposal = peers[0].propose(b"first".to_vec()).unwrap();
        drive(&mut peers, Some(2));
        assert_eq!(peers[0].commit_index(), before);
        assert_eq!(
            peers[0].raw.raft.prs().get(3).unwrap().matched,
            proposal.index
        );
        let ticks = peers[0].raw.raft.election_timeout();
        tick_leader(&mut peers, Some(2), ticks * 2);
        assert!(peers[0].is_leader());
        assert!(peers[0].witness_commit.as_ref().unwrap().count_witnesses);
        assert_eq!(peers[0].applied_index(), proposal.index);
        tick_leader(&mut peers, None, ticks * 2);
        assert_eq!(peers[1].applied_index(), proposal.index);
        assert!(!peers[0].witness_commit.as_ref().unwrap().count_witnesses);
        let next = peers[0].propose(b"second".to_vec()).unwrap();
        drive(&mut peers, Some(2));
        assert_eq!(peers[0].commit_index(), proposal.index);
        assert_eq!(peers[0].raw.raft.prs().get(3).unwrap().matched, next.index);
        tick_leader(&mut peers, None, ticks);
        assert_eq!(peers[0].applied_index(), next.index);
        assert_eq!(buffers(&peers[0]), original_buffers);
    }

    #[test]
    fn insufficient_data_majority_can_commit_using_actual_witness_votes_and_appends() {
        let (_directory, mut peers) = peers(&[1, 2, 3], &[1, 2, 3], &[2, 3]);
        peers[0].campaign().unwrap();
        drive(&mut peers, Some(2));
        assert!(peers[0].is_leader());
        let proposal = peers[0].propose(b"value".to_vec()).unwrap();
        drive(&mut peers, Some(2));
        assert_eq!(peers[0].applied_index(), proposal.index);
        assert_eq!(peers[0].raw.raft.prs().get(2).unwrap().matched, 0);
        assert!(peers[0].witness_commit.as_ref().unwrap().count_witnesses);
    }

    #[test]
    fn inactive_witness_identity_does_not_block_single_voter_persistence() {
        let (_directory, mut peers) = peers(&[1], &[1, 2], &[2]);
        peers[0].campaign().unwrap();
        drive(&mut peers, None);
        let proposal = peers[0].propose(b"value".to_vec()).unwrap();
        drive(&mut peers, None);
        assert_eq!(peers[0].applied_index(), proposal.index);
    }

    #[test]
    fn joint_configuration_needs_both_data_majorities_until_fallback() {
        let (_directory, mut peers) = peers(&[1, 2, 3], &[1, 2, 3, 4], &[3]);
        peers[0].campaign().unwrap();
        drive(&mut peers, None);
        peers[0]
            .propose_membership(MembershipChange::AddLearner(4))
            .unwrap();
        drive(&mut peers, None);
        peers[0]
            .propose_membership(MembershipChange::Promote(4))
            .unwrap();
        drive(&mut peers, None);
        assert_eq!(peers[0].membership().voters, vec![1, 2, 3, 4]);
        assert_eq!(peers[0].membership().outgoing_voters, vec![1, 2, 3]);
        let before = peers[0].commit_index();
        let proposal = peers[0].propose(b"joint".to_vec()).unwrap();
        drive(&mut peers, Some(4));
        assert_eq!(peers[0].commit_index(), before);
        let ticks = peers[0].raw.raft.election_timeout();
        tick_leader(&mut peers, Some(4), ticks * 2);
        assert_eq!(peers[0].applied_index(), proposal.index);
        assert!(peers[0].witness_commit.as_ref().unwrap().count_witnesses);
        tick_leader(&mut peers, None, ticks * 2);
        assert!(!peers[0].witness_commit.as_ref().unwrap().count_witnesses);
        peers[0]
            .propose_membership(MembershipChange::LeaveJoint)
            .unwrap();
        drive(&mut peers, None);
        assert!(peers[0].membership().outgoing_voters.is_empty());
    }

    #[test]
    fn preference_starts_fresh_after_a_real_new_term() {
        let (_directory, mut peers) = peers(&[1, 2, 3], &[1, 2, 3], &[3]);
        peers[0].campaign().unwrap();
        drive(&mut peers, None);
        let ticks = peers[0].raw.raft.election_timeout();
        tick_leader(&mut peers, Some(2), ticks * 2);
        assert!(peers[0].witness_commit.as_ref().unwrap().count_witnesses);
        peers[1].force_campaign().unwrap();
        drive(&mut peers, None);
        assert!(peers[1].is_leader());
        assert!(!peers[0].witness_commit.as_ref().unwrap().count_witnesses);
        assert_eq!(peers[0].witness_commit.as_ref().unwrap().leader_term, None);
        assert!(!peers[1].witness_commit.as_ref().unwrap().count_witnesses);
        assert_eq!(
            peers[1].witness_commit.as_ref().unwrap().leader_term,
            Some(peers[1].term())
        );
    }
}
