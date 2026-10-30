// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]
use matrixraft::peer_runtime::{DurablePeer, MembershipChange, PeerOptions};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

struct Cluster {
    root: tempfile::TempDir,
    peers: BTreeMap<u64, DurablePeer>,
    values: BTreeMap<u64, RefCell<Vec<u64>>>,
    isolated: BTreeSet<u64>,
}
impl Cluster {
    fn new() -> Self {
        let mut result = Self {
            root: tempfile::tempdir().unwrap(),
            peers: BTreeMap::new(),
            values: BTreeMap::new(),
            isolated: BTreeSet::new(),
        };
        for id in 1..=3 {
            result.open(id);
        }
        result.pump();
        result.peers.get_mut(&1).unwrap().campaign().unwrap();
        result.pump();
        assert!(result.peers[&1].is_leader());
        result
    }
    fn options(&self, id: u64) -> PeerOptions {
        PeerOptions {
            group_id: 71,
            node_id: id,
            voters: vec![1, 2, 3],
            wal_path: self.root.path().join(format!("peer-{id}.wal")),
        }
    }
    fn open(&mut self, id: u64) {
        let peer =
            DurablePeer::open_with_membership(self.options(id), vec![1, 2, 3, 4, 5]).unwrap();
        self.peers.insert(id, peer);
        self.values.insert(id, RefCell::new(Vec::new()));
    }
    fn pump(&mut self) {
        for _ in 0..1000 {
            let mut packets = Vec::new();
            for (id, peer) in &mut self.peers {
                let values = &self.values[id];
                let outgoing = peer
                    .process_ready_with_snapshot(
                        |_, payload| {
                            if !payload.is_empty() {
                                assert_eq!(payload.len(), 8);
                                values
                                    .borrow_mut()
                                    .push(u64::from_be_bytes(payload.try_into().unwrap()));
                            }
                            Ok(())
                        },
                        |_, payload| {
                            assert_eq!(payload.len() % 8, 0);
                            *values.borrow_mut() = payload
                                .as_chunks::<8>()
                                .0
                                .iter()
                                .map(|bytes| u64::from_be_bytes(*bytes))
                                .collect();
                            Ok(())
                        },
                    )
                    .unwrap();
                if !self.isolated.contains(id) {
                    packets.extend(
                        outgoing
                            .into_iter()
                            .filter(|p| !self.isolated.contains(&p.to)),
                    );
                }
            }
            if packets.is_empty() {
                return;
            }
            for packet in packets {
                if let Some(peer) = self.peers.get_mut(&packet.to) {
                    if let Err(error) = peer.step(&packet.bytes) {
                        // An in-flight response can outlive removal from the tracker.
                        assert!(
                            error.to_string().contains("cannot step as peer not found"),
                            "{error}"
                        );
                    }
                }
            }
        }
        panic!("membership packet exchange did not settle");
    }
    fn tick(&mut self) {
        for peer in self.peers.values_mut() {
            peer.tick().unwrap();
        }
        self.pump();
    }
    fn change(&mut self, leader: u64, change: MembershipChange) {
        self.pump();
        let log = self
            .peers
            .get_mut(&leader)
            .unwrap()
            .propose_membership(change)
            .unwrap();
        self.pump();
        assert!(self.peers[&leader].applied_index() >= log.index);
    }
    fn write(&mut self, leader: u64, value: u64) {
        self.peers
            .get_mut(&leader)
            .unwrap()
            .propose(value.to_be_bytes().to_vec())
            .unwrap();
        self.pump();
        assert!(self.values[&leader].borrow().contains(&value));
    }
    fn checkpoint(&mut self, id: u64) {
        self.pump();
        let bytes = self.values[&id]
            .borrow()
            .iter()
            .flat_map(|value| value.to_be_bytes())
            .collect();
        self.peers.get_mut(&id).unwrap().checkpoint(bytes).unwrap();
    }
    fn add_caught_up_learner(&mut self) {
        self.open(4);
        self.change(1, MembershipChange::AddLearner(4));
        for _ in 0..100 {
            if self.values[&4].borrow().as_slice() == self.values[&1].borrow().as_slice()
                && self.peers[&4].applied_index() == self.peers[&1].applied_index()
            {
                break;
            }
            self.tick();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(*self.values[&4].borrow(), *self.values[&1].borrow());
        assert_eq!(self.peers[&1].membership().learners, vec![4]);
    }
    fn reopen_all(&mut self) {
        let ids: Vec<_> = self.peers.keys().copied().collect();
        self.peers.clear();
        for id in ids {
            self.open(id);
        }
        self.pump();
    }
}

#[test]
fn learner_promotion_and_removal_replay_from_committed_history() {
    let mut c = Cluster::new();
    c.write(1, 11);
    c.add_caught_up_learner();
    assert!(c.peers.get_mut(&4).unwrap().propose(vec![1]).is_err());
    c.change(1, MembershipChange::Promote(4));
    assert_eq!(c.peers[&1].membership().voters, vec![1, 2, 3, 4]);
    assert_eq!(c.peers[&1].membership().outgoing_voters, vec![1, 2, 3]);
    assert!(c
        .peers
        .get_mut(&1)
        .unwrap()
        .propose_membership(MembershipChange::Remove(3))
        .is_err());
    c.change(1, MembershipChange::LeaveJoint);
    c.change(1, MembershipChange::Remove(3));
    assert_eq!(c.peers[&1].membership().outgoing_voters, vec![1, 2, 3, 4]);
    c.change(1, MembershipChange::LeaveJoint);
    c.write(1, 22);
    // The removed node need not learn the exit that no longer requires its vote.
    c.peers.remove(&3);
    c.reopen_all();
    for id in [1, 2, 4] {
        assert_eq!(c.peers[&id].membership().voters, vec![1, 2, 4]);
        assert!(c.peers[&id].membership().outgoing_voters.is_empty());
        assert_eq!(*c.values[&id].borrow(), vec![11, 22]);
    }
    c.peers.get_mut(&4).unwrap().campaign().unwrap();
    c.pump();
    assert!(c.peers[&4].is_leader());
    c.write(4, 33);
}

#[test]
fn joint_configuration_requires_both_majorities() {
    let mut c = Cluster::new();
    c.write(1, 1);
    c.add_caught_up_learner();
    c.change(1, MembershipChange::Promote(4));
    c.isolated = [3, 4].into_iter().collect();
    let pending = c
        .peers
        .get_mut(&1)
        .unwrap()
        .propose(2u64.to_be_bytes().to_vec())
        .unwrap();
    c.pump();
    assert!(c.peers[&1].commit_index() < pending.index);
    assert!(!c.values[&1].borrow().contains(&2));
    c.isolated.clear();
    for _ in 0..3 {
        c.tick();
    }
    assert!(c.values[&1].borrow().contains(&2));
    c.change(1, MembershipChange::LeaveJoint);
    c.change(1, MembershipChange::Remove(3));
    c.isolated = [2, 3].into_iter().collect();
    let pending = c
        .peers
        .get_mut(&1)
        .unwrap()
        .propose(3u64.to_be_bytes().to_vec())
        .unwrap();
    c.pump();
    assert!(c.peers[&1].commit_index() < pending.index);
    c.isolated.clear();
    for _ in 0..3 {
        c.tick();
    }
    assert!(c.values[&1].borrow().contains(&3));
}

#[test]
fn learner_catches_up_from_checkpoint_before_promotion() {
    let mut c = Cluster::new();
    for value in 1..=8 {
        c.write(1, value);
    }
    c.checkpoint(1);
    c.change(1, MembershipChange::AddLearner(4));
    assert!(c
        .peers
        .get_mut(&1)
        .unwrap()
        .propose_membership(MembershipChange::Promote(4))
        .is_err());
    // A compacted bootstrap image must include the newly committed learner.
    assert!(c.peers[&1].membership_checkpoint_needed());
    c.checkpoint(1);
    assert!(!c.peers[&1].membership_checkpoint_needed());
    c.open(4);
    c.pump();
    for _ in 0..100 {
        c.tick();
        if c.values[&4].borrow().len() == 8 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(*c.values[&4].borrow(), (1..=8).collect::<Vec<_>>());
    for _ in 0..3 {
        c.tick();
    }
    c.change(1, MembershipChange::Promote(4));
    c.checkpoint(1);
    c.reopen_all();
    assert_eq!(c.peers[&1].membership().outgoing_voters, vec![1, 2, 3]);
    assert_eq!(*c.values[&1].borrow(), (1..=8).collect::<Vec<_>>());
    c.peers.get_mut(&1).unwrap().campaign().unwrap();
    c.pump();
    c.change(1, MembershipChange::LeaveJoint);
    c.write(1, 9);
}

#[test]
fn inventory_and_mode_are_durable_and_uncommitted_changes_do_not_apply() {
    let mut c = Cluster::new();
    assert!(c
        .peers
        .get_mut(&1)
        .unwrap()
        .propose_membership(MembershipChange::AddLearner(9))
        .is_err());
    c.isolated = [2, 3].into_iter().collect();
    c.peers
        .get_mut(&1)
        .unwrap()
        .propose_membership(MembershipChange::AddLearner(4))
        .unwrap();
    c.pump();
    assert!(c.peers[&1].membership().learners.is_empty());
    c.peers.remove(&1);
    assert!(DurablePeer::open(c.options(1)).is_err());
    assert!(DurablePeer::open_with_membership(c.options(1), vec![1, 2, 3, 4]).is_err());
    c.open(1);
    c.pump();
    assert!(c.peers[&1].membership().learners.is_empty());
}

#[test]
fn staged_checkpoint_retains_its_configuration_and_replays_later_changes() {
    let mut c = Cluster::new();
    c.write(1, 11);
    let ticket = c.peers[&1].prepare_checkpoint().unwrap();
    let frozen = 11u64.to_be_bytes();
    c.add_caught_up_learner();
    c.change(1, MembershipChange::Promote(4));
    c.change(1, MembershipChange::LeaveJoint);
    c.write(1, 22);
    let prepared = ticket
        .capture(&mut frozen.as_slice(), frozen.len() as u64)
        .unwrap();
    let expected = c.peers[&1].membership();
    assert!(c
        .peers
        .get_mut(&1)
        .unwrap()
        .install_checkpoint(&prepared)
        .unwrap());
    assert_eq!(c.peers[&1].membership(), expected);
    assert!(c.peers[&1].membership_checkpoint_needed());
    drop(prepared);
    c.reopen_all();
    for id in 1..=4 {
        assert_eq!(c.peers[&id].membership(), expected);
        assert_eq!(*c.values[&id].borrow(), vec![11, 22]);
    }
}

#[test]
fn joint_membership_survives_leader_loss_and_all_peer_restart() {
    let mut c = Cluster::new();
    c.write(1, 11);
    c.add_caught_up_learner();
    c.change(1, MembershipChange::Promote(4));
    c.reopen_all();
    c.isolated.insert(1);
    c.peers.get_mut(&2).unwrap().campaign().unwrap();
    c.pump();
    assert!(c.peers[&2].is_leader());
    assert_eq!(c.peers[&2].membership().outgoing_voters, vec![1, 2, 3]);
    c.change(2, MembershipChange::LeaveJoint);
    c.write(2, 22);
    c.isolated.clear();
    for _ in 0..3 {
        c.tick();
    }
    c.reopen_all();
    for id in 1..=4 {
        assert!(c.peers[&id].membership().outgoing_voters.is_empty());
        assert_eq!(*c.values[&id].borrow(), vec![11, 22]);
    }
}

#[test]
fn removed_leader_cannot_resume_writes_or_elect_itself() {
    let mut c = Cluster::new();
    c.write(1, 11);
    c.add_caught_up_learner();
    c.change(1, MembershipChange::Promote(4));
    c.change(1, MembershipChange::LeaveJoint);
    c.change(1, MembershipChange::Remove(1));
    c.change(1, MembershipChange::LeaveJoint);
    for _ in 0..3 {
        c.tick();
    }
    assert!(c
        .peers
        .get_mut(&1)
        .unwrap()
        .propose(99u64.to_be_bytes().to_vec())
        .is_err());
    assert!(c.peers.get_mut(&1).unwrap().campaign().is_err());
    c.pump();
    assert!(!c.peers[&1].is_leader());
    let mut leader = None;
    for _ in 0..100 {
        c.tick();
        leader = [2, 3, 4].into_iter().find(|id| c.peers[id].is_leader());
        if leader.is_some() {
            break;
        }
    }
    c.write(leader.expect("remaining voters elect a leader"), 22);
    assert!(!c.values[&1].borrow().contains(&99));
}

#[test]
fn committed_membership_invalidates_previous_read_confirmation() {
    let mut c = Cluster::new();
    c.write(1, 11);
    let token = c.peers.get_mut(&1).unwrap().begin_read().unwrap();
    c.pump();
    c.change(1, MembershipChange::AddLearner(4));
    assert!(c.peers.get_mut(&1).unwrap().poll_read(token).is_err());
    let token = c.peers.get_mut(&1).unwrap().begin_read().unwrap();
    c.pump();
    assert!(c.peers.get_mut(&1).unwrap().poll_read(token).unwrap());
}

#[test]
fn every_complete_wal_prefix_recovers_only_committed_membership() {
    let mut c = Cluster::new();
    let mut states = vec![(0, c.peers[&1].membership())];
    c.write(1, 11);
    c.add_caught_up_learner();
    states.push((c.peers[&1].applied_index(), c.peers[&1].membership()));
    for change in [
        MembershipChange::Promote(4),
        MembershipChange::LeaveJoint,
        MembershipChange::Remove(3),
        MembershipChange::LeaveJoint,
    ] {
        c.change(1, change);
        states.push((c.peers[&1].applied_index(), c.peers[&1].membership()));
    }
    c.peers.remove(&1);
    let options = c.options(1);
    let bytes = std::fs::read(&options.wal_path).unwrap();
    let mut end = 0;
    let mut prefixes = 0;
    while end < bytes.len() {
        let length = u32::from_be_bytes(bytes[end + 4..end + 8].try_into().unwrap()) as usize;
        end += 24 + length;
        assert!(end <= bytes.len());
        std::fs::write(&options.wal_path, &bytes[..end]).unwrap();
        let mut peer =
            DurablePeer::open_with_membership(options.clone(), vec![1, 2, 3, 4, 5]).unwrap();
        let expected = &states
            .iter()
            .rev()
            .find(|(index, _)| *index <= peer.commit_index())
            .unwrap()
            .1;
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert_eq!(&peer.membership(), expected);
        drop(peer);
        prefixes += 1;
    }
    assert!(prefixes > states.len());
    std::fs::write(&options.wal_path, bytes).unwrap();
}

#[test]
fn reopened_peer_replays_membership_before_processing_inputs() {
    let mut c = Cluster::new();
    c.write(1, 11);
    c.add_caught_up_learner();
    c.change(1, MembershipChange::Promote(4));
    c.change(1, MembershipChange::LeaveJoint);
    c.change(1, MembershipChange::Remove(1));
    c.change(1, MembershipChange::LeaveJoint);
    c.peers.remove(&1);
    let mut peer = DurablePeer::open_with_membership(c.options(1), vec![1, 2, 3, 4, 5]).unwrap();
    assert!(peer.campaign().is_err());
    assert!(peer.tick().is_err());
    assert!(peer
        .step(&[])
        .unwrap_err()
        .to_string()
        .contains("recover membership"));
    peer.process_ready(|_, _| Ok(())).unwrap();
    assert!(!peer.membership().voters.contains(&1));
    assert!(peer.campaign().is_err());
    peer.tick().unwrap();
}

#[test]
fn file_checkpoint_seeds_new_learner_and_survives_restart() {
    let mut c = Cluster::new();
    for value in 1..=16 {
        c.write(1, value);
    }
    c.change(1, MembershipChange::AddLearner(4));
    let bytes: Vec<u8> = c.values[&1]
        .borrow()
        .iter()
        .flat_map(|v| v.to_be_bytes())
        .collect();
    c.peers
        .get_mut(&1)
        .unwrap()
        .checkpoint_from_reader(&mut std::io::Cursor::new(&bytes), bytes.len() as u64)
        .unwrap();
    c.open(4);
    c.pump();
    for _ in 0..1000 {
        if *c.values[&4].borrow() == *c.values[&1].borrow() {
            break;
        }
        c.tick();
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(*c.values[&4].borrow(), *c.values[&1].borrow());
    assert_eq!(c.peers[&4].membership().learners, vec![4]);
    c.change(1, MembershipChange::Promote(4));
    c.change(1, MembershipChange::LeaveJoint);
    c.write(1, 17);
    c.reopen_all();
    for peer in c.peers.values() {
        assert_eq!(peer.membership().voters, vec![1, 2, 3, 4]);
    }
    for values in c.values.values() {
        assert_eq!(*values.borrow(), (1..=17).collect::<Vec<_>>());
    }
}
