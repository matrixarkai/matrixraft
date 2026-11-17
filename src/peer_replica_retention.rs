// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

struct Contact {
    age: usize,
    heard: bool,
}
pub(super) struct Window {
    timeout: usize,
    term: Option<u64>,
    contacts: BTreeMap<u64, Contact>,
}
impl Window {
    pub(super) fn new(timeout: usize) -> Self {
        Self {
            timeout,
            term: None,
            contacts: BTreeMap::new(),
        }
    }
    fn recent(&self, id: u64, term: u64, require_response: bool) -> bool {
        if self.term != Some(term) {
            return !require_response;
        }
        self.contacts.get(&id).map_or(!require_response, |contact| {
            contact.age < self.timeout && (!require_response || contact.heard)
        })
    }
}

impl DurablePeer {
    /// Retention boundary supported by local application and every protected
    /// replica's actual acknowledged log position. Followers use local application.
    /// With no inactivity window, every configured replica remains protected.
    pub fn replication_retention_index(&self) -> Result<u64, RaftError> {
        self.healthy()?;
        self.membership_recovered()?;
        let mut index = self.applied_index();
        if self.is_leader() {
            for (&id, progress) in self.raw.raft.prs().iter() {
                if id != self.node_id() && self.replica_recent(id, false, true) {
                    index = index.min(progress.matched);
                }
            }
        }
        Ok(index)
    }

    /// Trim only history covered by durable application and the retention boundary.
    /// Returns the resulting log floor, which never moves backward. A replica
    /// returning after expiry may require a real checkpoint from the host.
    pub fn trim_flushed_log(&mut self) -> Result<u64, RaftError> {
        let index = self
            .replication_retention_index()?
            .min(self.journal.flushed_index);
        self.trim_log_to(index)?;
        self.store
            .first_index()
            .map(|first| first.saturating_sub(1))
            .map_err(storage_error)
    }

    pub(super) fn replica_recent(&self, id: u64, require_response: bool, fallback: bool) -> bool {
        self.replica_contacts.as_ref().map_or(fallback, |window| {
            window.recent(id, self.term(), require_response)
        })
    }
    pub(super) fn align_replica_contacts(&mut self) {
        self.reconcile_replica_contacts(false);
    }
    pub(super) fn refresh_replica_contacts(&mut self) {
        self.reconcile_replica_contacts(true);
    }
    fn reconcile_replica_contacts(&mut self, membership_changed: bool) {
        if self.replica_contacts.is_none() {
            return;
        }
        let term = self.term();
        let leader = self.is_leader();
        let local = self.node_id();
        let progress = self.raw.raft.prs();
        let window = self.replica_contacts.as_mut().unwrap();
        if !leader || window.term != Some(term) {
            window.contacts.clear();
            window.term = leader.then_some(term);
        } else if !membership_changed {
            return;
        }
        if !leader {
            return;
        }
        window
            .contacts
            .retain(|id, _| *id != local && progress.get(*id).is_some());
        for (&id, _) in progress.iter() {
            if id != local {
                window.contacts.entry(id).or_insert(Contact {
                    age: 0,
                    heard: false,
                });
            }
        }
    }
    pub(super) fn tick_replica_contacts(&mut self) {
        self.align_replica_contacts();
        if let Some(window) = self
            .replica_contacts
            .as_mut()
            .filter(|window| window.term.is_some())
        {
            for contact in window.contacts.values_mut() {
                contact.age = contact.age.saturating_add(1).min(window.timeout);
            }
        }
    }
    pub(super) fn note_replica_response(&mut self, from: u64, term: u64) {
        if self.replica_contacts.is_none() || !self.is_leader() || self.term() != term {
            return;
        }
        self.align_replica_contacts();
        if let Some(contact) = self
            .replica_contacts
            .as_mut()
            .unwrap()
            .contacts
            .get_mut(&from)
        {
            contact.age = 0;
            contact.heard = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(path: PathBuf, id: u64) -> PeerOptions {
        PeerOptions {
            group_id: 41,
            node_id: id,
            voters: vec![1],
            wal_path: path,
        }
    }
    fn tuning(timeout: Option<usize>) -> PeerTuning {
        PeerTuning {
            replica_inactivity_tick: timeout,
            max_checkpoint_sends: Some(1),
            ..Default::default()
        }
    }
    fn settle(peer: &mut DurablePeer) {
        for _ in 0..32 {
            peer.process_ready(|_, _| Ok(())).unwrap();
            if !peer.has_ready() {
                return;
            }
        }
        panic!("local peer work did not settle");
    }
    fn leader(path: PathBuf, timeout: Option<usize>) -> DurablePeer {
        let mut peer = DurablePeer::open_with_membership_and_tuning(
            options(path, 1),
            vec![1, 2, 3],
            tuning(timeout),
        )
        .unwrap();
        peer.campaign().unwrap();
        settle(&mut peer);
        peer
    }
    fn change(peer: &mut DurablePeer, change: MembershipChange) {
        peer.propose_membership(change).unwrap();
        settle(peer);
    }
    fn ticks(peer: &mut DurablePeer, count: usize) {
        for _ in 0..count {
            peer.tick().unwrap();
            settle(peer);
        }
    }

    #[test]
    fn new_members_receive_fresh_grace_without_restarting_existing_clocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer.wal");
        let mut peer = leader(path.clone(), Some(3));
        change(&mut peer, MembershipChange::AddLearner(2));
        ticks(&mut peer, 2);
        assert_eq!(peer.replica_contacts.as_ref().unwrap().contacts[&2].age, 2);
        change(&mut peer, MembershipChange::AddLearner(3));
        let window = peer.replica_contacts.as_ref().unwrap();
        assert_eq!(window.contacts[&2].age, 2);
        assert_eq!(window.contacts[&3].age, 0);
        assert!(!window.contacts[&3].heard);
        ticks(&mut peer, 1);
        assert!(!peer.replica_recent(2, false, true));
        assert!(peer.replica_recent(3, false, true));
        assert_eq!(peer.replication_retention_index().unwrap(), 0);
        ticks(&mut peer, 2);
        assert_eq!(
            peer.replication_retention_index().unwrap(),
            peer.applied_index()
        );
        change(&mut peer, MembershipChange::Remove(2));
        if !peer.membership().outgoing_voters.is_empty() {
            change(&mut peer, MembershipChange::LeaveJoint);
        }
        assert!(!peer
            .replica_contacts
            .as_ref()
            .unwrap()
            .contacts
            .contains_key(&2));
        change(&mut peer, MembershipChange::AddLearner(2));
        assert_eq!(peer.replica_contacts.as_ref().unwrap().contacts[&2].age, 0);
        assert_eq!(peer.replica_contacts.as_ref().unwrap().contacts[&3].age, 3);
        peer.stop();
        assert!(peer.replica_contacts.as_ref().unwrap().term.is_none());
        assert!(peer.replica_contacts.as_ref().unwrap().contacts.is_empty());
        drop(peer);
        let mut peer = DurablePeer::open_with_membership_and_tuning(
            options(path, 1),
            vec![1, 2, 3],
            tuning(Some(3)),
        )
        .unwrap();
        settle(&mut peer);
        peer.campaign().unwrap();
        settle(&mut peer);
        assert!(peer
            .replica_contacts
            .as_ref()
            .unwrap()
            .contacts
            .values()
            .all(|contact| contact.age == 0 && !contact.heard));
        assert_eq!(peer.replication_retention_index().unwrap(), 0);
    }

    #[test]
    fn invalid_windows_fail_before_storage_and_disabled_mode_retains_every_replica() {
        assert_eq!(
            std::mem::size_of::<Option<Box<Window>>>(),
            std::mem::size_of::<usize>()
        );
        let dir = tempfile::tempdir().unwrap();
        for timeout in [0, usize::MAX / 2 + 1, usize::MAX] {
            let path = dir.path().join(format!("{timeout}.wal"));
            assert!(
                DurablePeer::open_with_tuning(options(path.clone(), 1), tuning(Some(timeout)))
                    .is_err()
            );
            assert!(!path.exists());
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        let mut peer = leader(dir.path().join("disabled.wal"), None);
        change(&mut peer, MembershipChange::AddLearner(2));
        ticks(&mut peer, 32);
        assert!(peer.replica_contacts.is_none());
        assert_eq!(peer.replication_retention_index().unwrap(), 0);
        peer.set_flushed_index(peer.applied_index()).unwrap();
        assert_eq!(peer.trim_flushed_log().unwrap(), 0);
        peer.trim_log_to(peer.applied_index()).unwrap();
        assert_eq!(peer.trim_flushed_log().unwrap(), peer.applied_index());
    }

    #[test]
    fn only_live_checkpoint_transport_acknowledgments_refresh_contact_not_log_match() {
        let dir = tempfile::tempdir().unwrap();
        let mut sender = leader(dir.path().join("sender.wal"), Some(3));
        change(&mut sender, MembershipChange::AddLearner(2));
        sender.propose(vec![42]).unwrap();
        settle(&mut sender);
        sender.set_flushed_index(sender.applied_index()).unwrap();
        ticks(&mut sender, 3);
        sender.trim_flushed_log().unwrap();
        sender.checkpoint(b"application image".to_vec()).unwrap();
        let mut receiver = DurablePeer::open_with_membership_and_tuning(
            options(dir.path().join("receiver.wal"), 2),
            vec![1, 2, 3],
            tuning(Some(3)),
        )
        .unwrap();
        settle(&mut receiver);
        let mut held = None;
        for _ in 0..16 {
            sender.tick().unwrap();
            for packet in sender.process_ready(|_, _| Ok(())).unwrap() {
                if packet.to != 2 {
                    continue;
                }
                let (version, _, _) = decode_canonical_packet(&packet.bytes).unwrap();
                if version == 2 {
                    held = Some(packet);
                } else {
                    receiver.step(&packet.bytes).unwrap();
                }
            }
            for reply in receiver.process_ready(|_, _| Ok(())).unwrap() {
                sender.step(&reply.bytes).unwrap();
            }
            if held.is_some() {
                break;
            }
        }
        let held = held.expect("the real replication probe must start a framed checkpoint");
        ticks(&mut sender, 3);
        assert_eq!(
            sender.replication_retention_index().unwrap(),
            sender.applied_index()
        );
        receiver.step(&held.bytes).unwrap();
        let restored = std::cell::Cell::new(false);
        let replies = receiver
            .process_ready_with_snapshot(
                |_, _| Ok(()),
                |_, bytes| {
                    assert_eq!(bytes, b"application image");
                    restored.set(true);
                    Ok(())
                },
            )
            .unwrap();
        assert!(restored.get());
        assert_eq!(
            receiver.replication_retention_index().unwrap(),
            receiver.applied_index()
        );
        let ack = replies
            .into_iter()
            .find(|reply| decode_canonical_packet(&reply.bytes).unwrap().0 == 2)
            .expect("persisted checkpoint must return a transport acknowledgment");
        let commit = sender.commit_index();
        sender.step(&ack.bytes).unwrap();
        assert_eq!(sender.commit_index(), commit);
        assert_eq!(sender.raw.raft.prs().get(2).unwrap().matched, 0);
        assert_eq!(sender.replication_retention_index().unwrap(), 0);
        assert_eq!(sender.trim_flushed_log().unwrap(), sender.applied_index());
        ticks(&mut sender, 3);
        sender.step(&ack.bytes).unwrap();
        assert_eq!(
            sender.replication_retention_index().unwrap(),
            sender.applied_index()
        );
        assert_eq!(sender.raw.raft.prs().get(2).unwrap().matched, 0);
    }
}
