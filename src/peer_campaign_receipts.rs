// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::{
    sync::Weak,
    time::{Duration, Instant},
};

/// Retained outcome of a local campaign request, not a current-leadership lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerCampaignStatus {
    Pending,
    Elected { term: u64 },
    Lost,
    TimedOut,
    Stopped,
    Failed,
}

/// A volatile campaign completion that does not retain the peer's storage lock.
#[derive(Debug)]
pub struct PeerCampaignReceipt {
    state: Arc<Mutex<PeerCampaignStatus>>,
    owner: Weak<File>,
}
impl PeerCampaignReceipt {
    pub fn status(&self) -> PeerCampaignStatus {
        *self.state.lock().unwrap()
    }
}

struct Pending {
    state: Arc<Mutex<PeerCampaignStatus>>,
    deadline: Option<Instant>,
}
pub(super) struct Tracker {
    limit: usize,
    pending: Vec<Pending>,
}
impl Tracker {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            pending: Vec::new(),
        }
    }
    fn finish(&mut self, status: PeerCampaignStatus) {
        for request in self.pending.drain(..) {
            *request.state.lock().unwrap() = status;
        }
    }
}

impl DurablePeer {
    /// Request an ordinary election and wait for local success or a terminal result.
    /// A positive timeout bounds waiting, not the core's election activity.
    pub fn campaign_tracked(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<PeerCampaignReceipt, RaftError> {
        self.start_campaign_receipt(false, timeout)
    }
    /// Request a forced campaign with the same eligibility and busy checks as
    /// force_campaign. Completion still requires real votes and durable Ready work.
    pub fn force_campaign_tracked(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<PeerCampaignReceipt, RaftError> {
        self.start_campaign_receipt(true, timeout)
    }
    fn start_campaign_receipt(
        &mut self,
        forced: bool,
        timeout: Option<Duration>,
    ) -> Result<PeerCampaignReceipt, RaftError> {
        self.service_campaign_receipts(false);
        self.healthy()?;
        let deadline = timeout
            .map(|duration| {
                if duration.is_zero() {
                    return Err(invalid("campaign timeout must be positive"));
                }
                Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| invalid("campaign deadline exceeds clock bounds"))
            })
            .transpose()?;
        let tracker = self
            .campaign_receipts
            .as_ref()
            .ok_or_else(|| invalid("campaign receipts are disabled"))?;
        if tracker.pending.len() >= tracker.limit {
            return Err(invalid("campaign receipt admission limit reached"));
        }
        let was_leader = self.is_leader();
        if forced {
            self.force_campaign()?;
        } else {
            self.campaign()?;
        }
        if self.raw.raft.state == StateRole::Follower {
            return Err(invalid("local campaign was not admitted"));
        }
        let state = Arc::new(Mutex::new(PeerCampaignStatus::Pending));
        self.campaign_receipts
            .as_mut()
            .unwrap()
            .pending
            .push(Pending {
                state: Arc::clone(&state),
                deadline,
            });
        let receipt = PeerCampaignReceipt {
            state,
            owner: Arc::downgrade(&self.journal._lock),
        };
        self.service_campaign_receipts(was_leader);
        Ok(receipt)
    }
    /// Service deadlines and failures. Drive Ready processing to publish a win.
    pub fn poll_campaign(
        &mut self,
        receipt: &PeerCampaignReceipt,
    ) -> Result<PeerCampaignStatus, RaftError> {
        if !receipt.owner.ptr_eq(&Arc::downgrade(&self.journal._lock)) {
            return Err(invalid("campaign receipt belongs to another open peer"));
        }
        self.service_campaign_receipts(false);
        Ok(receipt.status())
    }
    pub(super) fn campaign_receipts_due(&self) -> bool {
        let Some(tracker) = &self.campaign_receipts else {
            return false;
        };
        if tracker.pending.is_empty() {
            return false;
        }
        if matches!(self.raw.raft.state, StateRole::Leader | StateRole::Follower)
            || tracker
                .pending
                .iter()
                .any(|request| Arc::strong_count(&request.state) == 1)
        {
            return true;
        }
        tracker
            .pending
            .iter()
            .filter_map(|request| request.deadline)
            .min()
            .is_some_and(|deadline| Instant::now() >= deadline)
    }
    pub(super) fn service_campaign_receipts(&mut self, ready_completed: bool) {
        if self
            .campaign_receipts
            .as_ref()
            .is_none_or(|tracker| tracker.pending.is_empty())
        {
            return;
        }
        let role = self.raw.raft.state;
        let term = self.term();
        let elected = ready_completed
            && role == StateRole::Leader
            && self.raw.raft.raft_log.unstable_entries().is_empty()
            && self.raw.raft.raft_log.unstable_snapshot().is_none()
            && {
                let stored = self.store.memory.rl();
                stored.hard_state().term == term && stored.hard_state().vote == self.options.node_id
            };
        let terminal = if self.failed || self.store.cache.check().is_err() {
            Some(PeerCampaignStatus::Failed)
        } else if self.stopped || self.drain_through.is_some() {
            Some(PeerCampaignStatus::Stopped)
        } else {
            None
        };
        let tracker = self.campaign_receipts.as_mut().unwrap();
        let now = tracker
            .pending
            .iter()
            .any(|request| request.deadline.is_some())
            .then(Instant::now);
        tracker.pending.retain(|request| {
            if Arc::strong_count(&request.state) == 1 {
                return false;
            }
            let timed_out = request
                .deadline
                .is_some_and(|deadline| now.unwrap() >= deadline);
            let outcome = if terminal.is_some() {
                terminal
            } else if timed_out {
                Some(PeerCampaignStatus::TimedOut)
            } else if role == StateRole::Follower {
                Some(PeerCampaignStatus::Lost)
            } else if elected {
                Some(PeerCampaignStatus::Elected { term })
            } else {
                None
            };
            if let Some(outcome) = outcome {
                *request.state.lock().unwrap() = outcome;
                false
            } else {
                true
            }
        });
    }
    pub(super) fn finish_campaign_receipts(&mut self, status: PeerCampaignStatus) {
        if let Some(tracker) = &mut self.campaign_receipts {
            tracker.finish(status);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(path: PathBuf, voters: Vec<u64>) -> PeerOptions {
        PeerOptions {
            group_id: 31,
            node_id: 1,
            voters,
            wal_path: path,
        }
    }
    fn tuning(limit: usize) -> PeerTuning {
        PeerTuning {
            max_pending_campaign_receipts: Some(limit),
            ..Default::default()
        }
    }
    fn expire(peer: &mut DurablePeer, receipt: &PeerCampaignReceipt) {
        peer.campaign_receipts
            .as_mut()
            .unwrap()
            .pending
            .iter_mut()
            .find(|request| Arc::ptr_eq(&request.state, &receipt.state))
            .unwrap()
            .deadline = Some(Instant::now());
    }

    #[test]
    fn receipt_limits_validate_before_storage_and_reclaim_dropped_requests() {
        let dir = tempfile::tempdir().unwrap();
        for limit in [0, 65537] {
            let path = dir.path().join(format!("invalid-{limit}.wal"));
            assert!(
                DurablePeer::open_with_tuning(options(path.clone(), vec![1]), tuning(limit))
                    .is_err()
            );
            assert!(!path.exists());
        }
        let mut disabled =
            DurablePeer::open(options(dir.path().join("disabled.wal"), vec![1])).unwrap();
        assert!(disabled.campaign_tracked(None).is_err());
        assert!(disabled.force_campaign_tracked(None).is_err());
        assert_eq!(disabled.term(), 0);
        disabled.campaign().unwrap();
        disabled.process_ready(|_, _| Ok(())).unwrap();
        assert!(disabled.is_leader());
        let mut peer =
            DurablePeer::open_with_tuning(options(dir.path().join("peer.wal"), vec![1]), tuning(1))
                .unwrap();
        assert!(peer.campaign_tracked(Some(Duration::ZERO)).is_err());
        assert!(peer.campaign_tracked(Some(Duration::MAX)).is_err());
        assert_eq!(peer.term(), 0);
        let first = peer.campaign_tracked(None).unwrap();
        let term = peer.term();
        assert!(peer.campaign_tracked(None).is_err());
        assert_eq!(peer.term(), term);
        drop(first);
        let next = peer.campaign_tracked(None).unwrap();
        assert_eq!(next.status(), PeerCampaignStatus::Pending);
        assert_eq!(peer.campaign_receipts.as_ref().unwrap().pending.len(), 1);
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert_eq!(next.status(), PeerCampaignStatus::Elected { term });
    }

    #[test]
    fn a_win_waits_for_durable_ready_and_an_established_leader_completes_inline() {
        let dir = tempfile::tempdir().unwrap();
        let config = options(dir.path().join("peer.wal"), vec![1]);
        let mut peer = DurablePeer::open_with_tuning(config.clone(), tuning(4)).unwrap();
        let before = peer.persisted_wal_barriers().unwrap();
        let receipt = peer.force_campaign_tracked(None).unwrap();
        assert!(peer.is_leader());
        assert_eq!(receipt.status(), PeerCampaignStatus::Pending);
        assert_eq!(
            peer.poll_campaign(&receipt).unwrap(),
            PeerCampaignStatus::Pending
        );
        assert_eq!(peer.persisted_wal_barriers().unwrap(), before);
        peer.process_ready(|_, _| Ok(())).unwrap();
        let won = PeerCampaignStatus::Elected { term: peer.term() };
        assert_eq!(receipt.status(), won);
        assert!(peer.persisted_wal_barriers().unwrap() > before);
        assert_eq!(peer.store.memory.rl().hard_state().vote, 1);
        let barriers = peer.persisted_wal_barriers().unwrap();
        assert_eq!(peer.campaign_tracked(None).unwrap().status(), won);
        assert_eq!(peer.force_campaign_tracked(None).unwrap().status(), won);
        assert_eq!(peer.persisted_wal_barriers().unwrap(), barriers);
        assert!(!peer.has_ready());
        drop(peer);
        assert_eq!(receipt.status(), won);
        let mut reopened = DurablePeer::open_with_tuning(config, tuning(4)).unwrap();
        assert!(reopened.poll_campaign(&receipt).is_err());
    }

    #[test]
    fn expired_waits_are_reclaimed_from_ticks_ready_and_polls_without_canceling_elections() {
        let dir = tempfile::tempdir().unwrap();
        for mode in 0..3 {
            let mut peer = DurablePeer::open_with_tuning(
                options(dir.path().join(format!("{mode}.wal")), vec![1, 2, 3]),
                tuning(1),
            )
            .unwrap();
            let receipt = peer
                .campaign_tracked(Some(Duration::from_secs(60)))
                .unwrap();
            expire(&mut peer, &receipt);
            assert!(peer.has_ready());
            match mode {
                0 => peer.tick().unwrap(),
                1 => {
                    peer.process_ready(|_, _| Ok(())).unwrap();
                }
                _ => {
                    peer.poll_campaign(&receipt).unwrap();
                }
            }
            assert_eq!(receipt.status(), PeerCampaignStatus::TimedOut);
            assert!(matches!(
                peer.raw.raft.state,
                StateRole::Candidate | StateRole::PreCandidate
            ));
            assert!(peer.campaign_receipts.as_ref().unwrap().pending.is_empty());
            let _next = peer.campaign_tracked(None).unwrap();
            assert_eq!(peer.campaign_receipts.as_ref().unwrap().pending.len(), 1);
            assert_eq!(receipt.status(), PeerCampaignStatus::TimedOut);
        }
        let mut single = DurablePeer::open_with_tuning(
            options(dir.path().join("single.wal"), vec![1]),
            tuning(1),
        )
        .unwrap();
        let receipt = single
            .campaign_tracked(Some(Duration::from_secs(60)))
            .unwrap();
        expire(&mut single, &receipt);
        single.process_ready(|_, _| Ok(())).unwrap();
        assert!(single.is_leader());
        assert_eq!(receipt.status(), PeerCampaignStatus::TimedOut);
    }

    #[test]
    fn forced_campaign_busy_and_shared_waiting_preserve_admission_budgets() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = DurablePeer::open_with_tuning(
            options(dir.path().join("peer.wal"), vec![1, 2, 3]),
            tuning(2),
        )
        .unwrap();
        let first = peer.force_campaign_tracked(None).unwrap();
        let term = peer.term();
        assert!(peer.force_campaign_tracked(None).is_err());
        assert_eq!(peer.term(), term);
        assert_eq!(peer.campaign_receipts.as_ref().unwrap().pending.len(), 1);
        let second = peer.campaign_tracked(None).unwrap();
        let term = peer.term();
        assert!(peer.campaign_tracked(None).is_err());
        assert_eq!(peer.term(), term);
        assert_eq!(first.status(), PeerCampaignStatus::Pending);
        assert_eq!(second.status(), PeerCampaignStatus::Pending);
    }

    #[test]
    fn stop_shutdown_drop_and_other_owners_never_resurrect_a_wait() {
        let dir = tempfile::tempdir().unwrap();
        for mode in 0..3 {
            let config = options(dir.path().join(format!("{mode}.wal")), vec![1, 2, 3]);
            let mut peer = DurablePeer::open_with_tuning(config.clone(), tuning(1)).unwrap();
            let receipt = peer.campaign_tracked(None).unwrap();
            let mut other = DurablePeer::open_with_tuning(
                options(dir.path().join(format!("other-{mode}.wal")), vec![1]),
                tuning(1),
            )
            .unwrap();
            assert!(other.poll_campaign(&receipt).is_err());
            if mode == 0 {
                peer.stop();
            }
            if mode == 1 {
                assert_eq!(peer.begin_shutdown().unwrap(), 0);
            }
            drop(peer);
            assert_eq!(receipt.status(), PeerCampaignStatus::Stopped);
            let mut reopened = DurablePeer::open_with_tuning(config, tuning(1)).unwrap();
            assert!(reopened.poll_campaign(&receipt).is_err());
            assert_eq!(receipt.status(), PeerCampaignStatus::Stopped);
        }
    }

    #[test]
    fn durable_wins_survive_later_application_failure_but_storage_errors_fail_waiting() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = DurablePeer::open_with_tuning(
            options(dir.path().join("application.wal"), vec![1]),
            tuning(1),
        )
        .unwrap();
        let receipt = peer.campaign_tracked(None).unwrap();
        let elected = PeerCampaignStatus::Elected { term: peer.term() };
        assert!(peer
            .process_ready(|_, _| {
                assert_eq!(receipt.status(), elected);
                Err(storage_error("application unavailable"))
            })
            .is_err());
        assert!(peer.failed);
        assert_eq!(receipt.status(), elected);
        #[cfg(target_os = "linux")]
        {
            let mut peer = DurablePeer::open_with_tuning(
                options(dir.path().join("storage.wal"), vec![1]),
                tuning(1),
            )
            .unwrap();
            let receipt = peer.campaign_tracked(None).unwrap();
            let original =
                std::mem::replace(&mut peer.journal.file, File::open("/dev/null").unwrap());
            let mut applied = false;
            assert!(peer
                .process_ready(|_, _| {
                    applied = true;
                    Ok(())
                })
                .is_err());
            peer.journal.file = original;
            assert!(!applied);
            assert_eq!(receipt.status(), PeerCampaignStatus::Failed);
        }
        let mut peer = DurablePeer::open_with_tuning(
            options(dir.path().join("guard.wal"), vec![1]),
            tuning(1),
        )
        .unwrap();
        let receipt = peer.campaign_tracked(None).unwrap();
        peer.failed = true;
        assert!(peer.process_ready(|_, _| Ok(())).is_err());
        assert_eq!(receipt.status(), PeerCampaignStatus::Failed);
    }

    #[test]
    fn election_completion_does_not_wait_for_the_application_worker() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = tuning(1);
        config.max_inflight_apply_tasks = Some(1);
        let mut peer =
            DurablePeer::open_with_tuning(options(dir.path().join("peer.wal"), vec![1]), config)
                .unwrap();
        let receipt = peer.campaign_tracked(None).unwrap();
        peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
            .unwrap();
        assert_eq!(
            receipt.status(),
            PeerCampaignStatus::Elected { term: peer.term() }
        );
        assert_eq!(peer.applied_index(), 0);
        let mut tasks = peer.take_application_tasks().unwrap();
        assert_eq!(tasks.len(), 1);
        peer.complete_application_task(tasks.pop().unwrap().finish())
            .unwrap();
        assert!(peer.applied_index() > 0);
    }

    #[test]
    fn a_due_campaign_expires_before_retrying_deferred_application_without_new_ready() {
        fn exchange(peers: &mut BTreeMap<u64, DurablePeer>) {
            for _ in 0..100 {
                let mut packets = Vec::new();
                for (&id, peer) in peers.iter_mut() {
                    packets.extend(
                        peer.process_ready_batch(|batch| {
                            if id == 1 && batch.iter().any(|entry| entry.payload == [91]) {
                                Ok(0)
                            } else {
                                Ok(batch.len())
                            }
                        })
                        .unwrap(),
                    );
                }
                if packets.is_empty() {
                    return;
                }
                for packet in packets {
                    peers
                        .get_mut(&packet.to)
                        .unwrap()
                        .step(&packet.bytes)
                        .unwrap();
                }
            }
            panic!("campaign deadline fixture did not settle");
        }
        let dir = tempfile::tempdir().unwrap();
        let mut peers: BTreeMap<_, _> = (1..=3)
            .map(|id| {
                let mut config = options(dir.path().join(format!("{id}.wal")), vec![1, 2, 3]);
                config.node_id = id;
                (
                    id,
                    DurablePeer::open_with_tuning(config, tuning(1)).unwrap(),
                )
            })
            .collect();
        peers.get_mut(&2).unwrap().campaign().unwrap();
        exchange(&mut peers);
        let position = peers.get_mut(&2).unwrap().propose(vec![91]).unwrap();
        exchange(&mut peers);
        let peer = peers.get_mut(&1).unwrap();
        assert!(peer.commit_index() >= position.index);
        assert!(peer.applied_index() < position.index);
        let receipt = peer
            .campaign_tracked(Some(Duration::from_secs(60)))
            .unwrap();
        peer.process_ready_batch(|_| Ok(0)).unwrap();
        assert!(!peer.raw.has_ready());
        assert_eq!(receipt.status(), PeerCampaignStatus::Pending);
        expire(peer, &receipt);
        let mut retried = false;
        peer.process_ready_batch(|batch| {
            assert!(batch.iter().any(|entry| entry.payload == [91]));
            assert_eq!(receipt.status(), PeerCampaignStatus::TimedOut);
            retried = true;
            Ok(0)
        })
        .unwrap();
        assert!(retried);
    }

    #[test]
    fn uncommitted_bootstrap_peers_cannot_request_campaign_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = options(dir.path().join("peer.wal"), vec![1, 2, 3]);
        config.node_id = 4;
        let mut peer =
            DurablePeer::open_with_membership_and_tuning(config, vec![1, 2, 3, 4], tuning(2))
                .unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let bytes = peer.persisted_wal_bytes().unwrap();
        assert!(peer.campaign_tracked(None).is_err());
        assert!(peer.force_campaign_tracked(None).is_err());
        assert!(peer.campaign_receipts.as_ref().unwrap().pending.is_empty());
        assert_eq!(peer.persisted_wal_bytes().unwrap(), bytes);
        assert_eq!(peer.term(), 0);
        let mut config = tuning(1);
        config.startup_election_delay = Some(Duration::from_secs(60));
        let mut held =
            DurablePeer::open_with_tuning(options(dir.path().join("held.wal"), vec![1]), config)
                .unwrap();
        assert!(held.campaign_tracked(None).is_err());
        assert_eq!(held.term(), 0);
        let forced = held.force_campaign_tracked(None).unwrap();
        assert_eq!(forced.status(), PeerCampaignStatus::Pending);
        held.process_ready(|_, _| Ok(())).unwrap();
        assert_eq!(
            forced.status(),
            PeerCampaignStatus::Elected { term: held.term() }
        );
    }
}
