// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Weak,
    },
    time::{Duration, Instant},
};

/// A volatile receipt for an admitted log operation and its ordered completion
/// boundary. It retains no payload and does not keep the storage lock alive.
#[derive(Debug)]
pub struct PeerProposalReceipt {
    position: LogId,
    state: Arc<AtomicU8>,
    owner: Weak<File>,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn options(path: PathBuf) -> PeerOptions {
        PeerOptions {
            group_id: 23,
            node_id: 1,
            voters: vec![1],
            wal_path: path,
        }
    }
    fn tuning() -> PeerTuning {
        PeerTuning {
            max_pending_proposal_receipts: Some(4),
            ..Default::default()
        }
    }
    fn leader(path: PathBuf) -> DurablePeer {
        let mut peer = DurablePeer::open_with_tuning(options(path), tuning()).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        peer
    }

    #[test]
    fn receipt_admission_is_bounded_atomic_and_term_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = leader(dir.path().join("peer.wal"));
        let before = peer.raw.raft.raft_log.last_index();
        assert!(peer
            .propose_tracked_in_term(peer.term() + 1, vec![1], None)
            .is_err());
        assert!(peer.propose_tracked(vec![1], Some(Duration::ZERO)).is_err());
        assert!(peer.propose_tracked(vec![1], Some(Duration::MAX)).is_err());
        assert!(peer.propose_batch_tracked(vec![vec![1]; 5], None).is_err());
        assert_eq!(peer.raw.raft.raft_log.last_index(), before);
        let receipts = peer.propose_batch_tracked(vec![vec![1]; 4], None).unwrap();
        assert_eq!(receipts[3].position().index, before + 4);
        assert!(receipts
            .iter()
            .all(|r| r.status() == PeerProposalStatus::Pending));
        assert!(peer.propose_tracked(vec![2], None).is_err());
        assert_eq!(peer.raw.raft.raft_log.last_index(), before + 4);
        drop(receipts);
        let receipt = peer
            .propose_tracked_in_term(peer.term(), vec![2], None)
            .unwrap();
        assert_eq!(peer.proposal_receipts.as_ref().unwrap().pending.len(), 1);
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert_eq!(receipt.status(), PeerProposalStatus::Applied);
        peer.checkpoint(vec![9]).unwrap();
        assert_eq!(
            peer.poll_proposal(&receipt).unwrap(),
            PeerProposalStatus::Applied
        );
    }

    #[test]
    fn timeout_reclaims_waiting_without_rolling_back_the_entry() {
        let dir = tempfile::tempdir().unwrap();
        for mode in 0..3 {
            let mut peer = leader(dir.path().join(format!("{mode}.wal")));
            peer.proposal_receipts.as_mut().unwrap().limit = 1;
            let receipt = peer
                .propose_tracked(vec![7], Some(Duration::from_secs(60)))
                .unwrap();
            let position = receipt.position();
            peer.proposal_receipts
                .as_mut()
                .unwrap()
                .pending
                .get_mut(&position.index)
                .unwrap()
                .deadline = Some(Instant::now());
            assert!(peer.has_ready());
            match mode {
                0 => peer.tick().unwrap(),
                1 => {
                    peer.process_ready_retryable(|_, _| Ok(ApplyDecision::Retry))
                        .unwrap();
                }
                _ => {
                    assert_eq!(
                        peer.poll_proposal(&receipt).unwrap(),
                        PeerProposalStatus::TimedOut
                    );
                }
            }
            let next = peer.propose_tracked(vec![8], None).unwrap();
            let mut values = Vec::new();
            peer.process_ready(|_, payload| {
                if !payload.is_empty() {
                    values.push(payload.to_vec());
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(values, vec![vec![7], vec![8]]);
            assert_eq!(receipt.status(), PeerProposalStatus::TimedOut);
            assert_eq!(next.status(), PeerProposalStatus::Applied);
        }
    }

    #[test]
    fn receipts_follow_partial_application_and_graceful_drain() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = leader(dir.path().join("peer.wal"));
        let receipts = peer
            .propose_batch_tracked(vec![vec![1], vec![2], vec![3]], None)
            .unwrap();
        peer.process_ready_batch(|_| Ok(1)).unwrap();
        assert_eq!(receipts[0].status(), PeerProposalStatus::Applied);
        assert_eq!(receipts[1].status(), PeerProposalStatus::Pending);
        assert_eq!(receipts[2].status(), PeerProposalStatus::Pending);
        let fence = peer.begin_shutdown().unwrap();
        assert_eq!(fence, receipts[2].position().index);
        assert_eq!(receipts[1].status(), PeerProposalStatus::Pending);
        while !peer.poll_shutdown().unwrap() {
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        assert!(receipts
            .iter()
            .all(|r| r.status() == PeerProposalStatus::Applied));
    }

    #[test]
    fn callback_failure_stop_drop_and_reopen_never_report_false_success() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer.wal");
        let mut peer = leader(path.clone());
        let receipts = peer
            .propose_batch_tracked(vec![vec![1], vec![2]], None)
            .unwrap();
        assert!(peer
            .process_ready(|_, payload| if payload == [2] {
                Err(storage_error("application refused"))
            } else {
                Ok(())
            })
            .is_err());
        assert_eq!(receipts[0].status(), PeerProposalStatus::Applied);
        assert_eq!(receipts[1].status(), PeerProposalStatus::Failed);
        drop(peer);
        let mut reopened = DurablePeer::open_with_tuning(options(path), tuning()).unwrap();
        assert!(reopened.poll_proposal(&receipts[0]).is_err());
        let mut other = leader(dir.path().join("other.wal"));
        assert!(other.poll_proposal(&receipts[1]).is_err());
        let stopped = other.propose_tracked(vec![3], None).unwrap();
        other.stop();
        assert_eq!(stopped.status(), PeerProposalStatus::Stopped);
        let path = dir.path().join("dropped.wal");
        let mut dropped = leader(path.clone());
        let receipt = dropped.propose_tracked(vec![4], None).unwrap();
        drop(dropped);
        assert_eq!(receipt.status(), PeerProposalStatus::Stopped);
        assert!(
            DurablePeer::open(options(path)).is_ok(),
            "receipts must not retain the storage lock"
        );
    }

    #[test]
    fn async_receipts_wait_for_the_contiguous_worker_completion_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = tuning();
        config.max_inflight_apply_tasks = Some(4);
        config.max_apply_bytes = Some(1);
        let mut peer =
            DurablePeer::open_with_tuning(options(dir.path().join("peer.wal")), config).unwrap();
        peer.campaign().unwrap();
        for _ in 0..8 {
            peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
                .unwrap();
            for task in peer.take_application_tasks().unwrap() {
                peer.complete_application_task(task.finish()).unwrap();
            }
        }
        let receipts = peer
            .propose_batch_tracked(vec![vec![1], vec![2]], None)
            .unwrap();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
                .unwrap();
            tasks.extend(peer.take_application_tasks().unwrap());
        }
        assert_eq!(tasks.len(), 2);
        assert!(peer.raw.raft.raft_log.applied >= receipts[1].position().index);
        peer.complete_application_task(tasks.pop().unwrap().finish())
            .unwrap();
        assert!(receipts
            .iter()
            .all(|r| r.status() == PeerProposalStatus::Pending));
        let completion = std::thread::spawn(move || tasks.pop().unwrap().finish())
            .join()
            .unwrap();
        peer.complete_application_task(completion).unwrap();
        assert!(receipts
            .iter()
            .all(|r| r.status() == PeerProposalStatus::Applied));
        let rejected = peer.propose_tracked(vec![3], None).unwrap();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
                .unwrap();
            tasks.extend(peer.take_application_tasks().unwrap());
            if !tasks.is_empty() {
                break;
            }
        }
        assert_eq!(tasks.len(), 1);
        let task = tasks.pop().unwrap();
        peer.complete_application_task(task.reject_after(0).unwrap())
            .unwrap();
        assert_eq!(
            peer.required_application_checkpoint(),
            Some(rejected.position().index)
        );
        assert_eq!(rejected.status(), PeerProposalStatus::LeadershipLost);
    }

    #[test]
    fn invalid_receipt_limits_fail_before_storage_and_default_stays_disabled() {
        assert_eq!(
            std::mem::size_of::<Option<Box<Tracker>>>(),
            std::mem::size_of::<usize>()
        );
        let dir = tempfile::tempdir().unwrap();
        for limit in [0, 65537, usize::MAX] {
            let config = PeerTuning {
                max_pending_proposal_receipts: Some(limit),
                ..Default::default()
            };
            let path = dir.path().join(format!("{limit}.wal"));
            assert!(DurablePeer::open_with_tuning(options(path.clone()), config).is_err());
            assert!(!path.exists());
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        let mut peer = DurablePeer::open(options(dir.path().join("default.wal"))).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let before = peer.raw.raft.raft_log.last_index();
        assert!(peer.proposal_receipts.is_none());
        assert!(peer.propose_tracked(vec![1], None).is_err());
        assert_eq!(peer.raw.raft.raft_log.last_index(), before);
        assert!(peer.propose(vec![1]).is_ok());
    }

    fn membership_leader(path: PathBuf, asynchronous: bool) -> DurablePeer {
        let mut config = tuning();
        if asynchronous {
            config.max_inflight_apply_tasks = Some(4);
            config.max_apply_bytes = Some(1);
        }
        let mut peer =
            DurablePeer::open_with_membership_and_tuning(options(path), vec![1, 2, 3], config)
                .unwrap();
        peer.campaign().unwrap();
        if asynchronous {
            for _ in 0..8 {
                peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
                    .unwrap();
                for task in peer.take_application_tasks().unwrap() {
                    peer.complete_application_task(task.finish()).unwrap();
                }
            }
        } else {
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        peer
    }

    #[test]
    fn membership_receipts_share_admission_and_timeouts_without_undoing_changes() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = membership_leader(dir.path().join("peer.wal"), false);
        peer.proposal_receipts.as_mut().unwrap().limit = 1;
        let write = peer.propose_tracked(vec![7], None).unwrap();
        let before = peer.raw.raft.raft_log.last_index();
        assert!(peer
            .propose_membership_tracked(MembershipChange::AddLearner(2), None)
            .is_err());
        assert_eq!(peer.raw.raft.raft_log.last_index(), before);
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert_eq!(write.status(), PeerProposalStatus::Applied);
        for operation in [
            MembershipChange::AddLearner(1),
            MembershipChange::AddLearner(9),
            MembershipChange::LeaveJoint,
        ] {
            assert!(peer
                .propose_membership_auto_tracked(operation, None)
                .is_err());
        }
        for timeout in [Duration::ZERO, Duration::MAX] {
            assert!(peer
                .propose_membership_tracked(MembershipChange::AddLearner(2), Some(timeout))
                .is_err());
        }
        assert_eq!(peer.raw.raft.raft_log.last_index(), before);
        assert!(peer.proposal_receipts.as_ref().unwrap().pending.is_empty());
        let change = peer
            .propose_membership_auto_tracked(
                MembershipChange::AddLearnerAuto(2),
                Some(Duration::from_secs(60)),
            )
            .unwrap();
        assert_eq!(change.status(), PeerProposalStatus::Pending);
        peer.proposal_receipts
            .as_mut()
            .unwrap()
            .pending
            .get_mut(&change.position().index)
            .unwrap()
            .deadline = Some(Instant::now());
        peer.tick().unwrap();
        assert_eq!(change.status(), PeerProposalStatus::TimedOut);
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert_eq!(peer.membership().learners, vec![2]);
        assert_eq!(peer.membership().automatic_learners, vec![2]);
        let remove = peer
            .propose_membership_auto_tracked(MembershipChange::Remove(2), None)
            .unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert_eq!(remove.status(), PeerProposalStatus::Applied);
        assert!(peer.membership().learners.is_empty());
        assert_eq!(change.status(), PeerProposalStatus::TimedOut);
    }

    #[test]
    fn membership_receipts_wait_for_ordered_workers_after_metadata_is_visible() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = membership_leader(dir.path().join("peer.wal"), true);
        let write = peer.propose_tracked(vec![8], None).unwrap();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
                .unwrap();
            tasks.extend(peer.take_application_tasks().unwrap());
        }
        assert_eq!(tasks.len(), 1);
        let change = peer
            .propose_membership_tracked(MembershipChange::AddLearner(2), None)
            .unwrap();
        for _ in 0..8 {
            peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
                .unwrap();
        }
        assert_eq!(peer.membership().learners, vec![2]);
        assert!(peer.raw.raft.raft_log.applied >= change.position().index);
        assert!(peer.applied_index() < change.position().index);
        assert_eq!(
            peer.poll_proposal(&change).unwrap(),
            PeerProposalStatus::Pending
        );
        let completion = std::thread::spawn(move || tasks.pop().unwrap().finish())
            .join()
            .unwrap();
        peer.complete_application_task(completion).unwrap();
        assert_eq!(write.status(), PeerProposalStatus::Applied);
        assert_eq!(change.status(), PeerProposalStatus::Applied);
        peer.checkpoint(vec![9]).unwrap();
        assert_eq!(
            peer.poll_proposal(&change).unwrap(),
            PeerProposalStatus::Applied
        );
    }

    #[test]
    fn membership_receipts_do_not_acknowledge_uncommitted_changes_during_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = membership_leader(dir.path().join("peer.wal"), false);
        let change = peer
            .propose_membership_auto_tracked(MembershipChange::AddLearner(2), None)
            .unwrap();
        let fence = peer.begin_shutdown().unwrap();
        assert!(fence < change.position().index);
        while !peer.poll_shutdown().unwrap() {
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        assert_eq!(change.status(), PeerProposalStatus::Stopped);
        assert!(peer.membership().learners.is_empty());
    }

    #[test]
    fn membership_receipts_complete_before_an_unrelated_later_callback_fails() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = membership_leader(dir.path().join("peer.wal"), false);
        let change = peer
            .propose_membership_tracked(MembershipChange::AddLearner(2), None)
            .unwrap();
        peer.propose(vec![99]).unwrap();
        assert!(peer
            .process_ready(|_, payload| {
                if payload == [99] {
                    assert_eq!(change.status(), PeerProposalStatus::Applied);
                    Err(storage_error("later application command refused"))
                } else {
                    Ok(())
                }
            })
            .is_err());
        assert_eq!(change.status(), PeerProposalStatus::Applied);
        assert_eq!(peer.membership().learners, vec![2]);
    }
}
impl PeerProposalReceipt {
    pub fn position(&self) -> LogId {
        self.position.clone()
    }
    /// Last outcome published by the exclusive peer worker. Drive that worker
    /// or call poll_proposal to service deadlines and leadership transitions.
    pub fn status(&self) -> PeerProposalStatus {
        match self.state.load(Ordering::Acquire) {
            0 => PeerProposalStatus::Pending,
            1 => PeerProposalStatus::Applied,
            2 => PeerProposalStatus::TimedOut,
            3 => PeerProposalStatus::LeadershipLost,
            4 => PeerProposalStatus::Stopped,
            _ => PeerProposalStatus::Failed,
        }
    }
}

/// Only Applied acknowledges the operation's committed, ordered application
/// boundary. Other terminal outcomes do not prove the operation was rolled back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PeerProposalStatus {
    Pending = 0,
    Applied = 1,
    TimedOut = 2,
    LeadershipLost = 3,
    Stopped = 4,
    Failed = 5,
}
struct Pending {
    term: u64,
    deadline: Option<Instant>,
    state: Arc<AtomicU8>,
    membership: Option<MembershipCompletion>,
}
struct MembershipCompletion {
    automatic: bool,
    waiting_exit: bool,
    apply_through: Option<u64>,
}
pub(super) struct Tracker {
    limit: usize,
    pending: BTreeMap<u64, Pending>,
}
impl Tracker {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            pending: BTreeMap::new(),
        }
    }
    pub(super) fn applied(&mut self, entries: &[Entry]) {
        // The application path supplies the exact consumed entries, not a core
        // applied cursor that can run ahead of host-worker completion.
        let mut membership_boundary = None;
        for entry in entries {
            if entry.entry_type != EntryType::EntryNormal as i32 {
                membership_boundary = Some(entry.index);
            }
            if let std::collections::btree_map::Entry::Occupied(slot) =
                self.pending.entry(entry.index)
            {
                if slot.get().membership.is_some() {
                    continue;
                }
                let pending = slot.remove();
                let outcome = if pending.term != entry.term {
                    PeerProposalStatus::LeadershipLost
                } else if pending
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    PeerProposalStatus::TimedOut
                } else {
                    PeerProposalStatus::Applied
                };
                pending.state.store(outcome as u8, Ordering::Release);
            }
        }
        if let Some(index) = membership_boundary {
            self.memberships_completed_through(index);
        }
    }
    fn memberships_completed_through(&mut self, index: u64) {
        let mut now = None;
        self.pending.retain(|_, pending| {
            if pending
                .membership
                .as_ref()
                .and_then(|completion| completion.apply_through)
                .is_some_and(|through| through <= index)
            {
                let outcome = if pending
                    .deadline
                    .is_some_and(|deadline| *now.get_or_insert_with(Instant::now) >= deadline)
                {
                    PeerProposalStatus::TimedOut
                } else {
                    PeerProposalStatus::Applied
                };
                pending.state.store(outcome as u8, Ordering::Release);
                false
            } else {
                true
            }
        });
    }
    pub(super) fn membership_applied(
        &mut self,
        entry: &Entry,
        automatic_joint: bool,
        leaving_joint: bool,
    ) {
        if let Some(pending) = self
            .pending
            .get_mut(&entry.index)
            .filter(|pending| pending.term == entry.term)
        {
            if let Some(completion) = &mut pending.membership {
                if completion.automatic && automatic_joint {
                    completion.waiting_exit = true;
                } else {
                    completion.apply_through = Some(entry.index);
                }
            }
        }
        if leaving_joint {
            // Automatic receipts retain their initial position but wait on the later exit.
            for (_, pending) in self.pending.range_mut(..entry.index) {
                if let Some(completion) = pending
                    .membership
                    .as_mut()
                    .filter(|completion| completion.waiting_exit)
                {
                    completion.waiting_exit = false;
                    completion.apply_through = Some(entry.index);
                }
            }
        }
    }
}

impl DurablePeer {
    /// Track one membership phase through committed metadata and ordered local
    /// application. Manual joint transitions need a separately requested exit.
    pub fn propose_membership_tracked(
        &mut self,
        operation: MembershipChange,
        timeout: Option<Duration>,
    ) -> Result<PeerProposalReceipt, RaftError> {
        self.propose_membership_with_receipt(operation, timeout, false)
    }
    /// Track an automatic membership transition through its committed joint exit
    /// and ordered application, not merely admission of its initial entry.
    pub fn propose_membership_auto_tracked(
        &mut self,
        operation: MembershipChange,
        timeout: Option<Duration>,
    ) -> Result<PeerProposalReceipt, RaftError> {
        self.propose_membership_with_receipt(operation, timeout, true)
    }
    fn propose_membership_with_receipt(
        &mut self,
        operation: MembershipChange,
        timeout: Option<Duration>,
        automatic: bool,
    ) -> Result<PeerProposalReceipt, RaftError> {
        let deadline = self.admit_proposal_receipts(1, timeout)?;
        let position = if automatic {
            self.propose_membership_auto(operation)?
        } else {
            self.propose_membership(operation)?
        };
        let receipt = self.register_proposal_receipt(position, deadline);
        self.proposal_receipts
            .as_mut()
            .unwrap()
            .pending
            .get_mut(&receipt.position.index)
            .unwrap()
            .membership = Some(MembershipCompletion {
            automatic,
            waiting_exit: false,
            apply_through: None,
        });
        Ok(receipt)
    }
    /// Admit one tracked proposal. A positive timeout bounds local waiting;
    /// None waits until application or a terminal lifecycle event.
    pub fn propose_tracked(
        &mut self,
        payload: Vec<u8>,
        timeout: Option<Duration>,
    ) -> Result<PeerProposalReceipt, RaftError> {
        self.propose_tracked_with_options(PeerProposalOptions::default(), payload, timeout)
    }
    /// Track a command or a term-fenced proposal through ordered application.
    pub fn propose_tracked_with_options(
        &mut self,
        options: PeerProposalOptions,
        payload: Vec<u8>,
        timeout: Option<Duration>,
    ) -> Result<PeerProposalReceipt, RaftError> {
        if let Some(term) = options.expected_term {
            self.require_proposal_term(term)?;
        }
        let deadline = self.admit_proposal_receipts(1, timeout)?;
        let position = self.propose_with_options(options, payload)?;
        Ok(self.register_proposal_receipt(position, deadline))
    }
    /// Fence tracked admission to the caller's expected leadership term.
    pub fn propose_tracked_in_term(
        &mut self,
        expected_term: u64,
        payload: Vec<u8>,
        timeout: Option<Duration>,
    ) -> Result<PeerProposalReceipt, RaftError> {
        self.require_proposal_term(expected_term)?;
        self.propose_tracked(payload, timeout)
    }
    /// Admit a whole batch or refuse it without allocating any log positions.
    /// Each returned receipt completes at its own contiguous application index.
    pub fn propose_batch_tracked(
        &mut self,
        payloads: Vec<Vec<u8>>,
        timeout: Option<Duration>,
    ) -> Result<Vec<PeerProposalReceipt>, RaftError> {
        self.propose_batch_tracked_with_options(PeerProposalOptions::default(), payloads, timeout)
    }
    /// Track a bounded batch under one command policy and optional term fence.
    pub fn propose_batch_tracked_with_options(
        &mut self,
        options: PeerProposalOptions,
        payloads: Vec<Vec<u8>>,
        timeout: Option<Duration>,
    ) -> Result<Vec<PeerProposalReceipt>, RaftError> {
        if let Some(term) = options.expected_term {
            self.require_proposal_term(term)?;
        }
        let deadline = self.admit_proposal_receipts(payloads.len(), timeout)?;
        let positions = self.propose_batch_with_options(options, payloads)?;
        Ok(positions
            .into_iter()
            .map(|position| self.register_proposal_receipt(position, deadline))
            .collect())
    }
    /// Service pending receipts and return this receipt's retained outcome.
    /// Receipts from another peer or a previous open are refused.
    pub fn poll_proposal(
        &mut self,
        receipt: &PeerProposalReceipt,
    ) -> Result<PeerProposalStatus, RaftError> {
        if !receipt.owner.ptr_eq(&Arc::downgrade(&self.journal._lock)) {
            return Err(invalid("proposal receipt belongs to another open peer"));
        }
        self.service_proposal_receipts();
        Ok(receipt.status())
    }
    fn admit_proposal_receipts(
        &mut self,
        count: usize,
        timeout: Option<Duration>,
    ) -> Result<Option<Instant>, RaftError> {
        self.healthy()?;
        let deadline = timeout
            .map(|duration| {
                if duration.is_zero() {
                    return Err(invalid("proposal timeout must be positive"));
                }
                Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| invalid("proposal deadline exceeds clock bounds"))
            })
            .transpose()?;
        self.service_proposal_receipts();
        let tracker = self
            .proposal_receipts
            .as_ref()
            .ok_or_else(|| invalid("proposal receipts are disabled"))?;
        if count == 0 || count > tracker.limit.saturating_sub(tracker.pending.len()) {
            return Err(invalid("proposal receipt admission limit reached"));
        }
        Ok(deadline)
    }
    fn register_proposal_receipt(
        &mut self,
        position: LogId,
        deadline: Option<Instant>,
    ) -> PeerProposalReceipt {
        let state = Arc::new(AtomicU8::new(PeerProposalStatus::Pending as u8));
        self.proposal_receipts.as_mut().unwrap().pending.insert(
            position.index,
            Pending {
                term: position.term,
                deadline,
                state: Arc::clone(&state),
                membership: None,
            },
        );
        PeerProposalReceipt {
            position,
            state,
            owner: Arc::downgrade(&self.journal._lock),
        }
    }
    pub(super) fn service_proposal_receipts(&mut self) {
        if self.proposal_receipts.is_none() {
            return;
        }
        let term = self.term();
        let leading = self.is_leader();
        let draining = self.drain_through;
        let applied = self.applied_index();
        let Some(tracker) = &mut self.proposal_receipts else {
            return;
        };
        let now = tracker
            .pending
            .values()
            .any(|pending| pending.deadline.is_some())
            .then(Instant::now);
        tracker.pending.retain(|index, pending| {
            // A durable membership boundary survives local removal or a later term.
            let membership_through = pending
                .membership
                .as_ref()
                .and_then(|completion| completion.apply_through);
            let outcome = if self.failed {
                Some(PeerProposalStatus::Failed)
            } else if self.stopped {
                Some(PeerProposalStatus::Stopped)
            } else if pending
                .deadline
                .is_some_and(|deadline| now.is_some_and(|now| now >= deadline))
            {
                Some(PeerProposalStatus::TimedOut)
            } else if membership_through.is_some_and(|index| index <= applied) {
                Some(PeerProposalStatus::Applied)
            } else if draining.is_some_and(|fence| *index > fence) {
                Some(PeerProposalStatus::Stopped)
            } else if draining.is_none()
                && (!leading || term != pending.term)
                && membership_through.is_none()
            {
                Some(PeerProposalStatus::LeadershipLost)
            } else {
                None
            };
            if let Some(outcome) = outcome {
                pending.state.store(outcome as u8, Ordering::Release);
                false
            } else {
                Arc::strong_count(&pending.state) > 1
            }
        });
    }
    pub(super) fn proposal_receipts_due(&self) -> bool {
        self.proposal_receipts.as_ref().is_some_and(|tracker| {
            tracker
                .pending
                .values()
                .filter_map(|pending| pending.deadline)
                .min()
                .is_some_and(|deadline| Instant::now() >= deadline)
        })
    }
    pub(super) fn finish_proposal_receipts(&mut self, outcome: PeerProposalStatus) {
        if let Some(tracker) = &mut self.proposal_receipts {
            for pending in std::mem::take(&mut tracker.pending).into_values() {
                pending.state.store(outcome as u8, Ordering::Release);
            }
        }
    }
    pub(super) fn apply_proposal_receipts(&mut self, entries: &[Entry]) {
        if let Some(tracker) = &mut self.proposal_receipts {
            tracker.applied(entries);
        }
    }
    pub(super) fn finish_membership_receipts(&mut self) {
        if self.proposal_receipts.is_none() {
            return;
        }
        let applied = self.applied_index();
        self.proposal_receipts
            .as_mut()
            .unwrap()
            .memberships_completed_through(applied);
    }
}
