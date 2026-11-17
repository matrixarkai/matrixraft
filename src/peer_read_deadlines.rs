// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::{
    sync::Weak,
    time::{Duration, Instant},
};

/// A volatile read request bound to one open peer. Dropping it does not cancel
/// the request; host timers or Ready processing reclaim it at its deadline.
#[derive(Debug)]
pub struct PeerReadTicket {
    token: u64,
    term: u64,
    deadline: Instant,
    owner: Weak<File>,
    outcome: Option<PeerReadStatus>,
}

/// Outcome of polling a timed quorum read. Ready consumes its confirmation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerReadStatus {
    Pending,
    Ready,
    TimedOut,
}

impl DurablePeer {
    /// Start a quorum read with a positive monotonic timeout. Timeouts cancel
    /// local waiting, not already delivered packets or application work.
    pub fn begin_read_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<PeerReadTicket, RaftError> {
        if timeout.is_zero() {
            return Err(invalid("read timeout must be positive"));
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| invalid("read deadline exceeds clock bounds"))?;
        let token = self.begin_read()?;
        self.pending_reads.get_mut(&token).unwrap().deadline = Some(deadline);
        Ok(PeerReadTicket {
            token,
            term: self.term(),
            deadline,
            owner: Arc::downgrade(&self.journal._lock),
            outcome: None,
        })
    }

    /// Poll a timed read. Leadership loss invalidates it. At or after its
    /// deadline a confirmation cannot succeed, even if received earlier.
    pub fn poll_timed_read(
        &mut self,
        ticket: &mut PeerReadTicket,
    ) -> Result<PeerReadStatus, RaftError> {
        self.validate_read_ticket(ticket)?;
        self.healthy()?;
        if let Some(outcome) = ticket.outcome {
            return Ok(outcome);
        }
        if !self.is_leader() || self.term() != ticket.term {
            self.cancel_read(ticket.token)?;
            return Err(RaftError::NotLeader(self.options.node_id));
        }
        if Instant::now() >= ticket.deadline {
            self.cancel_read(ticket.token)?;
            ticket.outcome = Some(PeerReadStatus::TimedOut);
            return Ok(PeerReadStatus::TimedOut);
        }
        if self.poll_read(ticket.token)? {
            ticket.outcome = Some(PeerReadStatus::Ready);
            Ok(PeerReadStatus::Ready)
        } else {
            Ok(PeerReadStatus::Pending)
        }
    }

    /// Cancel one timed read without affecting other admitted reads.
    pub fn cancel_timed_read(&mut self, ticket: &PeerReadTicket) -> Result<bool, RaftError> {
        self.validate_read_ticket(ticket)?;
        self.cancel_read(ticket.token)
    }

    fn validate_read_ticket(&self, ticket: &PeerReadTicket) -> Result<(), RaftError> {
        if !ticket.owner.ptr_eq(&Arc::downgrade(&self.journal._lock)) {
            return Err(invalid("read ticket belongs to another open peer"));
        }
        Ok(())
    }

    pub(super) fn read_deadline_due(&self) -> bool {
        let earliest = self
            .pending_reads
            .values()
            .filter_map(|read| read.deadline)
            .min();
        earliest.is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub(super) fn expire_read_deadlines(&mut self) -> Result<(), RaftError> {
        if !self.read_deadline_due() {
            return Ok(());
        }
        let now = Instant::now();
        let expired: Vec<_> = self
            .pending_reads
            .iter()
            .filter_map(|(&token, read)| {
                read.deadline
                    .filter(|deadline| now >= *deadline)
                    .map(|_| token)
            })
            .collect();
        for token in expired {
            self.cancel_read(token)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(path: PathBuf) -> PeerOptions {
        PeerOptions {
            group_id: 19,
            node_id: 1,
            voters: vec![1],
            wal_path: path,
        }
    }
    fn leader(path: PathBuf) -> DurablePeer {
        let mut peer = DurablePeer::open(options(path)).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        peer
    }
    fn expire(peer: &mut DurablePeer, ticket: &mut PeerReadTicket) {
        let deadline = Instant::now();
        ticket.deadline = deadline;
        peer.pending_reads.get_mut(&ticket.token).unwrap().deadline = Some(deadline);
    }

    #[test]
    fn deadlines_reclaim_admission_from_ticks_ready_and_new_requests() {
        let dir = tempfile::tempdir().unwrap();
        for mode in 0..3 {
            let mut peer = leader(dir.path().join(format!("{mode}.wal")));
            let bytes = peer.persisted_wal_bytes().unwrap();
            let mut ticket = peer
                .begin_read_with_timeout(Duration::from_secs(60))
                .unwrap();
            assert!(peer.begin_read().is_err());
            expire(&mut peer, &mut ticket);
            assert!(peer.has_ready());
            let next = match mode {
                0 => {
                    peer.tick().unwrap();
                    peer.begin_read().unwrap()
                }
                1 => {
                    peer.process_ready(|_, _| Ok(())).unwrap();
                    peer.begin_read().unwrap()
                }
                _ => peer.begin_read().unwrap(),
            };
            assert_ne!(ticket.token, next);
            assert!(!peer.pending_reads.contains_key(&ticket.token));
            assert!(peer
                .raw
                .raft
                .read_only
                .pending_read_index
                .keys()
                .all(|context| context.as_slice() != ticket_context(&ticket)));
            assert_eq!(
                peer.poll_timed_read(&mut ticket).unwrap(),
                PeerReadStatus::TimedOut
            );
            assert!(peer.pending_reads.contains_key(&next));
            assert_eq!(peer.persisted_wal_bytes().unwrap(), bytes);
        }
    }
    fn ticket_context(ticket: &PeerReadTicket) -> Vec<u8> {
        [ticket.term.to_be_bytes(), ticket.token.to_be_bytes()].concat()
    }

    #[test]
    fn deadline_rejects_confirmed_but_unapplied_reads_and_preserves_other_requests() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = leader(dir.path().join("peer.wal"));
        peer.read_limit = 2;
        let entry = peer.propose(vec![7]).unwrap();
        peer.process_ready_retryable(|_, _| Ok(ApplyDecision::Retry))
            .unwrap();
        let mut ticket = peer
            .begin_read_with_timeout(Duration::from_secs(60))
            .unwrap();
        let ordinary = peer.begin_read().unwrap();
        peer.process_ready_retryable(|_, _| Ok(ApplyDecision::Retry))
            .unwrap();
        assert_eq!(peer.commit_index(), entry.index);
        assert!(peer.applied_index() < entry.index);
        assert!(peer.pending_reads[&ticket.token].index.is_some());
        assert_eq!(
            peer.poll_timed_read(&mut ticket).unwrap(),
            PeerReadStatus::Pending
        );
        expire(&mut peer, &mut ticket);
        assert_eq!(
            peer.poll_timed_read(&mut ticket).unwrap(),
            PeerReadStatus::TimedOut
        );
        assert!(!peer.poll_read(ordinary).unwrap());
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert!(peer.poll_read(ordinary).unwrap());
        assert_eq!(
            peer.poll_timed_read(&mut ticket).unwrap(),
            PeerReadStatus::TimedOut
        );
    }

    #[test]
    fn timed_tickets_validate_duration_owner_reopen_and_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer.wal");
        let mut peer = leader(path.clone());
        assert!(peer.begin_read_with_timeout(Duration::ZERO).is_err());
        assert!(peer.begin_read_with_timeout(Duration::MAX).is_err());
        assert!(peer.pending_reads.is_empty());
        let mut ticket = peer
            .begin_read_with_timeout(Duration::from_secs(60))
            .unwrap();
        let mut other = leader(dir.path().join("other.wal"));
        assert!(other.poll_timed_read(&mut ticket).is_err());
        assert!(other.cancel_timed_read(&ticket).is_err());
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert_eq!(
            peer.poll_timed_read(&mut ticket).unwrap(),
            PeerReadStatus::Ready
        );
        assert_eq!(
            peer.poll_timed_read(&mut ticket).unwrap(),
            PeerReadStatus::Ready
        );
        let canceled = peer
            .begin_read_with_timeout(Duration::from_secs(60))
            .unwrap();
        assert!(peer.cancel_timed_read(&canceled).unwrap());
        assert!(!peer.cancel_timed_read(&canceled).unwrap());
        let mut old = peer
            .begin_read_with_timeout(Duration::from_secs(60))
            .unwrap();
        drop(peer);
        let mut reopened = DurablePeer::open(options(path)).unwrap();
        assert!(reopened.poll_timed_read(&mut old).is_err());
        assert!(reopened.pending_reads.is_empty());
    }
}
