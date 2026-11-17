// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::time::{Duration, Instant};

pub(super) struct Hold(Option<Instant>);
impl Hold {
    pub(super) fn validate(delay: Option<Duration>) -> Result<(), RaftError> {
        if delay.is_some_and(|delay| delay.is_zero() || delay > Duration::from_secs(60)) {
            return Err(invalid("startup election delay exceeds supported bounds"));
        }
        Ok(())
    }
    pub(super) fn new(delay: Option<Duration>) -> Result<Self, RaftError> {
        match delay {
            None => Ok(Self(None)),
            Some(delay) => Self::after(delay, Instant::now()),
        }
    }
    fn after(delay: Duration, now: Instant) -> Result<Self, RaftError> {
        Self::validate(Some(delay))?;
        now.checked_add(delay)
            .map(|deadline| Self(Some(deadline)))
            .ok_or_else(|| invalid("startup election deadline exceeds clock bounds"))
    }
    pub(super) fn active(&mut self) -> bool {
        self.0.is_some() && self.active_at(Instant::now())
    }
    fn active_at(&mut self, now: Instant) -> bool {
        if self.0.is_some_and(|deadline| now < deadline) {
            return true;
        }
        self.0 = None;
        false
    }
}
impl DurablePeer {
    pub(super) fn suppress_startup_vote(&mut self, message: &Message) -> bool {
        self.raw.raft.state != StateRole::Leader
            && message.term > self.term()
            && matches!(
                message.get_msg_type(),
                MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
            )
            && message.context.as_slice() != raft_core::CAMPAIGN_TRANSFER
            && self.startup_hold.active()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn startup_deadline_expires_exactly_once_without_rearming() {
        let now = Instant::now();
        let delay = Duration::from_millis(5);
        let mut hold = Hold::after(delay, now).unwrap();
        assert!(hold.active_at(now));
        assert!(hold.active_at(now + delay - Duration::from_nanos(1)));
        assert!(!hold.active_at(now + delay));
        assert!(hold.0.is_none());
        assert!(!hold.active_at(now));
        assert!(!Hold::new(None).unwrap().active());
    }
    #[test]
    fn startup_delay_has_explicit_nonzero_bounded_admission() {
        assert!(Hold::validate(None).is_ok());
        for delay in [
            Duration::ZERO,
            Duration::from_secs(60) + Duration::from_nanos(1),
            Duration::MAX,
        ] {
            assert!(Hold::validate(Some(delay)).is_err());
        }
        for delay in [Duration::from_nanos(1), Duration::from_secs(60)] {
            assert!(Hold::validate(Some(delay)).is_ok());
        }
    }
    #[test]
    fn elapsed_election_ticks_resume_when_the_startup_window_expires() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = DurablePeer::open_with_tuning(
            PeerOptions {
                group_id: 81,
                node_id: 1,
                voters: vec![1],
                wal_path: dir.path().join("peer.wal"),
            },
            PeerTuning {
                startup_election_delay: Some(Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let bytes = peer.persisted_wal_bytes().unwrap();
        for _ in 0..32 {
            peer.tick().unwrap();
            assert!(peer.process_ready(|_, _| Ok(())).unwrap().is_empty());
        }
        assert_eq!(peer.term(), 0);
        assert_eq!(peer.persisted_wal_bytes().unwrap(), bytes);
        assert!(peer.campaign().is_err());
        peer.startup_hold.0 = Some(Instant::now());
        peer.tick().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert!(peer.is_leader());
        assert!(peer.persisted_wal_bytes().unwrap() > bytes);
        assert!(peer.startup_hold.0.is_none());
    }
}
