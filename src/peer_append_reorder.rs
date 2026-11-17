// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::time::{Duration, Instant};

/// Optional follower append window. Memory charging includes encoded payloads
/// and message/entry structs, not allocator overhead or unrelated Ready work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendReorderTuning {
    pub max_messages: usize,
    pub max_bytes: u64,
    pub max_delay: Duration,
}
impl Default for AppendReorderTuning {
    fn default() -> Self {
        Self {
            max_messages: 128,
            max_bytes: 4 * 1024 * 1024,
            max_delay: Duration::from_millis(3),
        }
    }
}
impl AppendReorderTuning {
    pub(super) fn validate(self) -> Result<(), RaftError> {
        if !(1..=1024).contains(&self.max_messages)
            || self.max_bytes == 0
            || self.max_bytes > 8 * LIMIT as u64
            || self.max_delay.is_zero()
            || self.max_delay > Duration::from_secs(60)
        {
            return Err(invalid("append reorder window exceeds supported bounds"));
        }
        Ok(())
    }
}
struct Held {
    message: Message,
    bytes: u64,
}
struct Window {
    tuning: AppendReorderTuning,
    context: Option<(u64, u64)>,
    held: BTreeMap<u64, Held>,
    bytes: u64,
    started: Option<Instant>,
}
pub(super) struct Queue(Option<Box<Window>>);
impl Queue {
    pub(super) fn new(tuning: Option<AppendReorderTuning>) -> Self {
        Self(tuning.map(|tuning| {
            Box::new(Window {
                tuning,
                context: None,
                held: BTreeMap::new(),
                bytes: 0,
                started: None,
            })
        }))
    }
    pub(super) fn enabled(&self) -> bool {
        self.0.is_some()
    }
    pub(super) fn clear(&mut self) {
        if let Some(window) = self.0.as_mut() {
            window.held.clear();
            window.bytes = 0;
            window.started = None;
        }
    }
    pub(super) fn align(&mut self, term: u64, leader: Option<u64>, follower: bool) {
        let context = leader.filter(|_| follower).map(|leader| (term, leader));
        if self
            .0
            .as_ref()
            .is_some_and(|window| window.context != context)
        {
            self.clear();
            self.0.as_mut().unwrap().context = context;
        }
    }
    pub(super) fn expired(&self, now: Instant) -> bool {
        self.0.as_ref().is_some_and(|window| {
            window.started.is_some_and(|start| {
                now.saturating_duration_since(start) >= window.tuning.max_delay
            })
        })
    }
    pub(super) fn due(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|window| window.started.is_some())
            && self.expired(Instant::now())
    }
    pub(super) fn hold(&mut self, message: Message, now: Instant) -> Option<Message> {
        let Some(window) = self.0.as_mut() else {
            return Some(message);
        };
        let bytes = message.encoded_len() as u64
            + std::mem::size_of::<Message>() as u64
            + (message.entries.len() * std::mem::size_of::<Entry>()) as u64;
        let old_bytes = window.held.get(&message.index).map_or(0, |old| old.bytes);
        if window.context != Some((message.term, message.from))
            || (old_bytes == 0 && window.held.len() >= window.tuning.max_messages)
            || bytes
                > window
                    .tuning
                    .max_bytes
                    .saturating_sub(window.bytes - old_bytes)
        {
            return Some(message);
        }
        window.started.get_or_insert(now);
        window.bytes = window.bytes - old_bytes + bytes;
        window.held.insert(message.index, Held { message, bytes });
        None
    }
    pub(super) fn take_ready(&mut self, last_index: u64) -> Option<Message> {
        let window = self.0.as_mut()?;
        if *window.held.first_key_value()?.0 > last_index {
            return None;
        }
        let (_, held) = window.held.pop_first().unwrap();
        window.bytes -= held.bytes;
        if window.held.is_empty() {
            window.started = None;
        }
        Some(held.message)
    }
    fn take_one_and_clear(&mut self) -> Option<Message> {
        let message = self
            .0
            .as_mut()?
            .held
            .pop_first()
            .map(|(_, held)| held.message);
        self.clear();
        message
    }
}
impl DurablePeer {
    fn align_append_window(&mut self) {
        self.append_reorder.align(
            self.term(),
            self.leader_id(),
            self.raw.raft.state == StateRole::Follower,
        );
    }
    pub(super) fn service_append_window(&mut self) -> Result<(), RaftError> {
        if !self.append_reorder.enabled() {
            return Ok(());
        }
        self.service_append_window_at(Instant::now())
    }
    fn service_append_window_at(&mut self, now: Instant) -> Result<(), RaftError> {
        self.align_append_window();
        while let Some(message) = self
            .append_reorder
            .take_ready(self.raw.raft.raft_log.last_index())
        {
            self.raw.step(message).map_err(invalid)?;
        }
        if self.append_reorder.expired(now) {
            self.flush_append_window()?;
        }
        Ok(())
    }
    fn flush_append_window(&mut self) -> Result<(), RaftError> {
        if let Some(message) = self.append_reorder.take_one_and_clear() {
            self.raw.step(message).map_err(invalid)?;
        }
        Ok(())
    }
    pub(super) fn step_with_append_window(&mut self, message: Message) -> Result<(), RaftError> {
        if !self.append_reorder.enabled() {
            return self.raw.step(message).map_err(invalid);
        }
        self.align_append_window();
        let now = Instant::now();
        let buffer = self.append_reorder.enabled()
            && self.raw.raft.state == StateRole::Follower
            && message.get_msg_type() == MessageType::MsgAppend
            && !message.entries.is_empty()
            && message.term == self.term()
            && self.leader_id() == Some(message.from)
            && message.index > self.raw.raft.raft_log.last_index();
        let message = if buffer {
            self.service_append_window_at(now)?;
            match self.append_reorder.hold(message, now) {
                None => {
                    self.raw.raft.election_elapsed = 0;
                    return self.service_append_window_at(now);
                }
                Some(message) => {
                    self.flush_append_window()?;
                    message
                }
            }
        } else {
            message
        };
        self.raw.step(message).map_err(invalid)?;
        self.service_append_window_at(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(index: u64) -> Message {
        Message {
            from: 1,
            to: 3,
            term: 4,
            log_term: 4,
            index,
            msg_type: MessageType::MsgAppend as i32,
            entries: vec![Entry {
                term: 4,
                index: index + 1,
                data: vec![7; 32],
                ..Default::default()
            }],
            ..Default::default()
        }
    }
    fn queue(tuning: AppendReorderTuning) -> Queue {
        let mut queue = Queue::new(Some(tuning));
        queue.align(4, Some(1), true);
        queue
    }
    #[test]
    fn disabled_window_is_one_word_and_passes_messages_through() {
        assert_eq!(std::mem::size_of::<Queue>(), std::mem::size_of::<usize>());
        let mut queue = Queue::new(None);
        assert!(!queue.enabled());
        assert!(queue.hold(message(5), Instant::now()).is_some());
        assert!(queue.take_ready(10).is_none());
    }
    #[test]
    fn prefix_ordering_moves_payloads_and_coalesces_repeated_positions() {
        let mut queue = queue(AppendReorderTuning {
            max_messages: 2,
            ..Default::default()
        });
        let now = Instant::now();
        let high = message(6);
        let pointer = high.entries[0].data.as_ptr();
        assert!(queue.hold(high, now).is_none());
        assert!(queue.hold(message(5), now).is_none());
        assert!(queue.hold(message(5), now).is_none());
        assert!(queue.hold(message(7), now).is_some());
        assert_eq!(queue.0.as_ref().unwrap().held.len(), 2);
        assert!(queue.take_ready(4).is_none());
        assert_eq!(queue.take_ready(5).unwrap().index, 5);
        assert_eq!(
            queue.take_ready(6).unwrap().entries[0].data.as_ptr(),
            pointer
        );
        assert_eq!(queue.0.as_ref().unwrap().bytes, 0);
        assert!(queue.0.as_ref().unwrap().started.is_none());
    }
    #[test]
    fn replacement_respects_bytes_and_preserves_the_first_deadline() {
        let tuning = AppendReorderTuning::default();
        let mut queue = queue(tuning);
        let start = Instant::now();
        assert!(queue.hold(message(5), start).is_none());
        let charge = queue.0.as_ref().unwrap().bytes;
        queue.0.as_mut().unwrap().tuning.max_bytes = charge;
        let mut larger = message(5);
        larger.entries[0].data.push(8);
        assert!(queue.hold(larger, start).is_some());
        assert_eq!(queue.0.as_ref().unwrap().bytes, charge);
        assert!(queue
            .hold(message(5), start + tuning.max_delay / 2)
            .is_none());
        assert!(!queue.expired(start + tuning.max_delay - Duration::from_nanos(1)));
        assert!(queue.expired(start + tuning.max_delay));
        assert_eq!(queue.take_one_and_clear().unwrap().index, 5);
        assert!(queue.take_one_and_clear().is_none());
        assert!(!queue.expired(start + tuning.max_delay));
    }
    #[test]
    fn context_changes_discard_unaccepted_work() {
        let mut queue = queue(AppendReorderTuning::default());
        for (term, leader, follower) in [
            (5, Some(1), true),
            (4, Some(2), true),
            (4, None, true),
            (4, Some(1), false),
        ] {
            queue.align(4, Some(1), true);
            assert!(queue.hold(message(5), Instant::now()).is_none());
            queue.align(term, leader, follower);
            assert!(queue.0.as_ref().unwrap().held.is_empty());
            assert_eq!(queue.0.as_ref().unwrap().bytes, 0);
        }
    }
    #[test]
    fn expiry_fallback_retains_one_core_rejection_candidate() {
        let tuning = AppendReorderTuning::default();
        let mut queue = queue(tuning);
        let start = Instant::now();
        for index in [8, 5, 6] {
            assert!(queue.hold(message(index), start).is_none());
        }
        assert!(queue.expired(start + tuning.max_delay));
        assert_eq!(queue.take_one_and_clear().unwrap().index, 5);
        assert!(queue.take_ready(100).is_none());
        assert_eq!(queue.0.as_ref().unwrap().bytes, 0);
    }
}
