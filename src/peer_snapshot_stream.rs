// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded checkpoint framing for the independent peer runtime.
use super::*;

#[derive(Default)]
pub(super) struct WalAssembly {
    pending: Option<WalBatch>,
}
impl WalAssembly {
    pub(super) fn accept(
        &mut self,
        mut record: WalBatch,
        position: u64,
    ) -> Result<Option<WalBatch>, RaftError> {
        if record.version != 3 {
            if self.pending.is_some()
                || record.snapshot_offset != 0
                || record.snapshot_total != 0
                || record.snapshot_digest != 0
            {
                return Err(storage_error("interrupted or invalid checkpoint records"));
            }
            return Ok(Some(record));
        }
        if record.snapshot_total == 0
            || record.snapshot_total > SNAPSHOT_ENCODED_LIMIT as u64
            || record.snapshot.is_empty()
            || record.snapshot.len() > SNAPSHOT_FRAGMENT_BYTES
            || !record.entries.is_empty()
        {
            return Err(storage_error("invalid checkpoint record bounds"));
        }
        if let Some(pending) = &mut self.pending {
            if record.snapshot_offset != pending.snapshot.len() as u64
                || record.snapshot_total != pending.snapshot_total
                || record.snapshot_digest != pending.snapshot_digest
                || !record.hard_state.is_empty()
            {
                return Err(storage_error("noncontiguous checkpoint records"));
            }
            if pending.snapshot.len() + record.snapshot.len() > pending.snapshot_total as usize {
                return Err(storage_error("checkpoint records exceed declared length"));
            }
            pending.snapshot.extend_from_slice(&record.snapshot);
        } else {
            if position != 0
                || record.snapshot_offset != 0
                || record.hard_state.is_empty()
                || record.snapshot.len() > record.snapshot_total as usize
            {
                return Err(storage_error("checkpoint records must begin the WAL"));
            }
            // Never allocate the advertised total before its bytes actually arrive.
            record.snapshot_offset = 0;
            self.pending = Some(record);
        }
        let pending = self.pending.as_ref().unwrap();
        if pending.snapshot.len() != pending.snapshot_total as usize {
            return Ok(None);
        }
        if digest(&pending.snapshot) != pending.snapshot_digest {
            return Err(storage_error("checkpoint record stream checksum mismatch"));
        }
        let mut record = self.pending.take().unwrap();
        record.version = 2;
        record.snapshot_total = 0;
        record.snapshot_digest = 0;
        Ok(Some(record))
    }
    pub(super) fn finish(&self) -> Result<(), RaftError> {
        if self.pending.is_some() {
            return Err(storage_error("incomplete checkpoint record stream"));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Fragment {
    #[prost(uint64, tag = "1")]
    pub from: u64,
    #[prost(uint64, tag = "2")]
    pub to: u64,
    #[prost(uint64, tag = "3")]
    pub term: u64,
    #[prost(uint64, tag = "4")]
    index: u64,
    #[prost(uint64, tag = "5")]
    total: u64,
    #[prost(fixed64, tag = "6")]
    checksum: u64,
    #[prost(uint64, tag = "7")]
    offset: u64,
    #[prost(bytes = "vec", tag = "8")]
    data: Vec<u8>,
    #[prost(bool, tag = "9")]
    pub ack: bool,
}
impl Fragment {
    pub(super) fn validate(&self, options: &PeerOptions, term: u64) -> Result<(), RaftError> {
        if self.to != options.node_id
            || self.from == self.to
            || !options.voters.contains(&self.from)
            || self.term < term
            || self.term == 0
            || self.term == u64::MAX
            || self.index == 0
            || self.index == u64::MAX
            || self.total == 0
            || self.total > SNAPSHOT_ENCODED_LIMIT as u64
            || self.offset > self.total
        {
            return Err(invalid("invalid checkpoint fragment identity or bounds"));
        }
        if self.ack {
            if !self.data.is_empty() {
                return Err(invalid("checkpoint acknowledgment contains data"));
            }
        } else if self.offset == self.total
            || !self.offset.is_multiple_of(SNAPSHOT_FRAGMENT_BYTES as u64)
            || self.data.len()
                != (self.total - self.offset).min(SNAPSHOT_FRAGMENT_BYTES as u64) as usize
        {
            return Err(invalid("invalid checkpoint fragment extent"));
        }
        Ok(())
    }
    fn same_stream(&self, other: &Self) -> bool {
        self.from == other.from
            && self.to == other.to
            && self.term == other.term
            && self.index == other.index
            && self.total == other.total
            && self.checksum == other.checksum
    }
    fn acknowledgment(&self, offset: u64) -> Self {
        Self {
            from: self.to,
            to: self.from,
            offset,
            ack: true,
            data: Vec::new(),
            ..self.clone()
        }
    }
    fn identity(&self) -> Self {
        Self {
            data: Vec::new(),
            offset: 0,
            ..self.clone()
        }
    }
}
struct Sending {
    identity: Fragment,
    bytes: Vec<u8>,
    offset: usize,
    ticks: u8,
    idle: u8,
    ready: bool,
}
struct Receiving {
    identity: Fragment,
    bytes: Vec<u8>,
    idle: u8,
}
/// One outgoing and one incoming transfer per local peer bound retained memory.
/// Other lagging voters remain in Raft's retry path until the outgoing slot frees.
#[derive(Default)]
pub(super) struct Stream {
    sending: Option<Sending>,
    receiving: Option<Receiving>,
    completed: Option<Fragment>,
    ack: Option<Fragment>,
}
impl Stream {
    pub(super) fn sending_to(&self, to: u64) -> bool {
        self.sending.as_ref().is_some_and(|s| s.identity.to == to)
    }
    pub(super) fn completed_send(&mut self, to: u64) {
        if self.sending_to(to) {
            self.sending = None;
        }
    }
    pub(super) fn start(&mut self, message: Message) -> Result<bool, RaftError> {
        if let Some(sending) = &self.sending {
            return Ok(sending.identity.to == message.to
                && sending.identity.term == message.term
                && sending.identity.index == message.get_snapshot().get_metadata().index);
        }
        let bytes = message.encode_to_vec();
        if bytes.len() > SNAPSHOT_ENCODED_LIMIT {
            return Err(invalid("encoded checkpoint exceeds transfer bound"));
        }
        self.sending = Some(Sending {
            identity: Fragment {
                from: message.from,
                to: message.to,
                term: message.term,
                index: message.get_snapshot().get_metadata().index,
                total: bytes.len() as u64,
                checksum: digest(&bytes),
                offset: 0,
                data: Vec::new(),
                ack: false,
            },
            bytes,
            offset: 0,
            ticks: 0,
            idle: 0,
            ready: true,
        });
        Ok(true)
    }
    pub(super) fn tick(&mut self, term: u64, leader: bool) {
        if let Some(sending) = &mut self.sending {
            sending.ticks += 1;
            sending.idle += 1;
            if !leader || sending.identity.term != term || sending.idle >= 100 {
                self.sending = None;
            } else if sending.ticks >= 10 {
                sending.ready = true;
                sending.ticks = 0;
            }
        }
        if let Some(receiving) = &mut self.receiving {
            receiving.idle += 1;
            if receiving.identity.term < term || receiving.idle >= 100 {
                self.receiving = None;
            }
        }
        if self.completed.as_ref().is_some_and(|s| s.term < term) {
            self.completed = None;
        }
        if self.ack.as_ref().is_some_and(|s| s.term < term) {
            self.ack = None;
        }
    }
    pub(super) fn acknowledge(&mut self, part: &Fragment) -> bool {
        let Some(sending) = &mut self.sending else {
            return false;
        };
        if !sending.identity.acknowledgment(0).same_stream(part) {
            return false;
        }
        if part.offset == sending.bytes.len() as u64 {
            self.sending = None;
            return true;
        }
        let next = (sending.offset + SNAPSHOT_FRAGMENT_BYTES).min(sending.bytes.len());
        if part.offset == next as u64 {
            sending.offset = next;
            sending.idle = 0;
            sending.ticks = 0;
            sending.ready = true;
        } else if part.offset == 0 && sending.offset != 0 {
            // The receiver restarted or discarded an expired partial transfer.
            sending.offset = 0;
            sending.ready = true;
            sending.ticks = 0;
        }
        false
    }
    pub(super) fn receive(
        &mut self,
        part: Fragment,
        voters: &[u64],
    ) -> Result<Option<Message>, RaftError> {
        if self
            .completed
            .as_ref()
            .is_some_and(|s| s.same_stream(&part))
        {
            self.ack = Some(part.acknowledgment(part.total));
            return Ok(None);
        }
        let matches = self
            .receiving
            .as_ref()
            .is_some_and(|s| s.identity.same_stream(&part));
        if !matches {
            if self.receiving.as_ref().is_some_and(|s| {
                part.term < s.identity.term
                    || (part.term == s.identity.term
                        && (part.from != s.identity.from || part.index <= s.identity.index))
            }) {
                return Err(invalid("conflicting checkpoint transfer"));
            }
            if part.offset != 0 {
                self.ack = Some(part.acknowledgment(0));
                return Ok(None);
            }
            self.receiving = Some(Receiving {
                identity: part.identity(),
                bytes: Vec::new(),
                idle: 0,
            });
        }
        let receiving = self.receiving.as_mut().unwrap();
        receiving.idle = 0;
        let offset = part.offset as usize;
        if offset == receiving.bytes.len() {
            receiving.bytes.extend_from_slice(&part.data);
        } else if offset < receiving.bytes.len()
            && receiving.bytes.get(offset..offset + part.data.len()) != Some(part.data.as_slice())
        {
            return Err(invalid("conflicting duplicate checkpoint fragment"));
        }
        let accepted = receiving.bytes.len() as u64;
        if accepted != part.total {
            self.ack = Some(part.acknowledgment(accepted));
            return Ok(None);
        }
        let receiving = self.receiving.take().unwrap();
        if digest(&receiving.bytes) != part.checksum {
            return Err(invalid("checkpoint transfer checksum mismatch"));
        }
        let message = Message::decode(receiving.bytes.as_slice()).map_err(invalid)?;
        if message.encode_to_vec() != receiving.bytes
            || message.get_msg_type() != MessageType::MsgSnapshot
            || message.from != part.from
            || message.to != part.to
            || message.term != part.term
            || message.get_snapshot().get_metadata().index != part.index
            || message.get_snapshot().get_metadata().term > message.term
            || !message.entries.is_empty()
        {
            return Err(invalid("checkpoint transfer message mismatch"));
        }
        validate_snapshot(message.get_snapshot(), voters)?;
        self.completed = Some(part.identity());
        self.ack = Some(part.acknowledgment(part.total));
        Ok(Some(message))
    }
    pub(super) fn packets(&mut self, group: u64) -> Vec<PeerPacket> {
        let mut parts = Vec::new();
        if let Some(ack) = self.ack.take() {
            parts.push(ack);
        }
        if let Some(sending) = &mut self.sending {
            if sending.ready {
                let end = (sending.offset + SNAPSHOT_FRAGMENT_BYTES).min(sending.bytes.len());
                parts.push(Fragment {
                    offset: sending.offset as u64,
                    data: sending.bytes[sending.offset..end].to_vec(),
                    ..sending.identity.clone()
                });
                sending.ready = false;
            }
        }
        parts
            .into_iter()
            .map(|part| PeerPacket {
                to: part.to,
                bytes: Packet {
                    version: 2,
                    group,
                    message: part.encode_to_vec(),
                }
                .encode_to_vec(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn message() -> Message {
        Message {
            from: 1,
            to: 2,
            term: 4,
            msg_type: MessageType::MsgSnapshot as i32,
            snapshot: Some(Snapshot {
                data: vec![13; 5 * 1024 * 1024],
                metadata: Some(SnapshotMetadata {
                    index: 9,
                    term: 3,
                    conf_state: Some(ConfState {
                        voters: vec![1, 2, 3],
                        ..Default::default()
                    }),
                }),
            }),
            ..Default::default()
        }
    }
    fn fragment(stream: &mut Stream) -> Fragment {
        let packets = stream.packets(1);
        assert_eq!(packets.len(), 1);
        let packet = Packet::decode(packets[0].bytes.as_slice()).unwrap();
        assert_eq!(packet.version, 2);
        Fragment::decode(packet.message.as_slice()).unwrap()
    }
    #[test]
    fn fragment_validation_rejects_invalid_routes_extents_and_stale_terms() {
        let mut sender = Stream::default();
        sender.start(message()).unwrap();
        let valid = fragment(&mut sender);
        let options = PeerOptions {
            group_id: 1,
            node_id: 2,
            voters: vec![1, 2, 3],
            wal_path: PathBuf::new(),
        };
        valid.validate(&options, 4).unwrap();
        for change in 0..9 {
            let mut bad = valid.clone();
            match change {
                0 => bad.to = 3,
                1 => bad.from = 5,
                2 => bad.term = 3,
                3 => bad.term = u64::MAX,
                4 => bad.total = SNAPSHOT_ENCODED_LIMIT as u64 + 1,
                5 => bad.offset = 1,
                6 => {
                    bad.data.pop();
                }
                7 => bad.index = 0,
                8 => bad.ack = true,
                _ => unreachable!(),
            }
            assert!(bad.validate(&options, 4).is_err(), "case {change}");
        }
    }
    #[test]
    fn reordered_duplicate_corrupt_and_conflicting_fragments_never_restore_partial_state() {
        let mut sender = Stream::default();
        sender.start(message()).unwrap();
        let first = fragment(&mut sender);
        let mut receiver = Stream::default();
        assert!(receiver
            .receive(first.clone(), &[1, 2, 3])
            .unwrap()
            .is_none());
        let mut later = first.clone();
        later.offset = 2 * SNAPSHOT_FRAGMENT_BYTES as u64;
        assert!(receiver.receive(later, &[1, 2, 3]).unwrap().is_none());
        assert_eq!(
            receiver.receiving.as_ref().unwrap().bytes.len(),
            SNAPSHOT_FRAGMENT_BYTES
        );
        let mut corrupt = first.clone();
        corrupt.data[0] ^= 1;
        assert!(receiver.receive(corrupt, &[1, 2, 3]).is_err());
        let mut conflicting = first.clone();
        conflicting.from = 3;
        assert!(receiver.receive(conflicting, &[1, 2, 3]).is_err());
        let mut offset = SNAPSHOT_FRAGMENT_BYTES;
        while offset < sender.sending.as_ref().unwrap().bytes.len() {
            let sending = sender.sending.as_ref().unwrap();
            let end = (offset + SNAPSHOT_FRAGMENT_BYTES).min(sending.bytes.len());
            let mut part = Fragment {
                offset: offset as u64,
                data: sending.bytes[offset..end].to_vec(),
                ..first.identity()
            };
            let last = end == sending.bytes.len();
            if last {
                part.data[0] ^= 1;
            }
            let result = receiver.receive(part, &[1, 2, 3]);
            if last {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_none());
            }
            offset = end;
        }
        assert!(receiver.receiving.is_none());
        assert!(receiver.completed.is_none());
    }
    #[test]
    fn transfer_slots_expire_and_leadership_changes_discard_old_work() {
        let mut stream = Stream::default();
        stream.start(message()).unwrap();
        let first = fragment(&mut stream);
        let mut other = message();
        other.to = 3;
        assert!(!stream.start(other.clone()).unwrap());
        for _ in 0..10 {
            stream.tick(4, true);
        }
        assert_eq!(fragment(&mut stream), first);
        for _ in 0..90 {
            stream.tick(4, true);
        }
        assert!(!stream.sending_to(2));
        assert!(stream.start(other).unwrap());
        stream.tick(5, false);
        assert!(stream.sending.is_none());
        stream.receive(first, &[1, 2, 3]).unwrap();
        assert!(stream.receiving.is_some());
        stream.tick(5, false);
        assert!(stream.receiving.is_none());
        assert!(stream.packets(1).is_empty());
    }
    #[test]
    fn wal_stream_rejects_reordered_repeated_or_mixed_records() {
        let options = PeerOptions {
            group_id: 1,
            node_id: 2,
            voters: vec![1, 2, 3],
            wal_path: PathBuf::new(),
        };
        let mut first = batch(
            &options,
            Some(&HardState {
                term: 4,
                commit: 9,
                ..Default::default()
            }),
            &[],
        );
        first.version = 3;
        first.snapshot = vec![1; 16];
        first.snapshot_total = 32;
        first.snapshot_digest = 42;
        for kind in 0..5 {
            let mut assembly = WalAssembly::default();
            assert!(assembly.accept(first.clone(), 0).unwrap().is_none());
            let mut next = first.clone();
            next.hard_state.clear();
            next.snapshot_offset = 16;
            match kind {
                0 => next.snapshot_offset = 0,
                1 => next.snapshot_total = 33,
                2 => next.version = 2,
                3 => next.hard_state = vec![1],
                4 => {}
                _ => unreachable!(),
            }
            assert!(assembly.accept(next, 100).is_err(), "case {kind}");
        }
    }
}
