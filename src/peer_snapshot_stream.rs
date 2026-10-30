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
    #[prost(bool, tag = "10")]
    pub file_backed: bool,
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
            || self.total
                > if self.file_backed {
                    checkpoint_file::MAX_FILE_BYTES + 8192
                } else {
                    SNAPSHOT_ENCODED_LIMIT as u64
                }
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
            && self.file_backed == other.file_backed
    }
    fn acknowledgment(&self, offset: u64) -> Self {
        Self {
            from: self.to,
            to: self.from,
            offset,
            ack: true,
            ..self.identity()
        }
    }
    fn identity(&self) -> Self {
        Self {
            from: self.from,
            to: self.to,
            term: self.term,
            index: self.index,
            total: self.total,
            checksum: self.checksum,
            ack: self.ack,
            file_backed: self.file_backed,
            data: Vec::new(),
            offset: 0,
        }
    }
}
enum Source {
    Memory(Vec<u8>),
    File {
        prefix: Vec<u8>,
        reader: File,
        payload: Arc<checkpoint_file::Blob>,
    },
}
impl Source {
    fn len(&self) -> usize {
        match self {
            Self::Memory(bytes) => bytes.len(),
            Self::File { prefix, payload, .. } => prefix.len() + payload.manifest.length as usize,
        }
    }
    fn fragment(&self, offset: usize, size: usize) -> Result<Vec<u8>, RaftError> {
        match self {
            Self::Memory(bytes) => Ok(bytes[offset..offset + size].to_vec()),
            Self::File { prefix, reader, .. } => {
                let mut output = vec![0; size];
                let head = if offset < prefix.len() {
                    (prefix.len() - offset).min(size)
                } else {
                    0
                };
                if head > 0 {
                    output[..head].copy_from_slice(&prefix[offset..offset + head]);
                }
                if head < size {
                    let mut file = reader;
                    file.seek(SeekFrom::Start((offset + head - prefix.len()) as u64))
                        .map_err(storage_error)?;
                    file.read_exact(&mut output[head..])
                        .map_err(storage_error)?;
                }
                Ok(output)
            }
        }
    }
}
struct Sending {
    identity: Fragment,
    bytes: Source,
    offset: usize,
    ticks: u8,
    idle: u8,
    ready: bool,
}
struct Receiving {
    identity: Fragment,
    file: File,
    length: u64,
    idle: u8,
}
impl Receiving {
    fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(self.length))?;
        self.file.write_all(bytes)?;
        self.length += bytes.len() as u64;
        Ok(())
    }
    fn matches(&mut self, offset: u64, bytes: &[u8], scratch: &mut Vec<u8>) -> std::io::Result<bool> {
        self.file.seek(SeekFrom::Start(offset))?;
        scratch.resize(bytes.len(), 0);
        self.file.read_exact(scratch)?;
        Ok(scratch.as_slice() == bytes)
    }
    fn finish_file(
        mut self,
        directory: &Path,
    ) -> Result<(Message, Arc<checkpoint_file::Blob>), RaftError> {
        if self.length != self.identity.total
            || self.file.metadata().map_err(storage_error)?.len() != self.length
        {
            return Err(storage_error("file transfer length mismatch"));
        }
        self.file.seek(SeekFrom::Start(0)).map_err(storage_error)?;
        let mut header = [0; 12];
        self.file.read_exact(&mut header).map_err(storage_error)?;
        let len = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        if len == 0 || len > 4096 || len as u64 + 12 > self.length {
            return Err(invalid("invalid file checkpoint envelope"));
        }
        let mut bytes = vec![0; len];
        self.file.read_exact(&mut bytes).map_err(storage_error)?;
        if digest(&bytes) != u64::from_be_bytes(header[4..].try_into().unwrap()) {
            return Err(invalid("file checkpoint envelope checksum mismatch"));
        }
        let message = Message::decode(bytes.as_slice()).map_err(invalid)?;
        if message.encode_to_vec() != bytes || !message.get_snapshot().data.is_empty() {
            return Err(invalid("invalid file checkpoint metadata encoding"));
        }
        let payload = checkpoint_file::Blob::capture(
            directory,
            &mut self.file,
            self.length - len as u64 - 12,
        )?;
        if payload.manifest.checksum != self.identity.checksum {
            return Err(invalid("file checkpoint payload checksum mismatch"));
        }
        Ok((message, payload))
    }
    fn finish(mut self) -> Result<Vec<u8>, RaftError> {
        if self.length != self.identity.total
            || self.file.metadata().map_err(storage_error)?.len() != self.length
        {
            return Err(storage_error("checkpoint staging file length mismatch"));
        }
        self.file.seek(SeekFrom::Start(0)).map_err(storage_error)?;
        // The current protobuf decoder still needs one complete buffer. Retain
        // no growing payload buffer while fragments are arriving.
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.length as usize)
            .map_err(storage_error)?;
        (&mut self.file)
            .take(self.length + 1)
            .read_to_end(&mut bytes)
            .map_err(storage_error)?;
        if bytes.len() as u64 != self.length {
            return Err(storage_error("checkpoint staging file changed during read"));
        }
        Ok(bytes)
    }
}
/// One outgoing and one incoming transfer per local peer bound retained state.
/// Incoming bytes are staged beside the WAL in an automatically removed file.
/// Other lagging voters remain in Raft's retry path until the outgoing slot frees.
struct Finishing {
    identity: Fragment,
    worker: std::thread::JoinHandle<Result<(Message, Arc<checkpoint_file::Blob>), RaftError>>,
}

/// One outgoing and one incoming transfer per local peer bound retained state.
/// File completion runs on one owned worker while Raft continues processing.
pub(super) struct Stream {
    pub(super) permitted: Vec<u64>,
    spool_directory: PathBuf,
    pub(super) payload_directory: PathBuf,
    pub(super) received_payload: Option<Arc<checkpoint_file::Blob>>,
    sending: Option<Sending>,
    receiving: Option<Receiving>,
    finishing: Option<Finishing>,
    #[cfg(test)]
    finalization_gate: Option<std::sync::mpsc::Receiver<()>>,
    completed: Option<Fragment>,
    ack: Option<Fragment>,
}
#[cfg(test)]
impl Default for Stream {
    fn default() -> Self {
        Self::new(std::env::temp_dir())
    }
}
impl Stream {
    pub(super) fn new(spool_directory: PathBuf) -> Self {
        Self {
            permitted: Vec::new(),
            payload_directory: spool_directory.join("file-payloads"),
            received_payload: None,
            spool_directory,
            sending: None,
            receiving: None,
            finishing: None,
            #[cfg(test)]
            finalization_gate: None,
            completed: None,
            ack: None,
        }
    }
    pub(super) fn finish_shutdown(&mut self) {
        if let Some(finishing) = self.finishing.take() {
            let _ = finishing.worker.join();
        }
    }
    pub(super) fn finalizing(&self) -> bool {
        self.finishing.is_some()
    }
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
                file_backed: false,
            },
            bytes: Source::Memory(bytes),
            offset: 0,
            ticks: 0,
            idle: 0,
            ready: true,
        });
        Ok(true)
    }
    pub(super) fn start_file(
        &mut self,
        message: Message,
        payload: Arc<checkpoint_file::Blob>,
    ) -> Result<bool, RaftError> {
        if let Some(sending) = &self.sending {
            return Ok(sending.identity.to == message.to
                && sending.identity.term == message.term
                && sending.identity.index == message.get_snapshot().get_metadata().index
                && sending.identity.file_backed);
        }
        if !message.get_snapshot().data.is_empty() {
            return Err(invalid("file transfer includes inline payload"));
        }
        let bytes = message.encode_to_vec();
        if bytes.len() > 4096 {
            return Err(invalid("file transfer metadata exceeds bound"));
        }
        let mut prefix = (bytes.len() as u32).to_be_bytes().to_vec();
        prefix.extend_from_slice(&digest(&bytes).to_be_bytes());
        prefix.extend(bytes);
        let source = Source::File {
            prefix,
            reader: payload.reader()?,
            payload: Arc::clone(&payload),
        };
        self.sending = Some(Sending {
            identity: Fragment {
                from: message.from,
                to: message.to,
                term: message.term,
                index: message.get_snapshot().get_metadata().index,
                total: source.len() as u64,
                checksum: payload.manifest.checksum,
                offset: 0,
                data: Vec::new(),
                ack: false,
                file_backed: true,
            },
            bytes: source,
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
    #[cfg(test)]
    fn receive(
        &mut self,
        part: Fragment,
        voters: &[u64],
    ) -> Result<Option<Message>, RaftError> {
        self.receive_with_scratch(part, voters, &mut Vec::new())
    }
    pub(super) fn receive_with_scratch(
        &mut self,
        part: Fragment,
        voters: &[u64],
        scratch: &mut Vec<u8>,
    ) -> Result<Option<Message>, RaftError> {
        // Keep one finalization worker, even when another sender or term arrives.
        // No completion acknowledgment is sent until validation and sync finish.
        if self.finishing.is_some() {
            return Ok(None);
        }
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
            // A partial file is expendable transport state, never a checkpoint
            // commit. Its lifetime ends on completion, expiry, error or drop.
            self.ack = None;
            self.receiving = None;
            let file = tempfile::tempfile_in(&self.spool_directory).map_err(storage_error)?;
            self.receiving = Some(Receiving {
                identity: part.identity(),
                file,
                length: 0,
                idle: 0,
            });
        }
        self.ack = None;
        let receiving = self.receiving.as_mut().unwrap();
        receiving.idle = 0;
        let io_result = if part.offset == receiving.length {
            receiving.append(&part.data).map(|()| true)
        } else if part.offset < receiving.length {
            receiving.matches(part.offset, &part.data, scratch)
        } else {
            Ok(true)
        };
        match io_result {
            Ok(true) => {}
            Ok(false) => return Err(invalid("conflicting duplicate checkpoint fragment")),
            Err(error) => {
                self.receiving = None;
                return Err(storage_error(error));
            }
        }
        let accepted = self.receiving.as_ref().unwrap().length;
        if accepted != part.total {
            self.ack = Some(part.acknowledgment(accepted));
            return Ok(None);
        }
        let receiving = self.receiving.take().unwrap();
        let (message, payload) = if part.file_backed {
            let directory = self.payload_directory.clone();
            #[cfg(test)]
            let gate = self.finalization_gate.take();
            let worker = std::thread::Builder::new()
                .name("raft-checkpoint-receive".into())
                .spawn(move || {
                    #[cfg(test)]
                    if let Some(gate) = gate {
                        gate.recv().map_err(storage_error)?;
                    }
                    receiving.finish_file(&directory)
                })
                .map_err(storage_error)?;
            self.finishing = Some(Finishing {
                identity: part.identity(),
                worker,
            });
            return Ok(None);
        } else {
            let bytes = receiving.finish()?;
            if digest(&bytes) != part.checksum {
                return Err(invalid("checkpoint transfer checksum mismatch"));
            }
            let message = Message::decode(bytes.as_slice()).map_err(invalid)?;
            if message.encode_to_vec() != bytes {
                return Err(invalid("noncanonical checkpoint message"));
            }
            (message, None)
        };
        self.complete(part, message, payload, voters)
    }
    pub(super) fn poll_finished(
        &mut self,
        term: u64,
        voters: &[u64],
    ) -> Result<Option<Message>, RaftError> {
        if !self
            .finishing
            .as_ref()
            .is_some_and(|f| f.worker.is_finished())
        {
            return Ok(None);
        }
        let finishing = self.finishing.take().unwrap();
        let result = finishing
            .worker
            .join()
            .map_err(|_| storage_error("checkpoint receive worker panicked"))?;
        if finishing.identity.term < term {
            // Dropping a superseded result also removes its uncommitted payload.
            return Ok(None);
        }
        let (message, payload) = result?;
        self.complete(finishing.identity, message, Some(payload), voters)
    }
    fn complete(
        &mut self,
        part: Fragment,
        message: Message,
        payload: Option<Arc<checkpoint_file::Blob>>,
        voters: &[u64],
    ) -> Result<Option<Message>, RaftError> {
        if message.get_msg_type() != MessageType::MsgSnapshot
            || message.from != part.from
            || message.to != part.to
            || message.term != part.term
            || message.get_snapshot().get_metadata().index != part.index
            || message.get_snapshot().get_metadata().term > message.term
            || !message.entries.is_empty()
        {
            return Err(invalid("checkpoint transfer message mismatch"));
        }
        validate_snapshot_for(message.get_snapshot(), voters, &self.permitted)?;
        self.received_payload = payload;
        self.completed = Some(part.identity());
        self.ack = Some(part.acknowledgment(part.total));
        Ok(Some(message))
    }
    pub(super) fn packets(&mut self, group: u64) -> Result<Vec<PeerPacket>, RaftError> {
        let mut parts = Vec::new();
        if let Some(ack) = self.ack.take() {
            parts.push(ack);
        }
        if let Some(sending) = &mut self.sending {
            if sending.ready {
                let end = (sending.offset + SNAPSHOT_FRAGMENT_BYTES).min(sending.bytes.len());
                parts.push(Fragment {
                    offset: sending.offset as u64,
                    data: sending
                        .bytes
                        .fragment(sending.offset, end - sending.offset)?,
                    ..sending.identity.clone()
                });
                sending.ready = false;
            }
        }
        parts
            .into_iter()
            .map(|part| {
                Ok(PeerPacket {
                    to: part.to,
                    bytes: encode_envelope(if part.file_backed { 3 } else { 2 }, group, &part)?,
                })
            })
            .collect()
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.finish_shutdown();
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
        let packets = stream.packets(1).unwrap();
        assert_eq!(packets.len(), 1);
        let packet = Packet::decode(packets[0].bytes.as_slice()).unwrap();
        assert_eq!(packet.version, 2);
        Fragment::decode(packet.message.as_ref()).unwrap()
    }
    #[test]
    fn fragment_metadata_and_direct_envelopes_preserve_wire_bytes() {
        for file_backed in [false, true] {
            for group in [0, 17, u64::MAX] {
                for length in [0, 1, 127, 128, SNAPSHOT_FRAGMENT_BYTES] {
                    let part = Fragment {
                        from: 1,
                        to: 2,
                        term: 4,
                        index: 9,
                        total: 2 * SNAPSHOT_FRAGMENT_BYTES as u64,
                        checksum: u64::MAX,
                        offset: SNAPSHOT_FRAGMENT_BYTES as u64,
                        data: vec![13; length],
                        ack: false,
                        file_backed,
                    };
                    let mut expected_identity = part.clone();
                    expected_identity.data.clear();
                    expected_identity.offset = 0;
                    assert_eq!(part.identity(), expected_identity);
                    let mut expected_ack = expected_identity;
                    expected_ack.from = part.to;
                    expected_ack.to = part.from;
                    expected_ack.offset = part.total;
                    expected_ack.ack = true;
                    assert_eq!(part.acknowledgment(part.total), expected_ack);
                    for payload in [part, expected_ack] {
                        let version = if file_backed { 3 } else { 2 };
                        let expected = Packet {
                            version,
                            group,
                            message: payload.encode_to_vec().into(),
                        }.encode_to_vec();
                        assert_eq!(encode_envelope(version, group, &payload).unwrap(), expected);
                    }
                }
            }
        }
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
            receiver.receiving.as_ref().unwrap().length,
            SNAPSHOT_FRAGMENT_BYTES as u64
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
                data: sending.bytes.fragment(offset, end - offset).unwrap(),
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
        assert!(stream.packets(1).unwrap().is_empty());
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
    #[test]
    fn incoming_checkpoint_spools_only_received_bytes_and_expiry_removes_them() {
        let directory = tempfile::tempdir().unwrap();
        let mut receiver = Stream::new(directory.path().to_path_buf());
        let mut part = Fragment {
            from: 1,
            to: 2,
            term: 4,
            index: 9,
            total: 64 * SNAPSHOT_FRAGMENT_BYTES as u64,
            checksum: 42,
            offset: 0,
            data: vec![13; SNAPSHOT_FRAGMENT_BYTES],
            ack: false,
            file_backed: false,
        };
        for number in 0..16 {
            part.offset = number * SNAPSHOT_FRAGMENT_BYTES as u64;
            assert!(receiver
                .receive(part.clone(), &[1, 2, 3])
                .unwrap()
                .is_none());
            let receiving = receiver.receiving.as_ref().unwrap();
            let received = (number + 1) * SNAPSHOT_FRAGMENT_BYTES as u64;
            assert_eq!(receiving.length, received);
            assert_eq!(receiving.file.metadata().unwrap().len(), received);
            assert!(receiving.identity.data.is_empty());
            assert_eq!(fragment(&mut receiver).offset, received);
        }
        for _ in 0..100 {
            receiver.tick(4, false);
        }
        assert!(receiver.receiving.is_none());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        // An expired receiver asks the sender to restart from zero; it cannot
        // advertise the discarded partial bytes as a completed checkpoint.
        assert!(receiver.receive(part, &[1, 2, 3]).unwrap().is_none());
        assert_eq!(fragment(&mut receiver).offset, 0);
        assert!(receiver.receiving.is_none());
    }

    #[test]
    fn duplicate_comparison_reuses_validation_storage_and_checks_all_bytes() {
        for file_backed in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut sender = Stream::default();
            sender.start(message()).unwrap();
            let mut first = fragment(&mut sender);
            first.file_backed = file_backed;
            let mut second = first.clone();
            second.offset = SNAPSHOT_FRAGMENT_BYTES as u64;
            second.data.fill(29);
            let mut receiver = Stream::new(directory.path().to_path_buf());
            let mut scratch = second.encode_to_vec();
            let allocation = scratch.as_ptr();
            let capacity = scratch.capacity();
            for part in [&first, &second] {
                receiver.receive_with_scratch(part.clone(), &[1, 2, 3], &mut scratch).unwrap();
            }
            for _ in 0..16 {
                for part in [&first, &second] {
                    scratch.clear();
                    part.encode(&mut scratch).unwrap();
                    assert!(receiver.receive_with_scratch(
                        part.clone(), &[1, 2, 3], &mut scratch,
                    ).unwrap().is_none());
                    assert_eq!(scratch, part.data);
                    assert_eq!(scratch.as_ptr(), allocation);
                    assert_eq!(scratch.capacity(), capacity);
                    assert_eq!(receiver.ack.as_ref().unwrap().offset, 2 * SNAPSHOT_FRAGMENT_BYTES as u64);
                }
            }
            let mut conflicting = second.clone();
            *conflicting.data.last_mut().unwrap() ^= 1;
            assert!(receiver.receive_with_scratch(conflicting, &[1, 2, 3], &mut scratch).is_err());
            assert!(receiver.ack.is_none());
            assert_eq!(receiver.receiving.as_ref().unwrap().length, 2 * SNAPSHOT_FRAGMENT_BYTES as u64);
            receiver.receiving.as_mut().unwrap().file.set_len(SNAPSHOT_FRAGMENT_BYTES as u64).unwrap();
            assert!(receiver.receive_with_scratch(second, &[1, 2, 3], &mut scratch).is_err());
            assert!(receiver.receiving.is_none());
            assert!(receiver.ack.is_none());
        }
    }

    #[test]
    fn staging_io_errors_discard_partial_state_without_acknowledging_it() {
        let directory = tempfile::tempdir().unwrap();
        let mut sender = Stream::default();
        sender.start(message()).unwrap();
        let first = fragment(&mut sender);
        let mut unavailable = Stream::new(directory.path().join("missing"));
        assert!(unavailable.receive(first.clone(), &[1, 2, 3]).is_err());
        assert!(unavailable.packets(1).unwrap().is_empty());
        assert!(unavailable.receiving.is_none());

        let mut receiver = Stream::new(directory.path().to_path_buf());
        receiver.receive(first.clone(), &[1, 2, 3]).unwrap();
        receiver
            .receiving
            .as_mut()
            .unwrap()
            .file
            .set_len(0)
            .unwrap();
        // A duplicate must be checked against the actual staged bytes. Failure
        // to read them also clears the acknowledgment queued by the first part.
        assert!(receiver.receive(first.clone(), &[1, 2, 3]).is_err());
        assert!(receiver.receiving.is_none());
        assert!(receiver.completed.is_none());
        assert!(receiver.packets(1).unwrap().is_empty());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        let mut receiver = Stream::new(directory.path().to_path_buf());
        receiver.receive(first.clone(), &[1, 2, 3]).unwrap();
        let mut read_only = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
        read_only.as_file_mut().write_all(&first.data).unwrap();
        receiver.receiving.as_mut().unwrap().file = File::open(read_only.path()).unwrap();
        let mut next = first;
        next.offset = SNAPSHOT_FRAGMENT_BYTES as u64;
        assert!(receiver.receive(next, &[1, 2, 3]).is_err());
        assert!(receiver.receiving.is_none());
        assert!(receiver.completed.is_none());
        assert!(receiver.packets(1).unwrap().is_empty());
        drop(read_only);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn damaged_staging_file_never_becomes_a_completed_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let mut sender = Stream::default();
        sender.start(message()).unwrap();
        let first = fragment(&mut sender);
        let mut receiver = Stream::new(directory.path().to_path_buf());
        receiver.receive(first.clone(), &[1, 2, 3]).unwrap();
        let file = &mut receiver.receiving.as_mut().unwrap().file;
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xff]).unwrap();
        let sending = sender.sending.as_ref().unwrap();
        let mut offset = SNAPSHOT_FRAGMENT_BYTES;
        while offset < sending.bytes.len() {
            let end = (offset + SNAPSHOT_FRAGMENT_BYTES).min(sending.bytes.len());
            let part = Fragment {
                offset: offset as u64,
                data: sending.bytes.fragment(offset, end - offset).unwrap(),
                ..first.identity()
            };
            let result = receiver.receive(part, &[1, 2, 3]);
            if end == sending.bytes.len() {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_none());
            }
            offset = end;
        }
        assert!(receiver.receiving.is_none());
        assert!(receiver.completed.is_none());
        assert!(receiver.packets(1).unwrap().is_empty());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

#[cfg(test)]
mod file_tests {
    use super::*;
    #[test]
    fn stalled_file_finalization_keeps_raft_responsive_and_discards_old_terms() {
        for (superseded, corrupt, shutdown) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let options = PeerOptions {
                group_id: 1,
                node_id: 2,
                voters: vec![1, 2, 3],
                wal_path: directory.path().join("peer.wal"),
            };
            let mut peer = DurablePeer::open(options.clone()).unwrap();
            let mut source = std::io::repeat(17).take(2 * 1024 * 1024);
            let blob =
                checkpoint_file::Blob::capture(directory.path(), &mut source, 2 * 1024 * 1024)
                    .unwrap();
            if corrupt {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(directory.path().join(&blob.manifest.name))
                    .unwrap()
                    .write_all(&[99])
                    .unwrap();
            }
            let mut sender = Stream::new(directory.path().to_path_buf());
            sender
                .start_file(
                    Message {
                        from: 1,
                        to: 2,
                        term: 4,
                        msg_type: MessageType::MsgSnapshot as i32,
                        snapshot: Some(Snapshot {
                            metadata: Some(SnapshotMetadata {
                                index: 9,
                                term: 3,
                                conf_state: Some(ConfState {
                                    voters: vec![1, 2, 3],
                                    ..Default::default()
                                }),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    blob,
                )
                .unwrap();
            let (release, gate) = std::sync::mpsc::channel();
            peer.stream.finalization_gate = Some(gate);
            let mut final_packet = Vec::new();
            while peer.stream.finishing.is_none() {
                let packets = sender.packets(1).unwrap();
                assert_eq!(packets.len(), 1);
                final_packet = packets[0].bytes.clone();
                peer.step(&final_packet).unwrap();
                for packet in peer.stream.packets(1).unwrap() {
                    let packet = Packet::decode(packet.bytes.as_slice()).unwrap();
                    let ack = Fragment::decode(packet.message.as_ref()).unwrap();
                    assert!(ack.offset < ack.total);
                    sender.acknowledge(&ack);
                }
            }
            // The worker cannot complete until release is sent. Raft must still
            // answer heartbeats, without restoring or acknowledging the image.
            for _ in 0..30 {
                peer.tick().unwrap();
                let heartbeat = Message {
                    from: 1,
                    to: 2,
                    term: if superseded { 5 } else { 4 },
                    msg_type: MessageType::MsgHeartbeat as i32,
                    ..Default::default()
                };
                peer.step(
                    &Packet {
                        version: 1,
                        group: 1,
                        message: heartbeat.encode_to_vec().into(),
                    }
                    .encode_to_vec(),
                )
                .unwrap();
                if !superseded {
                    peer.step(&final_packet).unwrap();
                }
                let output = peer
                    .process_ready_with_snapshot_reader(
                        |_, _| Ok(()),
                        |_, _, _| panic!("unfinished image restored"),
                    )
                    .unwrap();
                assert!(output.iter().any(|packet| {
                    let packet = Packet::decode(packet.bytes.as_slice()).unwrap();
                    packet.version == 1
                        && Message::decode(packet.message.as_ref())
                            .unwrap()
                            .get_msg_type()
                            == MessageType::MsgHeartbeatResponse
                }));
                assert!(output
                    .iter()
                    .all(|packet| Packet::decode(packet.bytes.as_slice()).unwrap().version == 1));
                assert_eq!(peer.applied_index(), 0);
                assert!(!peer.is_leader());
            }
            if shutdown {
                let (started, dropping) = std::sync::mpsc::channel();
                let worker = std::thread::spawn(move || {
                    started.send(()).unwrap();
                    drop(peer);
                });
                dropping.recv().unwrap();
                assert!(DurablePeer::open(options.clone()).is_err());
                release.send(()).unwrap();
                worker.join().unwrap();
                assert!(DurablePeer::open(options).is_ok());
                continue;
            }
            release.send(()).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut restored = false;
            let mut completed = false;
            while peer.stream.finishing.is_some() {
                assert!(std::time::Instant::now() < deadline);
                for packet in peer
                    .process_ready_with_snapshot_reader(
                        |_, _| Ok(()),
                        |id, reader, length| {
                            assert!(!superseded && !corrupt);
                            assert_eq!(id.index, 9);
                            assert_eq!(length, 2 * 1024 * 1024);
                            let mut bytes = Vec::new();
                            reader.read_to_end(&mut bytes).unwrap();
                            assert!(bytes.iter().all(|&byte| byte == 17));
                            restored = true;
                            Ok(())
                        },
                    )
                    .unwrap()
                {
                    let packet = Packet::decode(packet.bytes.as_slice()).unwrap();
                    if packet.version == 3 {
                        let ack = Fragment::decode(packet.message.as_ref()).unwrap();
                        completed |= ack.ack && ack.offset == ack.total;
                    }
                }
                std::thread::yield_now();
            }
            assert_eq!(restored, !superseded && !corrupt);
            assert_eq!(completed, !superseded && !corrupt);
            assert_eq!(
                peer.applied_index(),
                if superseded || corrupt { 0 } else { 9 }
            );
            assert!(peer.healthy().is_ok());
            if superseded || corrupt {
                assert_eq!(
                    std::fs::read_dir(&peer.stream.payload_directory)
                        .unwrap()
                        .count(),
                    0
                );
            }
        }
    }

    #[test]
    #[cfg(unix)]
    fn file_fragments_reuse_the_open_handle_across_retries() {
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..2 * SNAPSHOT_FRAGMENT_BYTES + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        let blob = checkpoint_file::Blob::capture(
            dir.path(), &mut data.as_slice(), data.len() as u64,
        ).unwrap();
        let original = dir.path().join(&blob.manifest.name);
        let moved = dir.path().join("moved-payload");
        let metadata = Message {
            from: 1,
            to: 2,
            term: 4,
            msg_type: MessageType::MsgSnapshot as i32,
            snapshot: Some(Snapshot {
                metadata: Some(SnapshotMetadata {
                    index: 9,
                    term: 3,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut sender = Stream::new(dir.path().to_path_buf());
        sender.start_file(metadata, Arc::clone(&blob)).unwrap();
        drop(blob);
        // Removing the pathname proves subsequent reads use the retained handle.
        std::fs::rename(&original, &moved).unwrap();
        let source = &sender.sending.as_ref().unwrap().bytes;
        let Source::File { prefix, .. } = source else { panic!("expected file source") };
        let mut expected = prefix.clone();
        expected.extend_from_slice(&data);
        for offset in [0, SNAPSHOT_FRAGMENT_BYTES, 0, 2 * SNAPSHOT_FRAGMENT_BYTES,
            SNAPSHOT_FRAGMENT_BYTES, prefix.len() - 1, prefix.len()] {
            let size = SNAPSHOT_FRAGMENT_BYTES.min(expected.len() - offset);
            assert_eq!(source.fragment(offset, size).unwrap(), expected[offset..offset + size]);
        }
        std::fs::rename(&moved, &original).unwrap();
        sender.completed_send(2);
        assert!(!original.exists());
    }

    #[test]
    fn file_transfer_checks_payload_corruption_and_retains_active_send_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = std::io::repeat(17).take(5 * 1024 * 1024);
        let blob =
            checkpoint_file::Blob::capture(dir.path(), &mut source, 5 * 1024 * 1024).unwrap();
        let name = blob.manifest.name.clone();
        let message = Message {
            from: 1,
            to: 2,
            term: 4,
            msg_type: MessageType::MsgSnapshot as i32,
            snapshot: Some(Snapshot {
                data: Vec::new(),
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
        };
        let mut sender = Stream::new(dir.path().to_path_buf());
        sender.start_file(message, Arc::clone(&blob)).unwrap();
        blob.retire();
        drop(blob);
        assert!(dir.path().join(&name).exists());
        let mut receiver = Stream::new(dir.path().to_path_buf());
        let mut rejected = false;
        for _ in 0..20 {
            let packets = sender.packets(1).unwrap();
            assert_eq!(packets.len(), 1);
            let packet = Packet::decode(packets[0].bytes.as_slice()).unwrap();
            assert_eq!(packet.version, 3);
            let mut part = Fragment::decode(packet.message.as_ref()).unwrap();
            if part.offset > 0 {
                part.data[0] ^= 1;
            }
            let mut result = receiver.receive(part, &[1, 2, 3]);
            if receiver.finishing.is_some() {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                while receiver.finishing.is_some() {
                    assert!(std::time::Instant::now() < deadline);
                    result = receiver.poll_finished(4, &[1, 2, 3]);
                    std::thread::yield_now();
                }
            }
            match result {
                Err(_) => {
                    rejected = true;
                    break;
                }
                Ok(None) => {}
                Ok(Some(_)) => panic!("corrupt file checkpoint accepted"),
            }
            for ack in receiver.packets(1).unwrap() {
                let packet = Packet::decode(ack.bytes.as_slice()).unwrap();
                let ack = Fragment::decode(packet.message.as_ref()).unwrap();
                sender.acknowledge(&ack);
            }
        }
        assert!(rejected);
        assert!(receiver.received_payload.is_none());
        assert!(receiver.packets(1).unwrap().is_empty());
        drop(sender);
        assert!(!dir.path().join(name).exists());
        assert_eq!(
            std::fs::read_dir(&receiver.payload_directory)
                .unwrap()
                .count(),
            0
        );
    }
}
