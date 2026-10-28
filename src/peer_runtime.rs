// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Independent durable peers backed by the raft-rs consensus core.
//!
//! This API does not simulate remote peers. Call `process_ready` after each input,
//! deliver its packets to their destinations, and supply a fresh application on
//! restart: committed entries replay from the beginning. Membership is fixed;
//! snapshot installation and log compaction are not supported by this first API.
use crate::{LogId, RaftError};
use prost::Message as ProstMessage;
use raft_core::{
    eraftpb::{Entry, EntryType, HardState, Message, MessageType},
    storage::MemStorage,
    Config, RawNode, ReadOnlyOption, StateRole, Storage,
};
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const LIMIT: usize = 8 * 1024 * 1024;
const WAL_LIMIT: u64 = 512 * 1024 * 1024;
const MAGIC: &[u8; 4] = b"MRPW";

/// Static identity and voter set. The WAL refuses a different identity on reopen.
#[derive(Clone, Debug)]
pub struct PeerOptions {
    pub group_id: u64,
    pub node_id: u64,
    pub voters: Vec<u64>,
    /// A file in an existing directory, exclusively locked while the peer is open.
    pub wal_path: PathBuf,
}
/// A binary protobuf packet. Delivery must preserve these bytes.
#[derive(Clone, Debug)]
pub struct PeerPacket {
    pub to: u64,
    pub bytes: Vec<u8>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct Packet {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    group: u64,
    #[prost(bytes = "vec", tag = "3")]
    message: Vec<u8>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct WalBatch {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    group: u64,
    #[prost(uint64, tag = "3")]
    node: u64,
    #[prost(uint64, repeated, packed = "true", tag = "4")]
    voters: Vec<u64>,
    #[prost(bytes = "vec", tag = "5")]
    hard_state: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "6")]
    entries: Vec<Vec<u8>>,
}
fn invalid(s: impl ToString) -> RaftError {
    RaftError::InvalidRequest(s.to_string())
}
fn storage_error(s: impl ToString) -> RaftError {
    RaftError::Storage(s.to_string())
}
fn digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    })
}
fn batch(options: &PeerOptions, hs: Option<&HardState>, entries: &[Entry]) -> WalBatch {
    WalBatch {
        version: 1,
        group: options.group_id,
        node: options.node_id,
        voters: options.voters.clone(),
        hard_state: hs.map(ProstMessage::encode_to_vec).unwrap_or_default(),
        entries: entries.iter().map(ProstMessage::encode_to_vec).collect(),
    }
}
fn install(store: &MemStorage, batch: &WalBatch) -> Result<(), RaftError> {
    let entries = batch
        .entries
        .iter()
        .map(|b| Entry::decode(b.as_slice()).map_err(storage_error))
        .collect::<Result<Vec<_>, _>>()?;
    if entries
        .iter()
        .any(|e| e.entry_type != EntryType::EntryNormal as i32)
    {
        return Err(storage_error("peer WAL contains a membership change"));
    }
    let hs = if batch.hard_state.is_empty() {
        None
    } else {
        Some(HardState::decode(batch.hard_state.as_slice()).map_err(storage_error)?)
    };
    let previous = store.initial_state().map_err(storage_error)?.hard_state;
    let last = store.last_index().map_err(storage_error)?;
    if entries
        .windows(2)
        .any(|w| w[0].index.checked_add(1) != Some(w[1].index) || w[0].term > w[1].term)
        || entries
            .first()
            .is_some_and(|e| e.index == 0 || e.index > last.saturating_add(1))
    {
        return Err(storage_error("peer WAL log positions are invalid"));
    }
    for entry in entries.iter().filter(|e| e.index <= previous.commit) {
        let saved = store
            .entries(
                entry.index,
                entry.index + 1,
                None,
                raft_core::GetEntriesContext::empty(false),
            )
            .map_err(storage_error)?;
        if saved.as_slice() != std::slice::from_ref(entry) {
            return Err(storage_error("peer WAL changes a committed entry"));
        }
    }
    let next = hs.as_ref().unwrap_or(&previous);
    let next_last = entries.last().map_or(last, |e| e.index);
    if next.term < previous.term
        || next.commit < previous.commit
        || next.commit > next_last
        || (next.term == previous.term && previous.vote != 0 && next.vote != previous.vote)
        || (next.vote != 0 && !batch.voters.contains(&next.vote))
        || entries.iter().any(|e| e.term == 0 || e.term > next.term)
    {
        return Err(storage_error("peer WAL hard state is invalid"));
    }
    let mut core = store.wl();
    core.append(&entries).map_err(storage_error)?;
    if let Some(hs) = hs {
        core.set_hardstate(hs);
    }
    Ok(())
}
struct Journal {
    file: File,
    length: u64,
}
impl Journal {
    fn open(options: &PeerOptions, store: &MemStorage) -> Result<Self, RaftError> {
        let path = &options.wal_path;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(storage_error)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(storage_error)?;
        let length = file.metadata().map_err(storage_error)?.len();
        if length > WAL_LIMIT {
            return Err(storage_error("peer WAL exceeds size limit"));
        }
        let mut offset = 0;
        while offset < length {
            let remaining = length - offset;
            if remaining < 24 {
                let mut tail = vec![0; remaining as usize];
                file.read_exact(&mut tail).map_err(storage_error)?;
                let prefix = tail.len().min(4);
                if tail[..prefix] != MAGIC[..prefix] {
                    return Err(storage_error("damaged peer WAL tail"));
                }
                break;
            }
            let mut header = [0; 24];
            file.read_exact(&mut header).map_err(storage_error)?;
            if &header[..4] != MAGIC
                || digest(&header[..8]) != u64::from_be_bytes(header[8..16].try_into().unwrap())
            {
                return Err(storage_error("peer WAL header checksum mismatch"));
            }
            let size = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
            if size == 0 || size > LIMIT {
                return Err(storage_error("peer WAL record exceeds size limit"));
            }
            if remaining - 24 < size as u64 {
                break;
            }
            let mut bytes = vec![0; size];
            file.read_exact(&mut bytes).map_err(storage_error)?;
            if digest(&bytes) != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
                return Err(storage_error("peer WAL payload checksum mismatch"));
            }
            let record = WalBatch::decode(bytes.as_slice()).map_err(storage_error)?;
            if record.version != 1
                || record.group != options.group_id
                || record.node != options.node_id
                || record.voters != options.voters
                || record.encode_to_vec() != bytes
            {
                return Err(storage_error("peer WAL identity or encoding mismatch"));
            }
            install(store, &record)?;
            offset += 24 + size as u64;
        }
        if offset != length {
            file.set_len(offset).map_err(storage_error)?;
        }
        file.seek(SeekFrom::End(0)).map_err(storage_error)?;
        file.sync_all().map_err(storage_error)?;
        #[cfg(unix)]
        File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )
        .and_then(|f| f.sync_all())
        .map_err(storage_error)?;
        let mut journal = Self {
            file,
            length: offset,
        };
        if offset == 0 {
            journal.append(&batch(options, None, &[]))?;
        }
        Ok(journal)
    }
    fn append(&mut self, batch: &WalBatch) -> Result<(), RaftError> {
        let size = batch.encoded_len();
        if size > LIMIT || self.length.saturating_add(24 + size as u64) > WAL_LIMIT {
            return Err(storage_error(
                "peer WAL capacity reached; checkpoint support is required",
            ));
        }
        let bytes = batch.encode_to_vec();
        let mut record = MAGIC.to_vec();
        record.extend_from_slice(&(size as u32).to_be_bytes());
        record.extend_from_slice(&digest(&record).to_be_bytes());
        record.extend_from_slice(&digest(&bytes).to_be_bytes());
        record.extend_from_slice(&bytes);
        self.file
            .write_all(&record)
            .and_then(|_| self.file.sync_all())
            .map_err(storage_error)?;
        self.length += record.len() as u64;
        Ok(())
    }
}
struct PendingRead {
    token: u64,
    term: u64,
    index: Option<u64>,
}
impl PendingRead {
    fn context(&self) -> Vec<u8> {
        [self.term.to_be_bytes(), self.token.to_be_bytes()].concat()
    }
}
/// One actual Raft peer. Remote votes and append acknowledgments enter only via `step`.
pub struct DurablePeer {
    options: PeerOptions,
    raw: RawNode<MemStorage>,
    store: MemStorage,
    journal: Journal,
    failed: bool,
    read_sequence: u64,
    pending_read: Option<PendingRead>,
}
impl DurablePeer {
    pub fn open(mut options: PeerOptions) -> Result<Self, RaftError> {
        let unique: BTreeSet<_> = options.voters.iter().copied().collect();
        if options.group_id == 0
            || options.node_id == 0
            || unique.contains(&0)
            || unique.len() != options.voters.len()
            || !unique.contains(&options.node_id)
        {
            return Err(invalid("invalid peer identity or voter set"));
        }
        options.voters = unique.into_iter().collect();
        let store = MemStorage::new_with_conf_state((options.voters.clone(), Vec::<u64>::new()));
        let journal = Journal::open(&options, &store)?;
        let config = Config {
            id: options.node_id,
            election_tick: 10,
            heartbeat_tick: 2,
            check_quorum: true,
            read_only_option: ReadOnlyOption::Safe,
            pre_vote: true,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 64,
            max_uncommitted_size: 4 * 1024 * 1024,
            ..Default::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&config, store.clone(), &logger).map_err(invalid)?;
        Ok(Self {
            options,
            raw,
            store,
            journal,
            failed: false,
            read_sequence: 0,
            pending_read: None,
        })
    }
    fn healthy(&self) -> Result<(), RaftError> {
        if self.failed {
            Err(storage_error(
                "peer failed; restart with a fresh application",
            ))
        } else {
            Ok(())
        }
    }
    pub fn node_id(&self) -> u64 {
        self.options.node_id
    }
    pub fn leader_id(&self) -> Option<u64> {
        (self.raw.raft.leader_id != 0).then_some(self.raw.raft.leader_id)
    }
    pub fn term(&self) -> u64 {
        self.raw.raft.term
    }
    pub fn commit_index(&self) -> u64 {
        self.raw.raft.raft_log.committed
    }
    pub fn applied_index(&self) -> u64 {
        self.raw.raft.raft_log.applied
    }
    pub fn is_leader(&self) -> bool {
        !self.failed && self.raw.raft.state == StateRole::Leader
    }
    pub fn tick(&mut self) -> Result<(), RaftError> {
        self.healthy()?;
        self.raw.tick();
        Ok(())
    }
    pub fn campaign(&mut self) -> Result<(), RaftError> {
        self.healthy()?;
        self.raw.campaign().map_err(invalid)
    }
    /// Acceptance allocates a log position; it does not mean the proposal committed.
    pub fn propose(&mut self, payload: Vec<u8>) -> Result<LogId, RaftError> {
        self.healthy()?;
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.options.node_id));
        }
        if payload.is_empty() || payload.len() > 4 * 1024 * 1024 {
            return Err(invalid("peer proposal must contain 1..4 MiB"));
        }
        let before = self.raw.raft.raft_log.last_index();
        self.raw.propose(Vec::new(), payload).map_err(invalid)?;
        let index = self.raw.raft.raft_log.last_index();
        if index == before {
            return Err(invalid("peer proposal was not admitted"));
        }
        Ok(LogId {
            term: self.raw.raft.term,
            index,
        })
    }
    /// Start a quorum-confirmed read without appending an application entry.
    /// Only one read may be outstanding. Keep polling after a caller timeout:
    /// forgetting an unresolved read would allow unbounded consensus contexts.
    /// The token is local to this open peer and is not a durable identifier.
    pub fn begin_read(&mut self) -> Result<u64, RaftError> {
        self.healthy()?;
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.options.node_id));
        }
        if self
            .pending_read
            .as_ref()
            .is_some_and(|r| r.term != self.term())
        {
            self.pending_read = None;
        }
        if self.pending_read.is_some() {
            return Err(invalid("a peer read is already pending"));
        }
        self.read_sequence = self
            .read_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("peer read sequence exhausted"))?;
        let read = PendingRead {
            token: self.read_sequence,
            term: self.term(),
            index: None,
        };
        self.raw.read_index(read.context());
        self.pending_read = Some(read);
        Ok(self.read_sequence)
    }
    /// True consumes a quorum confirmation whose index has applied locally.
    /// Process and deliver Ready packets between polls. A lost leadership term
    /// invalidates the token; a local clock or an old confirmation cannot serve it.
    pub fn poll_read(&mut self, token: u64) -> Result<bool, RaftError> {
        self.healthy()?;
        let read = self
            .pending_read
            .as_ref()
            .filter(|r| r.token == token)
            .ok_or_else(|| invalid("unknown peer read token"))?;
        if !self.is_leader() || read.term != self.term() {
            self.pending_read = None;
            return Err(RaftError::NotLeader(self.options.node_id));
        }
        if read
            .index
            .is_some_and(|index| index <= self.applied_index())
        {
            self.pending_read = None;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub fn step(&mut self, bytes: &[u8]) -> Result<(), RaftError> {
        self.healthy()?;
        if bytes.len() > LIMIT {
            return Err(invalid("peer packet exceeds size limit"));
        }
        let packet = Packet::decode(bytes).map_err(invalid)?;
        if packet.version != 1
            || packet.group != self.options.group_id
            || packet.encode_to_vec() != bytes
        {
            return Err(invalid("peer packet identity or encoding mismatch"));
        }
        let message = Message::decode(packet.message.as_slice()).map_err(invalid)?;
        let kind = message.get_msg_type();
        let allowed = matches!(
            kind,
            MessageType::MsgAppend
                | MessageType::MsgAppendResponse
                | MessageType::MsgRequestVote
                | MessageType::MsgRequestVoteResponse
                | MessageType::MsgHeartbeat
                | MessageType::MsgHeartbeatResponse
                | MessageType::MsgRequestPreVote
                | MessageType::MsgRequestPreVoteResponse
        );
        if !allowed
            || message.term == 0
            || message.term == u64::MAX
            || message.index == u64::MAX
            || message.encode_to_vec() != packet.message
        {
            return Err(invalid("unsupported or malformed peer message"));
        }
        if message.to != self.options.node_id
            || message.from == self.options.node_id
            || !self.options.voters.contains(&message.from)
            || raft_core::raw_node::is_local_msg(kind)
            || matches!(
                kind,
                MessageType::MsgPropose | MessageType::MsgSnapshot | MessageType::MsgTransferLeader
            )
            || message
                .entries
                .iter()
                .any(|e| e.entry_type != EntryType::EntryNormal as i32)
        {
            return Err(invalid(
                "peer packet is not valid for this fixed-membership runtime",
            ));
        }
        if message.snapshot.is_some()
            || (kind != MessageType::MsgAppend && !message.entries.is_empty())
        {
            return Err(invalid("unexpected peer message payload"));
        }
        let mut index = message.index;
        let mut term = message.log_term;
        for entry in &message.entries {
            index = index
                .checked_add(1)
                .ok_or_else(|| invalid("entry index overflow"))?;
            if entry.index != index
                || entry.index == u64::MAX
                || entry.term == 0
                || entry.term < term
                || entry.term > message.term
            {
                return Err(invalid("invalid peer entry sequence"));
            }
            term = entry.term;
        }
        self.raw.step(message).map_err(invalid)
    }
    fn packets(&self, messages: Vec<Message>) -> Result<Vec<PeerPacket>, RaftError> {
        messages
            .into_iter()
            .map(|message| {
                let to = message.to;
                let packet = Packet {
                    version: 1,
                    group: self.options.group_id,
                    message: message.encode_to_vec(),
                };
                if packet.encoded_len() > LIMIT {
                    return Err(invalid("outgoing peer packet exceeds size limit"));
                }
                Ok(PeerPacket {
                    to,
                    bytes: packet.encode_to_vec(),
                })
            })
            .collect()
    }
    /// Persist before returning packets or applying committed entries. Empty payloads
    /// are election no-ops and still carry an application position. On restart the
    /// callback must rebuild a fresh application (or provide idempotent application).
    /// Any persistence or application failure permanently stops this peer instance.
    pub fn process_ready(
        &mut self,
        mut apply: impl FnMut(LogId, &[u8]) -> Result<(), RaftError>,
    ) -> Result<Vec<PeerPacket>, RaftError> {
        self.healthy()?;
        let result = self.process_inner(&mut apply);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn process_inner(
        &mut self,
        apply: &mut impl FnMut(LogId, &[u8]) -> Result<(), RaftError>,
    ) -> Result<Vec<PeerPacket>, RaftError> {
        let mut output = Vec::new();
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            for confirmation in ready.take_read_states() {
                if let Some(read) = &mut self.pending_read {
                    if confirmation.request_ctx == read.context() {
                        read.index = Some(confirmation.index);
                    }
                }
            }
            if !ready.snapshot().is_empty() {
                return Err(invalid("peer snapshot installation is not supported"));
            }
            if ready.hs().is_some() || !ready.entries().is_empty() {
                let record = batch(&self.options, ready.hs(), ready.entries());
                self.journal.append(&record)?;
                install(&self.store, &record)?;
            }
            output.extend(self.packets(ready.take_messages())?);
            output.extend(self.packets(ready.take_persisted_messages())?);
            for entry in ready.take_committed_entries() {
                apply(
                    LogId {
                        term: entry.term,
                        index: entry.index,
                    },
                    &entry.data,
                )?;
            }
            let mut light = self.raw.advance(ready);
            if let Some(commit) = light.commit_index() {
                let mut hs = self
                    .store
                    .initial_state()
                    .map_err(storage_error)?
                    .hard_state;
                hs.commit = commit;
                let record = batch(&self.options, Some(&hs), &[]);
                self.journal.append(&record)?;
                install(&self.store, &record)?;
            }
            output.extend(self.packets(light.take_messages())?);
            for entry in light.take_committed_entries() {
                apply(
                    LogId {
                        term: entry.term,
                        index: entry.index,
                    },
                    &entry.data,
                )?;
            }
            self.raw.advance_apply();
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "peer-wal-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    fn options(path: PathBuf) -> PeerOptions {
        PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: path,
        }
    }
    #[test]
    fn wal_failure_returns_no_packets_or_application_and_stops_peer() {
        let path = path("failure");
        let options = options(path.clone());
        let mut peer = DurablePeer::open(options.clone()).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let readonly = File::open(&path).unwrap();
        peer.journal.file = readonly;
        peer.propose(b"not durable".to_vec()).unwrap();
        let mut applied = 0;
        assert!(peer
            .process_ready(|_, _| {
                applied += 1;
                Ok(())
            })
            .is_err());
        assert_eq!(applied, 0);
        assert!(peer.tick().is_err());
        assert!(!peer.is_leader());
        drop(peer);
        let mut peer = DurablePeer::open(options).unwrap();
        peer.process_ready(|_, p| {
            assert_ne!(p, b"not durable");
            Ok(())
        })
        .unwrap();
        drop(peer);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn wal_corruption_and_each_torn_final_append_are_handled_safely() {
        let path = path("damage");
        let options = options(path.clone());
        let mut peer = DurablePeer::open(options.clone()).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let base = std::fs::read(&path).unwrap();
        peer.propose(b"new value".to_vec()).unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let full = std::fs::read(&path).unwrap();
        drop(peer);
        for cut in base.len()..full.len() {
            std::fs::write(&path, &full[..cut]).unwrap();
            let mut restored = DurablePeer::open(options.clone()).unwrap();
            restored
                .process_ready(|_, p| {
                    assert_ne!(p, b"new value");
                    Ok(())
                })
                .unwrap();
        }
        for index in 0..full.len() {
            let mut damaged = full.clone();
            damaged[index] ^= 1;
            std::fs::write(&path, &damaged).unwrap();
            assert!(DurablePeer::open(options.clone()).is_err(), "byte {index}");
            assert_eq!(std::fs::read(&path).unwrap(), damaged);
        }
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn packet_decoder_rejects_local_commands_and_deep_unknown_groups() {
        let path = path("packets");
        let mut options = options(path.clone());
        options.voters = vec![1, 2, 3];
        let mut peer = DurablePeer::open(options).unwrap();
        for kind in [
            MessageType::MsgHup,
            MessageType::MsgPropose,
            MessageType::MsgSnapshot,
            MessageType::MsgTransferLeader,
        ] {
            let message = Message {
                from: 2,
                to: 1,
                term: 1,
                msg_type: kind as i32,
                ..Default::default()
            };
            let bytes = Packet {
                version: 1,
                group: 1,
                message: message.encode_to_vec(),
            }
            .encode_to_vec();
            assert!(peer.step(&bytes).is_err());
        }
        for (index, term) in [(2, 1), (1, 0), (1, 2), (u64::MAX, 1)] {
            let message = Message {
                from: 2,
                to: 1,
                term: 1,
                msg_type: MessageType::MsgAppend as i32,
                entries: vec![Entry {
                    index,
                    term,
                    ..Default::default()
                }],
                ..Default::default()
            };
            let bytes = Packet {
                version: 1,
                group: 1,
                message: message.encode_to_vec(),
            }
            .encode_to_vec();
            assert!(peer.step(&bytes).is_err());
        }
        // Unknown protobuf groups are nested beyond the decoder's recursion limit.
        let mut bytes = vec![0x23; 1024];
        bytes.extend(vec![0x24; 1024]);
        assert!(peer.step(&bytes).is_err());
        drop(peer);
        std::fs::remove_file(path).unwrap();
    }
}
