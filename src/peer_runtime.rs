// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Independent durable peers backed by the raft-rs consensus core.
//!
//! This API does not simulate remote peers. Call `process_ready` after each input,
//! deliver its packets to their destinations, and supply a fresh application on
//! restart: committed entries replay from the beginning. Membership is fixed;
//! checkpoints require an application restore callback during recovery and transfer.
use crate::{LogId, RaftError};
use prost::Message as ProstMessage;
use raft_core::{
    eraftpb::{
        ConfState, Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
    },
    storage::MemStorage,
    Config, RawNode, ReadOnlyOption, StateRole, Storage,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[path = "peer_checkpoint_file.rs"]
mod checkpoint_file;
#[path = "peer_snapshot_stream.rs"]
mod snapshot_stream;

const LIMIT: usize = 8 * 1024 * 1024;
/// Maximum application checkpoint size. Transfers and WAL records are segmented.
pub const MAX_CHECKPOINT_BYTES: usize = 256 * 1024 * 1024;
/// Bound for the streaming file checkpoint API, independent of the inline API.
pub const MAX_FILE_CHECKPOINT_BYTES: u64 = checkpoint_file::MAX_FILE_BYTES;
const INLINE_CHECKPOINT_BYTES: usize = 4 * 1024 * 1024;
const SNAPSHOT_ENCODED_LIMIT: usize = MAX_CHECKPOINT_BYTES + 64 * 1024;
const SNAPSHOT_FRAGMENT_BYTES: usize = 1024 * 1024;
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
    #[prost(bytes = "vec", tag = "7")]
    snapshot: Vec<u8>,
    #[prost(uint64, tag = "8")]
    snapshot_offset: u64,
    #[prost(uint64, tag = "9")]
    snapshot_total: u64,
    #[prost(uint64, tag = "10")]
    snapshot_digest: u64,
    #[prost(bytes = "vec", tag = "11")]
    payload_manifest: Vec<u8>,
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
        snapshot: Vec::new(),
        snapshot_offset: 0,
        snapshot_total: 0,
        snapshot_digest: 0,
        payload_manifest: Vec::new(),
    }
}
#[derive(Clone)]
struct PeerStorage {
    memory: MemStorage,
    checkpoint: Arc<Mutex<Option<Snapshot>>>,
    payload_directory: PathBuf,
    payload: Arc<Mutex<Option<Arc<checkpoint_file::Blob>>>>,
}
impl std::ops::Deref for PeerStorage {
    type Target = MemStorage;
    fn deref(&self) -> &Self::Target {
        &self.memory
    }
}
impl Storage for PeerStorage {
    fn initial_state(&self) -> raft_core::Result<raft_core::RaftState> {
        self.memory.initial_state()
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: raft_core::GetEntriesContext,
    ) -> raft_core::Result<Vec<Entry>> {
        self.memory.entries(low, high, max_size, context)
    }
    fn term(&self, index: u64) -> raft_core::Result<u64> {
        self.memory.term(index)
    }
    fn first_index(&self) -> raft_core::Result<u64> {
        self.memory.first_index()
    }
    fn last_index(&self) -> raft_core::Result<u64> {
        self.memory.last_index()
    }
    fn snapshot(&self, request_index: u64, _to: u64) -> raft_core::Result<Snapshot> {
        self.checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .filter(|snapshot| snapshot.get_metadata().index >= request_index)
            .cloned()
            .ok_or(raft_core::Error::Store(
                raft_core::StorageError::SnapshotTemporarilyUnavailable,
            ))
    }
}
fn validate_snapshot(snapshot: &Snapshot, voters: &[u64]) -> Result<(), RaftError> {
    let meta = snapshot.get_metadata();
    let expected = ConfState {
        voters: voters.to_vec(),
        ..Default::default()
    };
    if meta.index == 0
        || meta.index == u64::MAX
        || meta.term == 0
        || meta.term == u64::MAX
        || meta.get_conf_state() != &expected
        || snapshot.data.len() > MAX_CHECKPOINT_BYTES
        || snapshot.encoded_len() > SNAPSHOT_ENCODED_LIMIT - 1024
    {
        return Err(invalid("invalid peer snapshot identity, position or size"));
    }
    Ok(())
}
fn install(store: &PeerStorage, batch: &WalBatch) -> Result<(), RaftError> {
    install_local(store, batch, None)
}
fn install_local(
    store: &PeerStorage,
    batch: &WalBatch,
    captured: Option<Arc<checkpoint_file::Blob>>,
) -> Result<(), RaftError> {
    let local_capture = captured.is_some();
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
    // Local compaction may retain committed entries after the captured index.
    // Compare them before replacing the in-memory snapshot, which clears the log.
    if local_capture {
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
                return Err(storage_error("local checkpoint changes a committed entry"));
            }
        }
    }
    if !batch.snapshot.is_empty() {
        let snapshot = Snapshot::decode(batch.snapshot.as_slice()).map_err(storage_error)?;
        let payload = if batch.version == 4 {
            if !snapshot.data.is_empty() {
                return Err(storage_error("file checkpoint contains inline data"));
            }
            Some(if let Some(payload) = captured {
                if payload.manifest.encode_to_vec() != batch.payload_manifest {
                    return Err(storage_error("captured checkpoint manifest mismatch"));
                }
                payload
            } else {
                checkpoint_file::Blob::open(&store.payload_directory, &batch.payload_manifest)?
            })
        } else {
            None
        };
        validate_snapshot(&snapshot, &batch.voters)?;
        let meta = snapshot.get_metadata();
        let next = hs
            .as_ref()
            .ok_or_else(|| storage_error("snapshot needs hard state"))?;
        if snapshot.encode_to_vec() != batch.snapshot
            || (!local_capture && meta.index < previous.commit)
            || meta.index > next.commit
            || meta.term > next.term
            || next.term < previous.term
            || (next.term == previous.term && previous.vote != 0 && next.vote != previous.vote)
        {
            return Err(storage_error("peer snapshot conflicts with durable state"));
        }
        store
            .wl()
            .apply_snapshot(snapshot.clone())
            .map_err(storage_error)?;
        *store.checkpoint.lock().unwrap() = Some(snapshot);
        let mut current = store.payload.lock().unwrap();
        if let Some(old) = current.as_ref() {
            if payload
                .as_ref()
                .is_none_or(|new| new.manifest.name != old.manifest.name)
            {
                old.retire();
            }
        }
        *current = payload;
    }

    let last = store.last_index().map_err(storage_error)?;
    if entries
        .windows(2)
        .any(|w| w[0].index.checked_add(1) != Some(w[1].index) || w[0].term > w[1].term)
        || entries.first().is_some_and(|e| {
            e.index < store.first_index().unwrap_or(u64::MAX) || e.index > last.saturating_add(1)
        })
    {
        return Err(storage_error("peer WAL log positions are invalid"));
    }
    for entry in entries
        .iter()
        .filter(|e| !local_capture && e.index <= previous.commit)
    {
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
    _lock: Arc<File>,
    length: u64,
}
impl Journal {
    fn open(options: &PeerOptions, store: &PeerStorage) -> Result<Self, RaftError> {
        let path = &options.wal_path;
        let lock_path = path.with_extension("runtime.lock");
        let lock_existed = lock_path.exists();
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(storage_error)?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(storage_error)?;
        let existed = path.exists();
        if lock_existed && !existed {
            return Err(storage_error("peer WAL is missing beside its runtime lock"));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(storage_error)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(storage_error)?;
        let length = file.metadata().map_err(storage_error)?.len();
        if existed && length == 0 {
            return Err(storage_error("existing peer WAL is empty"));
        }
        if length > WAL_LIMIT {
            return Err(storage_error("peer WAL exceeds size limit"));
        }
        let mut offset = 0;
        let mut assembly = snapshot_stream::WalAssembly::default();
        while offset < length {
            let remaining = length - offset;
            if remaining < 24 {
                if offset == 0 {
                    return Err(storage_error("incomplete initial peer WAL record"));
                }
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
                if offset == 0 {
                    return Err(storage_error("incomplete initial peer WAL payload"));
                }
                break;
            }
            let mut bytes = vec![0; size];
            file.read_exact(&mut bytes).map_err(storage_error)?;
            if digest(&bytes) != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
                return Err(storage_error("peer WAL payload checksum mismatch"));
            }
            let record = WalBatch::decode(bytes.as_slice()).map_err(storage_error)?;
            if !matches!(
                (record.version, record.snapshot.is_empty()),
                (1, true) | (2, false) | (3, false) | (4, false)
            ) || (record.version == 4) == record.payload_manifest.is_empty()
                || record.group != options.group_id
                || record.node != options.node_id
                || record.voters != options.voters
                || record.encode_to_vec() != bytes
            {
                return Err(storage_error("peer WAL identity or encoding mismatch"));
            }
            if let Some(record) = assembly.accept(record, offset)? {
                install(store, &record)?;
            }
            offset += 24 + size as u64;
        }
        assembly.finish()?;
        checkpoint_file::cleanup(
            &store.payload_directory,
            store
                .payload
                .lock()
                .unwrap()
                .as_ref()
                .map(|b| b.manifest.name.as_str()),
        )?;
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
            _lock: Arc::new(lock),
            length: offset,
        };
        if offset == 0 {
            journal.append(&batch(options, None, &[]))?;
        }
        Ok(journal)
    }
    fn rewrite(
        &mut self,
        options: &PeerOptions,
        snapshot: &Snapshot,
        hs: &HardState,
        entries: &[Entry],
        payload: Option<&checkpoint_file::Blob>,
    ) -> Result<(), RaftError> {
        let temporary = options.wal_path.with_extension("checkpoint.tmp");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&temporary)
            .map_err(storage_error)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(storage_error)?;
        file.set_len(0).map_err(storage_error)?;
        let mut replacement = Journal {
            file,
            length: 0,
            _lock: Arc::clone(&self._lock),
        };
        let mut floor_state = hs.clone();
        floor_state.commit = snapshot.get_metadata().index;
        let mut first = batch(options, Some(&floor_state), &[]);
        first.version = 2;
        first.snapshot = snapshot.encode_to_vec();
        if let Some(payload) = payload {
            if !snapshot.data.is_empty() {
                return Err(invalid("file checkpoint must not contain inline data"));
            }
            // Retain before publication: an error after rename must never delete a live payload.
            // Orphans before publication are removed while holding the runtime lock on reopen.
            payload.retain();
            #[cfg(test)]
            checkpoint_crash_point("after_payload_sync");
            first.version = 4;
            first.payload_manifest = payload.manifest.encode_to_vec();
            replacement.write_record(&first)?;
        } else if snapshot.data.len() <= INLINE_CHECKPOINT_BYTES {
            replacement.write_record(&first)?;
        } else {
            let encoded = std::mem::take(&mut first.snapshot);
            first.version = 3;
            first.snapshot_total = encoded.len() as u64;
            first.snapshot_digest = digest(&encoded);
            for (number, fragment) in encoded.chunks(SNAPSHOT_FRAGMENT_BYTES).enumerate() {
                first.snapshot_offset = (number * SNAPSHOT_FRAGMENT_BYTES) as u64;
                first.snapshot = fragment.to_vec();
                if number != 0 {
                    first.hard_state.clear();
                }
                replacement.write_record(&first)?;
                #[cfg(test)]
                checkpoint_crash_point("during_write");
            }
        }
        for entry in entries {
            replacement.write_record(&batch(options, None, std::slice::from_ref(entry)))?;
        }
        if hs != &floor_state {
            replacement.write_record(&batch(options, Some(hs), &[]))?;
        }
        // No replacement record is observable until publication. Flush the
        // complete image and retained suffix once, before replacing the live WAL.
        #[cfg(test)]
        checkpoint_crash_point("before_sync");
        replacement.file.sync_all().map_err(storage_error)?;
        #[cfg(test)]
        checkpoint_crash_point("before_publish");
        // The replacement inode is synced and locked before it becomes the WAL.
        // The stable runtime lock excludes openers across the inode replacement.
        std::fs::rename(&temporary, &options.wal_path).map_err(storage_error)?;
        *self = replacement;
        #[cfg(test)]
        checkpoint_crash_point("after_publish");
        #[cfg(unix)]
        File::open(
            options
                .wal_path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )
        .and_then(|directory| directory.sync_all())
        .map_err(storage_error)?;
        Ok(())
    }
    // A Ready can accumulate many independently bounded append packets. Its
    // entries need not fit one WAL record. Persist the new term/vote first,
    // entries in order, and the commit watermark last, then sync once before
    // the runtime exposes any acknowledgment. Every complete prefix reopens.
    fn append_ready(&mut self, record: &WalBatch, previous: &HardState) -> Result<(), RaftError> {
        if record.encoded_len() <= LIMIT {
            return self.append(record);
        }
        if record.version != 1 || !record.snapshot.is_empty() || !record.payload_manifest.is_empty()
        {
            return Err(storage_error(
                "oversized peer record is not an append batch",
            ));
        }
        let mut envelope = WalBatch {
            version: 1,
            group: record.group,
            node: record.node,
            voters: record.voters.clone(),
            ..Default::default()
        };
        let overhead = envelope.encoded_len();
        let mut spans = Vec::new();
        let mut start = 0;
        let mut size = overhead;
        for (index, entry) in record.entries.iter().enumerate() {
            let field = 1 + prost::encoding::encoded_len_varint(entry.len() as u64) + entry.len();
            if overhead.saturating_add(field) > LIMIT {
                return Err(storage_error("peer WAL entry exceeds record bound"));
            }
            if size + field > LIMIT {
                spans.push((start, index, size));
                start = index;
                size = overhead;
            }
            size += field;
        }
        if start < record.entries.len() {
            spans.push((start, record.entries.len(), size));
        }
        let mut prefix = envelope.clone();
        if !record.hard_state.is_empty() {
            let mut floor =
                HardState::decode(record.hard_state.as_slice()).map_err(storage_error)?;
            floor.commit = previous.commit;
            prefix.hard_state = floor.encode_to_vec();
        }
        let mut final_state = envelope.clone();
        if record.hard_state != prefix.hard_state {
            final_state.hard_state = record.hard_state.clone();
        }
        let mut bytes = spans
            .iter()
            .map(|(_, _, size)| 24 + *size as u64)
            .sum::<u64>();
        for state in [&prefix, &final_state] {
            if !state.hard_state.is_empty() {
                if state.encoded_len() > LIMIT {
                    return Err(storage_error("peer WAL hard state exceeds record bound"));
                }
                bytes = bytes.saturating_add(24 + state.encoded_len() as u64);
            }
        }
        // Refuse a bound violation before writing even a valid prefix.
        if self.length.saturating_add(bytes) > WAL_LIMIT {
            return Err(storage_error(
                "peer WAL capacity reached; checkpoint support is required",
            ));
        }
        if !prefix.hard_state.is_empty() {
            self.write_record(&prefix)?;
        }
        for (start, end, _) in spans {
            envelope.entries = record.entries[start..end].to_vec();
            self.write_record(&envelope)?;
        }
        if !final_state.hard_state.is_empty() {
            self.write_record(&final_state)?;
        }
        self.file.sync_all().map_err(storage_error)
    }
    fn append(&mut self, batch: &WalBatch) -> Result<(), RaftError> {
        self.write_record(batch)?;
        self.file.sync_all().map_err(storage_error)
    }
    // Only rewrite may defer durability until the entire replacement is written.
    fn write_record(&mut self, batch: &WalBatch) -> Result<(), RaftError> {
        let size = batch.encoded_len();
        if size > LIMIT {
            return Err(storage_error("peer WAL record exceeds size bound"));
        }
        if self.length.saturating_add(24 + size as u64) > WAL_LIMIT {
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
        self.file.write_all(&record).map_err(storage_error)?;
        self.length += record.len() as u64;
        Ok(())
    }
}
#[cfg(test)]
fn checkpoint_crash_point(point: &str) {
    if std::env::var("MATRIXRAFT_CHECKPOINT_TEST_PHASE")
        .ok()
        .as_deref()
        == Some(point)
    {
        println!("CHECKPOINT_CRASH_REACHED");
        let _ = std::io::stdout().flush();
        std::process::exit(0);
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
/// A capture position bound to this open peer. Move this ticket to a worker with
/// an immutable application snapshot taken at `position()`. Holding it prevents
/// another runtime from opening and cleaning up the same storage directory.
pub struct CheckpointCapture {
    position: LogId,
    directory: PathBuf,
    lock: Arc<File>,
}
impl CheckpointCapture {
    pub fn position(&self) -> LogId {
        self.position.clone()
    }
    /// Serialize and sync the payload without borrowing or blocking the peer.
    pub fn capture(
        self,
        reader: &mut dyn Read,
        length: u64,
    ) -> Result<PreparedCheckpoint, RaftError> {
        let payload = checkpoint_file::Blob::capture(&self.directory, reader, length)?;
        Ok(PreparedCheckpoint {
            ticket: self,
            payload,
        })
    }
}
/// A synced checkpoint awaiting publication. Dropping an unpublished result
/// deletes its payload. The originating runtime must install it.
pub struct PreparedCheckpoint {
    ticket: CheckpointCapture,
    payload: Arc<checkpoint_file::Blob>,
}
impl PreparedCheckpoint {
    pub fn position(&self) -> LogId {
        self.ticket.position.clone()
    }
}
/// One actual Raft peer. Remote votes and append acknowledgments enter only via `step`.
pub struct DurablePeer {
    options: PeerOptions,
    raw: RawNode<PeerStorage>,
    store: PeerStorage,
    journal: Journal,
    failed: bool,
    read_sequence: u64,
    pending_read: Option<PendingRead>,
    restore_on_open: Option<Snapshot>,
    snapshot_flights: BTreeMap<u64, (u64, u64, u8)>,
    stream: snapshot_stream::Stream,
    incoming_payload: Option<(LogId, Arc<checkpoint_file::Blob>)>,
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
        let store = PeerStorage {
            memory: MemStorage::new_with_conf_state((options.voters.clone(), Vec::<u64>::new())),
            checkpoint: Arc::new(Mutex::new(None)),
            payload_directory: checkpoint_file::directory(&options.wal_path),
            payload: Arc::new(Mutex::new(None)),
        };
        let journal = Journal::open(&options, &store)?;
        let restore_on_open = store.checkpoint.lock().unwrap().clone();
        let config = Config {
            applied: restore_on_open
                .as_ref()
                .map_or(0, |s| s.get_metadata().index),
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
        let mut stream = snapshot_stream::Stream::new(
            options
                .wal_path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new("."))
                .to_path_buf(),
        );
        stream.payload_directory = store.payload_directory.clone();
        Ok(Self {
            options,
            raw,
            store,
            journal,
            failed: false,
            read_sequence: 0,
            pending_read: None,
            restore_on_open,
            snapshot_flights: BTreeMap::new(),
            stream,
            incoming_payload: None,
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
        if !self.is_leader() {
            self.snapshot_flights.clear();
        }
        let term = self.term();
        self.stream
            .tick(term, self.raw.raft.state == StateRole::Leader);
        let mut expired = Vec::new();
        self.snapshot_flights.retain(|id, (sent_term, _, ticks)| {
            if *sent_term != term {
                return false;
            }
            if self.stream.sending_to(*id) {
                *ticks = 0;
                return true;
            }
            *ticks += 1;
            if *ticks >= 10 {
                expired.push(*id);
                false
            } else {
                true
            }
        });
        for id in expired {
            self.raw
                .report_snapshot(id, raft_core::SnapshotStatus::Failure);
        }
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
    /// Atomically replace durable history through the applied index with an
    /// application checkpoint. The caller must capture these exact applied bytes
    /// on its exclusive application worker. Uncommitted suffix entries survive.
    /// Use process_ready_with_snapshot after checkpointing, including on reopen.
    pub fn checkpoint(&mut self, data: Vec<u8>) -> Result<(), RaftError> {
        if data.len() > MAX_CHECKPOINT_BYTES {
            return Err(invalid("inline checkpoint exceeds size bound"));
        }
        self.checkpoint_payload(data, None)
    }
    /// Reserve the current settled application position for background capture.
    /// Snapshot application state before allowing further application mutations.
    pub fn prepare_checkpoint(&self) -> Result<CheckpointCapture, RaftError> {
        self.healthy()?;
        if self.raw.has_ready()
            || self.pending_read.is_some()
            || self.restore_on_open.is_some()
            || self.applied_index() == 0
        {
            return Err(invalid("checkpoint capture requires settled applied state"));
        }
        let index = self.applied_index();
        Ok(CheckpointCapture {
            position: LogId {
                index,
                term: self.store.term(index).map_err(storage_error)?,
            },
            directory: self.store.payload_directory.clone(),
            lock: Arc::clone(&self.journal._lock),
        })
    }
    /// Publish a worker result while preserving every entry after its position.
    /// Returns false if a newer checkpoint already superseded it. A busy peer
    /// returns an error; retain the result and retry after draining ready work.
    /// WAL replacement and sync still run on the caller; payload I/O does not.
    pub fn install_checkpoint(&mut self, prepared: &PreparedCheckpoint) -> Result<bool, RaftError> {
        self.healthy()?;
        if !Arc::ptr_eq(&prepared.ticket.lock, &self.journal._lock) {
            return Err(invalid("checkpoint belongs to another open peer"));
        }
        let id = prepared.position();
        if id.index < self.store.first_index().map_err(storage_error)? {
            return Ok(false);
        }
        if id.index > self.applied_index()
            || self.store.term(id.index).map_err(storage_error)? != id.term
        {
            return Err(invalid(
                "checkpoint position no longer matches applied history",
            ));
        }
        self.checkpoint_payload_at(Vec::new(), Some(Arc::clone(&prepared.payload)), id.index)?;
        Ok(true)
    }
    /// Capture a complete payload through a bounded buffer. The reader must end at length.
    /// Upgrade all peers and use the streaming restore callback before enabling this format.
    pub fn checkpoint_from_reader(
        &mut self,
        reader: &mut dyn Read,
        length: u64,
    ) -> Result<(), RaftError> {
        self.healthy()?;
        let payload =
            checkpoint_file::Blob::capture(&self.store.payload_directory, reader, length)?;
        self.checkpoint_payload(Vec::new(), Some(payload))
    }
    fn checkpoint_payload(
        &mut self,
        data: Vec<u8>,
        payload: Option<Arc<checkpoint_file::Blob>>,
    ) -> Result<(), RaftError> {
        self.checkpoint_payload_at(data, payload, self.applied_index())
    }
    fn checkpoint_payload_at(
        &mut self,
        data: Vec<u8>,
        payload: Option<Arc<checkpoint_file::Blob>>,
        index: u64,
    ) -> Result<(), RaftError> {
        self.healthy()?;
        if data.len() > MAX_CHECKPOINT_BYTES
            || self.raw.has_ready()
            || self.pending_read.is_some()
            || self.restore_on_open.is_some()
            || self.applied_index() == 0
        {
            return Err(invalid(
                "checkpoint requires a settled applied state within the snapshot bound",
            ));
        }
        let snapshot = Snapshot {
            data,
            metadata: Some(SnapshotMetadata {
                index,
                term: self.store.term(index).map_err(storage_error)?,
                conf_state: Some(ConfState {
                    voters: self.options.voters.clone(),
                    ..Default::default()
                }),
            }),
        };
        validate_snapshot(&snapshot, &self.options.voters)?;
        let first = self.store.first_index().map_err(storage_error)?;
        if index < first {
            return Err(invalid("applied state is already checkpointed"));
        }
        let last = self.store.last_index().map_err(storage_error)?;
        let suffix = self
            .store
            .entries(
                index + 1,
                last + 1,
                None,
                raft_core::GetEntriesContext::empty(false),
            )
            .map_err(storage_error)?;
        let hs = self
            .store
            .initial_state()
            .map_err(storage_error)?
            .hard_state;
        let result = self
            .journal
            .rewrite(&self.options, &snapshot, &hs, &suffix, payload.as_deref())
            .and_then(|()| {
                let mut record = batch(&self.options, Some(&hs), &suffix);
                record.version = 2;
                record.snapshot = snapshot.encode_to_vec();
                if let Some(payload) = &payload {
                    record.version = 4;
                    record.payload_manifest = payload.manifest.encode_to_vec();
                }
                install_local(&self.store, &record, payload.clone())
            });
        if result.is_err() {
            self.failed = true;
        }
        result
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
        if !matches!(packet.version, 1..=3)
            || packet.group != self.options.group_id
            || packet.encode_to_vec() != bytes
        {
            return Err(invalid("peer packet identity or encoding mismatch"));
        }
        if packet.version == 2 || packet.version == 3 {
            let part =
                snapshot_stream::Fragment::decode(packet.message.as_slice()).map_err(invalid)?;
            if part.file_backed != (packet.version == 3) {
                return Err(invalid("checkpoint transfer format mismatch"));
            }
            part.validate(&self.options, self.term())?;
            if part.encode_to_vec() != packet.message {
                return Err(invalid("noncanonical checkpoint fragment"));
            }
            if part.ack {
                if self.stream.acknowledge(&part) {
                    // Transport completion only unpauses probing. It never
                    // advances the follower match index or commits an entry.
                    self.raw
                        .report_snapshot(part.from, raft_core::SnapshotStatus::Finish);
                }
                return Ok(());
            }
            let from = part.from;
            let term = part.term;
            if let Some(message) = self.stream.receive(part, &self.options.voters)? {
                if let Some(payload) = self.stream.received_payload.take() {
                    let meta = message.get_snapshot().get_metadata();
                    if meta.index > self.raw.raft.raft_log.committed {
                        self.incoming_payload = Some((
                            LogId {
                                index: meta.index,
                                term: meta.term,
                            },
                            payload,
                        ));
                    }
                }
                self.step_message(message)?;
            } else {
                // Keep the recognized sender's term and election timer current
                // while its checkpoint travels through bounded transport frames.
                self.raw
                    .step(Message {
                        from,
                        to: self.options.node_id,
                        term,
                        msg_type: MessageType::MsgHeartbeat as i32,
                        ..Default::default()
                    })
                    .map_err(invalid)?;
            }
            return Ok(());
        }
        let message = Message::decode(packet.message.as_slice()).map_err(invalid)?;
        if message.encode_to_vec() != packet.message {
            return Err(invalid("noncanonical peer message"));
        }
        self.step_message(message)
    }
    fn step_message(&mut self, message: Message) -> Result<(), RaftError> {
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
                | MessageType::MsgSnapshot
        );
        if !allowed || message.term == 0 || message.term == u64::MAX || message.index == u64::MAX {
            return Err(invalid("unsupported or malformed peer message"));
        }
        if message.to != self.options.node_id
            || message.from == self.options.node_id
            || !self.options.voters.contains(&message.from)
            || raft_core::raw_node::is_local_msg(kind)
            || matches!(
                kind,
                MessageType::MsgPropose | MessageType::MsgTransferLeader
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
        if (message.snapshot.is_some() && kind != MessageType::MsgSnapshot)
            || (kind != MessageType::MsgAppend && !message.entries.is_empty())
        {
            return Err(invalid("unexpected peer message payload"));
        }
        if kind == MessageType::MsgSnapshot {
            let snapshot = message
                .snapshot
                .as_ref()
                .ok_or_else(|| invalid("missing peer snapshot"))?;
            validate_snapshot(snapshot, &self.options.voters)?;
            if snapshot.get_metadata().term > message.term {
                return Err(invalid("snapshot term exceeds sender term"));
            }
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
        if kind == MessageType::MsgAppendResponse
            && !message.reject
            && self
                .snapshot_flights
                .get(&message.from)
                .is_some_and(|(term, index, _)| message.term == *term && message.index >= *index)
        {
            self.snapshot_flights.remove(&message.from);
            self.stream.completed_send(message.from);
        }
        self.raw.step(message).map_err(invalid)
    }
    fn packets(&mut self, messages: Vec<Message>) -> Result<Vec<PeerPacket>, RaftError> {
        let mut output = Vec::new();
        for message in messages {
            let to = message.to;
            if message.get_msg_type() == MessageType::MsgSnapshot {
                self.snapshot_flights.insert(
                    to,
                    (message.term, message.get_snapshot().get_metadata().index, 0),
                );
                let payload = self.store.payload.lock().unwrap().clone();
                if let Some(payload) = payload {
                    let current = self
                        .store
                        .checkpoint
                        .lock()
                        .unwrap()
                        .clone()
                        .ok_or_else(|| storage_error("missing file checkpoint metadata"))?;
                    if current.get_metadata() != message.get_snapshot().get_metadata() {
                        return Err(storage_error(
                            "file checkpoint metadata changed during send",
                        ));
                    }
                    if !self.stream.start_file(message, payload)? {
                        self.raw
                            .report_snapshot(to, raft_core::SnapshotStatus::Failure);
                        self.snapshot_flights.remove(&to);
                    }
                    continue;
                }
                if message.get_snapshot().data.len() > INLINE_CHECKPOINT_BYTES {
                    if !self.stream.start(message)? {
                        self.raw
                            .report_snapshot(to, raft_core::SnapshotStatus::Failure);
                        self.snapshot_flights.remove(&to);
                    }
                    continue;
                }
            }
            let packet = Packet {
                version: 1,
                group: self.options.group_id,
                message: message.encode_to_vec(),
            };
            if packet.encoded_len() > LIMIT {
                return Err(invalid("outgoing peer packet exceeds size limit"));
            }
            output.push(PeerPacket {
                to,
                bytes: packet.encode_to_vec(),
            });
        }
        Ok(output)
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
        self.process_ready_with_snapshot(&mut apply, |_, _| {
            Err(storage_error(
                "application snapshot restore callback required",
            ))
        })
    }
    /// Restore a durable or incoming checkpoint before applying its suffix. Both
    /// callbacks must rebuild the same application; failure stops the peer.
    pub fn process_ready_with_snapshot(
        &mut self,
        apply: impl FnMut(LogId, &[u8]) -> Result<(), RaftError>,
        mut restore: impl FnMut(LogId, &[u8]) -> Result<(), RaftError>,
    ) -> Result<Vec<PeerPacket>, RaftError> {
        self.process_ready_with_snapshot_reader(apply, |id, reader, length| {
            if length > MAX_CHECKPOINT_BYTES as u64 {
                return Err(storage_error(
                    "file checkpoint requires streaming restore callback",
                ));
            }
            let mut bytes = Vec::new();
            reader
                .take(length)
                .read_to_end(&mut bytes)
                .map_err(storage_error)?;
            if bytes.len() as u64 != length {
                return Err(storage_error("checkpoint payload truncated during restore"));
            }
            restore(id, &bytes)
        })
    }
    /// Restore file or legacy checkpoints through a reader before applying the suffix.
    /// The callback must consume exactly length bytes; a failure stops this peer.
    pub fn process_ready_with_snapshot_reader(
        &mut self,
        mut apply: impl FnMut(LogId, &[u8]) -> Result<(), RaftError>,
        mut restore: impl FnMut(LogId, &mut dyn Read, u64) -> Result<(), RaftError>,
    ) -> Result<Vec<PeerPacket>, RaftError> {
        self.healthy()?;
        let result = (|| {
            if let Some(snapshot) = self.restore_on_open.take() {
                self.restore_payload(&snapshot, &mut restore)?;
            }
            let mut output = self.process_inner(&mut apply, &mut restore)?;
            // Raft may advance through matching entries without installing the received image.
            if self
                .incoming_payload
                .as_ref()
                .is_some_and(|(id, _)| id.index <= self.applied_index())
            {
                self.incoming_payload = None;
            }
            output.extend(self.stream.packets(self.options.group_id)?);
            Ok(output)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn restore_payload(
        &self,
        snapshot: &Snapshot,
        restore: &mut impl FnMut(LogId, &mut dyn Read, u64) -> Result<(), RaftError>,
    ) -> Result<(), RaftError> {
        let id = LogId {
            index: snapshot.get_metadata().index,
            term: snapshot.get_metadata().term,
        };
        let payload = self.store.payload.lock().unwrap().clone();
        let mut reader: Box<dyn Read> = match &payload {
            Some(payload) => Box::new(payload.reader()?),
            None => Box::new(std::io::Cursor::new(snapshot.data.as_slice())),
        };
        let length = payload
            .as_ref()
            .map_or(snapshot.data.len() as u64, |p| p.manifest.length);
        let mut limited = reader.by_ref().take(length);
        restore(id, &mut limited, length)?;
        if limited.limit() != 0 {
            return Err(storage_error(
                "checkpoint restore did not consume the payload",
            ));
        }
        Ok(())
    }
    fn process_inner(
        &mut self,
        apply: &mut impl FnMut(LogId, &[u8]) -> Result<(), RaftError>,
        restore: &mut impl FnMut(LogId, &mut dyn Read, u64) -> Result<(), RaftError>,
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
                let snapshot = ready.snapshot().clone();
                validate_snapshot(&snapshot, &self.options.voters)?;
                let hs = ready
                    .hs()
                    .cloned()
                    .ok_or_else(|| storage_error("snapshot ready needs hard state"))?;
                let position = LogId {
                    index: snapshot.get_metadata().index,
                    term: snapshot.get_metadata().term,
                };
                let payload = self
                    .incoming_payload
                    .as_ref()
                    .filter(|(id, _)| *id == position)
                    .map(|(_, p)| Arc::clone(p));
                self.journal.rewrite(
                    &self.options,
                    &snapshot,
                    &hs,
                    ready.entries(),
                    payload.as_deref(),
                )?;
                let mut record = batch(&self.options, Some(&hs), ready.entries());
                record.version = 2;
                record.snapshot = snapshot.encode_to_vec();
                if let Some(payload) = payload {
                    record.version = 4;
                    record.payload_manifest = payload.manifest.encode_to_vec();
                }
                install(&self.store, &record)?;
                self.restore_payload(&snapshot, restore)?;
                self.incoming_payload = None;
            } else if ready.hs().is_some() || !ready.entries().is_empty() {
                let record = batch(&self.options, ready.hs(), ready.entries());
                let previous = self
                    .store
                    .initial_state()
                    .map_err(storage_error)?
                    .hard_state;
                self.journal.append_ready(&record, &previous)?;
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
    // A spawned child can briefly retain another thread's file locks before
    // exec closes inherited descriptors. Keep process creation exclusive with
    // these WAL fixtures; ordinary file tests can still run together.
    static PROCESS_FIXTURE: std::sync::RwLock<()> = std::sync::RwLock::new(());

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
    fn oversized_ready_limits_refuse_before_writing_any_prefix() {
        let _fixture = PROCESS_FIXTURE.read().unwrap();
        let root = path("append-bound-preflight");
        std::fs::create_dir(&root).unwrap();
        let config = options(root.join("peer.wal"));
        let mut peer = DurablePeer::open(config.clone()).unwrap();
        let before = std::fs::read(&config.wal_path).unwrap();
        let hard = HardState {
            term: 2,
            vote: 1,
            commit: 9,
        };
        let entries: Vec<_> = (1..=9)
            .map(|index| Entry {
                term: 2,
                index,
                data: vec![7; 1024 * 1024],
                ..Default::default()
            })
            .collect();
        let record = batch(&config, Some(&hard), &entries);
        let saved_length = peer.journal.length;
        peer.journal.length = WAL_LIMIT - 1024;
        assert!(peer
            .journal
            .append_ready(&record, &HardState::default())
            .is_err());
        assert_eq!(std::fs::read(&config.wal_path).unwrap(), before);
        peer.journal.length = saved_length;
        let mut too_large = record;
        too_large.entries.push(vec![0; LIMIT]);
        assert!(peer
            .journal
            .append_ready(&too_large, &HardState::default())
            .is_err());
        assert_eq!(std::fs::read(&config.wal_path).unwrap(), before);
        drop(peer);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn accumulated_append_packets_persist_beyond_one_record() {
        let _fixture = PROCESS_FIXTURE.read().unwrap();
        let root = path("many-append-records");
        std::fs::create_dir(&root).unwrap();
        let config = PeerOptions {
            group_id: 1,
            node_id: 2,
            voters: vec![1, 2, 3],
            wal_path: root.join("peer.wal"),
        };
        let mut peer = DurablePeer::open(config.clone()).unwrap();
        for index in 1..=9 {
            let packet = Packet {
                version: 1,
                group: 1,
                message: Message {
                    from: 1,
                    to: 2,
                    term: 2,
                    msg_type: MessageType::MsgAppend as i32,
                    index: index - 1,
                    log_term: if index == 1 { 0 } else { 2 },
                    commit: index,
                    entries: vec![Entry {
                        term: 2,
                        index,
                        data: vec![index as u8; 1024 * 1024],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
                .encode_to_vec(),
            }
            .encode_to_vec();
            peer.step(&packet).unwrap();
        }
        let mut applied = Vec::new();
        peer.process_ready(|id, bytes| {
            assert_eq!(bytes, vec![id.index as u8; 1024 * 1024]);
            applied.push(id.index);
            Ok(())
        })
        .unwrap();
        assert_eq!(applied, (1..=9).collect::<Vec<_>>());
        drop(peer);
        let mut peer = DurablePeer::open(config.clone()).unwrap();
        let mut replayed = Vec::new();
        peer.process_ready(|id, bytes| {
            assert_eq!(bytes, vec![id.index as u8; 1024 * 1024]);
            replayed.push(id.index);
            Ok(())
        })
        .unwrap();
        assert_eq!(replayed, applied);
        drop(peer);
        let bytes = std::fs::read(&config.wal_path).unwrap();
        let mut offset = 0;
        let mut records = 0;
        while offset < bytes.len() {
            let length =
                u32::from_be_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            assert!(length <= LIMIT);
            offset += 24 + length;
            records += 1;
            std::fs::write(&config.wal_path, &bytes[..offset]).unwrap();
            let mut peer = DurablePeer::open(config.clone()).unwrap();
            let mut count = 0;
            peer.process_ready(|_, _| {
                count += 1;
                Ok(())
            })
            .unwrap();
            assert!(
                count == 0 || count == 9,
                "commit watermark precedes durable entries"
            );
            drop(peer);
        }
        assert!(records >= 5);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn incoming_snapshot_restore_failure_is_durable_and_returns_no_acknowledgment() {
        let _fixture = PROCESS_FIXTURE.read().unwrap();
        let root = path("restore-directory");
        std::fs::create_dir(&root).unwrap();
        let wal = root.join("peer.wal");
        let config = PeerOptions {
            group_id: 1,
            node_id: 2,
            voters: vec![1, 2, 3],
            wal_path: wal,
        };
        let mut peer = DurablePeer::open(config.clone()).unwrap();
        let snapshot = Snapshot {
            data: 42u64.to_be_bytes().to_vec(),
            metadata: Some(SnapshotMetadata {
                index: 9,
                term: 4,
                conf_state: Some(ConfState {
                    voters: vec![1, 2, 3],
                    ..Default::default()
                }),
            }),
        };
        let packet = |snapshot: Snapshot| {
            Packet {
                version: 1,
                group: 1,
                message: Message {
                    from: 1,
                    to: 2,
                    term: 5,
                    msg_type: MessageType::MsgSnapshot as i32,
                    snapshot: Some(snapshot),
                    ..Default::default()
                }
                .encode_to_vec(),
            }
            .encode_to_vec()
        };
        let before = std::fs::read(&config.wal_path).unwrap();
        let mut wrong = snapshot.clone();
        wrong.mut_metadata().mut_conf_state().voters = vec![1, 2];
        assert!(peer.step(&packet(wrong)).is_err());
        let mut wrong = snapshot.clone();
        wrong.mut_metadata().term = 6;
        assert!(peer.step(&packet(wrong)).is_err());
        assert_eq!(std::fs::read(&config.wal_path).unwrap(), before);
        peer.step(&packet(snapshot)).unwrap();
        assert!(peer
            .process_ready_with_snapshot(
                |_, _| Ok(()),
                |_, _| Err(storage_error("injected restore failure"))
            )
            .is_err());
        assert!(peer.tick().is_err());
        drop(peer);
        let mut restored = DurablePeer::open(config).unwrap();
        let mut seen = false;
        restored
            .process_ready_with_snapshot(
                |_, _| Ok(()),
                |id, bytes| {
                    assert_eq!(id.index, 9);
                    assert_eq!(id.term, 4);
                    assert_eq!(bytes, 42u64.to_be_bytes());
                    seen = true;
                    Ok(())
                },
            )
            .unwrap();
        assert!(seen);
        drop(restored);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    #[ignore = "child process used by checkpoint crash-boundary test"]
    fn checkpoint_crash_child() {
        let _fixture = PROCESS_FIXTURE.read().unwrap();
        let Some(path) = std::env::var_os("MATRIXRAFT_CHECKPOINT_TEST_WAL") else {
            return;
        };
        let mut peer = DurablePeer::open(options(PathBuf::from(path))).unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let size: usize = std::env::var("MATRIXRAFT_CHECKPOINT_TEST_SIZE")
            .unwrap_or_default()
            .parse()
            .unwrap_or(0);
        let bytes = if size == 0 {
            7u64.to_be_bytes().to_vec()
        } else {
            vec![7; size]
        };
        if std::env::var_os("MATRIXRAFT_CHECKPOINT_TEST_FILE").is_some() {
            peer.checkpoint_from_reader(&mut bytes.as_slice(), bytes.len() as u64)
                .unwrap();
        } else {
            peer.checkpoint(bytes).unwrap();
        }
        panic!("checkpoint crash hook did not run");
    }
    #[test]
    fn checkpoint_process_crashes_preserve_old_or_new_complete_history() {
        let _fixture = PROCESS_FIXTURE.write().unwrap();
        let root = path("crash-directory");
        std::fs::create_dir(&root).unwrap();
        let wal = root.join("peer.wal");
        let config = options(wal.clone());
        let mut peer = DurablePeer::open(config.clone()).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        for _ in 0..8 {
            peer.propose(7u64.to_be_bytes().to_vec()).unwrap();
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        drop(peer);
        let original = std::fs::read(&wal).unwrap();
        for (phase, size, file) in [
            ("before_sync", 0, false),
            ("before_publish", 0, false),
            ("after_publish", 0, false),
            ("during_write", 9 * 1024 * 1024, false),
            ("before_sync", 9 * 1024 * 1024, false),
            ("before_publish", 9 * 1024 * 1024, false),
            ("after_publish", 9 * 1024 * 1024, false),
            ("after_payload_sync", 9 * 1024 * 1024, true),
            ("before_sync", 9 * 1024 * 1024, true),
            ("before_publish", 9 * 1024 * 1024, true),
            ("after_publish", 9 * 1024 * 1024, true),
        ] {
            std::fs::write(&wal, &original).unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            if file {
                command.env("MATRIXRAFT_CHECKPOINT_TEST_FILE", "1");
            } else {
                command.env_remove("MATRIXRAFT_CHECKPOINT_TEST_FILE");
            }
            let child = command
                .args([
                    "--exact",
                    "peer_runtime::tests::checkpoint_crash_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("MATRIXRAFT_CHECKPOINT_TEST_PHASE", phase)
                .env("MATRIXRAFT_CHECKPOINT_TEST_WAL", &wal)
                .env("MATRIXRAFT_CHECKPOINT_TEST_SIZE", size.to_string())
                .output()
                .unwrap();
            assert!(child.status.success());
            assert!(String::from_utf8_lossy(&child.stdout).contains("CHECKPOINT_CRASH_REACHED"));
            if phase != "after_publish" {
                assert_eq!(std::fs::read(&wal).unwrap(), original);
            } else if size == 0 {
                assert!(std::fs::metadata(&wal).unwrap().len() < original.len() as u64);
            }
            let mut peer = DurablePeer::open(config.clone()).unwrap();
            let value = std::cell::Cell::new(0);
            let restored = std::cell::Cell::new(false);
            peer.process_ready_with_snapshot(
                |_, bytes| {
                    if !bytes.is_empty() {
                        value.set(u64::from_be_bytes(bytes.try_into().unwrap()));
                    }
                    Ok(())
                },
                |_, bytes| {
                    if size == 0 {
                        value.set(u64::from_be_bytes(bytes.try_into().unwrap()));
                    } else {
                        assert_eq!(bytes.len(), size);
                        assert!(bytes.iter().all(|b| *b == 7));
                        value.set(7);
                    }
                    restored.set(true);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(value.get(), 7);
            assert_eq!(restored.get(), phase == "after_publish");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn streamed_restore_failure_is_durable_and_emits_no_completion() {
        let _fixture = PROCESS_FIXTURE.read().unwrap();
        let root = path("stream-restore-directory");
        std::fs::create_dir(&root).unwrap();
        let config = PeerOptions {
            group_id: 1,
            node_id: 2,
            voters: vec![1, 2, 3],
            wal_path: root.join("peer.wal"),
        };
        let data = vec![42; 9 * 1024 * 1024];
        let message = Message {
            from: 1,
            to: 2,
            term: 5,
            msg_type: MessageType::MsgSnapshot as i32,
            snapshot: Some(Snapshot {
                data: data.clone(),
                metadata: Some(SnapshotMetadata {
                    index: 9,
                    term: 4,
                    conf_state: Some(ConfState {
                        voters: vec![1, 2, 3],
                        ..Default::default()
                    }),
                }),
            }),
            ..Default::default()
        };
        let mut sender = snapshot_stream::Stream::default();
        sender.start(message).unwrap();
        let mut peer = DurablePeer::open(config.clone()).unwrap();
        let mut failed = false;
        for _ in 0..20 {
            let packets = sender.packets(1).unwrap();
            assert_eq!(packets.len(), 1);
            peer.step(&packets[0].bytes).unwrap();
            match peer.process_ready_with_snapshot(
                |_, _| Ok(()),
                |_, _| Err(storage_error("injected streamed restore failure")),
            ) {
                Err(_) => {
                    failed = true;
                    break;
                }
                Ok(packets) => {
                    for packet in packets {
                        let envelope = Packet::decode(packet.bytes.as_slice()).unwrap();
                        if envelope.version == 2 {
                            let part =
                                snapshot_stream::Fragment::decode(envelope.message.as_slice())
                                    .unwrap();
                            assert!(part.ack);
                            assert!(
                                !sender.acknowledge(&part),
                                "completion must not escape a failed restore"
                            );
                        }
                    }
                }
            }
        }
        assert!(failed);
        assert!(peer.tick().is_err());
        assert!(peer
            .process_ready_with_snapshot(|_, _| Ok(()), |_, _| Ok(()))
            .is_err());
        drop(peer);
        let mut peer = DurablePeer::open(config).unwrap();
        let mut restored = false;
        peer.process_ready_with_snapshot(
            |_, _| Ok(()),
            |id, bytes| {
                assert_eq!(id.index, 9);
                assert_eq!(bytes, data);
                restored = true;
                Ok(())
            },
        )
        .unwrap();
        assert!(restored);
        drop(peer);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn wal_failure_returns_no_packets_or_application_and_stops_peer() {
        let _fixture = PROCESS_FIXTURE.read().unwrap();
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
        std::fs::remove_file(path.with_extension("runtime.lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn wal_corruption_and_each_torn_final_append_are_handled_safely() {
        let _fixture = PROCESS_FIXTURE.read().unwrap();
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
        std::fs::remove_file(path.with_extension("runtime.lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn packet_decoder_rejects_local_commands_and_deep_unknown_groups() {
        let _fixture = PROCESS_FIXTURE.read().unwrap();
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
        std::fs::remove_file(path.with_extension("runtime.lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
