// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::collections::VecDeque;

#[derive(Default)]
struct State {
    sizes: VecDeque<(u64, u64)>,
    bytes: u64,
    admitted: u64,
    reader: Option<PeerLogReader>,
    failure: Option<RaftError>,
}
struct Configured {
    target: u64,
    state: Mutex<State>,
}
#[derive(Clone, Default)]
pub(super) struct Cache {
    configured: Option<Arc<Configured>>,
}
pub(super) fn charge(entry: &Entry) -> u64 {
    (std::mem::size_of::<Entry>() + std::mem::size_of::<(u64, u64)>()) as u64
        + entry.data.len() as u64
        + entry.context.len() as u64
}
fn failed_read() -> raft_core::Error {
    // Direct reads still fail; the Storage adapter handles the core's Ready
    // boundary separately and the peer raises the retained fatal error.
    raft_core::Error::Store(raft_core::StorageError::LogTemporarilyUnavailable)
}
impl Cache {
    pub(super) fn new(target: Option<u64>) -> Self {
        Self {
            configured: target.map(|target| {
                Arc::new(Configured {
                    target,
                    state: Mutex::new(State::default()),
                })
            }),
        }
    }
    pub(super) fn enabled(&self) -> bool {
        self.configured.is_some()
    }
    pub(super) fn check(&self) -> Result<(), RaftError> {
        let Some(configured) = &self.configured else {
            return Ok(());
        };
        configured
            .state
            .lock()
            .unwrap()
            .failure
            .clone()
            .map_or(Ok(()), Err)
    }
    pub(super) fn reset(&self) {
        let Some(configured) = &self.configured else {
            return;
        };
        let mut state = configured.state.lock().unwrap();
        state.sizes.clear();
        state.bytes = 0;
        state.admitted = 0;
    }
    pub(super) fn account(&self, entries: impl IntoIterator<Item = (u64, u64)>) {
        let Some(configured) = &self.configured else {
            return;
        };
        let mut state = configured.state.lock().unwrap();
        let mut entries = entries.into_iter();
        let Some(first) = entries.next() else {
            return;
        };
        while state
            .sizes
            .back()
            .is_some_and(|(index, _)| *index >= first.0)
        {
            let (_, bytes) = state.sizes.pop_back().unwrap();
            state.bytes -= bytes;
        }
        for entry in std::iter::once(first).chain(entries) {
            state.bytes += entry.1;
            state.sizes.push_back(entry);
        }
    }
    pub(super) fn busy(&self, committed: u64) -> bool {
        let Some(configured) = &self.configured else {
            return false;
        };
        let state = configured.state.lock().unwrap();
        state.bytes.saturating_add(state.admitted) >= configured.target
            && (state.admitted != 0
                || state
                    .sizes
                    .back()
                    .is_some_and(|(index, _)| *index > committed))
    }
    pub(super) fn admit(&self, entries: &[Entry]) {
        if let Some(configured) = &self.configured {
            configured.state.lock().unwrap().admitted += entries.iter().map(charge).sum::<u64>();
        }
    }
    pub(super) fn entries(
        &self,
        low: u64,
        high: u64,
        max: Option<u64>,
    ) -> raft_core::Result<Vec<Entry>> {
        let configured = self.configured.as_ref().ok_or(raft_core::Error::Store(
            raft_core::StorageError::Unavailable,
        ))?;
        let mut state = configured.state.lock().unwrap();
        if state.failure.is_some() {
            return Err(failed_read());
        }
        let result = match state.reader.as_mut() {
            Some(reader) => reader.entries(low, high, max),
            None => Err(storage_error("durable log cache reader is unavailable")),
        };
        let mut entries = match result {
            Ok(entries) => entries,
            Err(error) => {
                state.failure = Some(error);
                return Err(failed_read());
            }
        };
        raft_core::util::limit_size(&mut entries, max);
        Ok(entries)
    }
}
impl PeerStorage {
    pub(super) fn entries_sync(
        &self,
        low: u64,
        high: u64,
        max: Option<u64>,
    ) -> raft_core::Result<Vec<Entry>> {
        if self.cache.enabled() {
            if low < self.first_index()? {
                return Err(raft_core::Error::Store(raft_core::StorageError::Compacted));
            }
            if low < self.memory.first_index()? {
                return self.cache.entries(low, high, max);
            }
        }
        self.memory
            .entries(low, high, max, raft_core::GetEntriesContext::empty(false))
    }
    pub(super) fn refresh_cache(
        &self,
        options: &PeerOptions,
        length: u64,
        replaced: bool,
    ) -> Result<(), RaftError> {
        if replaced {
            // The journal file was replaced. Readers captured earlier keep reading the old file, so
            // they keep its index; everything from here on gets a fresh one.
            *self.log_index.lock().unwrap() = Default::default();
        }
        let Some(configured) = &self.cache.configured else {
            return Ok(());
        };
        let first = self.first_index().map_err(storage_error)?;
        let last = self.last_index().map_err(storage_error)?;
        let cached_first = self.memory.first_index().map_err(storage_error)?;
        let committed = self
            .initial_state()
            .map_err(storage_error)?
            .hard_state
            .commit;
        let target = configured.target;
        let mut state = configured.state.lock().unwrap();
        state.admitted = 0;
        let mut evicted = false;
        if state.bytes > target {
            // Amortize prefix moves with a low watermark. Keep a committed
            // anchor and the entire uncommitted suffix resident.
            while state.bytes > target / 2
                && state.sizes.len() > 1
                && state
                    .sizes
                    .get(1)
                    .is_some_and(|(index, _)| *index <= committed)
            {
                let (_, bytes) = state.sizes.pop_front().unwrap();
                state.bytes -= bytes;
                evicted = true;
            }
        }
        let cut = evicted.then(|| state.sizes.front().unwrap().0);
        if cut.unwrap_or(cached_first) <= first {
            state.reader = None;
        } else if replaced || state.reader.is_none() {
            state.reader = Some(PeerLogReader {
                file: File::open(&options.wal_path).map_err(storage_error)?,
                length,
                first,
                last,
                group: options.group_id,
                node: options.node_id,
                voters: options.voters.clone(),
                permitted: self.permitted.clone(),
                index: self.current_log_index(),
            });
        } else if let Some(reader) = state.reader.as_mut() {
            reader.length = length;
            reader.first = first;
            reader.last = last;
        }
        drop(state);
        if let Some(cut) = cut {
            self.memory.wl().compact(cut).map_err(storage_error)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn options(root: &Path) -> PeerOptions {
        PeerOptions {
            group_id: 17,
            node_id: 1,
            voters: vec![1],
            wal_path: root.join("peer.wal"),
        }
    }
    fn tuning(bytes: u64) -> PeerTuning {
        PeerTuning {
            max_log_cache_bytes: Some(bytes),
            ..Default::default()
        }
    }
    fn drain(peer: &mut DurablePeer, values: &mut Vec<u8>) {
        for _ in 0..1000 {
            if !peer.has_ready() {
                return;
            }
            peer.process_ready(|_, data| {
                if !data.is_empty() {
                    values.push(data[0]);
                }
                Ok(())
            })
            .unwrap();
        }
        panic!("application did not drain");
    }
    fn resident_bytes(peer: &DurablePeer) -> u64 {
        let low = peer.store.memory.first_index().unwrap();
        let high = peer.store.memory.last_index().unwrap() + 1;
        let entries = peer
            .store
            .memory
            .entries(low, high, None, raft_core::GetEntriesContext::empty(false))
            .unwrap();
        let actual: Vec<_> = entries
            .iter()
            .map(|entry| (entry.index, charge(entry)))
            .collect();
        let state = peer
            .store
            .cache
            .configured
            .as_ref()
            .unwrap()
            .state
            .lock()
            .unwrap();
        assert_eq!(
            state.bytes,
            actual.iter().map(|(_, bytes)| bytes).sum::<u64>()
        );
        assert_eq!(state.sizes.iter().copied().collect::<Vec<_>>(), actual);
        state.bytes
    }
    #[test]
    fn cache_eviction_preserves_log_bounds_terms_and_startup_replay() {
        let root = tempfile::tempdir().unwrap();
        let opts = options(root.path());
        let mut peer = DurablePeer::open_with_tuning(opts.clone(), tuning(4096)).unwrap();
        peer.campaign().unwrap();
        let mut values = Vec::new();
        drain(&mut peer, &mut values);
        assert!(peer
            .store
            .cache
            .configured
            .as_ref()
            .unwrap()
            .state
            .lock()
            .unwrap()
            .reader
            .is_none());
        for value in 0..32u8 {
            peer.propose(vec![value; 512]).unwrap();
            drain(&mut peer, &mut values);
        }
        let last = peer.commit_index();
        assert!(peer.store.memory.first_index().unwrap() > 1);
        assert_eq!(peer.store.first_index().unwrap(), 1);
        assert!(peer.store.checkpoint.lock().unwrap().is_none());
        assert!(resident_bytes(&peer) <= 4096);
        let mut reader = peer.capture_log_reader().unwrap();
        assert_eq!(
            peer.store.entries_sync(1, last + 1, None).unwrap(),
            reader.entries(1, last + 1, None).unwrap()
        );
        for index in 1..=last {
            assert_eq!(peer.store.term(index).unwrap(), 1);
        }
        assert_eq!(peer.store.term(0).unwrap(), 0);
        drop(peer);
        let mut peer = DurablePeer::open_with_tuning(opts, tuning(4096)).unwrap();
        assert!(
            peer.store.memory.first_index().unwrap() > 1,
            "recovery must evict before application replay"
        );
        assert!(resident_bytes(&peer) <= 4096);
        let mut replay = Vec::new();
        drain(&mut peer, &mut replay);
        assert_eq!(replay, values);
        assert_eq!(peer.applied_index(), last);
    }
    #[test]
    fn an_oversized_committed_anchor_does_not_prevent_future_writes() {
        let root = tempfile::tempdir().unwrap();
        let mut peer = DurablePeer::open_with_tuning(options(root.path()), tuning(1)).unwrap();
        peer.campaign().unwrap();
        let mut values = Vec::new();
        drain(&mut peer, &mut values);
        for value in 0..8u8 {
            peer.propose(vec![value; 8192]).unwrap();
            assert!(
                peer.propose(vec![201]).is_err(),
                "pressure must include entries queued before Ready"
            );
            assert!(peer.propose_batch(vec![vec![201]]).is_err());
            drain(&mut peer, &mut values);
        }
        assert_eq!(values, (0..8).collect::<Vec<_>>());
        assert_eq!(
            peer.store
                .cache
                .configured
                .as_ref()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .sizes
                .len(),
            1
        );
        assert!(resident_bytes(&peer) > 1);
        assert!(!peer.store.cache.busy(peer.commit_index()));
        peer.checkpoint(values).unwrap();
        assert!(peer
            .store
            .cache
            .configured
            .as_ref()
            .unwrap()
            .state
            .lock()
            .unwrap()
            .reader
            .is_none());
    }
    #[test]
    fn prepared_checkpoints_can_publish_below_the_cache_floor_and_keep_the_suffix() {
        let root = tempfile::tempdir().unwrap();
        let opts = options(root.path());
        let mut peer = DurablePeer::open_with_tuning(opts.clone(), tuning(1024)).unwrap();
        peer.campaign().unwrap();
        let mut values = Vec::new();
        drain(&mut peer, &mut values);
        for value in 0..16u8 {
            peer.propose(vec![value; 512]).unwrap();
            drain(&mut peer, &mut values);
        }
        let ticket = peer.prepare_checkpoint().unwrap();
        let position = ticket.position();
        let prepared = ticket
            .capture(
                &mut std::io::Cursor::new(values.clone()),
                values.len() as u64,
            )
            .unwrap();
        let mut old_reader = peer.capture_log_reader().unwrap();
        for value in 16..32u8 {
            peer.propose(vec![value; 512]).unwrap();
            drain(&mut peer, &mut values);
        }
        assert!(peer.store.memory.first_index().unwrap() > position.index);
        assert!(peer.install_checkpoint(&prepared).unwrap());
        assert_eq!(peer.store.first_index().unwrap(), position.index + 1);
        assert_eq!(
            old_reader
                .entries(2, position.index + 1, None)
                .unwrap()
                .len(),
            16
        );
        drop(prepared);
        drop(peer);
        let mut peer = DurablePeer::open_with_tuning(opts, tuning(1024)).unwrap();
        let replay = std::cell::RefCell::new(Vec::new());
        for _ in 0..1000 {
            if !peer.has_ready() {
                break;
            }
            peer.process_ready_with_snapshot_reader(
                |_, data| {
                    if !data.is_empty() {
                        replay.borrow_mut().push(data[0]);
                    }
                    Ok(())
                },
                |_, reader, _| {
                    reader
                        .read_to_end(&mut replay.borrow_mut())
                        .map_err(storage_error)?;
                    Ok(())
                },
            )
            .unwrap();
        }
        assert_eq!(replay.into_inner(), values);
        assert!(resident_bytes(&peer) <= 1024);
    }
    #[test]
    fn disabled_cache_is_pointer_sized_and_invalid_targets_do_not_create_storage() {
        assert_eq!(std::mem::size_of::<Cache>(), std::mem::size_of::<usize>());
        let root = tempfile::tempdir().unwrap();
        let opts = options(root.path());
        for bytes in [0, WAL_LIMIT + 1] {
            assert!(DurablePeer::open_with_tuning(opts.clone(), tuning(bytes)).is_err());
            assert!(!opts.wal_path.exists());
            assert!(!opts.wal_path.with_extension("runtime.lock").exists());
        }
    }

    #[test]
    fn a_cold_read_failure_is_not_hidden_by_ready_generation() {
        let root = tempfile::tempdir().unwrap();
        let opts = options(root.path());
        let mut peer = DurablePeer::open_with_tuning(
            opts.clone(),
            PeerTuning {
                max_log_cache_bytes: Some(512),
                max_apply_bytes: Some(512),
                ..Default::default()
            },
        )
        .unwrap();
        peer.campaign().unwrap();
        let mut values = Vec::new();
        drain(&mut peer, &mut values);
        let before = peer.applied_index();
        peer.propose_batch(vec![vec![7; 512]; 16]).unwrap();
        for _ in 0..10 {
            peer.process_ready(|_, _| Ok(())).unwrap();
            if peer.applied_index() > before {
                break;
            }
        }
        assert!(peer.raw.has_ready());
        assert_eq!(peer.pending_apply, peer.applied_index());
        assert!(peer.store.memory.first_index().unwrap() > peer.applied_index() + 1);
        let mut file = OpenOptions::new().write(true).open(&opts.wal_path).unwrap();
        file.seek(SeekFrom::Start(peer.journal.length - 1)).unwrap();
        file.write_all(&[255]).unwrap();
        file.sync_all().unwrap();
        assert!(peer
            .process_ready(|_, _| panic!("failed cold read cannot apply"))
            .is_err());
        assert!(!peer.has_ready());
        assert!(peer.propose(vec![8]).is_err());
        drop(peer);
        assert!(DurablePeer::open_with_tuning(opts, tuning(512)).is_err());
    }
}
