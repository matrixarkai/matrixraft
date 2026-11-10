// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::collections::VecDeque;

pub(super) struct Request {
    low: u64,
    high: u64,
    max: Option<u64>,
    context: raft_core::GetEntriesContext,
}
pub(super) struct State {
    capacity: usize,
    outstanding: usize,
    pending: VecDeque<Request>,
    ready: Option<(u64, u64, Option<u64>, Vec<Entry>)>,
}
struct Permit(Arc<Mutex<Option<State>>>);
impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(state) = self.0.lock().unwrap().as_mut() {
            state.outstanding = state.outstanding.saturating_sub(1);
        }
    }
}

/// One bounded durable read, executable on a host-owned worker.
pub struct PeerLogReadTask {
    reader: PeerLogReader,
    request: Request,
    term: u64,
    group: u64,
    node: u64,
    permit: Permit,
}
/// An opaque result returned to the peer that issued the task.
pub struct PeerLogReadCompletion {
    request: Request,
    entries: Result<Vec<Entry>, RaftError>,
    term: u64,
    group: u64,
    node: u64,
    permit: Permit,
}
impl PeerLogReadTask {
    pub fn execute(mut self) -> PeerLogReadCompletion {
        let mut entries =
            self.reader
                .entries(self.request.low, self.request.high, self.request.max);
        if let Ok(entries) = entries.as_mut() {
            raft_core::util::limit_size(entries, self.request.max);
        }
        PeerLogReadCompletion {
            request: self.request,
            entries,
            term: self.term,
            group: self.group,
            node: self.node,
            permit: self.permit,
        }
    }
}

pub(super) fn fetch(
    store: &PeerStorage,
    low: u64,
    high: u64,
    max: Option<u64>,
    context: raft_core::GetEntriesContext,
) -> raft_core::Result<Vec<Entry>> {
    let mut guard = store.async_reads.lock().unwrap();
    if let Some(state) = guard.as_mut() {
        if state
            .ready
            .as_ref()
            .is_some_and(|(l, h, m, _)| (*l, *h, *m) == (low, high, max))
        {
            return Ok(state.ready.take().unwrap().3);
        }
        if context.can_async() {
            if state.outstanding < state.capacity {
                state.pending.push_back(Request {
                    low,
                    high,
                    max,
                    context,
                });
                state.outstanding += 1;
            }
            return Err(raft_core::Error::Store(
                raft_core::StorageError::LogTemporarilyUnavailable,
            ));
        }
    }
    drop(guard);
    store.memory.entries(low, high, max, context)
}

impl DurablePeer {
    /// Opt in to host-executed durable replication reads, bounded across
    /// queued tasks, running tasks, and unconsumed completions.
    pub fn enable_async_log_reads(&mut self, capacity: usize) -> Result<(), RaftError> {
        self.healthy()?;
        if !(1..=1024).contains(&capacity) {
            return Err(invalid("invalid async read capacity"));
        }
        let mut state = self.store.async_reads.lock().unwrap();
        if state.is_some() {
            return Err(invalid("async reads already enabled"));
        }
        *state = Some(State {
            capacity,
            outstanding: 0,
            pending: VecDeque::new(),
            ready: None,
        });
        Ok(())
    }
    pub fn take_log_read_tasks(&mut self) -> Result<Vec<PeerLogReadTask>, RaftError> {
        self.healthy()?;
        let mut tasks = Vec::new();
        loop {
            let mut state = self.store.async_reads.lock().unwrap();
            let Some(state) = state.as_mut() else {
                break;
            };
            if state.pending.is_empty() {
                break;
            }
            let reader = self.capture_log_reader()?;
            let request = state.pending.pop_front().unwrap();
            tasks.push(PeerLogReadTask {
                reader,
                request,
                term: self.term(),
                group: self.options.group_id,
                node: self.options.node_id,
                permit: Permit(Arc::clone(&self.store.async_reads)),
            });
        }
        Ok(tasks)
    }
    /// Revalidate and resume on the exclusive peer worker. False means the
    /// captured result is obsolete; it is never installed into consensus.
    pub fn complete_log_read(
        &mut self,
        completion: PeerLogReadCompletion,
    ) -> Result<bool, RaftError> {
        self.healthy()?;
        if completion.group != self.options.group_id
            || completion.node != self.options.node_id
            || !Arc::ptr_eq(&completion.permit.0, &self.store.async_reads)
        {
            return Err(invalid("log read completion belongs to another peer"));
        }
        let PeerLogReadCompletion {
            request,
            entries,
            term,
            permit,
            ..
        } = completion;
        drop(permit);
        if term != self.term() || !self.is_leader() {
            return Ok(false);
        }
        let current = if request.low >= self.store.first_index().map_err(storage_error)?
            && request.high
                <= self
                    .store
                    .last_index()
                    .map_err(storage_error)?
                    .saturating_add(1)
        {
            self.store
                .memory
                .entries(
                    request.low,
                    request.high,
                    request.max,
                    raft_core::GetEntriesContext::empty(false),
                )
                .ok()
        } else {
            None
        };
        let Some(current) = current else {
            self.raw.on_entries_fetched(request.context);
            return Ok(false);
        };
        let entries = match entries {
            Ok(entries) => entries,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        let valid = entries == current;
        if valid {
            self.store
                .async_reads
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .ready = Some((request.low, request.high, request.max, entries));
        }
        self.raw.on_entries_fetched(request.context);
        self.store
            .async_reads
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .ready = None;
        Ok(valid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outstanding_reads_are_bounded_released_on_drop_and_discarded_after_term_change() {
        let root = std::env::temp_dir().join(format!(
            "peer-async-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let mut peer = DurablePeer::open(PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.join("peer.wal"),
        })
        .unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        peer.propose(vec![1; 32]).unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        peer.enable_async_log_reads(1).unwrap();
        let low = peer.store.first_index().unwrap();
        let high = peer.store.last_index().unwrap() + 1;
        for _ in 0..3 {
            assert!(fetch(
                &peer.store,
                low,
                high,
                None,
                raft_core::GetEntriesContext::empty(true)
            )
            .is_err());
        }
        let tasks = peer.take_log_read_tasks().unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(peer.take_log_read_tasks().unwrap().is_empty());
        drop(tasks);
        assert_eq!(
            peer.store
                .async_reads
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .outstanding,
            0
        );
        assert!(fetch(
            &peer.store,
            low,
            high,
            None,
            raft_core::GetEntriesContext::empty(true)
        )
        .is_err());
        let task = peer.take_log_read_tasks().unwrap().pop().unwrap();
        let completion = std::thread::spawn(move || task.execute()).join().unwrap();
        peer.checkpoint(vec![3; 32]).unwrap();
        assert!(!peer.complete_log_read(completion).unwrap());
        peer.propose(vec![2; 32]).unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let low = peer.store.first_index().unwrap();
        let high = peer.store.last_index().unwrap() + 1;
        assert!(fetch(
            &peer.store,
            low,
            high,
            None,
            raft_core::GetEntriesContext::empty(true)
        )
        .is_err());
        let task = peer.take_log_read_tasks().unwrap().pop().unwrap();
        let completion = std::thread::spawn(move || task.execute()).join().unwrap();
        let term = peer.term() + 1;
        peer.raw.raft.become_follower(term, 0);
        assert!(!peer.complete_log_read(completion).unwrap());
        let state = peer.store.async_reads.lock().unwrap();
        assert_eq!(state.as_ref().unwrap().outstanding, 0);
        assert!(state.as_ref().unwrap().ready.is_none());
        drop(state);
        drop(peer);
        std::fs::remove_dir_all(root).unwrap();
    }
}
