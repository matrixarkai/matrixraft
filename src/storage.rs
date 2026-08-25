// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Generic log, state-machine, apply, and storage contracts.

pub use crate::{
    matrixraft_apply_entry, EntryPayload, RaftApply, RaftApplyRequest, RaftApplyResponse,
    RaftFsmAdapter, RaftFsmApplyOutcome, RaftFsmCheckpoint, RaftFsmReplayReport, RaftLogEntry,
    RaftStateMachine, RaftStorageApplyFence, RustRaftApplyRequest, RustRaftApplyResponse,
    RustRaftGenericApplyRequest, RustRaftGenericApplyResponse, RustRaftGenericLogEntry,
    RustRaftGroupId, RustRaftLogId, RustRaftLogIndex, RustRaftPayload, RustRaftStateMachine,
    RustRaftStorageApplyFence, RustRaftTerm,
};

use crate::{
    MatrixRaftEntry, MatrixRaftHardState, MatrixRaftInitialState, MatrixRaftMemberId,
    RustRaftError, RustRaftHardState, RustRaftLogEntry, RustRaftNodeId, RustRaftReplicaRole,
    RustRaftSnapshotChunk,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Persistent Raft log and snapshot storage API used by production adapters.
pub trait RustRaftStorage {
    fn append_entries(&mut self, entries: &[RustRaftLogEntry]) -> Result<(), RustRaftError>;
    fn read_entries(&self, start: u64, end: u64) -> Result<Vec<RustRaftLogEntry>, RustRaftError>;
    fn hard_state(&self) -> Result<RustRaftHardState, RustRaftError>;
    fn install_snapshot(&mut self, chunk: RustRaftSnapshotChunk) -> Result<(), RustRaftError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLogRange {
    pub start_index: RustRaftLogIndex,
    pub end_index: RustRaftLogIndex,
}

impl MatrixRaftLogRange {
    pub fn new(start_index: RustRaftLogIndex, end_index: RustRaftLogIndex) -> Self {
        Self {
            start_index,
            end_index,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MatrixRaftLogStorageWriteTask {
    pub sync_meta: bool,
    pub committed_index: RustRaftLogIndex,
    pub size_hint: usize,
    #[serde(default)]
    pub hard_state: Option<MatrixRaftHardState>,
    #[serde(default)]
    pub members: Vec<MatrixRaftMemberId>,
    #[serde(default)]
    pub entries: Vec<MatrixRaftEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MatrixRaftLogSegmentEventKind {
    Open,
    Switch,
    Release,
    Truncate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLogSegment {
    pub segment_id: u64,
    pub first_index: RustRaftLogIndex,
    pub last_index: RustRaftLogIndex,
    pub bytes: usize,
    pub sealed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLogSegmentEvent {
    pub kind: MatrixRaftLogSegmentEventKind,
    pub peer_id: RustRaftNodeId,
    pub segment_id: u64,
    pub previous_segment_id: Option<u64>,
    pub first_index: RustRaftLogIndex,
    pub last_index: RustRaftLogIndex,
    pub bytes: usize,
}

impl MatrixRaftLogSegmentEvent {
    fn from_segment(
        kind: MatrixRaftLogSegmentEventKind,
        peer_id: RustRaftNodeId,
        previous_segment_id: Option<u64>,
        segment: &MatrixRaftLogSegment,
    ) -> Self {
        Self {
            kind,
            peer_id,
            segment_id: segment.segment_id,
            previous_segment_id,
            first_index: segment.first_index,
            last_index: segment.last_index,
            bytes: segment.bytes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLogCompactionReport {
    pub initial_state: MatrixRaftInitialState,
    pub first_retained_index: RustRaftLogIndex,
    pub last_index: RustRaftLogIndex,
    pub released_segments: Vec<MatrixRaftLogSegmentEvent>,
    pub truncated_segments: Vec<MatrixRaftLogSegmentEvent>,
    pub retained_segments: Vec<MatrixRaftLogSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLogStoragePrepareOptions {
    pub peer_id: RustRaftNodeId,
    pub max_segment_bytes: usize,
    pub initial_state: MatrixRaftInitialState,
    pub role: RustRaftReplicaRole,
    pub local_id: MatrixRaftMemberId,
    pub members: Vec<MatrixRaftMemberId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLogStorageOptions {
    pub peer_id: RustRaftNodeId,
    pub max_segment_bytes: usize,
    pub applied_index: RustRaftLogIndex,
    pub local_id: MatrixRaftMemberId,
    pub sync: bool,
}

pub trait MatrixRaftLogStorage {
    fn reset(
        &mut self,
        initial_state: MatrixRaftInitialState,
        members: Vec<MatrixRaftMemberId>,
    ) -> Result<(), RustRaftError>;
    fn write(&mut self, task: MatrixRaftLogStorageWriteTask) -> Result<(), RustRaftError>;
    fn truncate_until(
        &mut self,
        initial_state: MatrixRaftInitialState,
    ) -> Result<(), RustRaftError>;
    fn compact_until(
        &mut self,
        initial_state: MatrixRaftInitialState,
    ) -> Result<MatrixRaftLogCompactionReport, RustRaftError>;
    fn truncate_from_index(&mut self, index: RustRaftLogIndex) -> Result<(), RustRaftError>;
    fn release_hint(&mut self, index: RustRaftLogIndex);
    fn set_committed_index(&mut self, committed_index: RustRaftLogIndex);
    fn load_entries(
        &self,
        range: MatrixRaftLogRange,
    ) -> Result<Vec<MatrixRaftEntry>, RustRaftError>;
    fn term(&self, index: RustRaftLogIndex) -> Result<RustRaftTerm, RustRaftError>;
    fn first_index(&self) -> RustRaftLogIndex;
    fn last_index(&self) -> RustRaftLogIndex;
    fn write_bytes(&self) -> usize;
    fn is_segment_based(&self) -> bool;
    fn range(&self) -> MatrixRaftLogRange;
    fn voted_for(&self) -> Option<RustRaftNodeId>;
    fn current_term(&self) -> RustRaftTerm;
    fn committed_index(&self) -> RustRaftLogIndex;
    fn stabled_committed_index(&self) -> RustRaftLogIndex;
    fn initial_state(&self) -> MatrixRaftInitialState;
    fn members(&self) -> Vec<MatrixRaftMemberId>;
    fn size_until(&self, index: RustRaftLogIndex) -> usize;
    fn role(&self) -> RustRaftReplicaRole;
    fn file_indexes(&self) -> Vec<u64>;
    fn segments(&self) -> Vec<MatrixRaftLogSegment>;
    fn segment_events(&self) -> Vec<MatrixRaftLogSegmentEvent>;
    fn drain_segment_events(&mut self) -> Vec<MatrixRaftLogSegmentEvent>;
    fn switch_segment(
        &mut self,
        next_first_index: RustRaftLogIndex,
    ) -> Result<MatrixRaftLogSegmentEvent, RustRaftError>;
    fn release_segments_until(&mut self, index: RustRaftLogIndex)
        -> Vec<MatrixRaftLogSegmentEvent>;
}

pub trait MatrixRaftGroupStorage {
    type Log: MatrixRaftLogStorage;

    fn prepare(
        &mut self,
        path: impl Into<String>,
        options: MatrixRaftLogStoragePrepareOptions,
    ) -> Result<(), RustRaftError>;
    fn open(
        &mut self,
        path: impl Into<String>,
        options: MatrixRaftLogStorageOptions,
    ) -> Result<Self::Log, RustRaftError>;
    fn setup_node_env(&mut self, options: &crate::MatrixRaftOptions) -> Result<(), RustRaftError>;
    fn clean_up_node_env(
        &mut self,
        options: &crate::MatrixRaftOptions,
    ) -> Result<(), RustRaftError>;
    fn begin(&mut self);
    fn commit(&mut self) -> Result<(), RustRaftError>;
    fn overflow(&self) -> bool;
    fn batch_size(&self) -> usize;
    fn group_id(&self) -> RustRaftGroupId;
    fn exists(&self, node_id: RustRaftNodeId) -> bool;
    fn delete(&mut self, node_id: RustRaftNodeId) -> Result<(), RustRaftError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMemoryLogStorage {
    peer_id: RustRaftNodeId,
    max_segment_bytes: usize,
    initial_state: MatrixRaftInitialState,
    role: RustRaftReplicaRole,
    local_id: MatrixRaftMemberId,
    members: Vec<MatrixRaftMemberId>,
    hard_state: MatrixRaftHardState,
    committed_index: RustRaftLogIndex,
    stabled_committed_index: RustRaftLogIndex,
    applied_index: RustRaftLogIndex,
    sync: bool,
    entries: BTreeMap<RustRaftLogIndex, MatrixRaftEntry>,
    write_bytes: usize,
    last_release_hint: RustRaftLogIndex,
    segments: Vec<MatrixRaftLogSegment>,
    segment_events: Vec<MatrixRaftLogSegmentEvent>,
}

impl MatrixRaftMemoryLogStorage {
    pub fn new(
        prepare: MatrixRaftLogStoragePrepareOptions,
        open: MatrixRaftLogStorageOptions,
    ) -> Self {
        let hard_state = MatrixRaftHardState {
            current_term: prepare.initial_state.term,
            voted_for: None,
        };
        Self {
            peer_id: prepare.peer_id,
            max_segment_bytes: prepare.max_segment_bytes,
            initial_state: prepare.initial_state,
            role: prepare.role,
            local_id: open.local_id,
            members: prepare.members,
            hard_state,
            committed_index: open.applied_index,
            stabled_committed_index: open.applied_index,
            applied_index: open.applied_index,
            sync: open.sync,
            entries: BTreeMap::new(),
            write_bytes: 0,
            last_release_hint: 0,
            segments: Vec::new(),
            segment_events: Vec::new(),
        }
    }

    pub fn peer_id(&self) -> RustRaftNodeId {
        self.peer_id
    }

    pub fn local_id(&self) -> &MatrixRaftMemberId {
        &self.local_id
    }

    pub fn max_segment_bytes(&self) -> usize {
        self.max_segment_bytes
    }

    pub fn sync(&self) -> bool {
        self.sync
    }

    pub fn last_release_hint(&self) -> RustRaftLogIndex {
        self.last_release_hint
    }

    fn entry_write_size(size_hint: usize, entry: &MatrixRaftEntry) -> usize {
        if size_hint > 0 {
            size_hint
        } else {
            entry.bytes_size as usize
        }
    }

    fn open_segment(&mut self, first_index: RustRaftLogIndex) -> MatrixRaftLogSegmentEvent {
        let previous_segment_id = self.seal_active_segment();
        let segment = MatrixRaftLogSegment {
            segment_id: first_index,
            first_index,
            last_index: first_index.saturating_sub(1),
            bytes: 0,
            sealed: false,
        };
        self.segments.push(segment);
        let event = MatrixRaftLogSegmentEvent::from_segment(
            if previous_segment_id.is_some() {
                MatrixRaftLogSegmentEventKind::Switch
            } else {
                MatrixRaftLogSegmentEventKind::Open
            },
            self.peer_id,
            previous_segment_id,
            self.segments.last().expect("segment was just opened"),
        );
        self.segment_events.push(event.clone());
        event
    }

    fn seal_active_segment(&mut self) -> Option<u64> {
        self.segments.last_mut().map(|segment| {
            segment.sealed = true;
            segment.segment_id
        })
    }

    fn append_entry_to_segment(
        &mut self,
        index: RustRaftLogIndex,
        bytes: usize,
    ) -> Result<(), RustRaftError> {
        if self.segments.is_empty() {
            self.open_segment(index);
        }
        let should_switch = self
            .segments
            .last()
            .map(|segment| {
                segment.bytes > 0
                    && self.max_segment_bytes > 0
                    && segment.bytes.saturating_add(bytes) > self.max_segment_bytes
            })
            .unwrap_or(false);
        if should_switch {
            self.switch_segment(index)?;
        }
        let segment = self
            .segments
            .last_mut()
            .expect("segment exists before appending entry");
        segment.last_index = index;
        segment.bytes = segment.bytes.saturating_add(bytes);
        Ok(())
    }

    fn trim_segments_from_index(&mut self, index: RustRaftLogIndex) {
        let mut retained = Vec::new();
        for mut segment in self.segments.drain(..) {
            if segment.first_index >= index {
                let event = MatrixRaftLogSegmentEvent::from_segment(
                    MatrixRaftLogSegmentEventKind::Truncate,
                    self.peer_id,
                    None,
                    &segment,
                );
                self.segment_events.push(event);
                continue;
            }
            if segment.last_index >= index {
                segment.last_index = index.saturating_sub(1);
                segment.bytes = self
                    .entries
                    .range(segment.first_index..=segment.last_index)
                    .map(|(_, entry)| entry.bytes_size as usize)
                    .sum();
                let event = MatrixRaftLogSegmentEvent::from_segment(
                    MatrixRaftLogSegmentEventKind::Truncate,
                    self.peer_id,
                    None,
                    &segment,
                );
                self.segment_events.push(event);
            }
            retained.push(segment);
        }
        if let Some(last) = retained.last_mut() {
            last.sealed = false;
        }
        self.segments = retained;
    }

    fn compact_segments_through(
        &mut self,
        index: RustRaftLogIndex,
    ) -> (
        Vec<MatrixRaftLogSegmentEvent>,
        Vec<MatrixRaftLogSegmentEvent>,
    ) {
        let mut released = Vec::new();
        let mut truncated = Vec::new();
        let mut retained = Vec::new();
        for mut segment in self.segments.drain(..) {
            if segment.last_index <= index {
                let event = MatrixRaftLogSegmentEvent::from_segment(
                    MatrixRaftLogSegmentEventKind::Release,
                    self.peer_id,
                    None,
                    &segment,
                );
                released.push(event.clone());
                self.segment_events.push(event);
                continue;
            }
            if segment.first_index <= index {
                segment.first_index = index.saturating_add(1);
                segment.segment_id = segment.first_index;
                segment.bytes = self
                    .entries
                    .range(segment.first_index..=segment.last_index)
                    .map(|(_, entry)| entry.bytes_size as usize)
                    .sum();
                let event = MatrixRaftLogSegmentEvent::from_segment(
                    MatrixRaftLogSegmentEventKind::Truncate,
                    self.peer_id,
                    None,
                    &segment,
                );
                truncated.push(event.clone());
                self.segment_events.push(event);
            }
            retained.push(segment);
        }
        if let Some(last) = retained.last_mut() {
            last.sealed = false;
        }
        self.segments = retained;
        (released, truncated)
    }
}

impl MatrixRaftLogStorage for MatrixRaftMemoryLogStorage {
    fn reset(
        &mut self,
        initial_state: MatrixRaftInitialState,
        members: Vec<MatrixRaftMemberId>,
    ) -> Result<(), RustRaftError> {
        self.entries.clear();
        self.initial_state = initial_state.clone();
        self.hard_state.current_term = initial_state.term;
        self.hard_state.voted_for = None;
        self.committed_index = initial_state.index;
        self.stabled_committed_index = initial_state.index;
        self.members = members;
        self.segments.clear();
        self.segment_events.clear();
        Ok(())
    }

    fn write(&mut self, task: MatrixRaftLogStorageWriteTask) -> Result<(), RustRaftError> {
        self.committed_index = self.committed_index.max(task.committed_index);
        if task.sync_meta {
            self.stabled_committed_index = self.stabled_committed_index.max(self.committed_index);
        }
        if let Some(hard_state) = task.hard_state {
            self.hard_state = hard_state;
        }
        if !task.members.is_empty() {
            self.members = task.members;
        }
        if let Some(first_new_index) = task.entries.first().map(|entry| entry.index) {
            self.truncate_from_index(first_new_index)?;
        }
        let size_hint = task.size_hint;
        for entry in task.entries {
            let write_size = Self::entry_write_size(size_hint, &entry);
            self.write_bytes += write_size;
            self.append_entry_to_segment(entry.index, write_size)?;
            self.entries.insert(entry.index, entry);
        }
        Ok(())
    }

    fn truncate_until(
        &mut self,
        initial_state: MatrixRaftInitialState,
    ) -> Result<(), RustRaftError> {
        self.compact_until(initial_state).map(|_| ())
    }

    fn compact_until(
        &mut self,
        initial_state: MatrixRaftInitialState,
    ) -> Result<MatrixRaftLogCompactionReport, RustRaftError> {
        if initial_state.index <= self.initial_state.index {
            return Ok(MatrixRaftLogCompactionReport {
                initial_state: self.initial_state.clone(),
                first_retained_index: self.first_index(),
                last_index: self.last_index(),
                released_segments: Vec::new(),
                truncated_segments: Vec::new(),
                retained_segments: self.segments.clone(),
            });
        }
        self.initial_state = initial_state.clone();
        self.committed_index = self.committed_index.max(initial_state.index);
        self.stabled_committed_index = self.stabled_committed_index.max(initial_state.index);
        self.entries.retain(|index, _| *index > initial_state.index);
        let (released_segments, truncated_segments) =
            self.compact_segments_through(initial_state.index);
        Ok(MatrixRaftLogCompactionReport {
            initial_state,
            first_retained_index: self.first_index(),
            last_index: self.last_index(),
            released_segments,
            truncated_segments,
            retained_segments: self.segments.clone(),
        })
    }

    fn truncate_from_index(&mut self, index: RustRaftLogIndex) -> Result<(), RustRaftError> {
        self.entries.retain(|entry_index, _| *entry_index < index);
        self.trim_segments_from_index(index);
        Ok(())
    }

    fn release_hint(&mut self, index: RustRaftLogIndex) {
        self.last_release_hint = self.last_release_hint.max(index);
        self.release_segments_until(index);
    }

    fn set_committed_index(&mut self, committed_index: RustRaftLogIndex) {
        self.committed_index = self.committed_index.max(committed_index);
    }

    fn load_entries(
        &self,
        range: MatrixRaftLogRange,
    ) -> Result<Vec<MatrixRaftEntry>, RustRaftError> {
        if range.end_index < range.start_index {
            return Err(RustRaftError::InvalidRequest(
                "log range end is before start".to_string(),
            ));
        }
        Ok(self
            .entries
            .range(range.start_index..range.end_index)
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    fn term(&self, index: RustRaftLogIndex) -> Result<RustRaftTerm, RustRaftError> {
        if index == self.initial_state.index {
            return Ok(self.initial_state.term);
        }
        self.entries
            .get(&index)
            .map(|entry| entry.term)
            .ok_or_else(|| RustRaftError::Storage(format!("log term for index {index} not found")))
    }

    fn first_index(&self) -> RustRaftLogIndex {
        self.entries
            .keys()
            .next()
            .copied()
            .unwrap_or(self.initial_state.index + 1)
    }

    fn last_index(&self) -> RustRaftLogIndex {
        self.entries
            .keys()
            .next_back()
            .copied()
            .unwrap_or(self.initial_state.index)
    }

    fn write_bytes(&self) -> usize {
        self.write_bytes
    }

    fn is_segment_based(&self) -> bool {
        self.max_segment_bytes > 0
    }

    fn range(&self) -> MatrixRaftLogRange {
        MatrixRaftLogRange {
            start_index: self.first_index(),
            end_index: self.last_index().saturating_add(1),
        }
    }

    fn voted_for(&self) -> Option<RustRaftNodeId> {
        self.hard_state.voted_for
    }

    fn current_term(&self) -> RustRaftTerm {
        self.hard_state.current_term
    }

    fn committed_index(&self) -> RustRaftLogIndex {
        self.committed_index
    }

    fn stabled_committed_index(&self) -> RustRaftLogIndex {
        self.stabled_committed_index
    }

    fn initial_state(&self) -> MatrixRaftInitialState {
        self.initial_state.clone()
    }

    fn members(&self) -> Vec<MatrixRaftMemberId> {
        self.members.clone()
    }

    fn size_until(&self, index: RustRaftLogIndex) -> usize {
        self.entries
            .range(..=index)
            .map(|(_, entry)| entry.bytes_size as usize)
            .sum()
    }

    fn role(&self) -> RustRaftReplicaRole {
        self.role
    }

    fn file_indexes(&self) -> Vec<u64> {
        self.segments
            .iter()
            .map(|segment| segment.first_index)
            .collect()
    }

    fn segments(&self) -> Vec<MatrixRaftLogSegment> {
        self.segments.clone()
    }

    fn segment_events(&self) -> Vec<MatrixRaftLogSegmentEvent> {
        self.segment_events.clone()
    }

    fn drain_segment_events(&mut self) -> Vec<MatrixRaftLogSegmentEvent> {
        std::mem::take(&mut self.segment_events)
    }

    fn switch_segment(
        &mut self,
        next_first_index: RustRaftLogIndex,
    ) -> Result<MatrixRaftLogSegmentEvent, RustRaftError> {
        if next_first_index <= self.last_index() {
            return Err(RustRaftError::InvalidRequest(format!(
                "segment switch index {next_first_index} is not after last log index {}",
                self.last_index()
            )));
        }
        Ok(self.open_segment(next_first_index))
    }

    fn release_segments_until(
        &mut self,
        index: RustRaftLogIndex,
    ) -> Vec<MatrixRaftLogSegmentEvent> {
        let mut released = Vec::new();
        let mut retained = Vec::new();
        for segment in self.segments.drain(..) {
            if segment.sealed && segment.last_index <= index {
                let event = MatrixRaftLogSegmentEvent::from_segment(
                    MatrixRaftLogSegmentEventKind::Release,
                    self.peer_id,
                    None,
                    &segment,
                );
                released.push(event.clone());
                self.segment_events.push(event);
            } else {
                retained.push(segment);
            }
        }
        self.segments = retained;
        released
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct MatrixRaftLogStorageKey {
    path: String,
    peer_id: RustRaftNodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMemoryGroupStorage {
    group_id: RustRaftGroupId,
    prepared: BTreeMap<MatrixRaftLogStorageKey, MatrixRaftLogStoragePrepareOptions>,
    opened: BTreeMap<MatrixRaftLogStorageKey, MatrixRaftMemoryLogStorage>,
    in_batch: bool,
    batch_size: usize,
    overflow_limit: usize,
}

impl MatrixRaftMemoryGroupStorage {
    pub fn new(group_id: RustRaftGroupId) -> Self {
        Self {
            group_id,
            prepared: BTreeMap::new(),
            opened: BTreeMap::new(),
            in_batch: false,
            batch_size: 0,
            overflow_limit: usize::MAX,
        }
    }

    pub fn with_overflow_limit(mut self, overflow_limit: usize) -> Self {
        self.overflow_limit = overflow_limit.max(1);
        self
    }

    fn key(path: impl Into<String>, peer_id: RustRaftNodeId) -> MatrixRaftLogStorageKey {
        MatrixRaftLogStorageKey {
            path: path.into(),
            peer_id,
        }
    }
}

impl MatrixRaftGroupStorage for MatrixRaftMemoryGroupStorage {
    type Log = MatrixRaftMemoryLogStorage;

    fn prepare(
        &mut self,
        path: impl Into<String>,
        options: MatrixRaftLogStoragePrepareOptions,
    ) -> Result<(), RustRaftError> {
        let key = Self::key(path, options.peer_id);
        if self.prepared.contains_key(&key) {
            return Err(RustRaftError::InvalidRequest(format!(
                "matrixraft log storage for peer {} is already prepared",
                key.peer_id
            )));
        }
        self.prepared.insert(key, options);
        self.batch_size += 1;
        Ok(())
    }

    fn open(
        &mut self,
        path: impl Into<String>,
        options: MatrixRaftLogStorageOptions,
    ) -> Result<Self::Log, RustRaftError> {
        let key = Self::key(path, options.peer_id);
        let prepare = self
            .prepared
            .get(&key)
            .cloned()
            .ok_or(RustRaftError::NodeNotFound(options.peer_id))?;
        let log = self
            .opened
            .entry(key)
            .or_insert_with(|| MatrixRaftMemoryLogStorage::new(prepare, options));
        Ok(log.clone())
    }

    fn setup_node_env(&mut self, options: &crate::MatrixRaftOptions) -> Result<(), RustRaftError> {
        let local_id = MatrixRaftMemberId::from(&crate::RustRaftPeer {
            node_id: options.peer_id,
            raft_addr: options.raft_addr.clone(),
            snapshot_addr: options.snapshot_addr.clone(),
            role: options.role,
            auto_promote: false,
        });
        self.prepare(
            options.wal_dir.clone(),
            MatrixRaftLogStoragePrepareOptions {
                peer_id: options.peer_id,
                max_segment_bytes: options.max_segment_bytes as usize,
                initial_state: MatrixRaftInitialState { index: 0, term: 0 },
                role: options.role,
                local_id,
                members: options.peers.iter().map(MatrixRaftMemberId::from).collect(),
            },
        )
    }

    fn clean_up_node_env(
        &mut self,
        options: &crate::MatrixRaftOptions,
    ) -> Result<(), RustRaftError> {
        self.delete(options.peer_id)
    }

    fn begin(&mut self) {
        self.in_batch = true;
        self.batch_size = 0;
    }

    fn commit(&mut self) -> Result<(), RustRaftError> {
        self.in_batch = false;
        Ok(())
    }

    fn overflow(&self) -> bool {
        self.batch_size > self.overflow_limit
    }

    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn group_id(&self) -> RustRaftGroupId {
        self.group_id
    }

    fn exists(&self, node_id: RustRaftNodeId) -> bool {
        self.prepared.keys().any(|key| key.peer_id == node_id)
    }

    fn delete(&mut self, node_id: RustRaftNodeId) -> Result<(), RustRaftError> {
        let prepared_before = self.prepared.len();
        self.prepared.retain(|key, _| key.peer_id != node_id);
        self.opened.retain(|key, _| key.peer_id != node_id);
        if prepared_before == self.prepared.len() {
            return Err(RustRaftError::NodeNotFound(node_id));
        }
        Ok(())
    }
}

pub fn matrixraft_validate_storage_apply_fence(
    fence: &RustRaftStorageApplyFence,
) -> Result<(), RustRaftError> {
    if fence.applied_index > fence.committed_index {
        return Err(RustRaftError::Storage(
            "storage apply fence is ahead of committed index".to_string(),
        ));
    }
    if fence.durable_applied_index > fence.applied_index {
        return Err(RustRaftError::Storage(
            "durable applied index is ahead of in-memory applied index".to_string(),
        ));
    }
    if fence.storage_flushed_index < fence.durable_applied_index {
        return Err(RustRaftError::Storage(
            "storage flush is behind durable applied index".to_string(),
        ));
    }
    if fence.installed_snapshot_index > fence.applied_index {
        return Err(RustRaftError::Storage(
            "installed snapshot is ahead of applied index".to_string(),
        ));
    }
    if fence.installed_snapshot_index > 0
        && fence.first_retained_log_index <= fence.installed_snapshot_index
    {
        return Err(RustRaftError::Storage(
            "first retained log index overlaps installed snapshot".to_string(),
        ));
    }
    Ok(())
}
