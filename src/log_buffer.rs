// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style in-memory log buffer with unstable flush tracking.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::{LogEntry, LogId, LogIndex, RaftError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogBufferFlush {
    pub entries: Vec<LogEntry>,
    pub first_index: LogIndex,
    pub last_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogBufferRelease {
    pub released_entries: Vec<LogEntry>,
    pub released_until: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogBuffer {
    cache_memory_limit: u64,
    initial_state: LogId,
    flushing: bool,
    flushing_offset: usize,
    waiting_offset: usize,
    log_buffer_bytes: u64,
    entries: VecDeque<LogEntry>,
}

impl LogBuffer {
    pub fn new(cache_memory_limit: u64, initial_state: LogId) -> Self {
        let dummy = LogEntry {
            log_id: initial_state.clone(),
            payload: Vec::new(),
            is_command: false,
        };
        let log_buffer_bytes = entry_size(&dummy);
        let mut entries = VecDeque::new();
        entries.push_back(dummy);
        Self {
            cache_memory_limit: cache_memory_limit.max(1),
            initial_state,
            flushing: false,
            flushing_offset: 1,
            waiting_offset: 1,
            log_buffer_bytes,
            entries,
        }
    }

    pub fn append(&mut self, entry: LogEntry) -> Result<(), RaftError> {
        let expected = self.last_index().saturating_add(1);
        if entry.log_id.index != expected {
            return Err(RaftError::InvalidRequest(format!(
                "log buffer append expects index {expected}, got {}",
                entry.log_id.index
            )));
        }
        self.log_buffer_bytes = self.log_buffer_bytes.saturating_add(entry_size(&entry));
        self.entries.push_back(entry);
        Ok(())
    }

    pub fn append_many(&mut self, entries: Vec<LogEntry>) -> Result<(), RaftError> {
        for entry in entries {
            self.append(entry)?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<Option<LogBufferFlush>, RaftError> {
        if self.flushing || self.entries.len() == self.flushing_offset {
            return Ok(None);
        }
        if self.flushing_offset != self.waiting_offset {
            return Err(RaftError::InvalidRequest(
                "log buffer has entries already waiting for storage flush".to_string(),
            ));
        }

        let flushed_entries = self
            .entries
            .iter()
            .skip(self.waiting_offset)
            .cloned()
            .collect::<Vec<_>>();
        let first_index = flushed_entries
            .first()
            .map(|entry| entry.log_id.index)
            .unwrap_or_default();
        let last_index = flushed_entries
            .last()
            .map(|entry| entry.log_id.index)
            .unwrap_or_default();
        self.waiting_offset = self.entries.len();
        self.flushing = true;
        Ok(Some(LogBufferFlush {
            entries: flushed_entries,
            first_index,
            last_index,
        }))
    }

    pub fn apply_append_result(
        &mut self,
        first_index: LogIndex,
        last_index: LogIndex,
    ) -> Result<LogIndex, RaftError> {
        if first_index == 0 || last_index < first_index {
            return Err(RaftError::InvalidRequest(format!(
                "invalid stabled range: {first_index}..={last_index}"
            )));
        }
        let first_offset = if first_index >= self.actual_first_index() {
            (first_index - self.actual_first_index()) as usize
        } else {
            return Err(RaftError::InvalidRequest(format!(
                "append result first index {first_index} is before first retained index {}",
                self.actual_first_index()
            )));
        };
        let written_size = (last_index - first_index + 1) as usize;
        let last_pending_offset = first_offset.saturating_add(written_size);
        let mut last_written_index = last_index;
        self.flushing = false;

        if self.waiting_offset != last_pending_offset {
            if self.waiting_offset >= last_pending_offset {
                return Err(RaftError::InvalidRequest(
                    "log buffer append result can only be rolled back".to_string(),
                ));
            }
            if first_offset != self.flushing_offset || self.flushing_offset == self.waiting_offset {
                if self.flushing_offset > first_offset {
                    return Err(RaftError::InvalidRequest(
                        "log buffer stabled offset moved past append result".to_string(),
                    ));
                }
                return Ok(0);
            }

            let available_written_size = self.waiting_offset - self.flushing_offset;
            last_written_index = first_index + available_written_size as u64 - 1;
        }
        self.flushing_offset = self.waiting_offset;
        Ok(last_written_index)
    }

    pub fn read_entries(
        &self,
        from: LogIndex,
        to: LogIndex,
        limit_bytes: usize,
    ) -> Result<Vec<LogEntry>, RaftError> {
        if from > to || from <= self.actual_first_index() || to > self.last_index() {
            return Err(RaftError::InvalidRequest(format!(
                "log buffer range {from}..={to} is outside retained range {:?}",
                self.range()
            )));
        }

        let offset = (from - self.actual_first_index()) as usize;
        let max_length = (to - from + 1) as usize;
        let length = if limit_bytes == 0 {
            max_length
        } else {
            let mut bytes = 0usize;
            let mut length = 0usize;
            while bytes < limit_bytes && length < max_length {
                bytes = bytes.saturating_add(entry_size(&self.entries[offset + length]) as usize);
                length += 1;
            }
            length
        };
        Ok(self
            .entries
            .iter()
            .skip(offset)
            .take(length)
            .cloned()
            .collect())
    }

    pub fn truncate_from_index(&mut self, index: LogIndex) -> Result<(), RaftError> {
        if index <= self.actual_first_index() {
            return Err(RaftError::InvalidRequest(format!(
                "truncate index {index} must be after first retained index {}",
                self.actual_first_index()
            )));
        }
        if index > self.last_index().saturating_add(1) {
            return Ok(());
        }

        let truncate_offset = (index - self.actual_first_index()) as usize;
        self.waiting_offset = self.waiting_offset.min(truncate_offset);
        self.flushing_offset = self.flushing_offset.min(truncate_offset);
        while self.entries.len() > truncate_offset {
            if let Some(entry) = self.entries.pop_back() {
                self.log_buffer_bytes = self.log_buffer_bytes.saturating_sub(entry_size(&entry));
            }
        }
        Ok(())
    }

    pub fn reset_initial_state(&mut self, initial_state: LogId) -> Result<(), RaftError> {
        if initial_state.index <= self.actual_first_index() {
            return Err(RaftError::InvalidRequest(format!(
                "reset initial index {} must be after first retained index {}",
                initial_state.index,
                self.actual_first_index()
            )));
        }
        *self = Self::new(self.cache_memory_limit, initial_state);
        Ok(())
    }

    pub fn mark_all_stabled(&mut self) {
        self.flushing_offset = self.entries.len();
        self.waiting_offset = self.entries.len();
        self.flushing = false;
    }

    pub fn release_memory(
        &mut self,
        replicate_index: LogIndex,
        applied_index: LogIndex,
    ) -> Result<Option<LogBufferRelease>, RaftError> {
        if replicate_index > applied_index {
            return Err(RaftError::InvalidRequest(format!(
                "replicate index {replicate_index} must not exceed applied index {applied_index}"
            )));
        }
        let Some(recommend_index) = self.find_releasable_index(replicate_index, applied_index)
        else {
            return Ok(None);
        };
        self.drain_until(recommend_index)
    }

    pub fn drain_until(&mut self, index: LogIndex) -> Result<Option<LogBufferRelease>, RaftError> {
        let first_index = self.actual_first_index();
        if index <= first_index {
            return Ok(None);
        }
        if index > self.last_index() {
            return Err(RaftError::InvalidRequest(format!(
                "drain index {index} exceeds last retained index {}",
                self.last_index()
            )));
        }

        let drain_length = (index - first_index) as usize;
        let mut released_entries = Vec::with_capacity(drain_length);
        for _ in 0..drain_length {
            if let Some(entry) = self.entries.pop_front() {
                self.log_buffer_bytes = self.log_buffer_bytes.saturating_sub(entry_size(&entry));
                released_entries.push(entry);
            }
        }
        let sentinel = self
            .entries
            .front()
            .expect("log buffer keeps a sentinel entry")
            .log_id
            .clone();
        self.initial_state = sentinel;
        self.flushing_offset = self.flushing_offset.saturating_sub(drain_length);
        self.waiting_offset = self.waiting_offset.saturating_sub(drain_length);
        Ok(Some(LogBufferRelease {
            released_entries,
            released_until: index,
        }))
    }

    pub fn term_at(&self, index: LogIndex) -> Option<u64> {
        if index < self.actual_first_index() || index > self.last_index() {
            return None;
        }
        self.entries
            .get((index - self.actual_first_index()) as usize)
            .map(|entry| entry.log_id.term)
    }

    pub fn range(&self) -> (LogIndex, LogIndex) {
        (
            self.actual_first_index().saturating_add(1),
            self.last_index().saturating_add(1),
        )
    }

    pub fn last_index(&self) -> LogIndex {
        self.actual_first_index() + self.entries.len() as u64 - 1
    }

    pub fn last_term(&self) -> u64 {
        self.entries
            .back()
            .expect("log buffer keeps a sentinel entry")
            .log_id
            .term
    }

    pub fn last_synced_index(&self) -> LogIndex {
        self.actual_first_index() + self.flushing_offset as u64 - 1
    }

    pub fn is_flushing(&self) -> bool {
        self.flushing
    }

    pub fn is_busy(&self) -> bool {
        self.cache_memory_limit <= self.log_buffer_bytes
    }

    pub fn is_releasable(&self, applied_index: LogIndex) -> bool {
        self.find_releasable_index(applied_index, applied_index)
            .is_some()
    }

    pub fn num_entries(&self) -> usize {
        self.entries.len()
    }

    pub fn total_bytes(&self) -> u64 {
        self.log_buffer_bytes
    }

    pub fn initial_state(&self) -> &LogId {
        &self.initial_state
    }

    fn find_releasable_index(
        &self,
        replicate_index: LogIndex,
        applied_index: LogIndex,
    ) -> Option<LogIndex> {
        let first_index = self.actual_first_index();
        let hint_index = if first_index < applied_index
            && ratio(self.cache_memory_limit) <= self.log_buffer_bytes
        {
            applied_index
        } else if first_index < replicate_index {
            replicate_index
        } else {
            0
        };

        if hint_index == 0 {
            return None;
        }
        let drain_length = ratio(hint_index - first_index);
        (drain_length > 0).then_some(first_index + drain_length)
    }

    fn actual_first_index(&self) -> LogIndex {
        self.initial_state.index
    }
}

fn ratio(value: u64) -> u64 {
    value.saturating_mul(9) / 10
}

fn entry_size(entry: &LogEntry) -> u64 {
    24 + entry.payload.len() as u64
}
