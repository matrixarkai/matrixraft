// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// WAL persistence/runtime structs and segmented WAL helpers.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftApplySnapshotFence {
    pub applied_index: RustRaftLogIndex,
    pub commit_index: RustRaftLogIndex,
    pub installed_snapshot_index: RustRaftLogIndex,
    pub first_retained_log_index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftStorageApplyFence {
    pub group_id: RustRaftGroupId,
    pub node_id: RustRaftNodeId,
    pub committed_index: RustRaftLogIndex,
    pub applied_index: RustRaftLogIndex,
    pub durable_applied_index: RustRaftLogIndex,
    pub storage_flushed_index: RustRaftLogIndex,
    pub installed_snapshot_index: RustRaftLogIndex,
    pub first_retained_log_index: RustRaftLogIndex,
}

pub type RaftStorageApplyFence = RustRaftStorageApplyFence;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftDurabilityParityReport {
    pub ready: bool,
    pub hard_state_persisted: bool,
    pub wal_record_valid: bool,
    pub segmented_wal_recovered: bool,
    pub corrupt_tail_truncated: bool,
    pub snapshot_install_valid: bool,
    pub snapshot_floor_preserved: bool,
    pub snapshot_tail_catchup_valid: bool,
    pub apply_snapshot_fence_valid: bool,
    pub storage_apply_fence_valid: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalRaftWal {
    pub max_records_per_segment: usize,
    segments: Vec<RaftWalSegment>,
    next_segment_id: u64,
}

impl LocalRaftWal {
    pub fn new(max_records_per_segment: usize) -> Result<Self, RaftError> {
        if max_records_per_segment == 0 {
            return Err(RaftError::InvalidRequest(
                "max_records_per_segment must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            max_records_per_segment,
            segments: vec![RaftWalSegment {
                segment_id: 0,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                sealed: false,
            }],
            next_segment_id: 1,
        })
    }

    pub fn append(&mut self, mut record: RaftWalRecord) -> Result<String, RaftError> {
        record.checksum = matrixraft_wal_checksum(&record);
        if self
            .segments
            .last()
            .map(|segment| segment.records.len() >= self.max_records_per_segment)
            .unwrap_or(true)
        {
            if let Some(segment) = self.segments.last_mut() {
                segment.sealed = true;
            }
            self.segments.push(RaftWalSegment {
                segment_id: self.next_segment_id,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                sealed: false,
            });
            self.next_segment_id += 1;
        }

        let checksum = record.checksum.clone();
        let record_index = record
            .hard_state
            .committed
            .as_ref()
            .map(|log_id| log_id.index)
            .or_else(|| record.entries.last().map(|entry| entry.log_id.index))
            .unwrap_or_default();
        let segment = self
            .segments
            .last_mut()
            .ok_or_else(|| RaftError::Storage("WAL has no active segment".to_string()))?;
        if segment.records.is_empty() {
            segment.first_index = record_index;
        }
        segment.last_index = record_index;
        segment.records.push(record);
        Ok(checksum)
    }

    pub fn segments(&self) -> &[RaftWalSegment] {
        &self.segments
    }

    pub fn records(&self) -> Vec<RaftWalRecord> {
        self.segments
            .iter()
            .flat_map(|segment| segment.records.iter().cloned())
            .collect()
    }

    pub fn retained_log_range(&self) -> RaftLogRetainedRange {
        wal_retained_range(&self.segments)
    }

    pub fn segment_index(&self) -> Vec<RaftWalSegmentIndex> {
        self.segments
            .iter()
            .map(|segment| RaftWalSegmentIndex {
                segment_id: segment.segment_id,
                first_log_index: segment.first_index,
                last_log_index: segment.last_index,
                record_count: segment.records.len() as u64,
                sealed: segment.sealed,
                bytes: 0,
            })
            .collect()
    }

    pub fn recover(&mut self) -> Result<RaftWalRecoveryReport, RaftError> {
        let mut records = self.records();
        let original_len = records.len();
        while matches!(records.last(), Some(record) if !matrixraft_wal_checksum_valid(record)) {
            records.pop();
        }
        let recovered = matrixraft_recover_latest_wal_record(&records).ok();
        let truncated_corrupt_tail = records.len() != original_len;
        if truncated_corrupt_tail {
            self.rebuild_from_records(records.clone())?;
        }
        Ok(RaftWalRecoveryReport {
            recovered,
            truncated_corrupt_tail,
            surviving_records: records.len(),
            removed_records: original_len.saturating_sub(records.len()),
            segments_scanned: self.segments.len() as u64,
            checksum_format: Some(matrixraft_wal_checksum_format()),
            retained_range: Some(wal_retained_range(&self.segments)),
        })
    }

    pub fn corrupt_tail_for_test(&mut self) -> Result<(), RaftError> {
        let record = self
            .segments
            .last_mut()
            .and_then(|segment| segment.records.last_mut())
            .ok_or_else(|| RaftError::Storage("WAL has no tail record".to_string()))?;
        record.checksum = "corrupt-tail".to_string();
        Ok(())
    }

    fn rebuild_from_records(&mut self, records: Vec<RaftWalRecord>) -> Result<(), RaftError> {
        let max_records_per_segment = self.max_records_per_segment;
        *self = LocalRaftWal::new(max_records_per_segment)?;
        for record in records {
            self.append(record)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistentRaftWalOptions {
    pub dir: PathBuf,
    pub max_records_per_segment: usize,
    pub max_segment_bytes: u64,
    pub min_keep_segments: usize,
    pub fsync_on_append: bool,
}

impl PersistentRaftWalOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_records_per_segment: 10_000,
            max_segment_bytes: 64 * 1024 * 1024,
            min_keep_segments: 2,
            fsync_on_append: true,
        }
    }

    pub fn validate(&self) -> Result<(), RaftError> {
        if self.max_records_per_segment == 0 {
            return Err(RaftError::InvalidRequest(
                "max_records_per_segment must be greater than zero".to_string(),
            ));
        }
        if self.max_segment_bytes == 0 {
            return Err(RaftError::InvalidRequest(
                "max_segment_bytes must be greater than zero".to_string(),
            ));
        }
        if self.min_keep_segments == 0 {
            return Err(RaftError::InvalidRequest(
                "min_keep_segments must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PersistentRaftWal {
    options: PersistentRaftWalOptions,
    segments: Vec<RaftWalSegment>,
    active_segment: File,
    next_segment_id: u64,
    released_segment_count: u64,
    truncated_corrupt_tail: bool,
    slow_fsync_threshold_ms: u64,
    slow_fsync_count: u64,
    consecutive_slow_fsync_count: u64,
    max_fsync_elapsed_ms: u64,
    compacted_after_slow_fsync_count: u64,
    inject_next_fsync_delay_ms: Option<u64>,
    /// What the active segment's records already describe, as
    /// (first index, last index, term at the last index). `None` means the
    /// segment is empty, so the next record has to be stored whole.
    active_covered: Option<(RustRaftLogIndex, RustRaftLogIndex, RustRaftTerm)>,
}

impl PersistentRaftWal {
    pub fn open(options: PersistentRaftWalOptions) -> Result<Self, RaftError> {
        options.validate()?;
        fs::create_dir_all(&options.dir).map_err(|err| {
            RaftError::Storage(format!(
                "failed to create WAL directory {}: {err}",
                options.dir.display()
            ))
        })?;
        let (mut segments, truncated_corrupt_tail) = read_wal_segments_from_dir(&options.dir)?;
        if segments.is_empty() {
            segments.push(RaftWalSegment {
                segment_id: 0,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                sealed: false,
            });
            write_wal_segment_file(&options.dir, &segments[0])?;
        }
        let active_id = segments
            .last()
            .map(|segment| segment.segment_id)
            .unwrap_or(0);
        for segment in segments.iter_mut() {
            segment.sealed = segment.segment_id != active_id;
        }
        let active_segment = open_segment_for_append(&options.dir, active_id)?;
        let next_segment_id = active_id + 1;
        let mut wal = Self {
            options,
            segments,
            active_segment,
            next_segment_id,
            released_segment_count: 0,
            truncated_corrupt_tail,
            slow_fsync_threshold_ms: 10,
            slow_fsync_count: 0,
            consecutive_slow_fsync_count: 0,
            max_fsync_elapsed_ms: 0,
            compacted_after_slow_fsync_count: 0,
            inject_next_fsync_delay_ms: None,
            active_covered: None,
        };
        wal.active_covered = wal.covered_from_active_segment();
        Ok(wal)
    }

    /// Advances coverage using the record just appended. The whole-log case
    /// resets it; a delta only moves the far end.
    fn advance_active_covered(&mut self) {
        let Some(record) = self.segments.last().and_then(|s| s.records.last()) else {
            self.active_covered = None;
            return;
        };
        let last = record.entries.last();
        if record.entries_are_delta {
            if let (Some((first_index, _, _)), Some(last)) = (self.active_covered, last) {
                self.active_covered = Some((first_index, last.log_id.index, last.log_id.term));
            }
            return;
        }
        self.active_covered = match (record.entries.first(), last) {
            (Some(first), Some(last)) => {
                Some((first.log_id.index, last.log_id.index, last.log_id.term))
            }
            _ => None,
        };
    }

    /// Turns a whole-log record into the delta this segment can store, or
    /// returns it whole when a delta would not be sound.
    fn delta_record(&self, record: &RaftWalRecord) -> RaftWalRecord {
        let Some((first_index, last_index, last_term)) = self.active_covered else {
            return whole_record(record);
        };
        let Some(base) =
            matrixraft_wal_delta_base(&record.entries, first_index, last_index, last_term)
        else {
            return whole_record(record);
        };
        let mut delta = record.clone();
        delta.entries = record.entries[base..].to_vec();
        delta.entries_are_delta = true;
        delta.checksum = matrixraft_wal_checksum(&delta);
        delta
    }

    /// Rebuilds the active segment's coverage by folding what is already on
    /// disk, so an appended-to WAL keeps writing deltas after a reopen.
    fn covered_from_active_segment(
        &self,
    ) -> Option<(RustRaftLogIndex, RustRaftLogIndex, RustRaftTerm)> {
        let segment = self.segments.last()?;
        let folded = matrixraft_fold_wal_records(&segment.records);
        let entries = &folded.last()?.entries;
        let first = entries.first()?;
        let last = entries.last()?;
        Some((first.log_id.index, last.log_id.index, last.log_id.term))
    }

    pub fn set_slow_fsync_threshold_ms(&mut self, threshold_ms: u64) {
        self.slow_fsync_threshold_ms = threshold_ms;
    }

    pub fn inject_next_fsync_delay_for_test(&mut self, delay_ms: u64) {
        self.inject_next_fsync_delay_ms = Some(delay_ms);
    }

    /// What the active segment already describes, for a caller that wants to
    /// build the delta itself rather than hand over the whole log.
    pub fn active_coverage(&self) -> Option<(RustRaftLogIndex, RustRaftLogIndex, RustRaftTerm)> {
        self.active_covered
    }

    /// Appends a record the caller builds on demand.
    ///
    /// `build` is handed the coverage the record should be written against, or
    /// `None` when a whole-log record is required -- at the start of a segment,
    /// and again if rolling turns out to be necessary after the record was
    /// built. Callers that can produce a delta cheaply should use this instead
    /// of [`Self::append`], which has to be given the whole log every time.
    pub fn append_built<F>(&mut self, mut build: F) -> Result<RaftWalWriteReport, RaftError>
    where
        F: FnMut(
            Option<(RustRaftLogIndex, RustRaftLogIndex, RustRaftTerm)>,
        ) -> Result<RaftWalRecord, RaftError>,
    {
        let active_records = self
            .segments
            .last()
            .map(|segment| segment.records.len())
            .unwrap_or_default();
        let rolling_by_count = active_records >= self.options.max_records_per_segment;
        let coverage = if rolling_by_count {
            None
        } else {
            self.active_covered
        };
        let record = build(coverage)?;
        self.write_record(record, active_records, rolling_by_count, || build(None))
    }

    pub fn append(&mut self, record: RaftWalRecord) -> Result<String, RaftError> {
        Ok(self.append_with_report(record)?.checksum)
    }

    pub fn append_with_report(
        &mut self,
        record: RaftWalRecord,
    ) -> Result<RaftWalWriteReport, RaftError> {
        let active_records = self
            .segments
            .last()
            .map(|segment| segment.records.len())
            .unwrap_or_default();
        let rolling_by_count = active_records >= self.options.max_records_per_segment;
        let stored = if rolling_by_count {
            whole_record(&record)
        } else {
            self.delta_record(&record)
        };
        self.write_record(stored, active_records, rolling_by_count, || {
            Ok(whole_record(&record))
        })
    }

    /// Writes a record that has already been reduced to what will be stored.
    ///
    /// `whole` is only called when rolling turns out to be necessary after the
    /// record was built, since the segment it was a delta against is then
    /// sealed and the new segment has to open with a whole-log record.
    fn write_record<W>(
        &mut self,
        mut record: RaftWalRecord,
        active_records: usize,
        rolling_by_count: bool,
        whole: W,
    ) -> Result<RaftWalWriteReport, RaftError>
    where
        W: FnOnce() -> Result<RaftWalRecord, RaftError>,
    {
        let hard_state_persisted = matrixraft_validate_hard_state_persistence(&record).is_ok();
        let active_len = self
            .active_segment
            .metadata()
            .map_err(|err| {
                RaftError::Storage(format!("failed to read WAL active segment metadata: {err}"))
            })?
            .len();
        let mut encoded = encode_wal_record(&record)?;
        let mut record_bytes = encoded.len() as u64 + 1;

        let mut segment_rolled = false;
        if rolling_by_count
            || (active_records > 0 && active_len + record_bytes > self.options.max_segment_bytes)
        {
            self.roll_segment()?;
            segment_rolled = true;
            if record.entries_are_delta {
                record = whole()?;
                record.entries_are_delta = false;
                record.checksum = matrixraft_wal_checksum(&record);
                encoded = encode_wal_record(&record)?;
                record_bytes = encoded.len() as u64 + 1;
            }
        }

        self.active_segment
            .write_all(encoded.as_bytes())
            .and_then(|_| self.active_segment.write_all(b"\n"))
            .map_err(|err| RaftError::Storage(format!("failed to append WAL record: {err}")))?;
        let mut fsync_elapsed_ms = 0;
        let mut slow_fsync_observed = false;
        if self.options.fsync_on_append {
            let fsync_started = Instant::now();
            if let Some(delay_ms) = self.inject_next_fsync_delay_ms.take() {
                thread::sleep(Duration::from_millis(delay_ms));
            }
            self.active_segment
                .sync_data()
                .map_err(|err| RaftError::Storage(format!("failed to fsync WAL record: {err}")))?;
            fsync_elapsed_ms = fsync_started
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX);
            self.max_fsync_elapsed_ms = self.max_fsync_elapsed_ms.max(fsync_elapsed_ms);
            slow_fsync_observed =
                self.slow_fsync_threshold_ms > 0 && fsync_elapsed_ms >= self.slow_fsync_threshold_ms;
            if slow_fsync_observed {
                self.slow_fsync_count += 1;
                self.consecutive_slow_fsync_count += 1;
            } else {
                self.consecutive_slow_fsync_count = 0;
            }
        }
        let checksum = record.checksum.clone();
        let record_index = wal_record_index(&record);
        let segment = self
            .segments
            .last_mut()
            .ok_or_else(|| RaftError::Storage("WAL has no active segment".to_string()))?;
        if segment.records.is_empty() {
            segment.first_index = record_index;
        }
        segment.last_index = record_index;
        segment.records.push(record);
        let segment_id = segment.segment_id;
        self.advance_active_covered();
        Ok(RaftWalWriteReport {
            segment_id,
            log_index: record_index,
            checksum,
            checksum_format: matrixraft_wal_checksum_format(),
            bytes_written: record_bytes,
            fsync_on_append: self.options.fsync_on_append,
            fsync_elapsed_ms,
            slow_fsync_threshold_ms: self.slow_fsync_threshold_ms,
            slow_fsync_observed,
            segment_rolled,
            hard_state_persisted,
            retained_range: wal_retained_range(&self.segments),
        })
    }

    pub fn recover(&mut self) -> Result<RaftWalRecoveryReport, RaftError> {
        let (segments, truncated_corrupt_tail) = read_wal_segments_from_dir(&self.options.dir)?;
        let original_len = self.records().len();
        let stored: Vec<_> = segments
            .iter()
            .flat_map(|segment| segment.records.iter().cloned())
            .collect();
        let records = matrixraft_fold_wal_records(&stored);
        self.segments = if segments.is_empty() {
            vec![RaftWalSegment {
                segment_id: 0,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                sealed: false,
            }]
        } else {
            segments
        };
        let active_id = self
            .segments
            .last()
            .map(|segment| segment.segment_id)
            .unwrap_or(0);
        for segment in self.segments.iter_mut() {
            segment.sealed = segment.segment_id != active_id;
        }
        self.active_segment = open_segment_for_append(&self.options.dir, active_id)?;
        self.next_segment_id = active_id + 1;
        self.active_covered = self.covered_from_active_segment();
        let observed_corrupt_tail = self.truncated_corrupt_tail || truncated_corrupt_tail;
        self.truncated_corrupt_tail = observed_corrupt_tail;
        Ok(RaftWalRecoveryReport {
            recovered: matrixraft_recover_latest_wal_record(&records).ok(),
            truncated_corrupt_tail: observed_corrupt_tail,
            surviving_records: records.len(),
            removed_records: original_len.saturating_sub(records.len()),
            segments_scanned: self.segments.len() as u64,
            checksum_format: Some(matrixraft_wal_checksum_format()),
            retained_range: Some(wal_retained_range(&self.segments)),
        })
    }

    pub fn compact_through(&mut self, log_index: RustRaftLogIndex) -> Result<u64, RaftError> {
        if self.segments.len() <= self.options.min_keep_segments {
            return Ok(0);
        }
        let removable_count = self
            .segments
            .len()
            .saturating_sub(self.options.min_keep_segments);
        let removable_ids: Vec<_> = self
            .segments
            .iter()
            .take(removable_count)
            .filter(|segment| segment.last_index > 0 && segment.last_index <= log_index)
            .map(|segment| segment.segment_id)
            .collect();
        for segment_id in &removable_ids {
            let path = wal_segment_path(&self.options.dir, *segment_id);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(RaftError::Storage(format!(
                        "failed to remove compacted WAL segment {}: {err}",
                        path.display()
                    )));
                }
            }
        }
        if !removable_ids.is_empty() {
            self.segments
                .retain(|segment| !removable_ids.contains(&segment.segment_id));
            self.released_segment_count += removable_ids.len() as u64;
            if self.slow_fsync_count > 0 {
                self.compacted_after_slow_fsync_count += removable_ids.len() as u64;
            }
        }
        Ok(removable_ids.len() as u64)
    }

    pub fn compact_through_with_fence(
        &mut self,
        log_index: RustRaftLogIndex,
        fence: &RustRaftStorageApplyFence,
    ) -> Result<RaftWalCompactionReport, RaftError> {
        if let Err(error) = matrixraft_validate_storage_apply_fence(fence) {
            return Ok(RaftWalCompactionReport {
                requested_log_index: log_index,
                released_segments: 0,
                retained_range: self.retained_log_range(),
                fence_valid: false,
                blocker: Some(error.to_string()),
            });
        }
        if fence.durable_applied_index < log_index || fence.storage_flushed_index < log_index {
            return Ok(RaftWalCompactionReport {
                requested_log_index: log_index,
                released_segments: 0,
                retained_range: self.retained_log_range(),
                fence_valid: false,
                blocker: Some("compaction fence is behind requested log index".to_string()),
            });
        }
        let released_segments = self.compact_through(log_index)?;
        Ok(RaftWalCompactionReport {
            requested_log_index: log_index,
            released_segments,
            retained_range: self.retained_log_range(),
            fence_valid: true,
            blocker: None,
        })
    }

    pub fn status(&self) -> RustRaftWalLifecycleStatus {
        let total_records = self.records().len() as u64;
        let total_bytes = self
            .segments
            .iter()
            .map(|segment| {
                fs::metadata(wal_segment_path(&self.options.dir, segment.segment_id))
                    .map(|metadata| metadata.len())
                    .unwrap_or_default()
            })
            .sum();
        let active_segment_bytes = self
            .segments
            .last()
            .and_then(|segment| {
                fs::metadata(wal_segment_path(&self.options.dir, segment.segment_id)).ok()
            })
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        RustRaftWalLifecycleStatus {
            segment_count: self.segments.len() as u64,
            active_segment_id: self
                .segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            first_retained_segment_id: self
                .segments
                .first()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            last_retained_segment_id: self
                .segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            total_bytes,
            active_segment_bytes,
            total_records,
            first_sequence: self
                .segments
                .first()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            last_sequence: self
                .segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            first_log_index: self
                .segments
                .first()
                .map(|segment| segment.first_index)
                .unwrap_or(0),
            last_log_index: self
                .segments
                .last()
                .map(|segment| segment.last_index)
                .unwrap_or(0),
            released_segment_count: self.released_segment_count,
            slow_fsync_backpressure_observed: self.slow_fsync_count > 0,
            slow_fsync_threshold_ms: self.slow_fsync_threshold_ms,
            slow_fsync_count: self.slow_fsync_count,
            consecutive_slow_fsync_count: self.consecutive_slow_fsync_count,
            max_fsync_elapsed_ms: self.max_fsync_elapsed_ms,
            compacted_after_slow_fsync_count: self.compacted_after_slow_fsync_count,
        }
    }

    pub fn segments(&self) -> &[RaftWalSegment] {
        &self.segments
    }

    pub fn segment_index(&self) -> Vec<RaftWalSegmentIndex> {
        wal_segment_index(&self.options.dir, &self.segments)
    }

    pub fn retained_log_range(&self) -> RaftLogRetainedRange {
        wal_retained_range(&self.segments)
    }

    pub fn checksum_format(&self) -> RaftWalChecksumFormat {
        matrixraft_wal_checksum_format()
    }

    pub fn records(&self) -> Vec<RaftWalRecord> {
        let stored: Vec<_> = self
            .segments
            .iter()
            .flat_map(|segment| segment.records.iter().cloned())
            .collect();
        matrixraft_fold_wal_records(&stored)
    }

    pub fn corrupt_tail_for_test(&mut self) -> Result<(), RaftError> {
        self.active_segment
            .seek(SeekFrom::End(0))
            .and_then(|_| self.active_segment.write_all(b"{\"corrupt_tail\":true\n"))
            .and_then(|_| self.active_segment.flush())
            .map_err(|err| RaftError::Storage(format!("failed to corrupt WAL tail: {err}")))?;
        Ok(())
    }

    fn roll_segment(&mut self) -> Result<(), RaftError> {
        if let Some(segment) = self.segments.last_mut() {
            segment.sealed = true;
            write_wal_segment_file(&self.options.dir, segment)?;
        }
        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;
        let segment = RaftWalSegment {
            segment_id,
            first_index: 0,
            last_index: 0,
            records: Vec::new(),
            sealed: false,
        };
        write_wal_segment_file(&self.options.dir, &segment)?;
        self.active_segment = open_segment_for_append(&self.options.dir, segment_id)?;
        self.segments.push(segment);
        Ok(())
    }
}

pub type FileRaftWal = PersistentRaftWal;

fn whole_record(record: &RaftWalRecord) -> RaftWalRecord {
    let mut whole = record.clone();
    whole.entries_are_delta = false;
    whole.checksum = matrixraft_wal_checksum(&whole);
    whole
}

fn encode_wal_record(record: &RaftWalRecord) -> Result<String, RaftError> {
    serde_json::to_string(record)
        .map_err(|err| RaftError::Storage(format!("failed to encode WAL record: {err}")))
}

fn wal_segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{segment_id:020}.wal"))
}

fn wal_record_index(record: &RaftWalRecord) -> RustRaftLogIndex {
    record
        .hard_state
        .committed
        .as_ref()
        .map(|log_id| log_id.index)
        .or_else(|| record.entries.last().map(|entry| entry.log_id.index))
        .unwrap_or_default()
}

fn wal_retained_range(segments: &[RaftWalSegment]) -> RaftLogRetainedRange {
    RaftLogRetainedRange {
        first_log_index: segments
            .iter()
            .find(|segment| segment.first_index > 0)
            .map(|segment| segment.first_index)
            .unwrap_or_default(),
        last_log_index: segments
            .iter()
            .rev()
            .find(|segment| segment.last_index > 0)
            .map(|segment| segment.last_index)
            .unwrap_or_default(),
        first_segment_id: segments
            .first()
            .map(|segment| segment.segment_id)
            .unwrap_or(0),
        last_segment_id: segments
            .last()
            .map(|segment| segment.segment_id)
            .unwrap_or(0),
        record_count: segments
            .iter()
            .map(|segment| segment.records.len() as u64)
            .sum(),
    }
}

fn wal_segment_index(dir: &Path, segments: &[RaftWalSegment]) -> Vec<RaftWalSegmentIndex> {
    segments
        .iter()
        .map(|segment| RaftWalSegmentIndex {
            segment_id: segment.segment_id,
            first_log_index: segment.first_index,
            last_log_index: segment.last_index,
            record_count: segment.records.len() as u64,
            sealed: segment.sealed,
            bytes: fs::metadata(wal_segment_path(dir, segment.segment_id))
                .map(|metadata| metadata.len())
                .unwrap_or_default(),
        })
        .collect()
}

fn open_segment_for_append(dir: &Path, segment_id: u64) -> Result<File, RaftError> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(wal_segment_path(dir, segment_id))
        .map_err(|err| RaftError::Storage(format!("failed to open WAL segment: {err}")))
}

fn write_wal_segment_file(dir: &Path, segment: &RaftWalSegment) -> Result<(), RaftError> {
    let mut file = File::create(wal_segment_path(dir, segment.segment_id))
        .map_err(|err| RaftError::Storage(format!("failed to create WAL segment: {err}")))?;
    for record in &segment.records {
        let encoded = serde_json::to_string(record)
            .map_err(|err| RaftError::Storage(format!("failed to encode WAL segment: {err}")))?;
        file.write_all(encoded.as_bytes())
            .and_then(|_| file.write_all(b"\n"))
            .map_err(|err| RaftError::Storage(format!("failed to write WAL segment: {err}")))?;
    }
    file.sync_data()
        .map_err(|err| RaftError::Storage(format!("failed to fsync WAL segment: {err}")))
}

fn read_wal_segments_from_dir(dir: &Path) -> Result<(Vec<RaftWalSegment>, bool), RaftError> {
    let mut files = fs::read_dir(dir)
        .map_err(|err| RaftError::Storage(format!("failed to read WAL directory: {err}")))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let segment_id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())?;
            (path.extension().and_then(|ext| ext.to_str()) == Some("wal"))
                .then_some((segment_id, path))
        })
        .collect::<Vec<_>>();
    files.sort_by_key(|(segment_id, _)| *segment_id);

    let mut segments = Vec::new();
    let mut truncated_corrupt_tail = false;
    let last_file_index = files.len().saturating_sub(1);
    for (file_position, (segment_id, path)) in files.into_iter().enumerate() {
        let (records, truncated) = read_wal_segment_file(&path)?;
        truncated_corrupt_tail |= truncated;
        let first_index = records.first().map(wal_record_index).unwrap_or_default();
        let last_index = records.last().map(wal_record_index).unwrap_or_default();
        segments.push(RaftWalSegment {
            segment_id,
            first_index,
            last_index,
            records,
            sealed: file_position != last_file_index,
        });
    }
    Ok((segments, truncated_corrupt_tail))
}

fn read_wal_segment_file(path: &Path) -> Result<(Vec<RaftWalRecord>, bool), RaftError> {
    let file = File::open(path)
        .map_err(|err| RaftError::Storage(format!("failed to open WAL segment: {err}")))?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut valid_end_offset = 0u64;
    let mut current_offset = 0u64;
    let mut truncated = false;
    for line in reader.split(b'\n') {
        let line = line.map_err(|err| {
            RaftError::Storage(format!(
                "failed to read WAL segment {}: {err}",
                path.display()
            ))
        })?;
        current_offset += line.len() as u64 + 1;
        if line.is_empty() {
            valid_end_offset = current_offset;
            continue;
        }
        let Ok(record) = serde_json::from_slice::<RaftWalRecord>(&line) else {
            truncated = true;
            break;
        };
        if !matrixraft_wal_checksum_valid(&record) {
            truncated = true;
            break;
        }
        valid_end_offset = current_offset;
        records.push(record);
    }
    if truncated {
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|err| RaftError::Storage(format!("failed to reopen WAL segment: {err}")))?;
        file.set_len(valid_end_offset)
            .map_err(|err| RaftError::Storage(format!("failed to truncate WAL segment: {err}")))?;
        file.sync_data()
            .map_err(|err| RaftError::Storage(format!("failed to fsync truncated WAL: {err}")))?;
    }
    Ok((records, truncated))
}
