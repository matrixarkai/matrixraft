// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// WAL persistence/runtime structs and segmented WAL helpers.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplySnapshotFence {
    pub applied_index: LogIndex,
    pub commit_index: LogIndex,
    pub installed_snapshot_index: LogIndex,
    pub first_retained_log_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageApplyFence {
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub committed_index: LogIndex,
    pub applied_index: LogIndex,
    pub durable_applied_index: LogIndex,
    pub storage_flushed_index: LogIndex,
    pub installed_snapshot_index: LogIndex,
    pub first_retained_log_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DurabilityParityReport {
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
    segments: Vec<WalSegment>,
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
            segments: vec![WalSegment {
                segment_id: 0,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                record_count: 0,
                sealed: false,
            }],
            next_segment_id: 1,
        })
    }

    pub fn append(&mut self, mut record: WalRecord) -> Result<String, RaftError> {
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
            self.segments.push(WalSegment {
                segment_id: self.next_segment_id,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                record_count: 0,
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
        segment.record_count = segment.records.len() as u64;
        Ok(checksum)
    }

    pub fn segments(&self) -> &[WalSegment] {
        &self.segments
    }

    pub fn records(&self) -> Vec<WalRecord> {
        self.segments
            .iter()
            .flat_map(|segment| segment.records.iter().cloned())
            .collect()
    }

    pub fn retained_log_range(&self) -> LogRetainedRange {
        wal_retained_range(&self.segments)
    }

    pub fn segment_index(&self) -> Vec<WalSegmentIndex> {
        self.segments
            .iter()
            .map(|segment| WalSegmentIndex {
                segment_id: segment.segment_id,
                first_log_index: segment.first_index,
                last_log_index: segment.last_index,
                record_count: segment.record_count,
                sealed: segment.sealed,
                bytes: 0,
            })
            .collect()
    }

    pub fn recover(&mut self) -> Result<WalRecoveryReport, RaftError> {
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
        Ok(WalRecoveryReport {
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

    fn rebuild_from_records(&mut self, records: Vec<WalRecord>) -> Result<(), RaftError> {
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
    segments: Vec<WalSegment>,
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
    /// Every fsync, not only the slow ones. An append is dominated by its
    /// fsync, so this is what says whether a batch actually amortised one.
    fsync_count: u64,
    /// What the active segment's records already describe, as
    /// (first index, last index, term at the last index). `None` means the
    /// segment is empty, so the next record has to be stored whole.
    active_covered: Option<(LogIndex, LogIndex, Term)>,
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
            segments.push(WalSegment {
                segment_id: 0,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                record_count: 0,
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
            fsync_count: 0,
            active_covered: None,
        };
        wal.active_covered = wal.covered_from_segments();
        // Coverage has been folded, so the sealed segments have served their
        // purpose in memory.
        wal.release_sealed_segment_records();
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
    fn delta_record(&self, record: &WalRecord) -> WalRecord {
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

    /// Rebuilds what the retained records already cover, by folding what is
    /// already on disk, so an appended-to WAL keeps writing deltas after a
    /// reopen.
    ///
    /// This folds every segment rather than only the active one: a segment's
    /// first record is now a delta against the segment before it, so the active
    /// segment on its own no longer describes the whole log.
    /// Reads back a segment's records.
    ///
    /// A sealed segment releases its records once they are on disk, so anything
    /// that needs them -- compaction, or a caller asking for the whole log --
    /// reads them from the file that already holds them.
    fn segment_records(&self, segment: &WalSegment) -> Result<Vec<WalRecord>, RaftError> {
        if segment.record_count == 0 || !segment.records.is_empty() {
            return Ok(segment.records.clone());
        }
        let (records, _) =
            read_wal_segment_file(&wal_segment_path(&self.options.dir, segment.segment_id))?;
        Ok(records)
    }

    /// Drops the in-memory records of every sealed segment.
    ///
    /// A segment is written before it is sealed, so this loses nothing. Holding
    /// them was what made resident memory grow with the whole log rather than
    /// with the segment currently being written.
    fn release_sealed_segment_records(&mut self) {
        for segment in self.segments.iter_mut() {
            if segment.sealed && !segment.records.is_empty() {
                segment.records = Vec::new();
            }
        }
    }

    /// How many records are retained, without materialising any of them.
    fn retained_record_count(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.record_count)
            .sum()
    }

    fn covered_from_segments(&self) -> Option<(LogIndex, LogIndex, Term)> {
        let mut entries: Vec<LogEntry> = Vec::new();
        for segment in &self.segments {
            let records = self.segment_records(segment).ok()?;
            entries = matrixraft_fold_wal_entries_from(entries, records.iter());
        }
        let first = entries.first()?;
        let last = entries.last()?;
        Some((first.log_id.index, last.log_id.index, last.log_id.term))
    }

    /// Fsyncs the active segment and records what it cost.
    fn fsync_active_segment(&mut self) -> Result<(u64, bool), RaftError> {
        let started = Instant::now();
        if let Some(delay_ms) = self.inject_next_fsync_delay_ms.take() {
            thread::sleep(Duration::from_millis(delay_ms));
        }
        self.active_segment
            .sync_data()
            .map_err(|err| RaftError::Storage(format!("failed to fsync WAL record: {err}")))?;
        self.fsync_count += 1;
        let elapsed_ms: u64 = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        self.max_fsync_elapsed_ms = self.max_fsync_elapsed_ms.max(elapsed_ms);
        let slow = self.slow_fsync_threshold_ms > 0 && elapsed_ms >= self.slow_fsync_threshold_ms;
        if slow {
            self.slow_fsync_count += 1;
            self.consecutive_slow_fsync_count += 1;
        } else {
            self.consecutive_slow_fsync_count = 0;
        }
        Ok((elapsed_ms, slow))
    }

    /// How many times this WAL has fsynced.
    pub fn fsync_count(&self) -> u64 {
        self.fsync_count
    }

    /// Appends several records and fsyncs once for all of them.
    ///
    /// A durable append is its fsync and very little else: 224 appends per
    /// second with `fsync_on_append`, against 85,014 with it off. Appending one
    /// record at a time therefore caps a group at a few hundred entries per
    /// second however fast the rest of it is.
    ///
    /// Every record is durable when this returns and none before, which is the
    /// trade group commit makes. A caller must not acknowledge any record in
    /// the batch until this returns, exactly as it must not acknowledge a
    /// single append before `append` returns.
    ///
    /// A torn tail is no more dangerous than it already was: records are
    /// line-delimited and recovery stops at the first record whose checksum
    /// does not validate, so a batch interrupted by a crash truncates at
    /// whatever was written whole.
    pub fn append_batch(
        &mut self,
        records: Vec<WalRecord>,
    ) -> Result<Vec<WalWriteReport>, RaftError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mut reports = Vec::with_capacity(records.len());
        for record in records {
            let active_records = self
                .segments
                .last()
                .map(|segment| segment.records.len())
                .unwrap_or_default();
            let rolling_by_count = active_records >= self.options.max_records_per_segment;
            let stored = self.delta_record(&record);
            // Deferred deliberately: one fsync covers the whole batch, below.
            reports.push(self.write_record(stored, active_records, rolling_by_count, false)?);
        }
        if self.options.fsync_on_append {
            let (elapsed_ms, slow) = self.fsync_active_segment()?;
            if let Some(last) = reports.last_mut() {
                last.fsync_on_append = true;
                last.fsync_elapsed_ms = elapsed_ms;
                last.slow_fsync_observed = slow;
            }
        }
        Ok(reports)
    }

    pub fn set_slow_fsync_threshold_ms(&mut self, threshold_ms: u64) {
        self.slow_fsync_threshold_ms = threshold_ms;
    }

    pub fn inject_next_fsync_delay_for_test(&mut self, delay_ms: u64) {
        self.inject_next_fsync_delay_ms = Some(delay_ms);
    }

    /// What the active segment already describes, for a caller that wants to
    /// build the delta itself rather than hand over the whole log.
    pub fn active_coverage(&self) -> Option<(LogIndex, LogIndex, Term)> {
        self.active_covered
    }

    /// Appends a record the caller builds on demand.
    ///
    /// `build` is handed the coverage the record should be written against, or
    /// `None` when a whole-log record is required -- at the very start of the
    /// log, before anything is covered. A roll no longer forces one: the new
    /// segment opens with the delta, and compaction materialises the whole
    /// record at the boundary it actually moves. Callers that can produce a
    /// delta cheaply should use this instead of [`Self::append`], which has to
    /// be given the whole log every time.
    pub fn append_built<F>(&mut self, mut build: F) -> Result<WalWriteReport, RaftError>
    where
        F: FnMut(
            Option<(LogIndex, LogIndex, Term)>,
        ) -> Result<WalRecord, RaftError>,
    {
        let active_records = self
            .segments
            .last()
            .map(|segment| segment.records.len())
            .unwrap_or_default();
        let rolling_by_count = active_records >= self.options.max_records_per_segment;
        let record = build(self.active_covered)?;
        self.write_record(
            record,
            active_records,
            rolling_by_count,
            self.options.fsync_on_append,
        )
    }

    pub fn append(&mut self, record: WalRecord) -> Result<String, RaftError> {
        Ok(self.append_with_report(record)?.checksum)
    }

    pub fn append_with_report(
        &mut self,
        record: WalRecord,
    ) -> Result<WalWriteReport, RaftError> {
        let active_records = self
            .segments
            .last()
            .map(|segment| segment.records.len())
            .unwrap_or_default();
        let rolling_by_count = active_records >= self.options.max_records_per_segment;
        let stored = self.delta_record(&record);
        self.write_record(
            stored,
            active_records,
            rolling_by_count,
            self.options.fsync_on_append,
        )
    }

    /// Writes a record that has already been reduced to what will be stored.
    ///
    /// Rolling a segment no longer rewrites the record whole. A segment used to
    /// open with the entire retained log so that it could be read without
    /// reading any other, which cost a copy of the log at every roll and made N
    /// appends cost about N^2/2S entries. Compaction materialises that record
    /// instead, at the one moment the boundary it protects actually moves.
    fn write_record(
        &mut self,
        record: WalRecord,
        active_records: usize,
        rolling_by_count: bool,
        fsync: bool,
    ) -> Result<WalWriteReport, RaftError> {
        let hard_state_persisted = matrixraft_validate_hard_state_persistence(&record).is_ok();
        let active_len = self
            .active_segment
            .metadata()
            .map_err(|err| {
                RaftError::Storage(format!("failed to read WAL active segment metadata: {err}"))
            })?
            .len();
        let encoded = encode_wal_record(&record)?;
        let record_bytes = encoded.len() as u64 + 1;

        let mut segment_rolled = false;
        if rolling_by_count
            || (active_records > 0 && active_len + record_bytes > self.options.max_segment_bytes)
        {
            self.roll_segment()?;
            segment_rolled = true;
        }

        self.active_segment
            .write_all(encoded.as_bytes())
            .and_then(|_| self.active_segment.write_all(b"\n"))
            .map_err(|err| RaftError::Storage(format!("failed to append WAL record: {err}")))?;
        let mut fsync_elapsed_ms = 0;
        let mut slow_fsync_observed = false;
        if fsync {
            let observed = self.fsync_active_segment()?;
            fsync_elapsed_ms = observed.0;
            slow_fsync_observed = observed.1;
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
        segment.record_count = segment.records.len() as u64;
        let segment_id = segment.segment_id;
        self.advance_active_covered();
        Ok(WalWriteReport {
            segment_id,
            log_index: record_index,
            checksum,
            checksum_format: matrixraft_wal_checksum_format(),
            bytes_written: record_bytes,
            fsync_on_append: fsync,
            fsync_elapsed_ms,
            slow_fsync_threshold_ms: self.slow_fsync_threshold_ms,
            slow_fsync_observed,
            segment_rolled,
            hard_state_persisted,
            retained_range: wal_retained_range(&self.segments),
        })
    }

    pub fn recover(&mut self) -> Result<WalRecoveryReport, RaftError> {
        let (segments, truncated_corrupt_tail) = read_wal_segments_from_dir(&self.options.dir)?;
        // Counting used to clone every retained record, payloads included.
        let original_len = self.retained_record_count() as usize;
        let stored: Vec<_> = segments
            .iter()
            .flat_map(|segment| segment.records.iter().cloned())
            .collect();
        // One streaming pass rather than a whole-log record per stored record.
        let (surviving_records, recovered) = matrixraft_recover_from_wal_records(&stored);
        self.segments = if segments.is_empty() {
            vec![WalSegment {
                segment_id: 0,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                record_count: 0,
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
        self.active_covered = self.covered_from_segments();
        self.release_sealed_segment_records();
        let observed_corrupt_tail = self.truncated_corrupt_tail || truncated_corrupt_tail;
        self.truncated_corrupt_tail = observed_corrupt_tail;
        Ok(WalRecoveryReport {
            recovered,
            truncated_corrupt_tail: observed_corrupt_tail,
            surviving_records,
            removed_records: original_len.saturating_sub(surviving_records),
            segments_scanned: self.segments.len() as u64,
            checksum_format: Some(matrixraft_wal_checksum_format()),
            retained_range: Some(wal_retained_range(&self.segments)),
        })
    }

    pub fn compact_through(&mut self, log_index: LogIndex) -> Result<u64, RaftError> {
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
        // A surviving segment can now open with a delta against a segment that
        // is about to be deleted. Turn it back into a whole record, and write it
        // before anything is removed: a crash between the two leaves the base
        // still on disk, and recovery folds it exactly as it did before.
        if !removable_ids.is_empty() {
            self.materialize_first_surviving_record(&removable_ids)?;
        }
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

    /// Turns the first surviving record into a whole one when compaction is
    /// about to delete the records it is a delta against.
    ///
    /// This is the cost that used to be paid at every segment roll. It is paid
    /// here instead, once per compaction that actually removes something, which
    /// is the only point where a segment stops having its base on disk.
    fn materialize_first_surviving_record(
        &mut self,
        removable_ids: &[u64],
    ) -> Result<(), RaftError> {
        let Some(position) = self
            .segments
            .iter()
            .position(|segment| !removable_ids.contains(&segment.segment_id))
        else {
            return Ok(());
        };
        // The survivor is usually sealed, so its records are on disk rather
        // than in memory.
        let mut survivor = self.segment_records(&self.segments[position])?;
        match survivor.first() {
            Some(first) if first.entries_are_delta => {}
            _ => return Ok(()),
        }
        // Fold the segments that are about to be deleted one at a time, so
        // compaction never holds all of them at once.
        let mut entries: Vec<LogEntry> = Vec::new();
        for segment in &self.segments[..position] {
            let records = self.segment_records(segment)?;
            entries = matrixraft_fold_wal_entries_from(entries, records.iter());
        }
        let head = survivor[0].clone();
        entries = matrixraft_fold_wal_entries_from(entries, std::iter::once(&head));

        survivor[0].entries = entries;
        survivor[0].entries_are_delta = false;
        survivor[0].checksum = matrixraft_wal_checksum(&survivor[0]);

        let sealed = self.segments[position].sealed;
        let mut segment = self.segments[position].clone();
        segment.records = survivor;
        write_wal_segment_file(&self.options.dir, &segment)?;
        if !sealed {
            // The active segment keeps its records; a sealed one goes back to
            // holding none.
            self.segments[position].records = segment.records;
        }
        Ok(())
    }

    pub fn compact_through_with_fence(
        &mut self,
        log_index: LogIndex,
        fence: &StorageApplyFence,
    ) -> Result<WalCompactionReport, RaftError> {
        if let Err(error) = matrixraft_validate_storage_apply_fence(fence) {
            return Ok(WalCompactionReport {
                requested_log_index: log_index,
                released_segments: 0,
                retained_range: self.retained_log_range(),
                fence_valid: false,
                blocker: Some(error.to_string()),
            });
        }
        if fence.durable_applied_index < log_index || fence.storage_flushed_index < log_index {
            return Ok(WalCompactionReport {
                requested_log_index: log_index,
                released_segments: 0,
                retained_range: self.retained_log_range(),
                fence_valid: false,
                blocker: Some("compaction fence is behind requested log index".to_string()),
            });
        }
        let released_segments = self.compact_through(log_index)?;
        Ok(WalCompactionReport {
            requested_log_index: log_index,
            released_segments,
            retained_range: self.retained_log_range(),
            fence_valid: true,
            blocker: None,
        })
    }

    pub fn status(&self) -> WalLifecycleStatus {
        // `status` is polled routinely, and this used to clone the entire
        // WAL on each call just to read its length.
        let total_records = self.retained_record_count();
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
        WalLifecycleStatus {
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
            fsync_count: self.fsync_count,
            slow_fsync_threshold_ms: self.slow_fsync_threshold_ms,
            slow_fsync_count: self.slow_fsync_count,
            consecutive_slow_fsync_count: self.consecutive_slow_fsync_count,
            max_fsync_elapsed_ms: self.max_fsync_elapsed_ms,
            compacted_after_slow_fsync_count: self.compacted_after_slow_fsync_count,
        }
    }

    pub fn segments(&self) -> &[WalSegment] {
        &self.segments
    }

    pub fn segment_index(&self) -> Vec<WalSegmentIndex> {
        wal_segment_index(&self.options.dir, &self.segments)
    }

    pub fn retained_log_range(&self) -> LogRetainedRange {
        wal_retained_range(&self.segments)
    }

    pub fn checksum_format(&self) -> WalChecksumFormat {
        matrixraft_wal_checksum_format()
    }

    /// The retained records, folded whole.
    ///
    /// Sealed segments are read back from disk, so this is no longer free.
    /// Callers that only want a count should use [`Self::status`], which no
    /// longer materialises anything. A segment that cannot be read yields an
    /// empty result rather than a partial one; [`Self::try_records`] returns
    /// the error instead.
    pub fn records(&self) -> Vec<WalRecord> {
        self.try_records().unwrap_or_default()
    }

    /// [`Self::records`], with the read failure surfaced.
    pub fn try_records(&self) -> Result<Vec<WalRecord>, RaftError> {
        let mut stored: Vec<WalRecord> = Vec::with_capacity(self.retained_record_count() as usize);
        for segment in &self.segments {
            stored.extend(self.segment_records(segment)?);
        }
        Ok(matrixraft_fold_wal_records(&stored))
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
            // The segment is on disk now, so nothing needs its records in
            // memory. This is what bounds resident memory by the segment being
            // written rather than by the whole log.
            segment.records = Vec::new();
        }
        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;
        let segment = WalSegment {
            segment_id,
            first_index: 0,
            last_index: 0,
            records: Vec::new(),
            record_count: 0,
            sealed: false,
        };
        write_wal_segment_file(&self.options.dir, &segment)?;
        self.active_segment = open_segment_for_append(&self.options.dir, segment_id)?;
        self.segments.push(segment);
        Ok(())
    }
}

pub type FileRaftWal = PersistentRaftWal;

fn whole_record(record: &WalRecord) -> WalRecord {
    let mut whole = record.clone();
    whole.entries_are_delta = false;
    whole.checksum = matrixraft_wal_checksum(&whole);
    whole
}

fn encode_wal_record(record: &WalRecord) -> Result<String, RaftError> {
    serde_json::to_string(record)
        .map_err(|err| RaftError::Storage(format!("failed to encode WAL record: {err}")))
}

fn wal_segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{segment_id:020}.wal"))
}

fn wal_record_index(record: &WalRecord) -> LogIndex {
    record
        .hard_state
        .committed
        .as_ref()
        .map(|log_id| log_id.index)
        .or_else(|| record.entries.last().map(|entry| entry.log_id.index))
        .unwrap_or_default()
}

fn wal_retained_range(segments: &[WalSegment]) -> LogRetainedRange {
    LogRetainedRange {
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
        record_count: segments.iter().map(|segment| segment.record_count).sum(),
    }
}

fn wal_segment_index(dir: &Path, segments: &[WalSegment]) -> Vec<WalSegmentIndex> {
    segments
        .iter()
        .map(|segment| WalSegmentIndex {
            segment_id: segment.segment_id,
            first_log_index: segment.first_index,
            last_log_index: segment.last_index,
            record_count: segment.record_count,
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

fn write_wal_segment_file(dir: &Path, segment: &WalSegment) -> Result<(), RaftError> {
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

fn read_wal_segments_from_dir(dir: &Path) -> Result<(Vec<WalSegment>, bool), RaftError> {
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
        segments.push(WalSegment {
            segment_id,
            first_index,
            last_index,
            record_count: records.len() as u64,
            records,
            sealed: file_position != last_file_index,
        });
    }
    Ok((segments, truncated_corrupt_tail))
}

fn read_wal_segment_file(path: &Path) -> Result<(Vec<WalRecord>, bool), RaftError> {
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
        let Ok(record) = serde_json::from_slice::<WalRecord>(&line) else {
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
