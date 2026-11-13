// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

/// The pinned entry wire shape with borrowed byte fields. Parsing a skipped
/// entry must still validate all its fields without allocating its payload.
#[derive(Clone, PartialEq, prost::Message)]
struct EntryView {
    #[prost(enumeration = "EntryType", tag = "1")]
    entry_type: i32,
    #[prost(uint64, tag = "2")]
    term: u64,
    #[prost(uint64, tag = "3")]
    index: u64,
    #[prost(bytes = "bytes", tag = "4")]
    data: prost::bytes::Bytes,
    #[prost(bool, tag = "5")]
    sync_log: bool,
    #[prost(bytes = "bytes", tag = "6")]
    context: prost::bytes::Bytes,
}
impl EntryView {
    fn into_owned(self) -> Entry {
        Entry {
            entry_type: self.entry_type,
            term: self.term,
            index: self.index,
            data: self.data.to_vec(),
            context: self.context.to_vec(),
            sync_log: self.sync_log,
        }
    }
}

/// A read-only journal generation and its durable log bounds.
/// Appends are excluded by the captured length; checkpoint replacement leaves
/// this descriptor attached to the previous generation.
pub struct PeerLogReader {
    pub(super) file: File,
    pub(super) length: u64,
    pub(super) first: u64,
    pub(super) last: u64,
    pub(super) group: u64,
    pub(super) node: u64,
    pub(super) voters: Vec<u64>,
    pub(super) permitted: Vec<u64>,
    /// Shared by every reader of this journal generation; see [`LogIndex`].
    pub(super) index: Arc<Mutex<LogIndex>>,
}

/// Most entry-carrying records the index will hold. Past it readers scan instead.
const INDEX_RECORD_CAP: usize = 4_000_000;

/// Entries `start..=end` are the final version of those indexes, as written by the record at
/// `offset`. Segments are kept disjoint and ascending.
#[derive(Clone, Copy)]
struct Segment {
    start: u64,
    end: u64,
    offset: u64,
}

/// One journal record that carries entries: `start..=end` as written at `offset`, ending at
/// `end_offset`. A later record can overwrite a suffix, which is why the owner of an index is the
/// last record that wrote it, not the first.
#[derive(Clone, Copy)]
struct RecordMeta {
    start: u64,
    end: u64,
    offset: u64,
    end_offset: u64,
}

/// What a journal generation holds, learned once and shared by every reader of it.
///
/// A cold read used to hash, decode and validate every record in the file however small the range,
/// so catching a lagging follower up in many reads cost the journal size once per read. This lets a
/// read go straight to the records that own its range. Every record is still validated in full the
/// first time it is indexed, which is the first read after it was written, and every record a read
/// touches is validated again as it is read. What a later read no longer re-verifies is a record it
/// does not touch.
#[derive(Default)]
pub(super) struct LogIndex {
    records: Vec<RecordMeta>,
    /// The owner of each entry index as of `scanned`.
    segments: Vec<Segment>,
    /// File offset through which every record has been validated and indexed.
    scanned: u64,
    /// Set when a record could not be indexed; readers then scan as before.
    unusable: bool,
}

impl LogIndex {
    /// A record starting at `start` replaces every entry at or above it.
    fn apply(segments: &mut Vec<Segment>, start: u64, end: u64, offset: u64) {
        while segments
            .last()
            .is_some_and(|segment| segment.start >= start)
        {
            segments.pop();
        }
        if let Some(last) = segments.last_mut() {
            if last.end >= start {
                last.end = start - 1;
            }
        }
        segments.push(Segment { start, end, offset });
    }

    /// The owners of each index as a reader that captured only `length` bytes saw them.
    fn segments_through(records: &[RecordMeta], length: u64) -> Vec<Segment> {
        let mut segments = Vec::new();
        for record in records
            .iter()
            .take_while(|record| record.end_offset <= length)
        {
            Self::apply(&mut segments, record.start, record.end, record.offset);
        }
        segments
    }
}

/// What a record must say about who wrote it.
struct Identity<'a> {
    group: u64,
    node: u64,
    voters: &'a [u64],
    permitted: &'a [u64],
}

impl PeerLogReader {
    /// Read [low, high) from this generation. A byte budget includes the
    /// crossing entry, guaranteeing progress even for a zero budget.
    pub fn entries(
        &mut self,
        low: u64,
        high: u64,
        max_bytes: Option<u64>,
    ) -> Result<Vec<Entry>, RaftError> {
        if low < self.first || low > high || high > self.last.saturating_add(1) {
            return Err(storage_error("peer log read outside captured bounds"));
        }
        if low == high {
            return Ok(Vec::new());
        }
        if let Some(entries) = self.indexed_entries(low, high, max_bytes)? {
            return Ok(entries);
        }
        self.scan_entries(low, high, max_bytes)
    }

    /// Validate every record in the captured journal, as every read used to.
    ///
    /// A read now goes straight to the records that hold its range, and a record is validated in
    /// full only when it is first indexed and whenever a read touches it. So damage to a record no
    /// read has touched since, such as bit rot in an old suffix, is not seen by reads. A host that
    /// wants to catch that calls this from time to time. Its cost grows with the captured journal.
    pub fn verify(&mut self) -> Result<(), RaftError> {
        let identity = Identity {
            group: self.group,
            node: self.node,
            voters: &self.voters,
            permitted: &self.permitted,
        };
        self.file.seek(SeekFrom::Start(0)).map_err(storage_error)?;
        let mut remaining = self.length;
        let mut input = (&mut self.file).take(remaining);
        let mut validation = Vec::new();
        let mut bytes = prost::bytes::BytesMut::new();
        while remaining > 0 {
            let (mut record, wire, _) = Self::read_record(
                &identity,
                &mut input,
                &mut remaining,
                &mut bytes,
                &mut validation,
            )?;
            for encoded in std::mem::take(&mut record.entries) {
                EntryView::decode(encoded).map_err(storage_error)?;
            }
            drop(record);
            bytes = wire.try_into_mut().unwrap_or_default();
        }
        Ok(())
    }

    /// Read [low, high) by validating every record in the captured journal in order. This is the
    /// original read, kept for when the index cannot serve one.
    fn scan_entries(
        &mut self,
        low: u64,
        high: u64,
        max_bytes: Option<u64>,
    ) -> Result<Vec<Entry>, RaftError> {
        self.file.seek(SeekFrom::Start(0)).map_err(storage_error)?;
        let mut input = (&mut self.file).take(self.length);
        let mut remaining = self.length;
        let mut selected: Vec<Entry> = Vec::new();
        let mut bytes_used = 0u64;
        let mut highest_selected = 0;
        let mut validation = Vec::new();
        let mut bytes = prost::bytes::BytesMut::new();
        while remaining > 0 {
            if remaining < 24 {
                return Err(storage_error("incomplete captured peer log header"));
            }
            let mut header = [0; 24];
            input.read_exact(&mut header).map_err(storage_error)?;
            if &header[..4] != MAGIC
                || digest(&header[..8]) != u64::from_be_bytes(header[8..16].try_into().unwrap())
            {
                return Err(storage_error("captured peer log header checksum mismatch"));
            }
            let size = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
            if size == 0 || size > LIMIT || size as u64 > remaining - 24 {
                return Err(storage_error("invalid captured peer log record size"));
            }
            bytes.resize(size, 0);
            input.read_exact(&mut bytes).map_err(storage_error)?;
            remaining -= 24 + size as u64;
            if digest(&bytes) != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
                return Err(storage_error("captured peer log payload checksum mismatch"));
            }
            let wire = std::mem::take(&mut bytes).freeze();
            let mut record = WalBatch::decode(wire.clone()).map_err(storage_error)?;
            if !matches!(
                (record.version, record.snapshot.is_empty()),
                (1, true) | (2, false) | (3, false) | (4, false)
            ) || (record.version == 4) == record.payload_manifest.is_empty()
                || record.group != self.group
                || record.node != self.node
                || record.voters != self.voters
                || record.permitted != self.permitted
                || !exact_batch_encoding(&record, &wire, &mut validation)
            {
                return Err(storage_error(
                    "captured peer log identity or encoding mismatch",
                ));
            }
            for (position, encoded) in std::mem::take(&mut record.entries).into_iter().enumerate() {
                let entry = EntryView::decode(encoded).map_err(storage_error)?;
                if position == 0 && !selected.is_empty() && highest_selected >= entry.index {
                    highest_selected = 0;
                    selected.retain(|old| {
                        if old.index >= entry.index {
                            bytes_used = bytes_used.saturating_sub(old.encoded_len() as u64);
                            false
                        } else {
                            highest_selected = highest_selected.max(old.index);
                            true
                        }
                    });
                }
                if entry.index < low || entry.index >= high {
                    continue;
                }
                if !selected.is_empty() && max_bytes.is_some_and(|limit| bytes_used >= limit) {
                    continue;
                }
                bytes_used = bytes_used.saturating_add(entry.encoded_len() as u64);
                highest_selected = highest_selected.max(entry.index);
                selected.push(entry.into_owned());
            }
            drop(record);
            bytes = wire.try_into_mut().unwrap_or_default();
        }
        if selected.first().map(|entry| entry.index) != Some(low)
            || selected
                .windows(2)
                .any(|pair| pair[0].index.checked_add(1) != Some(pair[1].index))
            || (!max_bytes.is_some_and(|limit| bytes_used >= limit)
                && selected.last().map(|entry| entry.index) != Some(high - 1))
        {
            return Err(storage_error("captured peer log range is incomplete"));
        }
        Ok(selected)
    }
}

impl PeerLogReader {
    /// Read and fully validate the next record. `remaining` counts the captured bytes not yet
    /// consumed. Returns the record, its wire bytes (so the caller can recycle the buffer) and its
    /// body size.
    fn read_record(
        identity: &Identity<'_>,
        input: &mut impl Read,
        remaining: &mut u64,
        bytes: &mut prost::bytes::BytesMut,
        validation: &mut Vec<u8>,
    ) -> Result<(WalBatch, prost::bytes::Bytes, usize), RaftError> {
        if *remaining < 24 {
            return Err(storage_error("incomplete captured peer log header"));
        }
        let mut header = [0; 24];
        input.read_exact(&mut header).map_err(storage_error)?;
        if &header[..4] != MAGIC
            || digest(&header[..8]) != u64::from_be_bytes(header[8..16].try_into().unwrap())
        {
            return Err(storage_error("captured peer log header checksum mismatch"));
        }
        let size = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
        if size == 0 || size > LIMIT || size as u64 > *remaining - 24 {
            return Err(storage_error("invalid captured peer log record size"));
        }
        bytes.resize(size, 0);
        input.read_exact(bytes).map_err(storage_error)?;
        *remaining -= 24 + size as u64;
        if digest(bytes) != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
            return Err(storage_error("captured peer log payload checksum mismatch"));
        }
        let wire = std::mem::take(bytes).freeze();
        let record = WalBatch::decode(wire.clone()).map_err(storage_error)?;
        if !matches!(
            (record.version, record.snapshot.is_empty()),
            (1, true) | (2, false) | (3, false) | (4, false)
        ) || (record.version == 4) == record.payload_manifest.is_empty()
            || record.group != identity.group
            || record.node != identity.node
            || record.voters != identity.voters
            || record.permitted != identity.permitted
            || !exact_batch_encoding(&record, &wire, validation)
        {
            return Err(storage_error(
                "captured peer log identity or encoding mismatch",
            ));
        }
        Ok((record, wire, size))
    }

    /// Validate and index every record this reader can see that the index has not yet covered.
    /// `Ok(false)` means the journal cannot be indexed and the caller must scan.
    fn extend_index(&mut self, index: &mut LogIndex) -> Result<bool, RaftError> {
        let identity = Identity {
            group: self.group,
            node: self.node,
            voters: &self.voters,
            permitted: &self.permitted,
        };
        self.file
            .seek(SeekFrom::Start(index.scanned))
            .map_err(storage_error)?;
        let mut remaining = self.length - index.scanned;
        let mut input = (&mut self.file).take(remaining);
        let mut offset = index.scanned;
        let mut added = Vec::new();
        let mut validation = Vec::new();
        let mut bytes = prost::bytes::BytesMut::new();
        while remaining > 0 {
            let (mut record, wire, size) = Self::read_record(
                &identity,
                &mut input,
                &mut remaining,
                &mut bytes,
                &mut validation,
            )?;
            let end_offset = offset + 24 + size as u64;
            let mut first = None;
            let mut count = 0u64;
            for encoded in std::mem::take(&mut record.entries) {
                let entry = EntryView::decode(encoded).map_err(storage_error)?;
                match first {
                    None => first = Some(entry.index),
                    Some(start) if entry.index == start + count => {}
                    Some(_) => {
                        index.unusable = true;
                        return Ok(false);
                    }
                }
                count += 1;
            }
            if let Some(start) = first {
                added.push(RecordMeta {
                    start,
                    end: start + count - 1,
                    offset,
                    end_offset,
                });
            }
            drop(record);
            bytes = wire.try_into_mut().unwrap_or_default();
            offset = end_offset;
        }
        if index.records.len() + added.len() > INDEX_RECORD_CAP {
            index.unusable = true;
            return Ok(false);
        }
        for record in &added {
            LogIndex::apply(&mut index.segments, record.start, record.end, record.offset);
        }
        index.records.extend(added);
        index.scanned = self.length;
        Ok(true)
    }

    /// Read [low, high) by going to the records that own it. `Ok(None)` when the index cannot
    /// serve this journal.
    fn indexed_entries(
        &mut self,
        low: u64,
        high: u64,
        max_bytes: Option<u64>,
    ) -> Result<Option<Vec<Entry>>, RaftError> {
        let shared = Arc::clone(&self.index);
        let plan: Vec<Segment> = {
            let mut index = shared.lock().unwrap();
            if index.unusable {
                return Ok(None);
            }
            if index.scanned < self.length && !self.extend_index(&mut index)? {
                return Ok(None);
            }
            // A reader that captured less than the index has since seen has its own view.
            let rebuilt;
            let segments: &[Segment] = if index.scanned == self.length {
                &index.segments
            } else {
                rebuilt = LogIndex::segments_through(&index.records, self.length);
                &rebuilt
            };
            let from = segments.partition_point(|segment| segment.end < low);
            let to = segments.partition_point(|segment| segment.start < high);
            segments[from..to].to_vec()
        };

        let identity = Identity {
            group: self.group,
            node: self.node,
            voters: &self.voters,
            permitted: &self.permitted,
        };
        let mut selected: Vec<Entry> = Vec::new();
        let mut bytes_used = 0u64;
        let mut validation = Vec::new();
        let mut bytes = prost::bytes::BytesMut::new();
        'segments: for segment in plan {
            self.file
                .seek(SeekFrom::Start(segment.offset))
                .map_err(storage_error)?;
            let mut remaining = self.length - segment.offset;
            let mut input = (&mut self.file).take(remaining);
            let (mut record, wire, _) = Self::read_record(
                &identity,
                &mut input,
                &mut remaining,
                &mut bytes,
                &mut validation,
            )?;
            for encoded in std::mem::take(&mut record.entries) {
                let entry = EntryView::decode(encoded).map_err(storage_error)?;
                if entry.index < segment.start
                    || entry.index > segment.end
                    || entry.index < low
                    || entry.index >= high
                {
                    continue;
                }
                if !selected.is_empty() && max_bytes.is_some_and(|limit| bytes_used >= limit) {
                    break 'segments;
                }
                bytes_used = bytes_used.saturating_add(entry.encoded_len() as u64);
                selected.push(entry.into_owned());
            }
            drop(record);
            bytes = wire.try_into_mut().unwrap_or_default();
        }
        if selected.first().map(|entry| entry.index) != Some(low)
            || selected
                .windows(2)
                .any(|pair| pair[0].index.checked_add(1) != Some(pair[1].index))
            || (!max_bytes.is_some_and(|limit| bytes_used >= limit)
                && selected.last().map(|entry| entry.index) != Some(high - 1))
        {
            return Err(storage_error("captured peer log range is incomplete"));
        }
        Ok(Some(selected))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(entries: &[Entry]) -> WalBatch {
        WalBatch {
            version: 1,
            group: 17,
            node: 1,
            voters: vec![1],
            entries: entries
                .iter()
                .map(|entry| entry.encode_to_vec().into())
                .collect(),
            ..Default::default()
        }
    }
    fn fixture(records: &[WalBatch], last: u64) -> (tempfile::TempDir, PeerLogReader) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("log");
        let mut file = File::create(&path).unwrap();
        for record in records {
            let bytes = record.encode_to_vec();
            let mut header = [0; 24];
            header[..4].copy_from_slice(MAGIC);
            header[4..8].copy_from_slice(&(bytes.len() as u32).to_be_bytes());
            let checksum = digest(&header[..8]);
            header[8..16].copy_from_slice(&checksum.to_be_bytes());
            header[16..].copy_from_slice(&digest(&bytes).to_be_bytes());
            file.write_all(&header).unwrap();
            file.write_all(&bytes).unwrap();
        }
        let length = file.metadata().unwrap().len();
        let reader = PeerLogReader {
            file: File::open(path).unwrap(),
            length,
            first: 1,
            last,
            group: 17,
            node: 1,
            voters: vec![1],
            permitted: Vec::new(),
            index: Default::default(),
        };
        (root, reader)
    }

    fn entries_of(low: u64, high: u64, term: u64) -> Vec<Entry> {
        (low..=high)
            .map(|index| Entry {
                index,
                term,
                entry_type: EntryType::EntryNormal as i32,
                data: vec![index as u8; index as usize * 17],
                context: vec![term as u8; index as usize % 7],
                sync_log: index % 2 == 0,
            })
            .collect()
    }

    /// The index must actually serve reads, not quietly hand them to the scan, and it must agree
    /// with the scan for every range and byte budget over a journal with overwritten suffixes.
    #[test]
    fn the_index_serves_reads_and_agrees_with_a_full_scan() {
        let batches = [
            entries_of(1, 8, 1),
            entries_of(4, 6, 2),
            entries_of(7, 10, 2),
            entries_of(9, 12, 3),
            Vec::new(),
        ];
        let records: Vec<_> = batches.iter().map(|entries| record(entries)).collect();
        let (_root, mut reader) = fixture(&records, 12);
        for low in 1..=12 {
            for high in low + 1..=13 {
                for budget in [None, Some(0), Some(1), Some(64), Some(400), Some(4096)] {
                    let by_index = reader.entries(low, high, budget).unwrap();
                    let by_scan = reader.scan_entries(low, high, budget).unwrap();
                    assert_eq!(by_index, by_scan, "range {low}..{high} budget {budget:?}");
                }
            }
        }
        let index = reader.index.lock().unwrap();
        assert!(!index.unusable, "this journal must be indexable");
        assert_eq!(index.scanned, reader.length, "the whole journal is indexed");
        assert_eq!(
            index.records.len(),
            4,
            "one index record per record that carries entries"
        );
    }

    /// A record that starts below several earlier ones replaces all of them, not just the last.
    #[test]
    fn a_record_that_starts_below_earlier_ones_replaces_all_of_them() {
        let batches = [
            entries_of(1, 8, 1),
            entries_of(4, 6, 2),
            entries_of(7, 10, 2),
            entries_of(2, 5, 4),
        ];
        let records: Vec<_> = batches.iter().map(|entries| record(entries)).collect();
        let (_root, mut reader) = fixture(&records, 5);
        for low in 1..=5 {
            for high in low + 1..=6 {
                for budget in [None, Some(0), Some(64), Some(4096)] {
                    let by_index = reader.entries(low, high, budget).unwrap();
                    let by_scan = reader.scan_entries(low, high, budget).unwrap();
                    assert_eq!(by_index, by_scan, "range {low}..{high} budget {budget:?}");
                }
            }
        }
        let view: Vec<(u64, u64)> = reader
            .entries(1, 6, None)
            .unwrap()
            .iter()
            .map(|entry| (entry.index, entry.term))
            .collect();
        assert_eq!(view, [(1, 1), (2, 4), (3, 4), (4, 4), (5, 4)]);
    }

    /// Readers share one index, but each sees only what it captured: a reader that captured less
    /// than the index has since learned must not see the records that came after.
    #[test]
    fn a_reader_that_captured_less_than_the_index_knows_sees_its_own_view() {
        let batches = [
            entries_of(1, 8, 1),
            entries_of(4, 6, 2),
            entries_of(5, 9, 3),
        ];
        let records: Vec<_> = batches.iter().map(|entries| record(entries)).collect();
        let prefix: u64 = records[..2]
            .iter()
            .map(|record| 24 + record.encode_to_vec().len() as u64)
            .sum();
        let terms = |entries: &[Entry]| {
            entries
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>()
        };
        let longer = [
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 2),
            (5, 3),
            (6, 3),
            (7, 3),
            (8, 3),
            (9, 3),
        ];
        // Both orders: the longer reader indexes first, and the shorter reader indexes first.
        for longer_first in [true, false] {
            let (root, mut long) = fixture(&records, 9);
            let mut short = PeerLogReader {
                file: File::open(root.path().join("log")).unwrap(),
                length: prefix,
                first: 1,
                last: 6,
                group: 17,
                node: 1,
                voters: vec![1],
                permitted: Vec::new(),
                index: Arc::clone(&long.index),
            };
            if longer_first {
                assert_eq!(terms(&long.entries(1, 10, None).unwrap()), longer);
                assert_eq!(
                    terms(&short.entries(1, 7, None).unwrap()),
                    [(1, 1), (2, 1), (3, 1), (4, 2), (5, 2), (6, 2)]
                );
            } else {
                assert_eq!(
                    terms(&short.entries(1, 7, None).unwrap()),
                    [(1, 1), (2, 1), (3, 1), (4, 2), (5, 2), (6, 2)]
                );
                assert_eq!(terms(&long.entries(1, 10, None).unwrap()), longer);
            }
            assert!(
                short.entries(7, 9, None).is_err(),
                "the shorter reader never captured entries 7 to 10"
            );
        }
    }

    /// What the index trades: a journal is validated in full the first time its records are
    /// indexed, and a later read verifies the records it touches, not the ones it does not.
    #[test]
    fn a_later_read_verifies_only_the_records_it_touches() {
        let records = [
            entries_of(1, 4, 1),
            entries_of(5, 8, 1),
            entries_of(9, 12, 1),
        ]
        .map(|entries| record(&entries));
        let (root, mut reader) = fixture(&records, 12);
        assert_eq!(reader.entries(1, 3, None).unwrap().len(), 2);
        reader.verify().unwrap();

        // Damage the last record's body on disk after the journal was indexed.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.path().join("log"))
            .unwrap();
        file.seek(SeekFrom::Start(reader.length - 1)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(reader.length - 1)).unwrap();
        file.write_all(&[byte[0] ^ 0xFF]).unwrap();
        file.sync_all().unwrap();

        assert_eq!(
            reader.entries(1, 3, None).unwrap().len(),
            2,
            "a range that does not touch the damaged record is still served"
        );
        assert!(
            reader.entries(10, 12, None).is_err(),
            "a range inside the damaged record is refused, because the record is verified as read"
        );
        assert!(
            reader.scan_entries(1, 3, None).is_err(),
            "the full scan still finds it from anywhere"
        );
        assert!(
            reader.verify().is_err(),
            "so does an explicit verification of the captured journal"
        );
    }

    #[test]
    fn entry_views_borrow_and_match_every_pinned_entry_field() {
        for kind in [-1, 0, 1, 2, i32::MAX] {
            for position in [0, 1, 127, 128, u64::MAX] {
                for size in [0, 1, 127, 128, 4096] {
                    for sync_log in [false, true] {
                        let entry = Entry {
                            entry_type: kind,
                            term: position,
                            index: position,
                            data: vec![37; size],
                            context: vec![19; size / 2],
                            sync_log,
                        };
                        let wire: prost::bytes::Bytes = entry.encode_to_vec().into();
                        let view = EntryView::decode(wire.clone()).unwrap();
                        assert_eq!(view.encoded_len(), entry.encoded_len());
                        assert_eq!(view.encode_to_vec(), wire.as_ref());
                        for field in [&view.data, &view.context] {
                            if !field.is_empty() {
                                let start = wire.as_ptr() as usize;
                                let pointer = field.as_ptr() as usize;
                                assert!(
                                    pointer >= start && pointer + field.len() <= start + wire.len()
                                );
                            }
                        }
                        assert_eq!(view.into_owned(), entry);
                    }
                }
            }
        }
        for bytes in [
            vec![0x0a, 0],
            vec![0x12, 0],
            vec![0x1a, 0],
            vec![0x20, 1],
            vec![0x2a, 0],
            vec![0x30, 1],
            vec![0x22, 0x80],
            vec![0x80],
            vec![0x18, 7, 0x9a, 6, 1, 9],
        ] {
            let wire: prost::bytes::Bytes = bytes.into();
            let owned = Entry::decode(wire.clone());
            let borrowed = EntryView::decode(wire).map(EntryView::into_owned);
            assert_eq!(owned.is_ok(), borrowed.is_ok());
            if let (Ok(owned), Ok(borrowed)) = (owned, borrowed) {
                assert_eq!(owned, borrowed);
            }
        }
    }

    #[test]
    fn bounded_selection_matches_replayed_overwrites_for_every_range_and_budget() {
        let make = |low, high, term| {
            (low..=high)
                .map(|index| Entry {
                    index,
                    term,
                    entry_type: EntryType::EntryNormal as i32,
                    data: vec![index as u8; index as usize * 17],
                    context: vec![term as u8; index as usize % 7],
                    sync_log: index % 2 == 0,
                })
                .collect::<Vec<_>>()
        };
        let batches = [
            make(1, 8, 1),
            make(4, 6, 2),
            make(7, 10, 2),
            make(9, 12, 3),
            Vec::new(),
        ];
        let mut replay: Vec<Entry> = Vec::new();
        for entries in &batches {
            if let Some(first) = entries.first() {
                replay.retain(|entry| entry.index < first.index);
            }
            replay.extend(entries.iter().cloned());
        }
        let records: Vec<_> = batches.iter().map(|entries| record(entries)).collect();
        let (_root, mut reader) = fixture(&records, 12);
        for low in 1..=12 {
            for high in low + 1..=13 {
                for budget in [None, Some(0), Some(1), Some(64), Some(400), Some(4096)] {
                    let mut expected = Vec::new();
                    let mut bytes = 0;
                    for entry in replay
                        .iter()
                        .filter(|entry| entry.index >= low && entry.index < high)
                    {
                        if !expected.is_empty() && budget.is_some_and(|limit| bytes >= limit) {
                            break;
                        }
                        bytes += entry.encoded_len() as u64;
                        expected.push(entry.clone());
                    }
                    assert_eq!(reader.entries(low, high, budget).unwrap(), expected);
                }
            }
        }
    }

    #[test]
    fn an_unselected_malformed_entry_is_still_rejected_after_the_budget_is_full() {
        let first = Entry {
            index: 1,
            term: 1,
            data: vec![1; 32],
            ..Default::default()
        };
        let mut second = record(&[]);
        second.entries.push(vec![0x12, 0, 0x18, 2].into());
        let (_root, mut reader) = fixture(&[record(&[first]), second], 2);
        assert!(reader.entries(1, 2, Some(1)).is_err());
    }

    #[test]
    fn corruption_outside_the_requested_range_is_still_rejected() {
        let entries: Vec<_> = (1..=2)
            .map(|index| Entry {
                index,
                term: 1,
                data: vec![index as u8; 32],
                ..Default::default()
            })
            .collect();
        let (root, mut reader) = fixture(&[record(&entries[..1]), record(&entries[1..])], 2);
        let mut file = OpenOptions::new()
            .write(true)
            .open(root.path().join("log"))
            .unwrap();
        file.seek(SeekFrom::Start(reader.length - 1)).unwrap();
        file.write_all(&[255]).unwrap();
        file.sync_all().unwrap();
        assert!(reader.entries(1, 2, Some(1)).is_err());
    }
}
