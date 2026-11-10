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
        };
        (root, reader)
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
