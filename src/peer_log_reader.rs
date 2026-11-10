// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

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
        let mut validation = Vec::new();
        let mut bytes = Vec::new();
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
            let record = WalBatch::decode(bytes.as_slice()).map_err(storage_error)?;
            if !matches!(
                (record.version, record.snapshot.is_empty()),
                (1, true) | (2, false) | (3, false) | (4, false)
            ) || (record.version == 4) == record.payload_manifest.is_empty()
                || record.group != self.group
                || record.node != self.node
                || record.voters != self.voters
                || record.permitted != self.permitted
                || !exact_batch_encoding(&record, &bytes, &mut validation)
            {
                return Err(storage_error(
                    "captured peer log identity or encoding mismatch",
                ));
            }
            let mut bytes_used = selected.iter().fold(0u64, |sum, entry| {
                sum.saturating_add(entry.encoded_len() as u64)
            });
            for (position, encoded) in record.entries.into_iter().enumerate() {
                let entry = Entry::decode(encoded).map_err(storage_error)?;
                if position == 0 {
                    selected.retain(|old| old.index < entry.index);
                    bytes_used = selected.iter().fold(0u64, |sum, old| {
                        sum.saturating_add(old.encoded_len() as u64)
                    });
                }
                if entry.index < low || entry.index >= high {
                    continue;
                }
                if !selected.is_empty() && max_bytes.is_some_and(|limit| bytes_used >= limit) {
                    continue;
                }
                bytes_used = bytes_used.saturating_add(entry.encoded_len() as u64);
                selected.push(entry);
            }
        }
        let bytes_used = selected.iter().fold(0u64, |sum, entry| {
            sum.saturating_add(entry.encoded_len() as u64)
        });
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
