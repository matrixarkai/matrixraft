// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style incremental checksums for WAL, snapshot, and transport IO.

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumType {
    Invalid,
    Crc32,
    Murmur32,
}

impl ChecksumType {
    pub fn from_name(name: &str) -> Self {
        match name {
            "crc32" | "crc32c" => Self::Crc32,
            "murmur32" | "murmurhash32" => Self::Murmur32,
            _ => Self::Invalid,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Crc32 => "crc32",
            Self::Murmur32 => "murmur32",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChecksumResult {
    pub checksum_type: ChecksumType,
    pub checksum_name: String,
    pub value: u32,
    pub bytes: u64,
    pub chunks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChecksumContext {
    checksum_type: ChecksumType,
    finished: bool,
    value: u32,
    length: u64,
    body_bytes: u64,
    tail: Vec<u8>,
    chunks: u64,
}

impl ChecksumContext {
    pub fn new(checksum_type: ChecksumType) -> Self {
        Self {
            checksum_type,
            finished: false,
            value: 0,
            length: 0,
            body_bytes: 0,
            tail: Vec::new(),
            chunks: 0,
        }
    }

    pub fn from_name(name: &str) -> Self {
        Self::new(ChecksumType::from_name(name))
    }

    pub fn extend(&mut self, data: &[u8]) -> Result<(), String> {
        if self.finished {
            return Err("checksum already finished".to_string());
        }
        if self.checksum_type == ChecksumType::Invalid {
            return Err("checksum type is invalid".to_string());
        }
        if data.is_empty() {
            return Ok(());
        }
        match self.checksum_type {
            ChecksumType::Crc32 => {
                self.value = crc32c_extend(self.value, data);
            }
            ChecksumType::Murmur32 => {
                self.extend_murmur32(data);
            }
            ChecksumType::Invalid => unreachable!(),
        }
        self.length = self.length.saturating_add(data.len() as u64);
        self.chunks = self.chunks.saturating_add(1);
        Ok(())
    }

    pub fn finalize(&mut self) -> ChecksumResult {
        if !self.finished && self.checksum_type == ChecksumType::Murmur32 {
            self.value = murmur3_x86_32_finalize(
                self.value,
                &self.tail,
                self.body_bytes.saturating_add(self.tail.len() as u64),
            );
            self.tail.clear();
        }
        self.finished = true;
        ChecksumResult {
            checksum_type: self.checksum_type,
            checksum_name: self.checksum_type.name().to_string(),
            value: self.value,
            bytes: self.length,
            chunks: self.chunks,
        }
    }

    pub fn checksum_type(&self) -> ChecksumType {
        self.checksum_type
    }

    pub fn checksum_name(&self) -> &'static str {
        self.checksum_type.name()
    }

    fn extend_murmur32(&mut self, data: &[u8]) {
        let mut combined = Vec::with_capacity(self.tail.len() + data.len());
        combined.extend_from_slice(&self.tail);
        combined.extend_from_slice(data);

        let body_len = combined.len() / 4 * 4;
        // `as_chunks` over a length that is already a multiple of four yields
        // exactly the blocks `chunks_exact(4)` did, with an empty remainder, so
        // the digest is unchanged -- and it hands back `[u8; 4]` directly
        // instead of a slice to index four times.
        let (blocks, _) = combined[..body_len].as_chunks::<4>();
        for chunk in blocks {
            let block = u32::from_le_bytes(*chunk);
            self.value = murmur3_x86_32_mix_block(self.value, block);
        }
        self.body_bytes = self.body_bytes.saturating_add(body_len as u64);
        self.tail.clear();
        self.tail.extend_from_slice(&combined[body_len..]);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileChecksumResult {
    pub checksum: ChecksumResult,
    pub files: Vec<PathBuf>,
    pub block_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileChecksumContext {
    path: PathBuf,
    checksum_type: ChecksumType,
    block_size: usize,
}

impl FileChecksumContext {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_type(path, ChecksumType::Crc32, 10_485_764)
    }

    pub fn with_type(
        path: impl Into<PathBuf>,
        checksum_type: ChecksumType,
        block_size: usize,
    ) -> Self {
        Self {
            path: path.into(),
            checksum_type,
            block_size: block_size.max(1),
        }
    }

    pub fn start(&self) -> io::Result<FileChecksumResult> {
        let files = matrixraft_checksum_file_list(&self.path)?;
        let mut context = ChecksumContext::new(self.checksum_type);
        let mut buffer = vec![0; self.block_size];
        for file in &files {
            let mut input = fs::File::open(file)?;
            matrixraft_checksum_extend_from_reader(&mut input, &mut context, &mut buffer)?;
        }
        Ok(FileChecksumResult {
            checksum: context.finalize(),
            files,
            block_size: self.block_size,
        })
    }
}

/// Reads `input` to its end, feeding every block into `context`.
///
/// **A read interrupted by a signal is retried, not reported.** `Read::read`
/// surfaces `ErrorKind::Interrupted` to the caller rather than retrying --
/// unlike `write_all`, `sync_data` and `set_len`, which retry internally -- so a
/// single `read(..)?` here would abandon a whole file's checksum because a
/// signal arrived, and report a failure for something that had not failed.
///
/// Separate from [`FileChecksumContext::start`] so the retry can be exercised
/// with a reader that interrupts on demand, rather than by arranging a real
/// signal.
pub fn matrixraft_checksum_extend_from_reader<R: Read>(
    input: &mut R,
    context: &mut ChecksumContext,
    buffer: &mut [u8],
) -> io::Result<()> {
    loop {
        match input.read(buffer) {
            Ok(0) => return Ok(()),
            Ok(read) => context
                .extend(&buffer[..read])
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
}

pub fn matrixraft_crc32c(data: &[u8]) -> u32 {
    crc32c_extend(0, data)
}

pub fn matrixraft_murmur32(data: &[u8]) -> u32 {
    murmur3_x86_32(data, 0)
}

pub fn matrixraft_checksum_file_list(path: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if path.is_dir() {
        collect_files(path, &mut files)?;
        files.sort();
    } else {
        files.push(path.to_path_buf());
    }
    Ok(files)
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
}

/// Eight 256-entry tables, built at compile time.
///
/// `CRC32C_TABLES[0]` is the usual byte-at-a-time table. `CRC32C_TABLES[k]` is what a byte
/// becomes after `k` more zero bytes have been shifted through, which is what lets eight input
/// bytes be folded in with eight lookups and no dependency between them.
static CRC32C_TABLES: [[u32; 256]; 8] = crc32c_tables();

const fn crc32c_tables() -> [[u32; 256]; 8] {
    /// 0x1EDC_6F41 bit-reversed, the form a right-shifting implementation needs.
    const POLYNOMIAL: u32 = 0x82f6_3b78;
    let mut tables = [[0u32; 256]; 8];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 1 {
                (value >> 1) ^ POLYNOMIAL
            } else {
                value >> 1
            };
            bit += 1;
        }
        tables[0][index] = value;
        index += 1;
    }
    let mut index = 0;
    while index < 256 {
        let mut value = tables[0][index];
        let mut table = 1;
        while table < 8 {
            value = tables[0][(value & 0xff) as usize] ^ (value >> 8);
            tables[table][index] = value;
            table += 1;
        }
        index += 1;
    }
    tables
}

/// Continues a CRC-32C over more bytes: `crc32c_extend(crc32c_extend(0, a), b)` is the CRC of
/// `a` followed by `b`.
///
/// The crate forbids `unsafe`, which rules out the hardware instruction, so this folds eight
/// bytes per step through [`CRC32C_TABLES`]. The loop it replaced did eight shifts per byte.
/// The streaming checksum of a whole file and the frame check of the binary WAL format
/// (`wal_binary`) both come through here.
pub(crate) fn crc32c_extend(crc: u32, data: &[u8]) -> u32 {
    let tables = &CRC32C_TABLES;
    let mut value = !crc;
    let (blocks, tail) = data.as_chunks::<8>();
    for block in blocks {
        let low = u32::from_le_bytes([block[0], block[1], block[2], block[3]]) ^ value;
        let high = u32::from_le_bytes([block[4], block[5], block[6], block[7]]);
        value = tables[7][(low & 0xff) as usize]
            ^ tables[6][((low >> 8) & 0xff) as usize]
            ^ tables[5][((low >> 16) & 0xff) as usize]
            ^ tables[4][(low >> 24) as usize]
            ^ tables[3][(high & 0xff) as usize]
            ^ tables[2][((high >> 8) & 0xff) as usize]
            ^ tables[1][((high >> 16) & 0xff) as usize]
            ^ tables[0][(high >> 24) as usize];
    }
    for byte in tail {
        value = tables[0][((value ^ u32::from(*byte)) & 0xff) as usize] ^ (value >> 8);
    }
    !value
}

fn murmur3_x86_32(data: &[u8], seed: u32) -> u32 {
    let mut hash = seed;
    let body_len = data.len() / 4 * 4;
    let (blocks, _) = data[..body_len].as_chunks::<4>();
    for chunk in blocks {
        let block = u32::from_le_bytes(*chunk);
        hash = murmur3_x86_32_mix_block(hash, block);
    }
    murmur3_x86_32_finalize(hash, &data[body_len..], data.len() as u64)
}

fn murmur3_x86_32_mix_block(mut hash: u32, mut block: u32) -> u32 {
    block = block.wrapping_mul(0xcc9e_2d51);
    block = block.rotate_left(15);
    block = block.wrapping_mul(0x1b87_3593);

    hash ^= block;
    hash = hash.rotate_left(13);
    hash = hash.wrapping_mul(5).wrapping_add(0xe654_6b64);
    hash
}

fn murmur3_x86_32_finalize(mut hash: u32, tail: &[u8], len: u64) -> u32 {
    let mut k1 = 0u32;
    match tail.len() {
        3 => {
            k1 ^= (tail[2] as u32) << 16;
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
        }
        2 => {
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
        }
        1 => {
            k1 ^= tail[0] as u32;
        }
        0 => {}
        _ => unreachable!("murmur32 tail is always shorter than a block"),
    }
    if k1 != 0 {
        k1 = k1.wrapping_mul(0xcc9e_2d51);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(0x1b87_3593);
        hash ^= k1;
    }

    hash ^= len as u32;
    fmix32(hash)
}

fn fmix32(mut hash: u32) -> u32 {
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85eb_ca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2_ae35);
    hash ^= hash >> 16;
    hash
}

#[cfg(test)]
mod crc32c_tests {
    use super::*;

    /// The loop this module used to run, kept as an independent reference. It shares no code with
    /// the table-driven version, so agreement is evidence about the tables and not about a
    /// helper both of them call.
    fn bitwise(crc: u32, data: &[u8]) -> u32 {
        let mut value = !crc;
        for byte in data {
            value ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = 0u32.wrapping_sub(value & 1);
                value = (value >> 1) ^ (0x82f6_3b78 & mask);
            }
        }
        !value
    }

    /// Deterministic bytes that are not periodic in 8, so a table indexed wrongly at one
    /// position cannot hide behind repetition.
    fn bytes(length: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    /// The published check value of CRC-32C, and the one for the other common CRC-32. The second
    /// assertion is the guard: a polynomial that is wrong in a plausible way still hashes.
    #[test]
    fn the_published_check_value_holds() {
        assert_eq!(matrixraft_crc32c(b"123456789"), 0xE306_9283);
        assert_ne!(matrixraft_crc32c(b"123456789"), 0xCBF4_3926);
        assert_eq!(matrixraft_crc32c(b""), 0);
    }

    /// Every length from empty to well past three blocks, at every alignment of the tail, against
    /// the reference. Lengths 0..8 never enter the block loop and 8..16 enter it once, so both
    /// halves of the function are held on their own as well as together.
    #[test]
    fn it_agrees_with_the_bitwise_reference_at_every_length() {
        for length in 0..=300usize {
            let data = bytes(length, 0x9E37_79B9_7F4A_7C15 ^ length as u64);
            assert_eq!(
                crc32c_extend(0, &data),
                bitwise(0, &data),
                "disagreed at length {length}"
            );
        }
    }

    /// Splitting the input anywhere and continuing gives the same CRC as doing it in one go.
    /// This is what lets a record be checksummed piece by piece as it is written.
    #[test]
    fn extending_across_any_split_matches_one_pass() {
        let data = bytes(173, 0xDEAD_BEEF_CAFE_F00D);
        let whole = crc32c_extend(0, &data);
        for split in 0..=data.len() {
            let (head, tail) = data.split_at(split);
            assert_eq!(
                crc32c_extend(crc32c_extend(0, head), tail),
                whole,
                "split at {split}"
            );
        }
        // Three pieces, with the seams on and off an eight-byte boundary.
        for (a, b) in [(7, 20), (8, 16), (1, 2), (64, 65), (100, 172)] {
            let value = crc32c_extend(
                crc32c_extend(crc32c_extend(0, &data[..a]), &data[a..b]),
                &data[b..],
            );
            assert_eq!(value, whole, "seams at {a} and {b}");
        }
    }

    /// One flipped bit anywhere changes the CRC. A table with one wrong entry still passes the
    /// agreement test at most lengths, because a wrong entry is only read for particular byte
    /// values; walking every position and bit makes a bad entry hard to miss.
    #[test]
    fn a_single_flipped_bit_always_changes_the_crc() {
        let data = bytes(61, 0x0123_4567_89AB_CDEF);
        let clean = crc32c_extend(0, &data);
        for position in 0..data.len() {
            for bit in 0..8 {
                let mut damaged = data.clone();
                damaged[position] ^= 1 << bit;
                assert_ne!(
                    crc32c_extend(0, &damaged),
                    clean,
                    "byte {position} bit {bit}"
                );
            }
        }
    }

    /// Every one of the 2048 table entries is read by some input. All 256 byte values at all 8
    /// positions inside a block, each compared with the reference, so an entry that is wrong but
    /// never reached by the random-looking inputs above is still caught.
    #[test]
    fn every_table_entry_is_exercised() {
        for position in 0..8usize {
            for value in 0..=255u8 {
                let mut block = [0u8; 16];
                block[position] = value;
                assert_eq!(
                    crc32c_extend(0, &block),
                    bitwise(0, &block),
                    "byte value {value} at position {position}"
                );
            }
        }
    }

    /// Prints the rate. Not a pass/fail test, because a threshold on a shared box would only
    /// measure the box.
    #[test]
    #[ignore = "reports a rate; run with --ignored --nocapture"]
    fn throughput_against_the_bitwise_reference() {
        let data = bytes(8 * 1024 * 1024, 0x5151_5151_5151_5151);
        let time = |f: &dyn Fn(&[u8]) -> u32| {
            let start = std::time::Instant::now();
            let mut sink = 0u32;
            for _ in 0..4 {
                sink ^= f(std::hint::black_box(&data));
            }
            std::hint::black_box(sink);
            (4 * data.len()) as f64 / start.elapsed().as_secs_f64() / 1e6
        };
        let fast = time(&|d| crc32c_extend(0, d));
        let slow = time(&|d| bitwise(0, d));
        println!(
            "CRC32C table-driven {fast:.0} MB/s, bitwise {slow:.0} MB/s, {:.1}x",
            fast / slow
        );
    }
}
