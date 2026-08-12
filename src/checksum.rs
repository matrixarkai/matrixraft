// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style incremental checksums for WAL, snapshot, and transport IO.

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftChecksumType {
    Invalid,
    Crc32,
    Murmur32,
}

impl RustRaftChecksumType {
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
pub struct RustRaftChecksumResult {
    pub checksum_type: RustRaftChecksumType,
    pub checksum_name: String,
    pub value: u32,
    pub bytes: u64,
    pub chunks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftChecksumContext {
    checksum_type: RustRaftChecksumType,
    finished: bool,
    value: u32,
    length: u64,
    body_bytes: u64,
    tail: Vec<u8>,
    chunks: u64,
}

impl RustRaftChecksumContext {
    pub fn new(checksum_type: RustRaftChecksumType) -> Self {
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
        Self::new(RustRaftChecksumType::from_name(name))
    }

    pub fn extend(&mut self, data: &[u8]) -> Result<(), String> {
        if self.finished {
            return Err("checksum already finished".to_string());
        }
        if self.checksum_type == RustRaftChecksumType::Invalid {
            return Err("checksum type is invalid".to_string());
        }
        if data.is_empty() {
            return Ok(());
        }
        match self.checksum_type {
            RustRaftChecksumType::Crc32 => {
                self.value = crc32c_extend(self.value, data);
            }
            RustRaftChecksumType::Murmur32 => {
                self.extend_murmur32(data);
            }
            RustRaftChecksumType::Invalid => unreachable!(),
        }
        self.length = self.length.saturating_add(data.len() as u64);
        self.chunks = self.chunks.saturating_add(1);
        Ok(())
    }

    pub fn finalize(&mut self) -> RustRaftChecksumResult {
        if !self.finished && self.checksum_type == RustRaftChecksumType::Murmur32 {
            self.value = murmur3_x86_32_finalize(
                self.value,
                &self.tail,
                self.body_bytes.saturating_add(self.tail.len() as u64),
            );
            self.tail.clear();
        }
        self.finished = true;
        RustRaftChecksumResult {
            checksum_type: self.checksum_type,
            checksum_name: self.checksum_type.name().to_string(),
            value: self.value,
            bytes: self.length,
            chunks: self.chunks,
        }
    }

    pub fn checksum_type(&self) -> RustRaftChecksumType {
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
        for chunk in combined[..body_len].chunks_exact(4) {
            let block = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            self.value = murmur3_x86_32_mix_block(self.value, block);
        }
        self.body_bytes = self.body_bytes.saturating_add(body_len as u64);
        self.tail.clear();
        self.tail.extend_from_slice(&combined[body_len..]);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftFileChecksumResult {
    pub checksum: RustRaftChecksumResult,
    pub files: Vec<PathBuf>,
    pub block_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftFileChecksumContext {
    path: PathBuf,
    checksum_type: RustRaftChecksumType,
    block_size: usize,
}

impl RustRaftFileChecksumContext {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_type(path, RustRaftChecksumType::Crc32, 10_485_764)
    }

    pub fn with_type(
        path: impl Into<PathBuf>,
        checksum_type: RustRaftChecksumType,
        block_size: usize,
    ) -> Self {
        Self {
            path: path.into(),
            checksum_type,
            block_size: block_size.max(1),
        }
    }

    pub fn start(&self) -> io::Result<RustRaftFileChecksumResult> {
        let files = rustraft_checksum_file_list(&self.path)?;
        let mut context = RustRaftChecksumContext::new(self.checksum_type);
        let mut buffer = vec![0; self.block_size];
        for file in &files {
            let mut input = fs::File::open(file)?;
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                context
                    .extend(&buffer[..read])
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
            }
        }
        Ok(RustRaftFileChecksumResult {
            checksum: context.finalize(),
            files,
            block_size: self.block_size,
        })
    }
}

pub fn rustraft_crc32c(data: &[u8]) -> u32 {
    crc32c_extend(0, data)
}

pub fn rustraft_murmur32(data: &[u8]) -> u32 {
    murmur3_x86_32(data, 0)
}

pub fn rustraft_checksum_file_list(path: &Path) -> io::Result<Vec<PathBuf>> {
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

fn crc32c_extend(crc: u32, data: &[u8]) -> u32 {
    let mut value = !crc;
    for byte in data {
        value ^= *byte as u32;
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(value & 1);
            value = (value >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !value
}

fn murmur3_x86_32(data: &[u8], seed: u32) -> u32 {
    let mut hash = seed;
    let body_len = data.len() / 4 * 4;
    for chunk in data[..body_len].chunks_exact(4) {
        let block = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
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
