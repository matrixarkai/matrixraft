// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-compatible packed unique ID generator.

use std::sync::atomic::{AtomicU64, Ordering};

pub const RUSTRAFT_UNIQUE_ID_MEMBER_BITS: u32 = 16;
pub const RUSTRAFT_UNIQUE_ID_TIMESTAMP_BITS: u32 = 40;
pub const RUSTRAFT_UNIQUE_ID_COUNTER_BITS: u32 = 8;
pub const RUSTRAFT_UNIQUE_ID_COUNTER_MASK: u64 = (1u64 << RUSTRAFT_UNIQUE_ID_COUNTER_BITS) - 1;
pub const RUSTRAFT_UNIQUE_ID_TIMESTAMP_MASK: u64 = (1u64 << RUSTRAFT_UNIQUE_ID_TIMESTAMP_BITS) - 1;
pub const RUSTRAFT_UNIQUE_ID_MEMBER_MASK: u64 = (1u64 << RUSTRAFT_UNIQUE_ID_MEMBER_BITS) - 1;

#[derive(Debug)]
pub struct RustRaftUniqueIdGenerator {
    member_prefix: u64,
    timestamp_and_counter: AtomicU64,
}

impl RustRaftUniqueIdGenerator {
    pub fn new(member_id: u64, time_millis: u64) -> Self {
        let member_prefix = (member_id & RUSTRAFT_UNIQUE_ID_MEMBER_MASK)
            << (RUSTRAFT_UNIQUE_ID_TIMESTAMP_BITS + RUSTRAFT_UNIQUE_ID_COUNTER_BITS);
        let timestamp_and_counter =
            (time_millis & RUSTRAFT_UNIQUE_ID_TIMESTAMP_MASK) << RUSTRAFT_UNIQUE_ID_COUNTER_BITS;
        Self {
            member_prefix,
            timestamp_and_counter: AtomicU64::new(timestamp_and_counter),
        }
    }

    pub fn next(&self) -> u64 {
        let timestamp_and_counter = self.timestamp_and_counter.fetch_add(1, Ordering::SeqCst) + 1;
        self.member_prefix
            | low_bits(
                timestamp_and_counter,
                RUSTRAFT_UNIQUE_ID_TIMESTAMP_BITS + RUSTRAFT_UNIQUE_ID_COUNTER_BITS,
            )
    }

    pub fn decode(id: u64) -> RustRaftUniqueIdParts {
        RustRaftUniqueIdParts {
            member_id: (id
                >> (RUSTRAFT_UNIQUE_ID_TIMESTAMP_BITS + RUSTRAFT_UNIQUE_ID_COUNTER_BITS))
                & RUSTRAFT_UNIQUE_ID_MEMBER_MASK,
            timestamp_millis: (id >> RUSTRAFT_UNIQUE_ID_COUNTER_BITS)
                & RUSTRAFT_UNIQUE_ID_TIMESTAMP_MASK,
            counter: id & RUSTRAFT_UNIQUE_ID_COUNTER_MASK,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RustRaftUniqueIdParts {
    pub member_id: u64,
    pub timestamp_millis: u64,
    pub counter: u64,
}

fn low_bits(value: u64, bits: u32) -> u64 {
    if bits >= 64 {
        value
    } else {
        value & ((1u64 << bits) - 1)
    }
}
