// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Wire-size probe: what does an AppendEntries RPC cost in bytes?
//!
//! The TCP transport frames one JSON document per RPC, so the wire cost of a
//! request is exactly its encoded length -- a count, not a timing, and so
//! independent of machine load.

use matrixraft::{AppendEntriesRequest, LogEntry, LogId, TcpRaftTransportRequest};

fn request(payload_bytes: usize, entries: usize) -> TcpRaftTransportRequest {
    TcpRaftTransportRequest::AppendEntries {
        target: 2,
        request: AppendEntriesRequest {
            group_id: 3,
            term: 1,
            leader_id: 1,
            prev_log_id: None,
            entries: (1..=entries as u64)
                .map(|index| LogEntry {
                    log_id: LogId { term: 1, index },
                    // Every byte value in rotation, so the number-array cost
                    // reflects real data rather than a single-digit best case:
                    // "7," is two characters, "142," is four.
                    payload: (0..payload_bytes).map(|byte| byte as u8).collect(),
                    is_command: true,
                })
                .collect(),
            leader_commit: 1,
            lease_epoch: 0,
        },
    }
}

fn main() {
    println!(
        "{:>12}  {:>8}  {:>12}  {:>16}",
        "payload", "entries", "wire bytes", "bytes/payload B"
    );
    for (payload, entries) in [(0usize, 1usize), (64, 1), (256, 1), (1024, 1), (256, 16)] {
        let encoded = serde_json::to_vec(&request(payload, entries)).expect("encode");
        let total_payload = payload * entries;
        let per = if total_payload == 0 {
            f64::NAN
        } else {
            encoded.len() as f64 / total_payload as f64
        };
        println!(
            "{payload:>12}  {entries:>8}  {:>12}  {per:>16.2}",
            encoded.len()
        );
    }
}
