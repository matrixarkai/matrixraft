// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `max_segment_bytes` is a roll threshold, not a hard cap.
//!
//! A WAL record is written whole or not at all, so a record larger than the
//! bound has to go somewhere. The roll condition is
//! `active_records > 0 && active_len + record_bytes > max_segment_bytes`, and
//! the `active_records > 0` half is what decides where: a segment that already
//! holds records is rolled away first, and an empty one takes the record
//! however large it is.
//!
//! Measured with a 1,024-byte bound and a 10,000-byte payload:
//!
//! ```text
//! with the guard:     segments=1  total_bytes=13825
//! guard removed:      segments=2  total_bytes=13825
//! ```
//!
//! So the guard buys exactly one thing -- not rolling a segment that was just
//! created and is still empty -- and removing it wasted a segment per
//! oversized record while the whole suite stayed green. It is a small enough
//! effect to be worth writing down, because nothing said what the half-line
//! was for and nothing noticed its absence.
//!
//! The defaults cannot reach this: `max_payload_bytes` is 8 MiB against a
//! 64 MiB segment. It is reachable by raising the payload bound above the
//! segment bound, which `Config::validate` accepts -- deliberately, because
//! refusing it would remove a working configuration to tidy a bound that
//! cannot be honoured for a single large record anyway.

use matrixraft::{Config, NodeOptions, NodeRuntime, Peer, ReplicaRole};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 283;
const SEGMENT_BOUND: u64 = 1_024;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 56_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 56_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn base_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-segment-bound-{name}-{}-{nonce}",
        std::process::id()
    ))
}

/// A config whose payload bound is above its segment bound, which is the only
/// way to reach an oversized record.
fn small_segment_config() -> Config {
    Config {
        max_payload_bytes: 10_000_000,
        max_segment_bytes: SEGMENT_BOUND,
        ..Default::default()
    }
}

fn options(base: &Path, config: Config) -> NodeOptions {
    NodeOptions {
        group_id: GROUP_ID,
        node_id: 1,
        raft_addr: "127.0.0.1:56001".to_string(),
        snapshot_addr: "127.0.0.1:56501".to_string(),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config,
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

/// `(segment_count, total_bytes)` after proposing each payload in turn.
fn wal_after(name: &str, config: Config, payloads: &[usize]) -> (u64, u64) {
    let base = base_dir(name);
    let _ = std::fs::remove_dir_all(&base);
    let mut runtime = NodeRuntime::create(options(&base, config)).expect("create the group");
    runtime.start().expect("start it");
    for len in payloads {
        runtime
            .propose(vec![b'x'; *len])
            .unwrap_or_else(|error| panic!("a {len}-byte proposal was refused: {error}"));
    }
    let status = runtime.wal_lifecycle_status().expect("a WAL status");
    let _ = runtime.shutdown();
    let _ = std::fs::remove_dir_all(&base);
    (status.segment_count, status.total_bytes)
}

#[test]
fn a_record_larger_than_the_segment_bound_is_written() {
    let (_, total_bytes) = wal_after("oversized", small_segment_config(), &[10_000]);
    assert!(
        total_bytes > SEGMENT_BOUND,
        "a record is written whole or not at all, so one larger than the bound \
         has to be written past it: {total_bytes} bytes against a {SEGMENT_BOUND}-byte bound"
    );
}

#[test]
fn it_does_not_leave_an_empty_segment_behind() {
    // The `active_records > 0` half of the roll condition. Without it the WAL
    // rolls a segment it has just created, and the record lands in the second
    // one -- a wasted segment per oversized record.
    let (segments, _) = wal_after("nostray", small_segment_config(), &[10_000]);
    assert_eq!(
        segments, 1,
        "an empty segment takes the oversized record rather than being rolled \
         away first"
    );
}

#[test]
fn the_bound_still_rolls_records_that_fit() {
    // The control. A bound that was simply ignored would satisfy both tests
    // above, so this one shows it still governs the ordinary case: enough
    // small records have to spill past one segment.
    let payloads = [200usize; 12];
    let (segments, _) = wal_after("rolls", small_segment_config(), &payloads);
    assert!(
        segments > 1,
        "twelve 200-byte records cannot all fit a {SEGMENT_BOUND}-byte segment, \
         so the bound has to have rolled at least once: segments={segments}"
    );
}

#[test]
fn the_defaults_cannot_reach_an_oversized_record() {
    // Why this is a documentation matter rather than a validation one: the
    // defaults put the payload bound an order of magnitude below the segment
    // bound, so a host only reaches the case above by choosing to.
    let config = Config::default();
    assert!(
        config.max_payload_bytes < config.max_segment_bytes,
        "max_payload_bytes {} should default below max_segment_bytes {}",
        config.max_payload_bytes,
        config.max_segment_bytes
    );
}
