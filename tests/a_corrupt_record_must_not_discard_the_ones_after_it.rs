// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A corrupt record in the middle of a segment must not discard the records
//! after it.
//!
//! The segment reader walks frames from the front and stopped at the first one
//! it could not use, because `decode_framed` returned `Ok(None)` for every
//! failure alike -- a short frame, a short body, a failed digest and an
//! undecodable payload. The recovery scan treats "stop" as "truncate here".
//!
//! Measured on a five-record segment, before the fix:
//!
//! | damage | read back |
//! |---|---|
//! | none | 5 records |
//! | one byte of record **3**'s payload | **`Ok`, 2 records** |
//! | one byte of record **5**'s payload | `Ok`, 4 records |
//! | the last 4 bytes cut off | `Ok`, 4 records |
//!
//! The second row is the defect. Records 4 and 5 were written completely and
//! therefore acknowledged, and they were dropped with no error -- loss of
//! acknowledged data wearing the shape of a torn tail. The third and fourth
//! rows are correct: those are a damaged and a torn final record, and
//! truncating one costs nothing that was acknowledged.
//!
//! ## What separates the two cases, and what does not
//!
//! **Not the digest.** A crash between a write and its fsync can leave a frame
//! whose length is fully persisted over a body that is only partly written.
//! That is a torn append, and it fails its digest. So "the digest failed" does
//! not mean "this record was acknowledged", and refusing on a bad digest alone
//! would turn every ordinary crash into a WAL a node will not open.
//!
//! **Nor stepping over the bad record** to see whether good ones follow: that
//! needs its declared length, the one field a failed digest gives no reason to
//! trust. Corrupted larger, it runs off the end exactly as a torn tail does;
//! corrupted smaller, it resyncs onto a boundary inside a record.
//!
//! What does separate them is **whether anything lies beyond the frame's
//! declared end** -- a fact about the file, not about the record. Nothing
//! beyond it, and truncating costs only this record. Bytes beyond it, and those
//! bytes are unreachable, so discarding them is the loss this guards.
//!
//! A corrupted length is covered by the same rule, in the safe direction:
//! larger and the input runs out, which is already the torn case; smaller and
//! bytes appear to follow, so the read refuses.
//!
//! ## Why refusing is the right failure
//!
//! `wal_runtime` already argues it for the writer lock: for a durability
//! component, silent loss is the worst shape a bug can take, and that module
//! turned a silent loss into an error naming both lengths for the same reason.
//!
//! A node that refuses to open a damaged WAL is loud, and can be rebuilt from
//! its peers. A node that opens one and silently drops acknowledged entries
//! cannot be told from a healthy one, and may have told a client that an entry
//! it no longer holds was committed.
//!
//! ## What is not changed
//!
//! The record-level checksum path, which `persistent_wal` covers: `recover()`
//! pops trailing records whose own `checksum` field does not validate. That is
//! a different mechanism from the frame digest and keeps its behaviour.

use matrixraft::{
    matrixraft_read_wal_segment_file, matrixraft_wal_checksum, matrixraft_write_wal_segment_file,
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, WalRecord,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const RECORDS: u64 = 5;
/// Measured: the five-record segment this file builds.
const SEGMENT_BYTES: usize = 379;

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-midsegment-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn wal_record(index: u64) -> WalRecord {
    let mut record = WalRecord {
        entries_are_delta: false,
        group_id: 9,
        node_id: 1,
        hard_state: HardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(LogId { term: 3, index }),
        },
        membership: Membership {
            group_id: 9,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 3,
        },
        entries: vec![LogEntry {
            log_id: LogId { term: 3, index },
            payload: format!("payload-{index:03}").into_bytes(),
            is_command: true,
        }],
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: index,
            commit_index: index,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

/// A segment of `RECORDS` records, and its bytes.
fn segment(dir: &Path) -> (PathBuf, Vec<u8>) {
    fs::create_dir_all(dir).expect("dir");
    let path = dir.join("seg.wal");
    let records: Vec<WalRecord> = (1..=RECORDS).map(wal_record).collect();
    matrixraft_write_wal_segment_file(&path, &records).expect("write the segment");
    let bytes = fs::read(&path).expect("read the segment");
    (path, bytes)
}

fn indexes(records: &[WalRecord]) -> Vec<u64> {
    records
        .iter()
        .flat_map(|record| record.entries.iter().map(|entry| entry.log_id.index))
        .collect()
}

/// Where record `index`'s payload sits in the segment.
fn payload_at(bytes: &[u8], index: u64) -> usize {
    let needle = format!("payload-{index:03}").into_bytes();
    bytes
        .windows(needle.len())
        .position(|window| window == needle.as_slice())
        .expect("the payload is stored in the segment")
}

#[test]
fn an_undamaged_segment_reads_every_record() {
    let dir = temp_dir("clean");
    let (path, bytes) = segment(&dir);
    assert_eq!(bytes.len(), SEGMENT_BYTES, "the segment's size moved");

    let records = matrixraft_read_wal_segment_file(&path).expect("read");
    assert_eq!(indexes(&records), vec![1, 2, 3, 4, 5]);

    fs::remove_dir_all(&dir).ok();
}

/// The defect this file is about.
#[test]
fn a_corrupt_record_in_the_middle_is_refused_rather_than_truncated() {
    let dir = temp_dir("middle");
    let (path, bytes) = segment(&dir);

    // One byte of record 3's payload. The length is untouched, so the frame is
    // the right size and only its digest disagrees -- and records 4 and 5 lie
    // beyond it, complete and acknowledged.
    let at = payload_at(&bytes, 3);
    let mut damaged = bytes.clone();
    damaged[at] ^= 0xFF;
    assert_eq!(
        damaged.len(),
        bytes.len(),
        "the damage must not change the segment's length"
    );
    fs::write(&path, &damaged).expect("write");

    let error = matrixraft_read_wal_segment_file(&path)
        .expect_err("a corrupt record with records after it must be refused");
    let text = error.to_string();

    // The message has to say that data follows, because that is the whole
    // reason this is an error rather than a truncation -- an operator reading
    // it needs to know something is at stake.
    assert!(
        text.contains("fails its frame digest"),
        "the error should name the digest: {text}"
    );
    assert!(
        text.contains("follow it"),
        "the error should say that records follow the damaged one: {text}"
    );
    assert!(
        text.contains("refusing"),
        "the error should say it is refusing rather than discarding: {text}"
    );

    fs::remove_dir_all(&dir).ok();
}

/// A damaged or torn final record still truncates, because a crash does that.
///
/// This is the half that must keep working. Refusing on a bad digest alone
/// would turn every crash between a write and its fsync into a WAL the node
/// will not open.
#[test]
fn a_damaged_or_torn_final_record_still_truncates() {
    let dir = temp_dir("tail");
    let (path, bytes) = segment(&dir);

    // Damaged last record: complete bytes, bad digest, nothing after it.
    let at = payload_at(&bytes, RECORDS);
    let mut damaged = bytes.clone();
    damaged[at] ^= 0xFF;
    fs::write(&path, &damaged).expect("write");
    let records = matrixraft_read_wal_segment_file(&path)
        .expect("a damaged final record is a torn append, not an error");
    assert_eq!(
        indexes(&records),
        vec![1, 2, 3, 4],
        "the four records before the damaged one should survive"
    );

    // Torn last record: genuinely fewer bytes than the frame declares.
    fs::write(&path, &bytes[..bytes.len() - 4]).expect("write");
    let records = matrixraft_read_wal_segment_file(&path).expect("a torn append is not an error");
    assert_eq!(indexes(&records), vec![1, 2, 3, 4]);

    // Torn so short there is not even a frame header for the last record.
    let at_last = payload_at(&bytes, RECORDS);
    fs::write(&path, &bytes[..at_last - 4]).expect("write");
    let records = matrixraft_read_wal_segment_file(&path).expect("a short tail is not an error");
    assert!(
        indexes(&records).len() >= 4,
        "cutting inside the last frame's header should still yield the records before it, got {:?}",
        indexes(&records)
    );

    fs::remove_dir_all(&dir).ok();
}

/// A corrupted length is covered by the same rule, in the safe direction.
#[test]
fn a_corrupted_frame_length_is_refused_when_it_points_inside_the_segment() {
    let dir = temp_dir("length");
    let (path, bytes) = segment(&dir);

    // The first record's frame begins just after the segment magic: 4 bytes of
    // magic, then the length, then the digest.
    let length_at = 4;
    let declared = u32::from_le_bytes([
        bytes[length_at],
        bytes[length_at + 1],
        bytes[length_at + 2],
        bytes[length_at + 3],
    ]);
    assert!(declared > 8, "the first frame should declare a real length");

    // Shrink it. The body then fails its digest, and because the frame claims
    // to end early, bytes appear to follow -- so this must be refused rather
    // than resynced onto a boundary inside a record.
    let mut damaged = bytes.clone();
    damaged[length_at..length_at + 4].copy_from_slice(&(declared - 4).to_le_bytes());
    fs::write(&path, &damaged).expect("write");
    let error = matrixraft_read_wal_segment_file(&path)
        .expect_err("a frame whose length points inside the segment must be refused");
    assert!(
        error.to_string().contains("refusing"),
        "unexpected error: {error}"
    );

    // Grown past the end of the file, it is indistinguishable from a torn
    // append and is treated as one.
    let mut grown = bytes.clone();
    grown[length_at..length_at + 4].copy_from_slice(&(bytes.len() as u32 + 1_000).to_le_bytes());
    fs::write(&path, &grown).expect("write");
    let records = matrixraft_read_wal_segment_file(&path)
        .expect("a length past the end of the file is a torn append");
    assert!(
        records.is_empty(),
        "the first frame is the torn one, so nothing should be read, got {:?}",
        indexes(&records)
    );

    fs::remove_dir_all(&dir).ok();
}

/// A corrupt frame followed by a single byte is still refused.
///
/// The rule turns on "does anything lie beyond this frame's declared end", so
/// the boundary worth testing is the smallest possible *anything*: one byte.
/// A plant weakening the test to `end + 1 >= bytes.len()` passed the rest of
/// this file, because no other fixture ever leaves exactly one trailing byte --
/// the records here are ~70 bytes each, so every other case is far from the
/// boundary.
///
/// One byte cannot be a record, so little is lost by truncating it. The reason
/// to refuse anyway is that the reader cannot know that: whatever follows a
/// frame it could not validate is unreachable, and the size of it is not
/// evidence about what it was.
#[test]
fn a_corrupt_frame_followed_by_a_single_byte_is_refused() {
    let dir = temp_dir("one-trailing-byte");
    let (path, bytes) = segment(&dir);

    // The first frame: 4 bytes of segment magic, then its length, then its
    // digest, then the body.
    let declared = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
    let first_frame_end = 4 + 8 + declared;
    assert!(
        first_frame_end + 1 < bytes.len(),
        "the segment should be longer than one frame plus a byte"
    );

    // Keep exactly one byte beyond the first frame, and corrupt that frame.
    let mut damaged = bytes[..first_frame_end + 1].to_vec();
    let at = payload_at(&damaged, 1);
    damaged[at] ^= 0xFF;
    fs::write(&path, &damaged).expect("write");

    let error = matrixraft_read_wal_segment_file(&path)
        .expect_err("one trailing byte is still something beyond the frame");
    assert!(
        error.to_string().contains("1 bytes follow it"),
        "the error should name the single trailing byte: {error}"
    );

    fs::remove_dir_all(&dir).ok();
}

/// Damage to the second-to-last record is still refused.
///
/// The boundary matters: "is this the last frame" is what the rule turns on, so
/// the record immediately before the last one is the case most likely to be got
/// wrong by an off-by-one.
#[test]
fn damage_to_the_second_to_last_record_is_refused() {
    let dir = temp_dir("second-to-last");
    let (path, bytes) = segment(&dir);

    let at = payload_at(&bytes, RECORDS - 1);
    let mut damaged = bytes.clone();
    damaged[at] ^= 0xFF;
    fs::write(&path, &damaged).expect("write");

    let error = matrixraft_read_wal_segment_file(&path)
        .expect_err("one record still follows, so this must be refused");
    assert!(
        error.to_string().contains("follow it"),
        "unexpected error: {error}"
    );

    fs::remove_dir_all(&dir).ok();
}
