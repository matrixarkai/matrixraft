// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a checkpoint's descriptor sidecar costs, and what it deliberately does
//! not carry.
//!
//! `PersistentRaftSnapshotStore` writes a `{id}.descriptor` beside each
//! `{id}.snapshot` so that listing a store does not have to decode every
//! payload. It was JSON. It is now the same framed protobuf record the
//! checkpoint beside it uses -- `MRSD`, a little-endian `u32` length, a
//! little-endian `u32` CRC-32C, then the body.
//!
//! | snapshot id | as JSON | framed | |
//! |---|---:|---:|---:|
//! | `a` | 79 | **23** | 3.43x |
//! | `checkpoint-0020` | 95 | **37** | 2.57x |
//! | a 35-character id | 114 | **57** | 2.00x |
//!
//! The saving narrows as the id grows because the id is the only part that was
//! never compressible: it is a string in both forms, so a longer one dilutes a
//! fixed envelope saving. At the short end the old form was 79 bytes to carry
//! four small values.
//!
//! ## The byte count is gone, and that is the point
//!
//! `StoredSnapshot` has a `bytes` field. The JSON form wrote it; the binary
//! form does not. Every reader of a descriptor has always replaced it with the
//! length of the checkpoint file itself, because a sidecar that disagreed with
//! the disk would make the byte totals fiction -- and a host watches those
//! totals to decide whether to release anything.
//!
//! So the durable copy was a field that every reader discarded. It is not
//! written any more, and
//! `two_checkpoints_that_differ_only_in_size_get_identical_descriptors` holds
//! that structurally rather than by inspection: overwrite a checkpoint with a
//! larger payload under the same id and the descriptor bytes do not change,
//! which they could not fail to do if a size were in there.
//!
//! The JSON form still has the field and is still read, because files already
//! on disk have it written.
//!
//! ## A bad descriptor is a hint that cannot be used, not an error
//!
//! This is the one place in the crate where a failed frame digest is neither
//! truncation nor an error: `describe` falls back to decoding the checkpoint
//! itself, which is the authority the sidecar was only ever caching. That is
//! why the sidecar can be deleted, damaged, or left behind by an older build
//! without a group losing the ability to list or release.

use matrixraft::{
    LogId, PersistentRaftSnapshotStore, PersistentRaftSnapshotStoreOptions, RaftSnapshot,
    SnapshotMetadata,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Measured, exact.
const AT_SHORT_ID: usize = 23;
const AT_CHECKPOINT_ID: usize = 37;
const AT_LONG_ID: usize = 57;

/// What the same three cost as JSON, kept as the comparison this file is about.
const JSON_AT_SHORT_ID: usize = 79;
const JSON_AT_CHECKPOINT_ID: usize = 95;
const JSON_AT_LONG_ID: usize = 114;

const LONG_ID: &str = "a-rather-longer-snapshot-identifier";

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-descriptor-cost-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn snapshot(id: &str, payload_len: usize) -> RaftSnapshot {
    RaftSnapshot {
        group_id: 9,
        meta: SnapshotMetadata {
            snapshot_id: id.to_string(),
            last_log_id: LogId { term: 3, index: 42 },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: payload_len as u64,
        },
        payload: vec![b'x'; payload_len],
    }
}

fn store_in(dir: &Path) -> PersistentRaftSnapshotStore {
    PersistentRaftSnapshotStore::open(PersistentRaftSnapshotStoreOptions {
        dir: dir.to_path_buf(),
        chunk_size: 64 * 1024,
    })
    .expect("open")
}

/// The descriptor bytes for one saved checkpoint.
fn descriptor_of(dir: &Path, id: &str) -> Vec<u8> {
    fs::read(dir.join(format!("{id}.descriptor"))).expect("read the descriptor")
}

/// What the JSON form of the same descriptor weighed.
///
/// Built here rather than quoted from a constant alone, so the comparison is
/// against a shape this file can show rather than a number from a commit
/// message. The constants above then pin what that shape measured.
fn as_json(id: &str, group_id: u64, last_log_id: &LogId, bytes: u64) -> String {
    serde_json::to_string(&serde_json::json!({
        "snapshot_id": id,
        "group_id": group_id,
        "last_log_id": {"term": last_log_id.term, "index": last_log_id.index},
        "bytes": bytes,
    }))
    .expect("the legacy shape encodes")
}

#[test]
fn a_descriptor_is_a_framed_record_of_about_two_dozen_bytes() {
    let dir = temp_dir("size");
    let store = store_in(&dir);

    for (id, payload, expected) in [
        ("a", 0usize, AT_SHORT_ID),
        ("checkpoint-0020", 4096, AT_CHECKPOINT_ID),
        (LONG_ID, 100, AT_LONG_ID),
    ] {
        store.save_checkpoint(&snapshot(id, payload)).expect("save");
        let raw = descriptor_of(&dir, id);
        assert_eq!(
            raw.len(),
            expected,
            "the descriptor for {id:?} is {} bytes, not {expected}",
            raw.len()
        );
        assert_eq!(
            &raw[..4],
            b"MRSD",
            "the descriptor for {id:?} is not framed"
        );

        // The declared length matches what follows it, so the frame is
        // self-consistent and not merely the right total size.
        let declared = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
        assert_eq!(
            declared,
            raw.len() - 12,
            "the frame for {id:?} declares {declared} bytes and carries {}",
            raw.len() - 12
        );
    }

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn the_framed_descriptor_is_two_to_three_times_smaller_than_the_json_was() {
    let dir = temp_dir("against-json");
    let store = store_in(&dir);
    let last_log_id = LogId { term: 3, index: 42 };

    for (id, payload, framed_const, json_const) in [
        ("a", 0usize, AT_SHORT_ID, JSON_AT_SHORT_ID),
        (
            "checkpoint-0020",
            4096,
            AT_CHECKPOINT_ID,
            JSON_AT_CHECKPOINT_ID,
        ),
        (LONG_ID, 100, AT_LONG_ID, JSON_AT_LONG_ID),
    ] {
        store.save_checkpoint(&snapshot(id, payload)).expect("save");
        let framed = descriptor_of(&dir, id).len();
        let on_disk = fs::metadata(dir.join(format!("{id}.snapshot")))
            .expect("stat")
            .len();
        let json = as_json(id, 9, &last_log_id, on_disk).len();

        assert_eq!(framed, framed_const, "the framed size for {id:?} moved");
        assert_eq!(json, json_const, "the JSON comparison for {id:?} moved");
        // At least twice, not more than twice: the long id is exactly 2.00x
        // (114 against 57), which is the narrow end this test is about.
        assert!(
            json >= framed * 2,
            "{id:?}: JSON {json} should be at least twice the framed {framed}"
        );
    }

    // And the shape of it: the saving narrows as the id grows, because the id
    // is a string in both forms and dilutes a fixed envelope saving.
    let short_ratio = JSON_AT_SHORT_ID * 100 / AT_SHORT_ID;
    let long_ratio = JSON_AT_LONG_ID * 100 / AT_LONG_ID;
    assert!(
        short_ratio > long_ratio,
        "the saving should narrow as the id grows: {short_ratio} then {long_ratio}"
    );

    fs::remove_dir_all(&dir).ok();
}

/// The descriptor does not carry the checkpoint's size.
///
/// Held structurally rather than by reading the bytes: overwrite a checkpoint
/// with a much larger payload under the same id, and the descriptor is
/// byte-identical. A stored size could not survive that.
#[test]
fn two_checkpoints_that_differ_only_in_size_get_identical_descriptors() {
    let dir = temp_dir("no-size");
    let store = store_in(&dir);

    store
        .save_checkpoint(&snapshot("same-id", 16))
        .expect("save");
    let small_descriptor = descriptor_of(&dir, "same-id");
    let small_file = fs::metadata(dir.join("same-id.snapshot"))
        .expect("stat")
        .len();

    store
        .save_checkpoint(&snapshot("same-id", 64 * 1024))
        .expect("save again");
    let large_descriptor = descriptor_of(&dir, "same-id");
    let large_file = fs::metadata(dir.join("same-id.snapshot"))
        .expect("stat")
        .len();

    assert!(
        large_file > small_file * 100,
        "the checkpoint should have grown a long way: {small_file} then {large_file}"
    );
    assert_eq!(
        small_descriptor, large_descriptor,
        "the descriptor changed when only the checkpoint's size did, so a size is being stored"
    );

    // And the size the store reports is the file's, which is where it has
    // always come from.
    let listed = store.list_checkpoints().expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].bytes, large_file);

    fs::remove_dir_all(&dir).ok();
}

/// A damaged descriptor costs a decode of the checkpoint, not an error.
///
/// The damage is put **inside the snapshot id**, which matters. A flip almost
/// anywhere else makes the protobuf undecodable, and an undecodable sidecar
/// reaches the same fallback whether or not a digest was checked -- so such a
/// test passes on a build with no integrity check at all. A flipped character
/// of the id is damage the decoder would happily *accept*, returning a
/// plausible descriptor naming a checkpoint that does not exist. That is the
/// case the digest is actually there for, and asserting the reported id is how
/// it is held.
#[test]
fn a_descriptor_that_fails_its_digest_falls_back_to_the_checkpoint() {
    let dir = temp_dir("damaged");
    let store = store_in(&dir);
    store
        .save_checkpoint(&snapshot("checkpoint-0007", 128))
        .expect("save");

    let path = dir.join("checkpoint-0007.descriptor");
    let good = fs::read(&path).expect("read");

    // Find the id in the body and change one of its characters, leaving the
    // frame the right length so it is the digest that refuses it.
    let needle = b"checkpoint-0007";
    let at = good
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("the id is stored in the descriptor");
    let mut damaged = good.clone();
    damaged[at + 10] = b'9';
    assert_eq!(
        damaged.len(),
        good.len(),
        "the damage must not change the frame's length"
    );
    fs::write(&path, &damaged).expect("write");

    let listed = store.list_checkpoints().expect("listing must still work");
    assert_eq!(listed.len(), 1, "a damaged sidecar hid its checkpoint");
    assert_eq!(listed[0].snapshot_id, "checkpoint-0007");
    assert_eq!(
        listed[0].last_log_id.index, 42,
        "the fallback should have read the checkpoint itself"
    );

    // A zero-length sidecar, which a crash between create and write leaves
    // behind, reaches the same fallback by a different branch.
    fs::write(&path, b"").expect("write empty");
    let listed = store.list_checkpoints().expect("listing must still work");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].last_log_id.index, 42);

    fs::remove_dir_all(&dir).ok();
}

/// A descriptor an older build wrote is still read.
///
/// The sidecar is given a log index that **disagrees** with the checkpoint
/// beside it, and the disagreeing value is what the listing must report. That
/// is the only way to tell this path from the fallback: `describe` can always
/// recover the same answer by decoding the checkpoint, so a test written with
/// an agreeing sidecar stays green even if the JSON reader is deleted outright.
/// It is the same trap as asserting a value two code paths both produce.
#[test]
fn a_json_descriptor_from_an_older_build_is_still_read() {
    let dir = temp_dir("legacy");
    let store = store_in(&dir);
    store
        .save_checkpoint(&snapshot("checkpoint-0011", 64))
        .expect("save");

    let on_disk = fs::metadata(dir.join("checkpoint-0011.snapshot"))
        .expect("stat")
        .len();
    let path = dir.join("checkpoint-0011.descriptor");

    // The checkpoint itself says index 42. This says 99, and carries a byte
    // count that is wrong as well, because a stale one is the case the field
    // existing at all made possible.
    fs::write(
        &path,
        as_json("checkpoint-0011", 9, &LogId { term: 3, index: 99 }, 999_999).as_bytes(),
    )
    .expect("write");

    let listed = store.list_checkpoints().expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].snapshot_id, "checkpoint-0011");
    assert_eq!(
        listed[0].last_log_id.index, 99,
        "the legacy sidecar was not read -- 42 means the checkpoint was decoded instead"
    );
    assert_eq!(
        listed[0].bytes, on_disk,
        "the file's own length should win over the recorded one"
    );

    fs::remove_dir_all(&dir).ok();
}

/// A valid descriptor is read without decoding the checkpoint.
///
/// The point of the sidecar is to make listing cheap, and "cheap" is not
/// observable from the outside -- so it is held the other way round, by making
/// the checkpoint undecodable and showing that listing still works. If the
/// framed descriptor were being ignored, `describe` would fall through to
/// `load_checkpoint` and fail.
#[test]
fn a_valid_descriptor_is_read_without_decoding_the_checkpoint() {
    let dir = temp_dir("no-fallback");
    let store = store_in(&dir);
    store
        .save_checkpoint(&snapshot("checkpoint-0013", 256))
        .expect("save");

    // Keep the descriptor, ruin the checkpoint's body. Its length on disk is
    // unchanged, so the byte total is still whatever the file says.
    let checkpoint = dir.join("checkpoint-0013.snapshot");
    let mut raw = fs::read(&checkpoint).expect("read");
    let len = raw.len();
    raw[len / 2] ^= 0xFF;
    fs::write(&checkpoint, &raw).expect("write");

    assert!(
        store.load_checkpoint("checkpoint-0013").is_err(),
        "the checkpoint must be undecodable for this test to mean anything"
    );

    let listed = store
        .list_checkpoints()
        .expect("listing must not need the checkpoint when a descriptor is good");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].snapshot_id, "checkpoint-0013");
    assert_eq!(listed[0].last_log_id.index, 42);

    fs::remove_dir_all(&dir).ok();
}
