// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a snapshot checkpoint costs on disk, per payload byte.
//!
//! The chunk types in `snapshot_runtime` have carried their bytes as base64
//! since that codec was written, and the snapshot itself did not. So the wire
//! stopped paying for a JSON number array while the durable checkpoint went on
//! paying it, and the comment on the chunk field -- "a number array costs about
//! 3.9 bytes per data byte ... the largest single wire cost in the protocol" --
//! was describing a cost that had moved to disk rather than one that was gone.
//!
//! Measured, before and after, with `payload_bytes` declared and three voters:
//!
//! ```text
//!    payload   as a number array   as base64
//!      3 KiB          12,436          4,245
//!     64 KiB         262,293         87,534
//!      1 MiB       4,194,455      1,398,256
//! ```
//!
//! so **4.000 bytes per payload byte, now 1.333**: a 1 MiB snapshot occupied
//! 4.19 MB and occupies 1.40 MB. The slope is exact in both cases and for the
//! same reason -- `serde_json` writes a `Vec<u8>` as decimal numbers, and a
//! byte of value 120 spells `120,` at four characters, while base64 writes
//! three bytes as four.
//!
//! Exact, but not a clean 4/3 between any two points: `meta` declares
//! `payload_bytes` and that is itself a decimal number, so the envelope grows
//! two characters between 65,536 and 1,048,576. The identity below carries that
//! term explicitly rather than being asserted between two points chosen to hide
//! it.
//!
//! The floor below this is **1.000**, which a binary encoding reaches: a
//! protobuf `bytes` field costs a tag, a length varint and the bytes, so 4
//! bytes of overhead on a 1 MiB payload. That is a further 1.33x and is not
//! this file's change; this one is a serde attribute and a migration that was
//! already written, where that is a durable-format decision.
//!
//! ## What is not measured here
//!
//! - The wire. Chunks were already base64 and are untouched.
//! - Compression. Nothing in this path compresses, and a snapshot of real state
//!   would likely compress well, which would change every figure above. These
//!   are the costs of the encoding, not a claim about what a deployment stores.

use matrixraft::{
    GenericSnapshot, LogId, PersistentRaftSnapshotStore, PersistentRaftSnapshotStoreOptions,
    RaftSnapshot, SnapshotMetadata,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Measured, exact: the same snapshot always produces the same bytes.
const FILE_AT_3072: u64 = 4_245;
const FILE_AT_65536: u64 = 87_534;
const FILE_AT_1MIB: u64 = 1_398_256;

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("rustraft-{name}-{}-{nonce}", std::process::id()))
}

fn snapshot(id: &str, payload_len: usize) -> RaftSnapshot {
    GenericSnapshot {
        group_id: 9,
        meta: SnapshotMetadata {
            snapshot_id: id.to_string(),
            last_log_id: LogId { term: 3, index: 7 },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: payload_len as u64,
        },
        payload: vec![b'x'; payload_len],
    }
}

fn store(dir: PathBuf) -> PersistentRaftSnapshotStore {
    PersistentRaftSnapshotStore::open(PersistentRaftSnapshotStoreOptions::new(dir))
        .expect("the snapshot store opens")
}

/// Saves one checkpoint and returns its file size.
fn checkpoint_bytes(payload_len: usize) -> u64 {
    let dir = temp_dir("snapshot-cost");
    let path = store(dir.clone())
        .save_checkpoint(&snapshot("snap-1", payload_len))
        .expect("the checkpoint saves");
    let bytes = fs::metadata(&path).expect("checkpoint metadata").len();
    let _ = fs::remove_dir_all(&dir);
    bytes
}

#[test]
fn a_checkpoint_costs_four_thirds_of_its_payload_and_not_four_times() {
    let small = checkpoint_bytes(3_072);
    let medium = checkpoint_bytes(65_536);
    let large = checkpoint_bytes(1_048_576);
    assert_eq!(small, FILE_AT_3072, "the 3 KiB point moved");
    assert_eq!(medium, FILE_AT_65536, "the 64 KiB point moved");
    assert_eq!(large, FILE_AT_1MIB, "the 1 MiB point moved");

    // Both payloads are multiples of three, so base64 adds no padding and the
    // slope is exact. Asserted as an integer identity rather than a float
    // comparison, which would need a tolerance nobody can justify.
    //
    // The envelope is NOT constant between the two points, which is worth
    // stating rather than hiding by picking two that disagree less: `meta`
    // declares `payload_bytes`, and that is a decimal number in the JSON, so it
    // widens from five characters at 65,536 to seven at 1,048,576. The measured
    // difference is 1,310,722 where 4/3 of the payload is 1,310,720, and those
    // two bytes are the whole of it. Choosing points of equal digit width would
    // make the identity prettier and would stop it describing the file.
    const DECLARED_LENGTH_WIDENING: u64 = 2;
    let extra_file = FILE_AT_1MIB - FILE_AT_65536;
    let extra_payload = 1_048_576 - 65_536;
    assert_eq!(
        extra_payload % 3,
        0,
        "the payload difference must divide by three or base64 padding enters the arithmetic"
    );
    assert_eq!(
        extra_file,
        extra_payload * 4 / 3 + DECLARED_LENGTH_WIDENING,
        "a payload byte must cost exactly 4/3 on disk, plus the two characters `payload_bytes` gains: {extra_file} file bytes for {extra_payload} payload bytes is {:.6}, not {:.6}",
        extra_file as f64 / extra_payload as f64,
        4.0 / 3.0
    );

    // The figure this file exists for. A number array costs 4.0 and would put
    // the 1 MiB point back above four million bytes.
    //
    // Asserted on the MEASURED value rather than on `FILE_AT_1MIB`, which would
    // compare two constants, be folded at compile time, and check nothing about
    // the code that produced the file.
    assert!(
        large < 1_048_576 * 2,
        "a 1 MiB snapshot costs {large} bytes, over 2x its payload, which is the number-array regime this change left"
    );
}

/// The bytes on disk are base64 characters, not decimal numbers.
///
/// The byte count above would also be met by some other encoding of the same
/// width, so this looks at what is actually in the file. It is the assertion
/// that would have caught the original defect: the chunk types carried base64
/// and the snapshot did not, and nothing compared the two.
#[test]
fn the_payload_on_disk_is_base64_and_not_a_list_of_numbers() {
    let dir = temp_dir("snapshot-shape");
    let path = store(dir.clone())
        .save_checkpoint(&snapshot("snap-1", 3_072))
        .expect("the checkpoint saves");
    let text = fs::read_to_string(&path).expect("the checkpoint is text");
    let _ = fs::remove_dir_all(&dir);

    assert!(
        text.contains("\"payload\":\""),
        "the payload is not a JSON string, so it is not base64"
    );
    assert!(
        !text.contains("\"payload\":["),
        "the payload is a JSON array, which is the number-array form costing 4.0 bytes per payload byte"
    );
    assert!(
        !text.contains("120,120,120"),
        "the file holds runs of decimal 120, so the payload is still being written as numbers"
    );
}

/// A checkpoint written before this change still loads.
///
/// The migration claim, tested rather than asserted. The decoder accepts both
/// forms, so the saving closes behind each file as it is rewritten and no
/// migration pass is needed -- but "the decoder accepts it" is a statement
/// about a serde helper, and what matters is whether `load_checkpoint` on a
/// real file does. This writes the old form onto the path a save produced, so
/// the filename convention is the real one rather than a guess.
#[test]
fn a_checkpoint_written_as_a_number_array_still_loads() {
    let dir = temp_dir("snapshot-legacy");
    let keeper = store(dir.clone());
    let path = keeper
        .save_checkpoint(&snapshot("legacy-1", 4))
        .expect("the checkpoint saves");

    // The same snapshot, with its payload spelled the way it used to be.
    let legacy = concat!(
        r#"{"group_id":9,"meta":{"snapshot_id":"legacy-1","#,
        r#""last_log_id":{"term":3,"index":7},"membership":[1,2,3],"#,
        r#""members":[],"payload_bytes":4},"payload":[1,2,3,255]}"#
    );
    fs::write(&path, legacy).expect("the older form is written");

    let loaded = keeper
        .load_checkpoint("legacy-1")
        .expect("a checkpoint written as a number array still loads");
    assert_eq!(
        loaded.payload,
        vec![1u8, 2, 3, 255],
        "the payload did not survive being read from the older form"
    );
    assert_eq!(loaded.meta.snapshot_id, "legacy-1");
    assert_eq!(loaded.meta.last_log_id, LogId { term: 3, index: 7 });
    assert_eq!(loaded.meta.payload_bytes, 4);
    assert_eq!(loaded.group_id, 9);

    let _ = fs::remove_dir_all(&dir);
}

/// And the round trip through the new form is byte-exact for every byte value.
///
/// 0..=255 rather than a readable payload, because base64 is where an encoder
/// gets padding and the high bit wrong, and a payload of `b'x'` repeated cannot
/// see either.
#[test]
fn every_byte_value_survives_the_round_trip() {
    for length in [0usize, 1, 2, 3, 4, 255, 256, 257] {
        let dir = temp_dir("snapshot-bytes");
        let keeper = store(dir.clone());
        let mut value = snapshot("round-1", 0);
        value.payload = (0..length).map(|index| (index % 256) as u8).collect();
        value.meta.payload_bytes = length as u64;
        keeper.save_checkpoint(&value).expect("saves");
        let loaded = keeper.load_checkpoint("round-1").expect("loads");
        assert_eq!(
            loaded.payload, value.payload,
            "a {length}-byte payload did not survive the round trip"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
