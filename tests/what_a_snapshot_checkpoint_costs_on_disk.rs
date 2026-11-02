// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a snapshot checkpoint costs on disk, per payload byte.
//!
//! Three shapes have been written here, and the figure for the same payload
//! moved by 4x across them:
//!
//! ```text
//!    payload   number array   base64 in JSON   framed protobuf
//!      3 KiB         12,436            4,245             3,113
//!     64 KiB        262,293           87,534            65,579
//!      1 MiB      4,194,455        1,398,256         1,048,619
//! ```
//!
//! **4.000 bytes per payload byte, then 1.333, now 1.000.** A 1 MiB snapshot
//! occupied 4.19 MB and occupies 1.05 MB.
//!
//! Each step had one cause. `serde_json` writes a `Vec<u8>` as decimal numbers
//! and a byte of value 120 spells `120,` at four characters. base64 writes
//! three bytes as four. A protobuf `bytes` field writes the bytes, behind a tag
//! and a length varint, so the payload is no longer encoded at all -- which is
//! why the slope is exactly 1 and not merely close to it: between the 64 KiB
//! and 1 MiB points the extra file bytes and the extra payload bytes are both
//! 983,040.
//!
//! The envelope is 35 bytes at an empty payload and 43 at a megabyte. It grows
//! because two of its own fields are varints -- the payload's length, and
//! `payload_bytes` in the metadata -- so it widens with the number it carries.
//! That is also why the slope identity below is asserted between two points of
//! equal varint width rather than against the smallest point.
//!
//! ## What is not measured here
//!
//! - The wire. Chunks still travel as base64 inside JSON, which is a separate
//!   format with a compatibility window of its own.
//! - Compression. Nothing on this path compresses, and real state would likely
//!   compress well, which would change every figure above. These are the costs
//!   of the encoding, not a claim about what a deployment stores.
//! - Allocation. `encode_checkpoint` clones the payload into its message. The
//!   JSON path did too, and built a four-times-larger copy, so this is strictly
//!   better and still a full copy of a potentially large snapshot.

use matrixraft::{
    GenericSnapshot, LogId, PersistentRaftSnapshotStore, PersistentRaftSnapshotStoreOptions,
    RaftSnapshot, SnapshotMetadata,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Measured, exact: the same snapshot always produces the same bytes.
const FILE_AT_3072: u64 = 3_113;
const FILE_AT_65536: u64 = 65_579;
const FILE_AT_1MIB: u64 = 1_048_619;

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
fn a_checkpoint_costs_its_payload_and_a_small_envelope() {
    let small = checkpoint_bytes(3_072);
    let medium = checkpoint_bytes(65_536);
    let large = checkpoint_bytes(1_048_576);
    assert_eq!(small, FILE_AT_3072, "the 3 KiB point moved");
    assert_eq!(medium, FILE_AT_65536, "the 64 KiB point moved");
    assert_eq!(large, FILE_AT_1MIB, "the 1 MiB point moved");

    // The slope, as an exact integer identity. Both points carry a three-byte
    // length varint and a three-byte `payload_bytes`, so the envelope is the
    // same 43 bytes in each and the difference is payload and nothing else.
    // Asserted between these two rather than from the smallest point, where the
    // envelope is 41 and the identity would have to carry the widening.
    let extra_file = FILE_AT_1MIB - FILE_AT_65536;
    let extra_payload = 1_048_576 - 65_536;
    assert_eq!(
        extra_file, extra_payload,
        "a payload byte must cost exactly one byte: {extra_file} file bytes for {extra_payload} payload bytes is {:.6}, not 1.0",
        extra_file as f64 / extra_payload as f64
    );

    // The envelope, named rather than left as the remainder of a subtraction.
    assert_eq!(
        medium - 65_536,
        43,
        "the envelope at 64 KiB moved, so the figures in this file's table describe a different record"
    );

    // The figure this file exists for, on the measured value rather than on a
    // constant, which would compare two literals and check nothing.
    assert!(
        large < 1_048_576 + 1_024,
        "a 1 MiB snapshot costs {large} bytes, more than its payload plus a kilobyte, which is not the regime this file describes"
    );
}

/// The payload is in the file as itself, not as an encoding of itself.
///
/// The byte count above would also be met by some other encoding of the same
/// width, so this looks at what is actually in the file. It is the assertion
/// that would have caught the original defect: the chunk types carried base64
/// and the snapshot carried a number array, and nothing compared the two.
#[test]
fn the_payload_is_in_the_file_verbatim_and_not_encoded() {
    let dir = temp_dir("snapshot-shape");
    let path = store(dir.clone())
        .save_checkpoint(&snapshot("snap-1", 3_072))
        .expect("the checkpoint saves");
    let bytes = fs::read(&path).expect("the checkpoint is readable");
    let _ = fs::remove_dir_all(&dir);

    assert_eq!(
        &bytes[..4],
        b"MRCP",
        "the checkpoint does not open with its magic"
    );

    // 3,072 bytes of `x` have to be present as 3,072 bytes of `x`. base64 would
    // spell them as 4,096 characters of `eHh4`, and a number array as runs of
    // `120,`, and neither contains this.
    let run = vec![b'x'; 3_072];
    assert!(
        bytes
            .windows(run.len())
            .any(|window| window == run.as_slice()),
        "the payload is not in the file verbatim, so something is still encoding it"
    );
    assert!(
        !bytes.windows(4).any(|window| window == b"eHh4"),
        "the file holds base64 of the payload"
    );
    assert!(
        !bytes.windows(8).any(|window| window == b"120,120,"),
        "the file holds the payload as decimal numbers"
    );
}

/// Both older shapes still load.
///
/// The migration claim, tested rather than asserted, and for **two** forms
/// rather than one: a checkpoint written as a JSON number array, and one
/// written as base64 inside JSON, which was the shape this crate produced until
/// the framed one replaced it. Each writes onto the path a real save produced,
/// so the filename convention is the real one rather than a guess.
#[test]
fn a_checkpoint_in_either_older_shape_still_loads() {
    for (label, body) in [
        (
            "a JSON number array",
            concat!(
                r#"{"group_id":9,"meta":{"snapshot_id":"legacy-1","#,
                r#""last_log_id":{"term":3,"index":7},"membership":[1,2,3],"#,
                r#""members":[],"payload_bytes":4},"payload":[1,2,3,255]}"#
            ),
        ),
        (
            "base64 inside JSON",
            concat!(
                r#"{"group_id":9,"meta":{"snapshot_id":"legacy-1","#,
                r#""last_log_id":{"term":3,"index":7},"membership":[1,2,3],"#,
                r#""members":[],"payload_bytes":4},"payload":"AQID/w=="}"#
            ),
        ),
    ] {
        let dir = temp_dir("snapshot-legacy");
        let keeper = store(dir.clone());
        let path = keeper
            .save_checkpoint(&snapshot("legacy-1", 4))
            .expect("the checkpoint saves");
        fs::write(&path, body).unwrap_or_else(|err| panic!("writing {label}: {err}"));

        let loaded = keeper
            .load_checkpoint("legacy-1")
            .unwrap_or_else(|err| panic!("a checkpoint written as {label} did not load: {err}"));
        assert_eq!(
            loaded.payload,
            vec![1u8, 2, 3, 255],
            "the payload did not survive being read from {label}"
        );
        assert_eq!(loaded.meta.snapshot_id, "legacy-1", "{label}");
        assert_eq!(
            loaded.meta.last_log_id,
            LogId { term: 3, index: 7 },
            "{label}"
        );
        assert_eq!(loaded.meta.payload_bytes, 4, "{label}");
        assert_eq!(loaded.group_id, 9, "{label}");
        let _ = fs::remove_dir_all(&dir);
    }
}

/// Every byte value survives the round trip, at every length near a boundary.
///
/// 0..=255 rather than a readable payload, because an encoder gets the high bit
/// and its padding wrong and a payload of `b'x'` repeated cannot see either.
/// The framed form has no padding, but the arm is kept: it is the check that
/// the legacy forms needed, and the next format will need it too.
#[test]
fn every_byte_value_survives_the_round_trip() {
    for length in [0usize, 1, 2, 3, 4, 127, 128, 255, 256, 257] {
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
        assert_eq!(loaded.meta, value.meta, "the metadata changed at {length}");
        assert_eq!(loaded.group_id, value.group_id);
        let _ = fs::remove_dir_all(&dir);
    }
}

/// A damaged checkpoint is refused, and says so recognisably.
///
/// A WAL cuts a torn tail, because a crash between a write and its barrier is
/// ordinary there. A checkpoint is renamed into place and a rename is atomic,
/// so a reader sees a whole file or no file -- which makes a bad frame evidence
/// of damage rather than of a crash, and refusing is the only answer that does
/// not turn a corrupt checkpoint into a missing one.
#[test]
fn a_damaged_checkpoint_is_refused_rather_than_read_as_absent() {
    for (label, damage) in [
        ("a flipped payload byte", 60usize),
        ("a flipped length byte", 5),
        ("a flipped digest byte", 9),
    ] {
        let dir = temp_dir("snapshot-damage");
        let keeper = store(dir.clone());
        let path = keeper
            .save_checkpoint(&snapshot("damaged-1", 3_072))
            .expect("saves");
        let mut bytes = fs::read(&path).expect("read");
        bytes[damage] ^= 0xFF;
        fs::write(&path, &bytes).expect("write back");

        let error = keeper
            .load_checkpoint("damaged-1")
            .expect_err(&format!("{label} loaded as a valid checkpoint"));
        assert!(
            error.to_string().contains("failed to decode snapshot"),
            "{label} was refused with {error}, which callers matching on \"failed to decode snapshot\" will not recognise"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
