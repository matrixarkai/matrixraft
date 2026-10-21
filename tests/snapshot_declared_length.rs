// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A snapshot install that stops early is refused, because the sender says how
//! many payload bytes it is sending.
//!
//! Before this, `done: true` was the only thing that ended an install, and the
//! offset check could only prove the chunks that *did* arrive were contiguous.
//! Measured: one chunk carrying `done: true` installed 1,024 bytes of a
//! 4,096-byte snapshot, `finish` accepted it, and the receiver then reported
//! `installed_snapshot` at the snapshot's full index -- a claim to state it did
//! not hold. A follower in that position serves reads from a truncated machine.
//!
//! The positive arm below is a control, not the point. A validator tested only
//! on input it should accept is indistinguishable from no validator at all, so
//! the two refusal arms are the ones that carry the guarantee.

use matrixraft::{
    LogId, RaftSnapshot, SnapshotChunk, SnapshotInstallState, SnapshotLifecycle, SnapshotMetadata,
};

const GROUP_ID: u64 = 311;
const CHUNK: u64 = 1024;

/// 4,096 bytes, so `CHUNK` splits it into four.
fn whole() -> RaftSnapshot {
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    RaftSnapshot {
        group_id: GROUP_ID,
        meta: SnapshotMetadata {
            snapshot_id: "g311-40".to_string(),
            last_log_id: LogId { term: 3, index: 40 },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: payload.len() as u64,
        },
        payload,
    }
}

fn chunks_of(snapshot: &RaftSnapshot) -> Vec<SnapshotChunk> {
    SnapshotLifecycle::checkpoint(snapshot, CHUNK).expect("chunk the snapshot")
}

#[test]
fn chunking_declares_the_payload_length_even_when_the_stored_metadata_does_not() {
    // A snapshot whose stored metadata predates the field, or disagrees with
    // the payload beside it. The chunker derives the length from the payload it
    // is actually sending, so neither case can put a wrong number on the wire.
    let mut stale = whole();
    stale.meta.payload_bytes = 0;
    for chunk in chunks_of(&stale) {
        assert_eq!(chunk.meta.payload_bytes, 4096);
    }

    let mut wrong = whole();
    wrong.meta.payload_bytes = 7;
    for chunk in chunks_of(&wrong) {
        assert_eq!(chunk.meta.payload_bytes, 4096);
    }
}

#[test]
fn an_install_of_every_chunk_is_accepted() {
    let snapshot = whole();
    let chunks = chunks_of(&snapshot);
    assert_eq!(chunks.len(), 4, "expected four chunks of {CHUNK} bytes");

    let mut install = SnapshotInstallState::new(chunks[0].meta.clone());
    for chunk in &chunks {
        install.install_chunk(chunk.clone()).expect("install chunk");
    }
    let installed = install.finish(GROUP_ID).expect("finish the install");
    assert_eq!(installed.payload, snapshot.payload);
    assert_eq!(installed.meta.last_log_id.index, 40);
}

#[test]
fn an_install_that_stops_early_and_says_done_is_refused() {
    let snapshot = whole();
    let chunks = chunks_of(&snapshot);

    // A sender that sends a quarter of the payload and claims to be finished.
    let mut first = chunks[0].clone();
    first.done = true;

    let mut install = SnapshotInstallState::new(chunks[0].meta.clone());
    install
        .install_chunk(first)
        .expect("the chunk itself is fine");

    let err = install
        .finish(GROUP_ID)
        .expect_err("a quarter of a snapshot must not finish as a whole one");
    let message = err.to_string();
    assert!(
        message.contains("1024") && message.contains("4096"),
        "the error should name what arrived and what was declared: {message}"
    );
}

#[test]
fn a_chunk_that_overruns_the_declared_length_is_refused() {
    let snapshot = whole();
    let chunks = chunks_of(&snapshot);

    let mut install = SnapshotInstallState::new(chunks[0].meta.clone());
    for chunk in &chunks {
        install.install_chunk(chunk.clone()).expect("install chunk");
    }

    // A further chunk, contiguous with the last, carrying bytes nobody declared.
    let overrun = SnapshotChunk {
        meta: chunks[0].meta.clone(),
        offset: snapshot.payload.len() as u64,
        data: vec![0u8; 16],
        done: true,
    };
    let err = install
        .install_chunk(overrun)
        .expect_err("an overrunning chunk must be refused on arrival");
    let message = err.to_string();
    assert!(
        message.contains("4112") && message.contains("4096"),
        "the error should name the overrun and the declared length: {message}"
    );
}

#[test]
fn a_snapshot_that_declares_nothing_installs_as_it_always_did() {
    // Zero means "not declared", so a sender or a stored record written before
    // this field behaves exactly as it used to. That is deliberate, and it is
    // also the limit of the check: it catches truncation, overrun and a
    // miscount, not a peer determined to lie about its own length.
    let snapshot = whole();
    let mut undeclared = SnapshotMetadata {
        payload_bytes: 0,
        ..snapshot.meta.clone()
    };
    undeclared.snapshot_id = "g311-40-undeclared".to_string();

    let mut install = SnapshotInstallState::new(undeclared.clone());
    let short = SnapshotChunk {
        meta: undeclared,
        offset: 0,
        data: snapshot.payload[..1024].to_vec(),
        done: true,
    };
    install.install_chunk(short).expect("install the one chunk");
    let installed = install
        .finish(GROUP_ID)
        .expect("an undeclared snapshot is not length-checked");
    assert_eq!(installed.payload.len(), 1024);
}
