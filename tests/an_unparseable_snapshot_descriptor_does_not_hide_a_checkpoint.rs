// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A descriptor sidecar that will not decode does not hide the checkpoint
//! beside it.
//!
//! `PersistentRaftSnapshotStore` writes a `{id}.descriptor` next to each
//! `{id}.snapshot` so listing does not have to decode every payload.
//! `describe()` treats it as a **hint** and has three fallbacks. Planting each
//! one away showed that two were already held, under names a search for
//! "corrupt" or "damaged" does not find:
//!
//! | fallback | held by |
//! |---|---|
//! | no sidecar at all | `a_store_written_without_sidecars_still_lists_and_releases` |
//! | a sidecar whose byte count disagrees with the file | `a_descriptor_that_disagrees_with_the_disk_does_not_decide_the_bytes` |
//! | **a sidecar that will not decode** | **nothing** |
//!
//! So this file covers only the third, and deliberately does not duplicate the
//! other two.
//!
//! The distinction between the first and third is worth keeping: a file that is
//! absent and a file that is present but unreadable reach the fallback by
//! different branches, and only one of them was exercised. A sidecar can end up
//! in the unreadable state without anybody corrupting anything -- a crash
//! between create and write leaves a zero-length file, which is present and
//! will not decode.
//!
//! What it costs if it regresses: `release_checkpoints` lists before it
//! decides, so a listing that fails on one bad sidecar stops a group releasing
//! any snapshot, and `load_checkpoint` could not find the one it should restore
//! from. The second test pins that consequence rather than just the listing.

use matrixraft::{
    LogId, PersistentRaftSnapshotStore, PersistentRaftSnapshotStoreOptions, RaftSnapshot,
    SnapshotMetadata,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP: u64 = 7;
/// The middle checkpoint, so the damage is not at either edge of the listing.
const DAMAGED_INDEX: u64 = 20;

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-descriptor-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn checkpoint_id(index: u64) -> String {
    format!("checkpoint-{index:04}")
}

fn snapshot(index: u64) -> RaftSnapshot {
    let payload = b"snapshot-payload-bytes".to_vec();
    RaftSnapshot {
        group_id: GROUP,
        meta: SnapshotMetadata {
            snapshot_id: checkpoint_id(index),
            last_log_id: LogId { term: 2, index },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: payload.len() as u64,
        },
        payload,
    }
}

/// Three checkpoints at 10, 20 and 30, and the directory holding them.
fn store_with_three(name: &str) -> (PersistentRaftSnapshotStore, PathBuf) {
    let dir = temp_dir(name);
    let store = PersistentRaftSnapshotStore::open(PersistentRaftSnapshotStoreOptions {
        dir: dir.clone(),
        chunk_size: 8,
    })
    .expect("open the store");
    for index in [10u64, DAMAGED_INDEX, 30] {
        store.save_checkpoint(&snapshot(index)).expect("save");
    }
    (store, dir)
}

/// Leaves the sidecar present and unreadable, which is what a crash between
/// create and write produces.
fn make_sidecar_undecodable(dir: &Path, index: u64) {
    let descriptor = dir.join(format!("{}.descriptor", checkpoint_id(index)));
    assert!(
        descriptor.exists(),
        "the premise: a sidecar was written, so there is one to damage"
    );
    fs::write(&descriptor, b"").expect("truncate the sidecar to nothing");
    assert!(
        descriptor.exists(),
        "and it is still present -- absent is the other fallback, already tested"
    );
}

fn listed_indexes(store: &PersistentRaftSnapshotStore) -> Vec<u64> {
    store
        .list_checkpoints()
        .expect("the store lists what it holds")
        .iter()
        .map(|stored| stored.last_log_id.index)
        .collect()
}

#[test]
fn a_sidecar_that_will_not_decode_still_lists_its_checkpoint() {
    let (store, dir) = store_with_three("undecodable");
    make_sidecar_undecodable(&dir, DAMAGED_INDEX);

    assert_eq!(
        listed_indexes(&store),
        vec![10, DAMAGED_INDEX, 30],
        "the checkpoint is on disk, so a hint that will not decode must not hide it"
    );
}

#[test]
fn retention_still_runs_over_a_sidecar_that_will_not_decode() {
    // The consequence, and the reason this is worth pinning: release_checkpoints
    // lists before it decides, so a listing that failed would stop a group
    // releasing anything.
    let (store, dir) = store_with_three("retention");
    make_sidecar_undecodable(&dir, DAMAGED_INDEX);

    let report = store
        .release_checkpoints(30, 30)
        .expect("retention runs over an undecodable sidecar");
    assert_eq!(
        report
            .released
            .iter()
            .map(|stored| stored.last_log_id.index)
            .collect::<Vec<_>>(),
        vec![10, DAMAGED_INDEX],
        "both older checkpoints go, the damaged one included"
    );
    assert_eq!(
        listed_indexes(&store),
        vec![30],
        "and the newest is kept, as it always is"
    );
}

#[test]
fn an_intact_store_lists_all_three() {
    // The control: the fixture is sound, so each test above differs from it by
    // exactly the one sidecar it damages.
    let (store, _dir) = store_with_three("intact");
    assert_eq!(listed_indexes(&store), vec![10, DAMAGED_INDEX, 30]);
}
