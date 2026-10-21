// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A checkpoint is renamed into place, so a `.snapshot` file is never a prefix.
//!
//! `save_checkpoint` used `File::create` on the final path, which **truncates**
//! it. A crash between that create and the sync left a torn checkpoint where a
//! good one had been, and the store went on advertising it. Measured on a file
//! cut to a third of its length:
//!
//! ```text
//! PROBE A: still listed as ["g301-10"]
//! PROBE B: load refused: failed to decode snapshot: EOF while parsing a list
//! PROBE C: owned_bytes = 4139
//! ```
//!
//! Listed, counted, and unloadable -- which is exactly the state
//! `save_checkpoint`'s own comment says the sidecar ordering exists to
//! prevent ("a sidecar without its snapshot would advertise a checkpoint that
//! cannot be loaded"), reached by the other route. One route was guarded and
//! the other was not.
//!
//! The write now goes to `{id}.snapshot.tmp` and is renamed in. A rename
//! within a directory is atomic, so a reader sees the whole old checkpoint or
//! the whole new one.
//!
//! **What these tests do and do not cover.** They cover the properties the
//! staging approach depends on: a staging file is not mistaken for a
//! checkpoint, a successful save leaves none behind, and a replacement is
//! all-or-nothing. They do **not** cover the atomicity itself, because the
//! window is a crash between two syscalls -- opening it would need a process
//! killed at that instant, or an injected failure between the write and the
//! rename, and this crate has no fault injection. The detection behaviour for
//! a file torn from outside the store is pinned below instead, since that is
//! the one case still reachable.

use matrixraft::{
    LogId, PersistentRaftSnapshotStore, PersistentRaftSnapshotStoreOptions, RaftSnapshot,
    SnapshotMetadata,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 301;

fn store_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "matrixraft-snapshot-staging-{name}-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("the store directory");
    dir
}

fn store(dir: &Path) -> PersistentRaftSnapshotStore {
    PersistentRaftSnapshotStore::open(PersistentRaftSnapshotStoreOptions::new(dir.to_path_buf()))
        .expect("open the store")
}

fn snapshot(id: &str, index: u64, payload: Vec<u8>) -> RaftSnapshot {
    RaftSnapshot {
        group_id: GROUP_ID,
        meta: SnapshotMetadata {
            snapshot_id: id.to_string(),
            last_log_id: LogId { term: 1, index },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: 0,
        },
        payload,
    }
}

fn listed_ids(store: &PersistentRaftSnapshotStore) -> Vec<String> {
    store
        .list_checkpoints()
        .expect("list the checkpoints")
        .into_iter()
        .map(|stored| stored.snapshot_id)
        .collect()
}

#[test]
fn a_successful_save_leaves_no_staging_file_behind() {
    let dir = store_dir("clean");
    let store = store(&dir);
    store
        .save_checkpoint(&snapshot("g301-10", 10, vec![b'a'; 2048]))
        .expect("saved");

    let staging = dir.join("g301-10.snapshot.tmp");
    assert!(
        !staging.exists(),
        "the staging file is renamed into place, not left beside the checkpoint"
    );
    assert!(dir.join("g301-10.snapshot").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_staging_file_left_by_a_crash_is_not_a_checkpoint() {
    // The property the whole approach rests on: listing keeps only entries
    // whose extension is `snapshot`, and this name's extension is `tmp`. If
    // that ever stopped holding, a crashed write would start advertising a
    // prefix again -- the very thing the rename removes.
    let dir = store_dir("stale");
    let store = store(&dir);
    store
        .save_checkpoint(&snapshot("g301-10", 10, vec![b'a'; 2048]))
        .expect("saved");

    // A crash between the create and the rename leaves exactly this.
    std::fs::write(dir.join("g301-20.snapshot.tmp"), b"{\"partial\"").expect("stage");

    assert_eq!(
        listed_ids(&store),
        vec!["g301-10".to_string()],
        "a staging file is not a checkpoint"
    );
    assert_eq!(
        store.owned_bytes().expect("owned bytes"),
        std::fs::metadata(dir.join("g301-10.snapshot"))
            .expect("metadata")
            .len(),
        "and it is not counted against the store's bytes either"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn replacing_a_checkpoint_at_the_same_id_is_all_or_nothing() {
    // The rename path, exercised: the second save replaces the first, and what
    // loads afterwards is wholly the second.
    let dir = store_dir("replace");
    let store = store(&dir);
    store
        .save_checkpoint(&snapshot("g301-10", 10, vec![b'a'; 4096]))
        .expect("first save");
    store
        .save_checkpoint(&snapshot("g301-10", 10, vec![b'b'; 64]))
        .expect("second save");

    let loaded = store.load_checkpoint("g301-10").expect("loads");
    assert_eq!(
        loaded.payload.len(),
        64,
        "the newer payload is the one held"
    );
    assert!(
        loaded.payload.iter().all(|byte| *byte == b'b'),
        "and none of the older payload survives in it"
    );
    assert_eq!(listed_ids(&store), vec!["g301-10".to_string()]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_checkpoint_torn_from_outside_the_store_is_refused_rather_than_half_loaded() {
    // Still reachable: the store cannot leave a prefix behind any more, but
    // the file can be damaged by something else. The decode has to fail rather
    // than hand back a snapshot built from part of a file.
    let dir = store_dir("torn");
    let store = store(&dir);
    let path = store
        .save_checkpoint(&snapshot("g301-10", 10, vec![b'a'; 4096]))
        .expect("saved");
    let whole = std::fs::read(&path).expect("read it back");
    std::fs::write(&path, &whole[..whole.len() / 3]).expect("tear it");

    let error = store
        .load_checkpoint("g301-10")
        .expect_err("a prefix of a checkpoint is not a checkpoint");
    assert!(
        error.to_string().contains("failed to decode snapshot"),
        "{error}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
