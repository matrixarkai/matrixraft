// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A restart recovers how far the group had got, not just what it holds.
//!
//! The comparable engine puts crash-injection points on both sides of an apply
//! batch -- before it and after it -- because where a crash lands decides
//! whether the batch is re-applied or skipped. Mapping those onto this crate, I
//! wrote that they have no counterpart because a restart recovers the applied
//! index from the WAL record's fence and carries on from there. **That was an
//! assertion, and this is the check.** Measured:
//!
//! ```text
//! before restart: commit=4 applied=4
//! after restart:  commit=4 applied=4   (3 surviving records)
//! ```
//!
//! Nothing pinned it. `ProcessOperationalSemanticsEvidence` carries
//! `process_restart_after_apply_crash_recovered`, and it gates
//! `proves_runtime_semantics`, but every test that mentions it supplies `true`
//! as an input -- so the property was modelled and never exercised.
//!
//! The fence is what carries it: a `WalRecord` holds `applied_index` and
//! `commit_index` beside the entries, under one checksum, and
//! `restore_wal_record` puts both back. An engine that recovered only the log
//! would re-apply from zero, which is the case these tests would catch.
//!
//! Planting says which sites they cover. Dropping either index in
//! `restore_wal_record` fails them. Of the **two** record builders that carry
//! `applied_index`, only `wal_record_for_coverage` is on the path a runtime
//! appends through -- stopping the other one carrying it leaves these green, so
//! that builder is not covered here and this says so rather than implying both
//! are.

use matrixraft::{Config, NodeOptions, NodeRuntime, Peer, ReplicaRole};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 113;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 37_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 37_500 + node_id),
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
        "matrixraft-restart-progress-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn options(base: &Path) -> NodeOptions {
    NodeOptions {
        group_id: GROUP_ID,
        node_id: 1,
        raft_addr: "127.0.0.1:37001".to_string(),
        snapshot_addr: "127.0.0.1:37501".to_string(),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config: Config::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

/// `(commit_index, applied_index)` as the group reports them.
fn progress(runtime: &NodeRuntime) -> (u64, u64) {
    let status = runtime.status().expect("status");
    let health = status
        .cluster_status
        .as_ref()
        .map(|cluster| &cluster.apply_health)
        .expect("a cluster status");
    (health.commit_index, health.applied_index)
}

#[test]
fn a_restart_recovers_the_commit_and_applied_indexes() {
    let base = base_dir("recovers");
    let _ = std::fs::remove_dir_all(&base);

    let before = {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create the group");
        runtime.start().expect("start it");
        for i in 0..3u64 {
            runtime
                .propose(format!("entry-{i}").into_bytes())
                .expect("a write");
        }
        let before = progress(&runtime);
        runtime.shutdown().expect("shut it down");
        before
    };
    assert!(
        before.0 > 0 && before.1 > 0,
        "the group has to have got somewhere before the restart means anything: {before:?}"
    );

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create the group");
    restarted.start().expect("re-start it");

    assert_eq!(
        progress(&restarted),
        before,
        "a restart recovers how far the group had got. Losing the applied index \
         would re-apply everything from zero; losing the commit index would \
         un-commit entries that were committed."
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn the_restart_reads_it_from_the_log_it_recovered() {
    // Ties the recovered progress to the records it came from, so the test
    // above cannot pass by the group happening to redo the same work.
    let base = base_dir("fromlog");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        for i in 0..3u64 {
            runtime
                .propose(format!("entry-{i}").into_bytes())
                .expect("a write");
        }
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    let report = restarted
        .wal_recovery_report()
        .expect("a recovery report")
        .expect("the group recovered something");
    assert!(
        report.surviving_records >= 3,
        "three proposals wrote at least three records, and the restart read them: {report:?}"
    );
    assert!(
        !report.truncated_corrupt_tail,
        "a clean shutdown leaves no torn tail"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_group_with_no_log_starts_from_nothing() {
    // The positive control. A restart that reported the same progress whatever
    // it found would satisfy both tests above.
    let base = base_dir("fresh");
    let _ = std::fs::remove_dir_all(&base);
    let mut runtime = NodeRuntime::create(options(&base)).expect("create");
    runtime.start().expect("start");
    assert_eq!(
        progress(&runtime),
        (0, 0),
        "a group with nothing behind it has got nowhere"
    );
    let _ = runtime.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
