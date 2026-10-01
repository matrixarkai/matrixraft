// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a hosted group allocates while it does nothing but tick.
//!
//! `tests/tick_allocations.rs` counts the cluster calls a tick makes, one at a
//! time, and they are all at zero. This counts the whole path instead, by letting
//! a real hosted group tick on its own runtime and watching the process -- which
//! is the only way to see the allocations that are in the tick itself rather than
//! in something it calls. There were 3.499 a tick when this was written and the
//! call-level test said zero.
//!
//! Divided by the ticks the runtime says it ran, not by elapsed milliseconds.
//! Elapsed time is an upper bound on ticks delivered, and on a loaded box the two
//! differ enough to move the figure almost twofold: two runs read 1.95 and 1.10
//! before the denominator was the real count, and 2.250 twice afterwards.

use matrixraft::{
    MatrixRaftGroupContextBuilder, MatrixRaftMultiRaftServer, MatrixRaftOptions,
    MatrixRaftTransportBuilder, Peer, ReplicaRole,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn probe_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-shared-{label}-{}-{nonce}",
        std::process::id()
    ))
}

fn peer(group_id: u64) -> Peer {
    Peer {
        node_id: 1,
        // Nothing dials these: no transport is started.
        raft_addr: format!("127.0.0.1:{}", 31_000 + (group_id % 1_000)),
        snapshot_addr: format!("127.0.0.1:{}", 41_000 + (group_id % 1_000)),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn options(group_id: u64, interval_ms: u64, wal: &Path, snapshot: &Path) -> MatrixRaftOptions {
    MatrixRaftOptions {
        group_id,
        peer_id: 1,
        raft_addr: peer(group_id).raft_addr,
        snapshot_addr: peer(group_id).snapshot_addr,
        wal_dir: wal.display().to_string(),
        snapshot_dir: snapshot.display().to_string(),
        // A single voter is its own quorum, so it reaches leadership without
        // a transport and the test is about hosting, not about elections.
        peers: vec![peer(group_id)],
        role: ReplicaRole::Voter,
        wal_sync: false,
        election_cycle_tick: 4,
        transfer_timeout_tick: 3,
        offline_timeout_tick: 10,
        tick_interval_ms: interval_ms,
        lease_duration_ms: 20,
        last_lease_duration_ms: 10,
        assume_lease_when_start: false,
        max_memory_replicate_log_bytes: 64 * 1024,
        max_disk_replicate_log_num: 64,
        max_cache_memory_bytes: 1024 * 1024,
        max_apply_batch_bytes: 64 * 1024,
        enable_reorder_queue: true,
        reorder_timeout_us: 3_000,
        reorder_window_size: 128,
        max_inflights_apply_task: 5,
        max_inflights_replicate: 128,
        enable_pre_vote: true,
        max_segment_bytes: 4 * 1024 * 1024,
        min_keep_segment_num: 2,
        can_trigger_snapshot: true,
        max_applied_log_bytes: u64::MAX,
        send_snapshot_timeout_ms: 60_000,
    }
}

#[test]
fn how_much_a_hosted_group_allocates_while_only_ticking() {
    let root = probe_root("window");
    let wal = root.join("g1/wal");
    let snapshot = root.join("g1/snapshot");
    std::fs::create_dir_all(&wal).expect("wal dir");
    std::fs::create_dir_all(&snapshot).expect("snapshot dir");

    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(1)
        .set_num_connection_group(1)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .tick_interval(1)
        .worker_num(1)
        .shared_runtime(true)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);
    server
        .create_node(options(1, 1, &wal, &snapshot), 0)
        .expect("create");
    server.start_all(0).expect("start");

    // Settle, so construction is not charged to the window.
    std::thread::sleep(Duration::from_millis(400));

    // The runtime's own count of ticks it ran, rather than the elapsed
    // milliseconds. Elapsed time is only an upper bound on ticks delivered, and on
    // a loaded box the two differ enough to move the per-tick figure almost
    // twofold: two runs of this test read 1.95 and 1.10 before the denominator was
    // the real thing.
    let ticks_before = server.shared_stats().expect("shared runtime");
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(2_000));
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    let ticks_after = server.shared_stats().expect("shared runtime");
    let ticks = (ticks_after.ticks_in_place + ticks_after.ticks_handed_over)
        .saturating_sub(ticks_before.ticks_in_place + ticks_before.ticks_handed_over)
        as f64;
    assert!(
        ticks > 100.0,
        "only {ticks} ticks ran, too few to divide by"
    );
    let per_tick = allocations as f64 / ticks;
    println!("window: {allocations} allocations over {ticks:.0} ticks actually run = {per_tick:.3} per tick");

    // 1.250 on this box, three times, to three decimals. The ceiling carries a
    // quarter of an allocation of slack rather than none, because the figure is not
    // a whole number: this group runs a four-tick election cycle, so a quarter of
    // the ticks do extra work and a machine that times them differently can land
    // slightly either side. A whole allocation arriving still fails it.
    //
    // The history, because each step was found by this instrument: 3.499 before
    // `last_tick_reason` stopped turning a literal into a `String` every tick,
    // 2.250 before `has_live_quorum` stopped building a `Membership` to ask how
    // large a quorum is, 1.250 now.
    const CEILING_PER_TICK: f64 = 1.5;
    assert!(
        per_tick <= CEILING_PER_TICK,
        "a hosted group now allocates {per_tick:.3} times a tick, above the          {CEILING_PER_TICK} recorded here. Every group pays this on every interval,          so if it is deliberate, move the ceiling and say what needs the heap."
    );

    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}
