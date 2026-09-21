// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What does one more raft group cost when the groups share a driver?
//!
//! `examples/group_scaling.rs` asks the same question of `MatrixRaftMultiRaftServer`,
//! where each group owns a `NodeRuntime` and so an OS thread: it measures 1.00
//! threads per group at every size from 1 to 1024. This measures the other
//! shape, where `Driver` ticks every group from one thread and
//! `DriverWorkerPool` runs their mail on a fixed pool.
//!
//! Run both at the same size to compare. The number to watch is threads: memory
//! per group is the smaller half of the bill either way.
//!
//! Reports per group: resident bytes, and the process thread count. Threads are
//! read exactly from `/proc/self/status`; resident memory moves for reasons that
//! have nothing to do with this process, which is why the probe registers many
//! groups and divides rather than trusting a single reading.
//!
//! Linux only, for `/proc/self/status`.
//!
//! ```bash
//! cargo run --release --example driver_scaling            # 1024 groups
//! cargo run --release --example driver_scaling -- 4096 8  # groups, workers
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use matrixraft::{
    Driver, DriverGroupKey, DriverMailHandler, DriverOptions, DriverTickReceiver, DriverWorkerPool,
    MailPriority,
};

/// A value from `/proc/self/status`, in its own units.
fn status_field(name: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(name) {
            return rest.trim().trim_end_matches(" kB").trim().parse().ok();
        }
    }
    None
}

fn rss_bytes() -> Option<u64> {
    status_field("VmRSS:").map(|kb| kb * 1024)
}

fn thread_count() -> Option<u64> {
    status_field("Threads:")
}

/// Stands in for a group: counts what the driver sends it.
#[derive(Debug, Default)]
struct Group {
    ticks: AtomicU64,
    mails: AtomicU64,
}

impl DriverTickReceiver for Group {
    fn fire_tick(&self) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }
}

impl DriverMailHandler<u64> for Group {
    fn handle_mail(&self, mails: Vec<u64>) {
        self.mails.fetch_add(mails.len() as u64, Ordering::Relaxed);
    }
}

fn main() {
    let groups: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(1024);
    let worker_num: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(4);

    if rss_bytes().is_none() || thread_count().is_none() {
        println!("/proc/self/status unavailable: this probe needs Linux");
        return;
    }

    let options = DriverOptions {
        worker_num,
        // Slow enough that the probe measures holding groups rather than
        // ticking them. `group_scaling` suppresses its tick for the same reason.
        tick_interval_ms: 60_000,
        ..DriverOptions::default()
    };

    let rss_before = rss_bytes().expect("VmRSS");
    let threads_before = thread_count().expect("Threads");

    let driver = Driver::start(options).expect("driver");
    let pool: DriverWorkerPool<u64> = DriverWorkerPool::start(options).expect("pool");
    let threads_idle = thread_count().expect("Threads");

    let started = Instant::now();
    let handles: Vec<Arc<Group>> = (1..=groups)
        .map(|group_id| {
            let group = Arc::new(Group::default());
            let key = DriverGroupKey::new(group_id, 1);
            driver
                .register_group(key, Arc::clone(&group) as Arc<dyn DriverTickReceiver>)
                .expect("register tick");
            pool.register_group(key, Arc::clone(&group) as Arc<dyn DriverMailHandler<u64>>)
                .expect("register mail");
            group
        })
        .collect();
    let elapsed = started.elapsed();

    let rss_after = rss_bytes().expect("VmRSS");
    let threads_after = thread_count().expect("Threads");

    assert_eq!(
        driver.group_count() as u64,
        groups,
        "the driver did not take every group"
    );
    assert_eq!(
        pool.group_count() as u64,
        groups,
        "the pool did not take every group"
    );

    // Prove the groups are reachable, not merely counted: a registry that held
    // them and drove nothing would report the same memory.
    for group_id in 1..=groups {
        pool.send(DriverGroupKey::new(group_id, 1), MailPriority::Normal, 1)
            .expect("send");
    }
    let deadline = Instant::now() + std::time::Duration::from_secs(60);
    while Instant::now() < deadline
        && handles
            .iter()
            .any(|group| group.mails.load(Ordering::Relaxed) == 0)
    {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let delivered = handles
        .iter()
        .filter(|group| group.mails.load(Ordering::Relaxed) > 0)
        .count() as u64;
    assert_eq!(
        delivered, groups,
        "only {delivered} of {groups} groups received their mail"
    );

    std::hint::black_box(&handles);

    let rss_delta = rss_after.saturating_sub(rss_before);
    let group_threads = threads_after.saturating_sub(threads_idle);
    println!(
        "groups={groups:<6} workers={worker_num}  rss_delta={:.1} MiB  per_group={:.0} B  \
         threads={threads_after} (driver+pool={}, per_group={group_threads})  \
         register={elapsed:?} ({:?}/group)",
        rss_delta as f64 / 1048576.0,
        rss_delta as f64 / groups as f64,
        threads_idle.saturating_sub(threads_before),
        elapsed / groups as u32
    );
    println!(
        "  every group received its mail: {delivered}/{groups}; \
         a thread-per-group runtime would be at {groups} threads here"
    );
}
