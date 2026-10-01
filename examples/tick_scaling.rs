// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Can one ticker keep up with many groups?
//!
//! The driver's worker pool scales with threads and is near flat in the group
//! count. The ticker is the one part that cannot be spread: a single thread
//! holds a due-time heap and must visit **every** group on its interval. That
//! is deliberate — it is what makes a group cost no thread — but it puts a
//! ceiling somewhere, and nothing has measured where.
//!
//! The arithmetic says a store with G groups on a T millisecond interval needs
//! `G / T * 1000` ticks per second. 50,000 groups at 10ms is five million.
//! This measures what the ticker actually delivers, and reports the shortfall
//! rather than only the rate: a ticker that is behind is one whose groups are
//! not heartbeating on the interval they were configured with, which is a
//! correctness-shaped problem and not only a slow one.
//!
//! # What it found
//!
//! Writing this turned up a defect rather than a ceiling. The logical clock
//! used to advance one millisecond per loop iteration, and the loop sleeps a
//! millisecond -- but `thread::sleep` guarantees *at least* the duration, and
//! the rest of the iteration (draining the heap, firing every due group) costs
//! time too. So the clock ran slow, and it ran slower the more groups there
//! were, because the work per iteration grew while the clock still counted
//! one. At 50,000 groups each step fires about 5,000 groups and the clock had
//! fallen to roughly half of wall time.
//!
//! Reading the clock from elapsed time instead, ABBA-interleaved, six samples
//! an arm, on a 16-core box:
//!
//! ```text
//!   groups   counting sleeps   reading elapsed
//!      100             92.3%            100.0%
//!   50,000             55.2%             99.9%
//! ```
//!
//! With that fixed the ticker holds 100% to 50,000 groups -- five million
//! ticks a second from one thread -- and first falls behind between 50,000 and
//! 80,000. The earlier shortfalls were the clock, not capacity.
//!
//! Linux only, for `/proc/self/status`.
//!
//! ```bash
//! cargo run --release --example tick_scaling              # sweep group counts
//! cargo run --release --example tick_scaling -- 50000 10 3
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use matrixraft::{Driver, DriverGroupKey, DriverOptions, DriverTickReceiver};

/// Counts ticks and does nothing else, so this measures the ticker.
#[derive(Debug, Default)]
struct TickCounter {
    ticks: AtomicU64,
}

impl DriverTickReceiver for TickCounter {
    fn fire_tick(&self) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }
}

fn thread_count() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("Threads:") {
            return rest.trim().parse().unwrap_or(0);
        }
    }
    0
}

struct Observed {
    groups: u64,
    wanted_per_sec: f64,
    actual_per_sec: f64,
    threads: u64,
}

fn measure(groups: u64, interval_ms: u64, seconds: u64) -> Observed {
    let counter = Arc::new(TickCounter::default());
    let driver = Driver::start(DriverOptions {
        worker_num: 1,
        tick_interval_ms: interval_ms,
        ..DriverOptions::default()
    })
    .expect("driver");

    for group_id in 1..=groups {
        driver
            .register_group(
                DriverGroupKey::new(group_id, 1),
                Arc::clone(&counter) as Arc<dyn DriverTickReceiver>,
            )
            .expect("register");
    }
    assert_eq!(
        driver.group_count() as u64,
        groups,
        "every group registered"
    );

    // Let registration settle before the window opens, so the rate is of
    // steady-state ticking and not of the first interval filling up.
    std::thread::sleep(Duration::from_millis(interval_ms * 2 + 50));

    let before = counter.ticks.load(Ordering::Relaxed);
    let started = Instant::now();
    std::thread::sleep(Duration::from_secs(seconds));
    let elapsed = started.elapsed();
    let fired = counter.ticks.load(Ordering::Relaxed) - before;
    let threads = thread_count();

    Observed {
        groups,
        wanted_per_sec: groups as f64 * 1000.0 / interval_ms as f64,
        actual_per_sec: fired as f64 / elapsed.as_secs_f64(),
        threads,
    }
}

fn main() {
    let only: Option<u64> = std::env::args().nth(1).and_then(|a| a.parse().ok());
    let interval_ms: u64 = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(10);
    let seconds: u64 = std::env::args()
        .nth(3)
        .and_then(|a| a.parse().ok())
        .unwrap_or(3);

    println!("interval={interval_ms}ms window={seconds}s, one driver per row\n");
    println!(
        "  {:>8}  {:>14}  {:>14}  {:>9}  {:>8}",
        "groups", "wanted/sec", "actual/sec", "kept up", "threads"
    );

    let counts: Vec<u64> = match only {
        Some(g) => vec![g],
        None => vec![100, 1_000, 10_000, 50_000, 60_000, 80_000],
    };

    for groups in counts {
        let seen = measure(groups, interval_ms, seconds);
        let kept = seen.actual_per_sec / seen.wanted_per_sec;
        println!(
            "  {:>8}  {:>14.0}  {:>14.0}  {:>8.1}%  {:>8}",
            seen.groups,
            seen.wanted_per_sec,
            seen.actual_per_sec,
            kept * 100.0,
            seen.threads
        );
    }

    println!(
        "\n  \"kept up\" below 100% means groups are not being ticked on the interval\n  \
         they were configured with -- a heartbeat and an election clock hang off\n  \
         that tick, so a shortfall is not only slowness."
    );
}
