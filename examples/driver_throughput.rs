// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Does adding worker threads make a driver carry more?
//!
//! `examples/driver_scaling.rs` measures what it costs to *hold* groups: at
//! 10,000 groups the driver is 10 threads and 5.3 MiB. It says nothing about
//! throughput, and holding groups cheaply is worth little if the work does not
//! go faster when the pool grows.
//!
//! This sweeps `worker_num` at a fixed group count and reports messages per
//! second. A pool that scales shows the rate climbing with threads; one that is
//! bound by a shared lock shows it flat, or falling as the contention gets
//! worse.
//!
//! The handler does nothing on purpose. Any real work per message would hide
//! the thing being measured -- this is asking what the *delivery* path costs,
//! and that is where a shared lock would show.
//!
//! ```bash
//! cargo run --release --example driver_throughput              # sweep 1,2,4,8
//! cargo run --release --example driver_throughput -- 512 200000
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use matrixraft::{
    DriverGroupKey, DriverMailHandler, DriverOptions, DriverWorkerPool, MailPriority,
};

/// Counts and does nothing else, so the measurement is of delivery.
#[derive(Debug, Default)]
struct Counter {
    seen: AtomicU64,
}

impl DriverMailHandler<u64> for Counter {
    fn handle_mail(&self, mails: Vec<u64>) {
        self.seen.fetch_add(mails.len() as u64, Ordering::Relaxed);
    }
}

struct Run {
    workers: usize,
    delivered: u64,
    elapsed: Duration,
    refused: u64,
}

impl Run {
    fn per_second(&self) -> f64 {
        self.delivered as f64 / self.elapsed.as_secs_f64()
    }
}

fn measure(groups: u64, messages: u64, workers: usize, burst: u64) -> Run {
    let counter = Arc::new(Counter::default());
    let pool: DriverWorkerPool<u64> = DriverWorkerPool::start(DriverOptions {
        worker_num: workers,
        max_messages_each_poll: 64,
        // Deep enough that the probe measures delivery rather than the depth
        // bound refusing most of the offered load.
        max_queue_depth: 1 << 20,
        ..DriverOptions::default()
    })
    .expect("pool");

    for group_id in 1..=groups {
        pool.register_group(
            DriverGroupKey::new(group_id, 1),
            Arc::clone(&counter) as Arc<dyn DriverMailHandler<u64>>,
        )
        .expect("register");
    }

    let started = Instant::now();
    let mut refused = 0_u64;
    for n in 0..messages {
        // `burst` consecutive messages to one group before moving on. At 1 this
        // is round robin, where every message lands on a different worker and
        // wakes a different thread; at a larger value a group receives a run,
        // which is what a group under load actually looks like.
        let key = DriverGroupKey::new(((n / burst.max(1)) % groups) + 1, 1);
        if pool.send(key, MailPriority::Normal, n).is_err() {
            refused += 1;
        }
    }

    // Wait for delivery, so the rate covers the work and not just the sending.
    let deadline = Instant::now() + Duration::from_secs(120);
    let expected = messages - refused;
    while counter.seen.load(Ordering::Relaxed) < expected && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let elapsed = started.elapsed();
    let delivered = counter.seen.load(Ordering::Relaxed);

    Run {
        workers,
        delivered,
        elapsed,
        refused,
    }
}

fn main() {
    let groups: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(256);
    let messages: u64 = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(100_000);
    let burst: u64 = std::env::args()
        .nth(3)
        .and_then(|a| a.parse().ok())
        .unwrap_or(1);

    println!("groups={groups} messages={messages} burst={burst}, one pool per row\n");
    println!(
        "  {:>7}  {:>12}  {:>14}  {:>10}  {:>8}",
        "workers", "delivered", "msgs/sec", "elapsed", "vs 1"
    );

    let mut baseline: Option<f64> = None;
    for workers in [1usize, 2, 4, 8] {
        let run = measure(groups, messages, workers, burst);
        let rate = run.per_second();
        let relative = match baseline {
            None => {
                baseline = Some(rate);
                1.0
            }
            Some(first) => rate / first,
        };
        println!(
            "  {:>7}  {:>12}  {:>14.0}  {:>9.2?}  {:>7.2}x",
            run.workers, run.delivered, rate, run.elapsed, relative
        );
        assert_eq!(
            run.delivered + run.refused,
            messages,
            "every message must be delivered or refused; {} delivered, {} refused",
            run.delivered,
            run.refused
        );
        assert_eq!(
            run.refused, 0,
            "the queue depth should be deep enough that nothing is refused"
        );
    }

    println!(
        "\n  A pool that scales shows msgs/sec climbing with threads. Flat or\n  \
         falling means the delivery path is serialised, not the handler.\n  \
         Compare burst=1 against a larger burst before concluding: at 1 every\n  \
         message wakes a different worker, which measures wake-ups, not capacity."
    );
}
