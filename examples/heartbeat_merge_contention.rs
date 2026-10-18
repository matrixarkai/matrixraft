// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What does it cost many groups to offer a heartbeat at the same time?
//!
//! A hosted group offers its heartbeat to the merger on the path that would
//! otherwise send it, so at thousands of groups across a pool of workers this
//! is one of the busiest calls in the library. The merger buckets by the peer's
//! address; this measures whether those buckets buy anything when the callers
//! are concurrent.
//!
//! Run as `heartbeat_merge_contention <threads> <peers>`. Each thread offers
//! heartbeats for its own peers, and a flusher thread drains on an interval the
//! way a driver tick would.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use matrixraft::{
    AppendEntriesRequest, HeartbeatFlusher, HeartbeatMerger, MergedHeartbeatBatch,
    MergedHeartbeatSender, Message,
};

/// Counts what it is handed and drops it, so the figure is the merge path and
/// not a transport.
#[derive(Default)]
struct Counting {
    batches: AtomicU64,
    messages: AtomicU64,
}

impl MergedHeartbeatSender for Counting {
    fn send_merged(&self, batches: Vec<MergedHeartbeatBatch>) {
        self.batches
            .fetch_add(batches.len() as u64, Ordering::Relaxed);
        self.messages.fetch_add(
            batches
                .iter()
                .map(|batch| batch.messages.len() as u64)
                .sum(),
            Ordering::Relaxed,
        );
    }
}

fn main() {
    let threads: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(8);
    let peers_per_thread: u64 = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(4);
    let per_thread: u64 = 200_000;

    // One address per (group, peer) pair, so threads do not share buckets by
    // accident: what is being measured is whether distinct peers contend.
    let mut resolver = HashMap::new();
    for thread in 0..threads {
        for peer in 0..peers_per_thread {
            let node = thread * peers_per_thread + peer + 1;
            resolver.insert((node, node + 1), format!("10.0.{thread}.{peer}:7000"));
        }
    }
    let resolver = Arc::new(resolver);

    let sender = Arc::new(Counting::default());
    let flusher = HeartbeatFlusher::new(
        HeartbeatMerger::enabled(),
        Arc::clone(&sender) as Arc<dyn MergedHeartbeatSender>,
    );

    let stop = Arc::new(AtomicBool::new(false));
    let flush_stop = Arc::clone(&stop);
    let flush_flusher = Arc::clone(&flusher);
    // Drains the way a driver tick would, so the buckets do not simply grow.
    let flush_thread = std::thread::spawn(move || {
        while !flush_stop.load(Ordering::Relaxed) {
            flush_flusher.flush_now();
            std::thread::sleep(Duration::from_millis(1));
        }
        flush_flusher.flush_now();
    });

    let barrier = Arc::new(Barrier::new(threads as usize + 1));
    let mut workers = Vec::new();
    for thread in 0..threads {
        let flusher = Arc::clone(&flusher);
        let resolver = Arc::clone(&resolver);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            for round in 0..per_thread {
                let peer = round % peers_per_thread;
                let node = thread * peers_per_thread + peer + 1;
                let message = Message::AppendEntries {
                    target: node + 1,
                    request: AppendEntriesRequest {
                        group_id: node,
                        term: 4,
                        leader_id: node,
                        prev_log_id: None,
                        entries: Vec::new(),
                        leader_commit: round,
                        lease_epoch: 1,
                    },
                };
                let absorbed = flusher
                    .maybe_merge(message, resolver.as_ref())
                    .expect("the resolver knows every pair this offers");
                assert!(
                    absorbed.is_none(),
                    "an empty append is a heartbeat and must be absorbed"
                );
            }
        }));
    }

    barrier.wait();
    let started = Instant::now();
    for worker in workers {
        worker.join().expect("worker");
    }
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    flush_thread.join().expect("flusher");

    let offered = threads * per_thread;
    println!("threads:              {threads}");
    println!("peers per thread:     {peers_per_thread}");
    println!("heartbeats offered:   {offered}");
    println!(
        "elapsed ms:           {:.1}",
        elapsed.as_secs_f64() * 1000.0
    );
    println!(
        "ns per offer:         {:.1}",
        elapsed.as_nanos() as f64 / offered as f64
    );
    println!(
        "offers per second:    {:.0}",
        offered as f64 / elapsed.as_secs_f64()
    );
    let stats = flusher.stats();
    println!(
        "queued/flushed/pending: {}/{}/{}",
        stats.merger.queued_requests, stats.merger.flushed_requests, stats.pending
    );
    println!(
        "batches sent:         {}  messages in them: {}",
        sender.batches.load(Ordering::Relaxed),
        sender.messages.load(Ordering::Relaxed)
    );
}
