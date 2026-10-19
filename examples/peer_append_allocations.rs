// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one follower costs a proposal.
//!
//! A proposal does the same three things for every follower -- queue the
//! append, flush a batch, handle the response -- so this measures that unit.
//!
//! **Every arm asserts that it did what its name says**, and that is the point
//! of the file as much as the number is. Two earlier versions of it reported
//! figures for work that was not happening:
//!
//! - A queue-and-flush loop with no response flushes **nothing** after the
//!   first `max_inflights_replicate` cycles, because only a response brings
//!   `inflight_entries` down. It reported a flush as free.
//! - A queue-only loop fills `append_queue` to its limit and then **fails**
//!   every call, allocating the rejection's message. It reported a queue as
//!   costing an allocation, which was the error string.
//!
//! Both printed a plausible number from an arm that had stopped doing the
//! thing. So the cycle here is the whole unit, and it checks the flush
//! returned a batch and the queue returned `Ok` on every pass.
//!
//! ```text
//! cargo run --release --example peer_append_allocations
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{
    AppendEntriesResponse, InflightAppend, LogEntry, LogId, PipelineLimits, ReplicationPipeline,
    SnapshotState,
};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn limits() -> PipelineLimits {
    PipelineLimits {
        max_inflights_replicate: 64,
        max_memory_replicate_log_bytes: 1 << 30,
        max_inflights_apply_task: 64,
        max_apply_batch_bytes: 1 << 20,
        enable_reorder_queue: false,
        reorder_window_size: 0,
        reorder_timeout_us: 0,
    }
}

fn entry(index: u64, payload_bytes: usize) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: vec![7u8; payload_bytes],
        is_command: true,
    }
}

fn accepted(index: u64) -> AppendEntriesResponse {
    AppendEntriesResponse {
        term: 1,
        success: true,
        match_index: index,
        rejection_hint: None,
        rejected_index: None,
        require_snapshot: None,
        snapshot_state: SnapshotState::None,
        lease_confirmation_epoch: 0,
        lease_duration_ms: 0,
    }
}

/// One follower's share of a proposal: queue, flush, handle the response.
///
/// `lend` chooses between the two flush shapes. Everything else is identical,
/// which is what makes the difference between the two rows attributable.
fn cycle(
    pipeline: &mut ReplicationPipeline,
    index: u64,
    payload: usize,
    lend: Option<&mut Vec<InflightAppend>>,
) {
    let queued = entry(index, payload);
    pipeline
        .queue_append(&queued)
        .expect("the queue must accept: a rejection allocates its message and is not this unit");
    let batches = match lend {
        Some(flushed) => {
            flushed.clear();
            pipeline.flush_append_batch_into(64, 1 << 20, flushed)
        }
        None => pipeline.flush_append_batch(64, 1 << 20).len(),
    };
    assert_eq!(
        batches, 1,
        "the flush must produce a batch: a paused pipeline flushes nothing and reports it free"
    );
    pipeline
        .handle_append_response(&accepted(index))
        .expect("response");
}

/// `(allocs, bytes)` per follower-cycle.
fn measure(label: &str, cycles: usize, payload: usize, lend: bool) {
    let mut pipeline = ReplicationPipeline::new(2, 1, limits());
    let mut lent: Vec<InflightAppend> = Vec::new();
    let mut index = 0u64;
    let run =
        |pipeline: &mut ReplicationPipeline, index: &mut u64, lent: &mut Vec<InflightAppend>| {
            *index += 1;
            if lend {
                cycle(pipeline, *index, payload, Some(lent));
            } else {
                cycle(pipeline, *index, payload, None);
            }
        };

    for _ in 0..64 {
        run(&mut pipeline, &mut index, &mut lent);
    }

    let before_allocs = ALLOCATIONS.load(Ordering::Relaxed);
    let before_bytes = BYTES.load(Ordering::Relaxed);
    for _ in 0..cycles {
        run(&mut pipeline, &mut index, &mut lent);
    }
    let allocs = ALLOCATIONS.load(Ordering::Relaxed) - before_allocs;
    let bytes = BYTES.load(Ordering::Relaxed) - before_bytes;
    println!(
        "{label:<44} {:>8.3} {:>10.1}",
        allocs as f64 / cycles as f64,
        bytes as f64 / cycles as f64
    );
}

fn main() {
    println!(
        "{:<44} {:>8} {:>10}",
        "one follower's share of a proposal", "allocs", "bytes"
    );
    // The entry built per cycle is one allocation of `payload` bytes in both
    // rows, so it is the floor rather than something the pipeline did.
    measure("flush into a fresh vector (what it was)", 2000, 256, false);
    measure("flush into a lent vector (what it is)", 2000, 256, true);
    println!();
    println!("The queue and the response keep their containers, so the whole");
    println!("difference between the rows is the vector the flush writes into.");
}
