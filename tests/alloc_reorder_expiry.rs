// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Reorder timeout expiry does not allocate a list of keys to remove.
//!
//! A 32-entry expiry used to build and grow that list four times, allocating
//! 480 bytes. The in-place tree pass retains no scratch between calls.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{LogEntry, LogId, PipelineLimits, ReplicationPipeline};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = TRACK_ALLOCATIONS.try_with(|tracking| {
            if tracking.get() {
                ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
                BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: Vec::new(),
        is_command: true,
    }
}

#[test]
fn expiring_a_full_reorder_window_uses_no_heap_scratch() {
    let mut pipeline = ReplicationPipeline::new(
        2,
        1,
        PipelineLimits {
            enable_reorder_queue: true,
            reorder_window_size: 32,
            reorder_timeout_us: 100,
            ..PipelineLimits::default()
        },
    );
    for index in 2..=33 {
        pipeline
            .receive_out_of_order(&entry(index))
            .expect("entry fits the reorder window");
    }

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let dropped = pipeline.tick_reorder_queue(100);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    println!("32-entry reorder expiry: {allocations} allocations and {bytes} bytes");
    assert_eq!(dropped, 32);
    assert_eq!(pipeline.status().reorder_queue_depth, 0);
    assert_eq!(allocations, 0, "reorder expiry allocated key scratch");
    assert_eq!(bytes, 0, "reorder expiry retained heap scratch");
}
