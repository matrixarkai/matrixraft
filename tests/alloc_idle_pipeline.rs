// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Idle replication pipelines retain no heap block for zero-valued telemetry.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{LogEntry, LogId, PipelineLimits, ProgressState, ReplicationPipeline};

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

fn measured<T>(run: impl FnOnce() -> T) -> (T, usize, usize) {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let value = run();
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    (
        value,
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

#[test]
fn idle_status_stays_allocation_free_and_telemetry_allocates_once() {
    let (_, construct_allocations, construct_bytes) = measured(|| {
        black_box(ReplicationPipeline::new(1, 1, PipelineLimits::default()));
    });
    assert_eq!((construct_allocations, construct_bytes), (0, 0));

    let mut pipeline = ReplicationPipeline::new(2, 1, PipelineLimits::default());

    let (_, idle_allocations, idle_bytes) = measured(|| {
        for _ in 0..1_000 {
            black_box(pipeline.status());
        }
    });
    assert_eq!((idle_allocations, idle_bytes), (0, 0));

    let mut receiving = ReplicationPipeline::new(3, 1, PipelineLimits::default());
    let entry = LogEntry {
        log_id: LogId { term: 1, index: 1 },
        payload: Vec::new(),
        is_command: true,
    };
    let (received, receive_allocations, receive_bytes) =
        measured(|| receiving.receive_out_of_order(&entry));
    assert!(received.is_ok());
    assert_eq!((receive_allocations, receive_bytes), (0, 0));

    let (transitioned, first_allocations, first_bytes) =
        measured(|| pipeline.record_network_error());
    assert!(
        !transitioned,
        "a probing peer does not change progress state"
    );
    assert_eq!(first_allocations, 1, "the first counter earns one block");
    assert!(first_bytes > 0, "the telemetry block must have storage");

    let (_, reset_allocations, reset_bytes) = measured(|| {
        pipeline.reset_for_leader_transition(0, 1, ProgressState::Probe);
    });
    assert_eq!((reset_allocations, reset_bytes), (0, 0));
    assert_eq!(pipeline.status().packet_loss_events, 0);

    let (_, reuse_allocations, reuse_bytes) = measured(|| {
        black_box(pipeline.record_network_error());
    });
    assert_eq!((reuse_allocations, reuse_bytes), (0, 0));

    let (_, repeated_allocations, repeated_bytes) = measured(|| {
        for _ in 0..1_000 {
            black_box(pipeline.record_network_error());
        }
    });
    assert_eq!((repeated_allocations, repeated_bytes), (0, 0));
    assert_eq!(pipeline.status().packet_loss_events, 1_001);
}
