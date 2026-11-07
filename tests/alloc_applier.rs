// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The shared applier moves the pool's owned batch into the state machine.
//!
//! The pool already bounds each delivery by `apply_max_batch_count`. Cloning
//! that vector inside `GroupApplier` allocated a second vector and deep-cloned
//! every entry payload. Moving it removed that copy but still dropped the
//! delivery vector after every call. A consuming handler can now return the
//! empty storage to the worker, making the warmed path allocation-free.
//!
//! One test, in a file of its own, because the counter is the process
//! allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use matrixraft::{
    Applier, ApplierOptions, ApplyHandler, ApplyReport, ApplyTask, DriverGroupKey, LogEntry, LogId,
};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

#[derive(Default)]
struct Handler {
    handled: AtomicU64,
    payload_address: AtomicUsize,
}

impl Handler {
    fn record(&self, tasks: &[ApplyTask]) -> u64 {
        if let Some(payload) = tasks
            .first()
            .and_then(|task| task.entries.first())
            .map(|entry| &entry.payload)
        {
            self.payload_address
                .store(payload.as_ptr() as usize, Ordering::Relaxed);
        }
        self.handled
            .fetch_add(tasks.len() as u64, Ordering::Relaxed);
        0
    }
}

impl ApplyHandler for Handler {
    fn apply(&self, tasks: Vec<ApplyTask>) -> u64 {
        self.record(&tasks)
    }

    fn apply_reporting_recycling(
        &self,
        mut tasks: Vec<ApplyTask>,
    ) -> (ApplyReport, Vec<ApplyTask>) {
        let report = ApplyReport::reached(self.record(&tasks));
        tasks.clear();
        (report, tasks)
    }
}

fn wait_for(handler: &Handler, want: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while handler.handled.load(Ordering::Relaxed) < want {
        if Instant::now() >= deadline {
            panic!(
                "only {} of {want} tasks were handled",
                handler.handled.load(Ordering::Relaxed)
            );
        }
        std::thread::yield_now();
    }
}

#[test]
fn a_bounded_apply_moves_one_owned_batch_without_cloning_it() {
    let rounds = 20_000u64;
    let key = DriverGroupKey::new(1, 1);
    let applier = Applier::start(ApplierOptions {
        applier_num: 1,
        apply_max_batch_count: 1,
        max_queue_depth: 8,
    })
    .expect("applier");
    let handler = Arc::new(Handler::default());
    applier
        .register_group(key, Arc::clone(&handler) as Arc<dyn ApplyHandler>)
        .expect("register");

    let owned_payload = vec![7u8; 64];
    let payload_address = owned_payload.as_ptr() as usize;
    applier
        .submit(
            key,
            ApplyTask {
                entries: vec![LogEntry {
                    log_id: LogId { term: 1, index: 1 },
                    payload: owned_payload,
                    is_command: false,
                }],
                snapshot: None,
            },
        )
        .expect("submit owned task");
    wait_for(&handler, 1);
    assert_eq!(
        handler.payload_address.load(Ordering::Relaxed),
        payload_address,
        "the state machine should receive the submitted payload allocation"
    );

    for done in 2..=1_001 {
        applier
            .submit(
                key,
                ApplyTask {
                    entries: Vec::new(),
                    snapshot: None,
                },
            )
            .expect("warm submit");
        wait_for(&handler, done);
    }

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for done in 1..=rounds {
        applier
            .submit(
                key,
                ApplyTask {
                    entries: Vec::new(),
                    snapshot: None,
                },
            )
            .expect("measured submit");
        wait_for(&handler, 1_001 + done);
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    let per_apply = allocations as f64 / rounds as f64;
    println!("allocations per one-task apply: {per_apply:.4}");
    assert_eq!(
        allocations, 0,
        "a bounded apply should reuse the pool delivery vector, got {per_apply:.4} allocations per apply"
    );
}
