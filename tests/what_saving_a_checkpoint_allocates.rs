// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What saving a snapshot checkpoint allocates, against what it writes.
//!
//! A checkpoint file costs 1.000 bytes per payload byte, which is the floor.
//! Allocation is a separate question, and it was **3.0x the payload**: three
//! full copies of a snapshot that can be the whole state machine.
//!
//! ```text
//!    payload   allocated before   allocated now
//!      1 KiB          3,646              1,568
//!     64 KiB        197,186             66,082
//!      1 MiB      3,146,306          1,049,122
//!      8 MiB     25,166,406          8,389,156
//! ```
//!
//! so **3.0006x, now 1.0005x**. The three copies were:
//!
//! 1. the payload cloned into the message,
//! 2. `encode_to_vec`'s output buffer,
//! 3. that buffer copied again into the framed output.
//!
//! The third arrived with the frame itself. Only one is unavoidable -- the
//! bytes have to exist somewhere -- so the encoder now writes the message
//! straight into the framed buffer and appends the payload field itself, and
//! patches the frame's length and digest afterwards because a digest cannot be
//! known before the bytes it covers.
//!
//! **The format did not change.** The output is byte-identical to what a
//! filled-in message encodes, which
//! `the_hand_written_payload_field_matches_what_prost_would_write` asserts
//! directly against prost across nine payload lengths, including both sides of
//! two varint boundaries. It caught one real difference while this was written:
//! proto3 omits an empty `bytes` field, and a hand-written zero-length one
//! produced two bytes prost does not -- invisible to every byte count in the
//! sibling suite, whose smallest payload is 3 KiB.
//!
//! ## Why this is measured here rather than inferred
//!
//! "One fewer copy" is a claim about code. This counts the bytes, with a
//! `#[global_allocator]` that adds up allocations while a save runs, so a
//! regression shows up as a number rather than as a review comment nobody
//! writes. Bounded rather than pinned exactly, because an allocator's growth
//! pattern is its own business -- the bound is set where it separates one copy
//! from two, which is the thing worth defending.
//!
//! The counter is **per-thread**, because cargo runs tests in parallel and a
//! global one measures whatever every other thread is doing. That failure does
//! not announce itself: it looks like a plausible figure in the wrong place. A
//! 1 MiB payload first read 8,389,156 bytes -- the 8 MiB case's number -- and
//! putting a mutex around each measurement only moved it to 2.0088x, because a
//! lock serialises the measurements and not the allocations happening beside
//! them. Per-thread needs no lock and measures the right thing.
//!
//! The box this runs on is RAM-bound before it is disk-bound: a release link
//! here needs about 5 GB against roughly 3.8 GB available. For a snapshot of
//! real size the difference between holding one copy and three is the
//! difference between saving it and being killed.

use matrixraft::{
    GenericSnapshot, LogId, PersistentRaftSnapshotStore, PersistentRaftSnapshotStoreOptions,
    RaftSnapshot, SnapshotMetadata,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

thread_local! {
    /// Bytes this thread has allocated while it was counting.
    ///
    /// Per-thread, not global, because cargo runs tests in parallel and a
    /// global counter measures whatever every other thread happens to be
    /// doing. That failure does not look like an error: it looks like a
    /// plausible number in the wrong place. A 1 MiB payload first reported
    /// 8,389,156 bytes -- the 8 MiB case's figure -- and after a mutex was put
    /// around each measurement it reported 2.0088x, because a lock serialises
    /// the MEASUREMENTS and not the other threads' ordinary allocations, which
    /// keep landing in whichever count is open.
    ///
    /// `const` init and a `Cell` with no destructor, so touching this from
    /// inside the allocator cannot allocate or run code during thread
    /// teardown.
    static ALLOCATED: Cell<u64> = const { Cell::new(0) };
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

fn count(bytes: usize) {
    // `try_with`, because an allocation can happen after this thread's locals
    // have been destroyed, and that must not panic inside the allocator.
    let _ = COUNTING.try_with(|on| {
        if on.get() {
            let _ = ALLOCATED.try_with(|total| total.set(total.get() + bytes as u64));
        }
    });
}

/// Counts this thread's allocated bytes while it is counting.
///
/// A growing `realloc` is counted as the growth only, since that is what the
/// program asked for; a shrinking one is not counted at all. Deallocation is
/// not tracked, because the question is how much a save asks for and not what
/// it holds at any instant -- the two differ, and the cheaper one to measure is
/// the one that moves when a copy is added.
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > layout.size() {
            count(new_size - layout.size());
        }
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("rustraft-{name}-{}-{nonce}", std::process::id()))
}

fn snapshot(payload_len: usize) -> RaftSnapshot {
    GenericSnapshot {
        group_id: 9,
        meta: SnapshotMetadata {
            snapshot_id: "alloc-1".to_string(),
            last_log_id: LogId { term: 3, index: 7 },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: payload_len as u64,
        },
        payload: vec![b'x'; payload_len],
    }
}

/// Bytes allocated by one `save_checkpoint`, and the file it produced.
fn allocated_by_a_save(payload_len: usize) -> (u64, u64) {
    let dir = temp_dir("alloc");
    let store =
        PersistentRaftSnapshotStore::open(PersistentRaftSnapshotStoreOptions::new(dir.clone()))
            .expect("the store opens");
    // Built BEFORE counting starts, so the fixture's own payload is not counted
    // as part of the save.
    let value = snapshot(payload_len);

    ALLOCATED.with(|total| total.set(0));
    COUNTING.with(|on| on.set(true));
    let path = store.save_checkpoint(&value).expect("the checkpoint saves");
    COUNTING.with(|on| on.set(false));

    let allocated = ALLOCATED.with(|total| total.get());
    let written = std::fs::metadata(&path).expect("metadata").len();
    let _ = std::fs::remove_dir_all(&dir);
    (allocated, written)
}

#[test]
fn saving_a_checkpoint_allocates_one_copy_of_the_payload_and_not_three() {
    for payload in [65_536usize, 1_048_576, 8 * 1_048_576] {
        let (allocated, written) = allocated_by_a_save(payload);

        // The bound separates one copy from two. Measured at 1.0005x for a
        // megabyte; 1.5x leaves room for the envelope and for an allocator
        // rounding a request up, and still reddens the moment a second copy of
        // the payload appears.
        assert!(
            allocated < payload as u64 * 3 / 2,
            "saving a {payload}-byte payload allocated {allocated} bytes, which is {:.4}x -- a second copy of the payload is back",
            allocated as f64 / payload as f64
        );

        // And it is not LESS than the file it wrote, which would mean the
        // measurement is not seeing the buffer the bytes were assembled in --
        // the failure that would make the bound above pass for the wrong reason.
        assert!(
            allocated >= written,
            "a save of a {payload}-byte payload allocated {allocated} bytes and wrote {written}, so the counter is missing the buffer"
        );
    }
}

/// The ratio falls as the payload grows, which is what a fixed envelope does.
///
/// Stated as a trend rather than a figure because the envelope is constant and
/// the payload is not, so the ratio has to approach 1 from above. If it stopped
/// doing that, the cost would have gained a term that scales with the payload
/// -- which is exactly the regression the bound above is for, seen from the
/// other side.
#[test]
fn the_allocation_ratio_approaches_one_as_the_payload_grows() {
    let (small, _) = allocated_by_a_save(65_536);
    let (large, _) = allocated_by_a_save(8 * 1_048_576);
    let small_ratio = small as f64 / 65_536.0;
    let large_ratio = large as f64 / (8.0 * 1_048_576.0);
    assert!(
        large_ratio < small_ratio,
        "the ratio did not fall with the payload: {small_ratio:.4} at 64 KiB and {large_ratio:.4} at 8 MiB, so something in the cost now scales with the payload"
    );
    assert!(
        large_ratio < 1.01,
        "an 8 MiB payload allocated {large_ratio:.4}x, which is more than a fixed envelope over a payload that size can account for"
    );
}
