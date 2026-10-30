// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Allocation volume for durable single-voter Ready processing.
//! Run with `cargo run --features peer-runtime --example durable_ready_allocations`.
//! Payload construction and proposal admission are outside the measured region.
//! These are allocated bytes, not peak memory, disk traffic, or throughput.

#[cfg(feature = "peer-runtime")]
mod probe {
    use matrixraft::peer_runtime::{DurablePeer, PeerOptions};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static BYTES: AtomicUsize = AtomicUsize::new(0);
    struct Counting;
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
    }
    #[global_allocator]
    static ALLOCATOR: Counting = Counting;

    pub fn run() {
        println!("entries,payload_bytes,ready_allocated_bytes");
        for count in [1, 32] {
            for width in [4096, 65536] {
                let dir = tempfile::tempdir().unwrap();
                let options = PeerOptions {
                    group_id: 17,
                    node_id: 1,
                    voters: vec![1],
                    wal_path: dir.path().join("peer.wal"),
                };
                let mut peer = DurablePeer::open(options.clone()).unwrap();
                peer.process_ready(|_, _| Ok(())).unwrap();
                peer.campaign().unwrap();
                peer.process_ready(|_, _| Ok(())).unwrap();
                assert!(peer.is_leader());
                let payloads: Vec<_> = (0..count).map(|i| vec![i as u8; width]).collect();
                let positions = peer.propose_batch(payloads).unwrap();
                let mut applied = 0;
                let before = BYTES.load(Ordering::Relaxed);
                let packets = peer
                    .process_ready(|id, bytes| {
                        assert_eq!(id, positions[applied]);
                        assert_eq!(bytes.len(), width);
                        assert!(bytes.iter().all(|b| *b == applied as u8));
                        applied += 1;
                        Ok(())
                    })
                    .unwrap();
                let allocated = BYTES.load(Ordering::Relaxed) - before;
                assert!(packets.is_empty());
                assert_eq!(applied, count);
                drop(peer);
                let mut replay = DurablePeer::open(options).unwrap();
                let mut restored = 0;
                replay
                    .process_ready(|id, bytes| {
                        if bytes.is_empty() {
                            assert!(id.index < positions[0].index);
                            return Ok(());
                        }
                        assert_eq!(id, positions[restored]);
                        assert_eq!(bytes.len(), width);
                        assert!(bytes.iter().all(|b| *b == restored as u8));
                        restored += 1;
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(restored, count);
                println!("{count},{width},{allocated}");
            }
        }
    }
}

fn main() {
    #[cfg(feature = "peer-runtime")]
    probe::run();
    #[cfg(not(feature = "peer-runtime"))]
    eprintln!("enable --features peer-runtime");
}
