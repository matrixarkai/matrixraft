// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Allocation volume for durable Ready processing, packet admission, and checkpoint reopen.
//! Run with `cargo run --features peer-runtime --example durable_ready_allocations`.
//! Payload construction and proposal admission are outside the measured region.
//! These are allocated bytes, not peak memory, disk traffic, or throughput.

#[cfg(feature = "peer-runtime")]
mod probe {
    use matrixraft::{
        peer_runtime::{DurablePeer, PeerOptions},
        LogId,
    };
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

    type Applied = Vec<Vec<(LogId, Vec<u8>)>>;

    fn exchange(peers: &mut [DurablePeer], applied: &mut Applied) -> (usize, usize, usize) {
        let mut received = 0;
        let mut wire_bytes = 0;
        let mut allocated = 0;
        for _ in 0..100 {
            let mut packets = Vec::new();
            for (index, peer) in peers.iter_mut().enumerate() {
                packets.extend(
                    peer.process_ready(|id, bytes| {
                        if !bytes.is_empty() {
                            applied[index].push((id, bytes.to_vec()));
                        }
                        Ok(())
                    })
                    .unwrap(),
                );
            }
            if packets.is_empty() {
                return (received, wire_bytes, allocated);
            }
            for packet in packets {
                let peer = &mut peers[packet.to as usize - 1];
                let before = BYTES.load(Ordering::Relaxed);
                peer.step(&packet.bytes).unwrap();
                allocated += BYTES.load(Ordering::Relaxed) - before;
                received += 1;
                wire_bytes += packet.bytes.len();
            }
        }
        panic!("packet exchange did not settle");
    }

    fn receive_cost() {
        println!("voters,payload_bytes,packets,packet_bytes,step_allocated_bytes");
        for width in [64, 4096, 1024 * 1024] {
            let dir = tempfile::tempdir().unwrap();
            let options: Vec<_> = (1..=3)
                .map(|node_id| PeerOptions {
                    group_id: 17,
                    node_id,
                    voters: vec![1, 2, 3],
                    wal_path: dir.path().join(format!("peer-{node_id}.wal")),
                })
                .collect();
            let open = || {
                options
                    .iter()
                    .map(|o| DurablePeer::open(o.clone()).unwrap())
                    .collect::<Vec<_>>()
            };
            let mut peers = open();
            let mut applied = vec![Vec::new(); 3];
            peers[0].campaign().unwrap();
            exchange(&mut peers, &mut applied);
            assert!(peers[0].is_leader());
            assert!(applied.iter().all(Vec::is_empty));
            let data = vec![42; width];
            let id = peers[0].propose(data.clone()).unwrap();
            let (packets, bytes, allocated) = exchange(&mut peers, &mut applied);
            let expected = vec![(id, data)];
            assert!(packets >= 4);
            assert!(applied.iter().all(|values| *values == expected));
            drop(peers);
            let mut peers = open();
            let mut replay = vec![Vec::new(); 3];
            exchange(&mut peers, &mut replay);
            assert_eq!(replay, applied);
            println!("3,{width},{packets},{bytes},{allocated}");
        }
    }

    pub fn run() {
        println!("entries,payload_bytes,ready_allocated_bytes,replay_open_allocated_bytes");
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
                let before = BYTES.load(Ordering::Relaxed);
                let mut replay = DurablePeer::open(options).unwrap();
                let reopen_allocated = BYTES.load(Ordering::Relaxed) - before;
                assert!(reopen_allocated < 6 * count * width + 16384 + 1024 * count);
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
                println!("{count},{width},{allocated},{reopen_allocated}");
            }
        }
        receive_cost();
        reopen_cost();
    }

    fn reopen_cost() {
        println!("checkpoint_bytes,reopen_allocated_bytes");
        for width in [1024 * 1024, 5 * 1024 * 1024] {
            let dir = tempfile::tempdir().unwrap();
            let options = PeerOptions {
                group_id: 17,
                node_id: 1,
                voters: vec![1],
                wal_path: dir.path().join("peer.wal"),
            };
            let mut peer = DurablePeer::open(options.clone()).unwrap();
            peer.campaign().unwrap();
            peer.process_ready(|_, _| Ok(())).unwrap();
            let position = peer.applied_index();
            peer.checkpoint(vec![42; width]).unwrap();
            drop(peer);
            let before = BYTES.load(Ordering::Relaxed);
            let mut peer = DurablePeer::open(options).unwrap();
            let allocated = BYTES.load(Ordering::Relaxed) - before;
            let mut seen = false;
            peer.process_ready_with_snapshot_reader(
                |_, _| panic!("unexpected suffix"),
                |id, reader, length| {
                    assert_eq!(id.index, position);
                    assert_eq!(length, width as u64);
                    let mut buffer = [0; 8192];
                    let mut total = 0;
                    loop {
                        let count = reader.read(&mut buffer).unwrap();
                        if count == 0 {
                            break;
                        }
                        assert!(buffer[..count].iter().all(|b| *b == 42));
                        total += count;
                    }
                    assert_eq!(total, width);
                    seen = true;
                    Ok(())
                },
            )
            .unwrap();
            assert!(seen);
            println!("{width},{allocated}");
        }
    }
}

fn main() {
    #[cfg(feature = "peer-runtime")]
    probe::run();
    #[cfg(not(feature = "peer-runtime"))]
    eprintln!("enable --features peer-runtime");
}
