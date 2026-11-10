// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]
use matrixraft::{
    peer_runtime::{DurablePeer, PeerOptions, RestoreDecision},
    RaftError,
};
use std::{collections::BTreeMap, io::Read};
static DROP_FILE_PACKET: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[derive(prost::Message)]
struct PacketVersion {
    #[prost(uint32, tag = "1")]
    version: u32,
}
struct Measured;
static LARGEST: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static ALLOCATED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static EXERCISE: std::sync::Mutex<()> = std::sync::Mutex::new(());
unsafe impl std::alloc::GlobalAlloc for Measured {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        LARGEST.fetch_max(layout.size(), std::sync::atomic::Ordering::Relaxed);
        ALLOCATED.fetch_add(layout.size(), std::sync::atomic::Ordering::Relaxed);
        std::alloc::System.alloc(layout)
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        std::alloc::System.dealloc(pointer, layout);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        LARGEST.fetch_max(size, std::sync::atomic::Ordering::Relaxed);
        ALLOCATED.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        std::alloc::System.realloc(pointer, layout, size)
    }
}
#[global_allocator]
static ALLOCATOR: Measured = Measured;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct State {
    length: u64,
    value: u8,
}
struct Generated {
    remaining: u64,
    value: u8,
    largest: usize,
}
impl Read for Generated {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        self.largest = self.largest.max(out.len());
        let n = self.remaining.min(out.len() as u64) as usize;
        out[..n].fill(self.value);
        self.remaining -= n as u64;
        Ok(n)
    }
}

#[test]
fn file_checkpoint_restore_can_retry_from_the_beginning_without_loading_the_payload() {
    let _exercise = EXERCISE.lock().unwrap();
    let root = tempfile::tempdir().unwrap();
    let options = PeerOptions {
        group_id: 17,
        node_id: 1,
        voters: vec![1],
        wal_path: root.path().join("peer.wal"),
    };
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    let position = peer.applied_index();
    let length = 128 * 1024;
    let mut generated = Generated {
        remaining: length,
        value: 37,
        largest: 0,
    };
    peer.checkpoint_from_reader(&mut generated, length).unwrap();
    drop(peer);
    let mut peer = DurablePeer::open(options).unwrap();
    assert_eq!(peer.applied_index(), 0);
    for _ in 0..3 {
        peer.process_ready_batch_with_restore(
            |_| panic!("unexpected suffix"),
            |_, reader, size| {
                assert_eq!(size, length);
                let mut first = [0; 8];
                reader.read_exact(&mut first).unwrap();
                assert_eq!(first, [37; 8]);
                Ok(RestoreDecision::Retry)
            },
        )
        .unwrap();
        assert_eq!(peer.applied_index(), 0);
        assert!(peer.has_ready());
    }
    peer.process_ready_batch_with_restore(
        |_| panic!("unexpected suffix"),
        |_, reader, size| {
            let mut remaining = size;
            let mut buffer = [0; 8192];
            while remaining > 0 {
                let count = remaining.min(buffer.len() as u64) as usize;
                reader.read_exact(&mut buffer[..count]).unwrap();
                assert!(buffer[..count].iter().all(|byte| *byte == 37));
                remaining -= count as u64;
            }
            Ok(RestoreDecision::Restored)
        },
    )
    .unwrap();
    assert_eq!(peer.applied_index(), position);
    assert!(!peer.has_ready());
}

#[test]
fn a_bounded_log_range_materializes_only_the_requested_payloads() {
    let _exercise = EXERCISE.lock().unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut peer = DurablePeer::open(PeerOptions {
        group_id: 17,
        node_id: 1,
        voters: vec![1],
        wal_path: root.path().join("peer.wal"),
    })
    .unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    let low = peer.commit_index() + 1;
    for value in 0..40u8 {
        peer.propose_batch(vec![vec![value; 32768]; 4]).unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    let high = peer.commit_index() + 1;
    let mut reader = peer.capture_log_reader().unwrap();
    ALLOCATED.store(0, std::sync::atomic::Ordering::Relaxed);
    let selected = reader.entries(low, high, Some(1)).unwrap();
    let allocated = ALLOCATED.load(std::sync::atomic::Ordering::Relaxed);
    println!("bounded read allocated {allocated} bytes for a 32 KiB result across 5 MiB of payload history");
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].index, low);
    assert_eq!(selected[0].data, vec![0; 32768]);
    assert!(
        allocated < 1024 * 1024,
        "bounded read allocated {allocated} bytes"
    );
}

fn drive(
    peers: &mut BTreeMap<u64, DurablePeer>,
    states: &mut BTreeMap<u64, State>,
    isolate: Option<u64>,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut exchanges = 0;
    while exchanges < 2000 {
        assert!(
            std::time::Instant::now() < deadline,
            "checkpoint exchange timed out"
        );
        let mut packets = Vec::new();
        for (id, peer) in peers.iter_mut() {
            let state = std::cell::Cell::new(states.get(id).copied().unwrap_or_default());
            let result = peer
                .process_ready_with_snapshot_reader(
                    |_, bytes| {
                        if !bytes.is_empty() {
                            assert_eq!(bytes.len(), 9);
                            state.set(State {
                                length: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
                                value: bytes[8],
                            });
                        }
                        Ok(())
                    },
                    |_, reader, length| {
                        let mut buffer = [0; 65536];
                        let mut remaining = length;
                        let mut value = None;
                        while remaining > 0 {
                            let n = remaining.min(buffer.len() as u64) as usize;
                            reader
                                .read_exact(&mut buffer[..n])
                                .map_err(|e| RaftError::Storage(e.to_string()))?;
                            let byte = *value.get_or_insert(buffer[0]);
                            assert!(buffer[..n].iter().all(|v| *v == byte));
                            remaining -= n as u64;
                        }
                        state.set(State {
                            length,
                            value: value.unwrap_or(0),
                        });
                        Ok(())
                    },
                )
                .unwrap();
            states.insert(*id, state.get());
            packets.extend(
                result
                    .into_iter()
                    .filter(|p| Some(*id) != isolate && Some(p.to) != isolate),
            );
        }
        if packets.is_empty() {
            if peers.values().any(DurablePeer::checkpoint_receive_pending) {
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            return;
        }
        exchanges += 1;
        for packet in packets {
            assert!(packet.bytes.len() < 2 * 1024 * 1024);
            let version = <PacketVersion as prost::Message>::decode(packet.bytes.as_slice())
                .unwrap()
                .version;
            if version == 3 && DROP_FILE_PACKET.swap(false, std::sync::atomic::Ordering::Relaxed) {
                continue;
            }
            if version == 3 {
                peers
                    .get_mut(&packet.to)
                    .unwrap()
                    .step(&packet.bytes)
                    .unwrap();
            }
            let before = ALLOCATED.load(std::sync::atomic::Ordering::Relaxed);
            peers
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
            let allocated = ALLOCATED.load(std::sync::atomic::Ordering::Relaxed) - before;
            if version == 3 && packet.bytes.len() >= 1024 * 1024 {
                // Envelope and fragment each own one decoded buffer. Reject
                // extra payload copies or a separate duplicate-read allocation.
                assert!(
                    allocated < 2 * packet.bytes.len() + 8192,
                    "duplicate packet of {} bytes allocated {allocated}",
                    packet.bytes.len()
                );
            }
        }
    }
    panic!("streamed checkpoint exchange did not settle");
}
fn exercise(length: u64) {
    let _measurement = EXERCISE.lock().unwrap();
    LARGEST.store(0, std::sync::atomic::Ordering::Relaxed);
    DROP_FILE_PACKET.store(true, std::sync::atomic::Ordering::Relaxed);
    let dir = tempfile::tempdir().unwrap();
    let options = |id| PeerOptions {
        group_id: 71,
        node_id: id,
        voters: vec![1, 2, 3],
        wal_path: dir.path().join(format!("{id}.wal")),
    };
    let mut peers: BTreeMap<_, _> = (1..=3)
        .map(|id| (id, DurablePeer::open(options(id)).unwrap()))
        .collect();
    let mut states = BTreeMap::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    drive(&mut peers, &mut states, None);
    let mut command = length.to_be_bytes().to_vec();
    command.push(5);
    peers.get_mut(&1).unwrap().propose(command).unwrap();
    drive(&mut peers, &mut states, Some(3));
    assert_eq!(states[&1], State { length, value: 5 });
    let mut input = Generated {
        remaining: length,
        value: 5,
        largest: 0,
    };
    peers
        .get_mut(&1)
        .unwrap()
        .checkpoint_from_reader(&mut input, length)
        .unwrap();
    assert!(input.largest <= 65536);
    assert!(std::fs::metadata(&options(1).wal_path).unwrap().len() < 65536);
    let mut command = length.to_be_bytes().to_vec();
    command.push(7);
    peers.get_mut(&1).unwrap().propose(command).unwrap();
    drive(&mut peers, &mut states, None);
    for _ in 0..40 {
        if states.get(&3) == Some(&State { length, value: 7 }) {
            break;
        }
        for peer in peers.values_mut() {
            peer.tick().unwrap();
        }
        drive(&mut peers, &mut states, None);
    }
    for id in 1..=3 {
        assert_eq!(states[&id], State { length, value: 7 });
    }
    drop(peers);
    states.clear();
    let mut peers: BTreeMap<_, _> = (1..=3)
        .map(|id| (id, DurablePeer::open(options(id)).unwrap()))
        .collect();
    drive(&mut peers, &mut states, None);
    for id in 1..=3 {
        assert_eq!(states[&id], State { length, value: 7 });
    }
    drop(peers);
    assert!(!DROP_FILE_PACKET.load(std::sync::atomic::Ordering::Relaxed));
    let largest = LARGEST.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        largest < 8 * 1024 * 1024,
        "checkpoint used a full-payload allocation: {largest}"
    );
    println!("FILE_CHECKPOINT bytes={length} largest_allocation={largest}");
    // A referenced payload cannot be missing or damaged on reopen.
    let payload_dir = dir.path().join("1.wal.payloads");
    let payload = std::fs::read_dir(&payload_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(payload).unwrap();
    assert!(DurablePeer::open(options(1)).is_err());
}
#[test]
fn file_checkpoints_transfer_to_lagging_voters_and_recover_the_suffix() {
    exercise(9 * 1024 * 1024);
}
#[test]
#[ignore = "bounded 272 MiB file capture, transfer and full restart"]
fn file_checkpoints_above_the_inline_limit_restore_without_full_payload_buffers() {
    exercise(272 * 1024 * 1024);
}
