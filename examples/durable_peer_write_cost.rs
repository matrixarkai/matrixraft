// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Durable WAL growth for independent peers exchanging encoded packets in process.
//! Run with --features peer-runtime. This does not measure network throughput.
//! Ready timing includes persistence and application capture on the local host.
//! Individual admission defers Ready processing; grouped admission uses propose_batch.

#[cfg(feature = "peer-runtime")]
mod probe {
    use matrixraft::peer_runtime::{DurablePeer, PeerOptions};
    use std::collections::BTreeMap;
    use std::time::Instant;

    type Peers = BTreeMap<u64, DurablePeer>;
    type Applied = BTreeMap<u64, Vec<Vec<u8>>>;

    #[derive(Default)]
    struct Cost {
        packets: usize,
        packet_bytes: usize,
        max_ready_us: u128,
    }

    fn pump(peers: &mut Peers, applied: &mut Applied) -> Cost {
        let mut cost = Cost::default();
        for _ in 0..100 {
            let mut packets = Vec::new();
            for (id, peer) in peers.iter_mut() {
                let started = Instant::now();
                let output = peer
                    .process_ready(|_, payload| {
                        // Elections apply an empty no-op; proposals here are nonempty.
                        if !payload.is_empty() {
                            applied.entry(*id).or_default().push(payload.to_vec());
                        }
                        Ok(())
                    })
                    .expect("persist and apply");
                cost.max_ready_us = cost.max_ready_us.max(started.elapsed().as_micros());
                packets.extend(output);
            }
            if packets.is_empty() {
                return cost;
            }
            for packet in packets {
                cost.packets += 1;
                cost.packet_bytes += packet.bytes.len();
                peers
                    .get_mut(&packet.to)
                    .expect("destination")
                    .step(&packet.bytes)
                    .expect("receive packet");
            }
        }
        panic!("message exchange did not settle");
    }

    pub fn run() {
        const ENTRIES: usize = 64;
        println!(
            "peers,payload_bytes,batch,admission,proposals,wal_growth_bytes,cluster_bytes_per_payload_byte,packets,packet_bytes,max_ready_us"
        );
        for count in [1u64, 3, 5, 9] {
            for payload_bytes in [64usize, 4096] {
                for (batch, grouped) in [(1usize, false), (16, false), (16, true)] {
                    let dir = tempfile::tempdir().expect("temporary directory");
                    let voters: Vec<_> = (1..=count).collect();
                    let options = |id| PeerOptions {
                        group_id: 17,
                        node_id: id,
                        voters: voters.clone(),
                        wal_path: dir.path().join(format!("peer-{id}.wal")),
                    };
                    let open = || {
                        voters
                            .iter()
                            .map(|id| (*id, DurablePeer::open(options(*id)).expect("open peer")))
                            .collect::<Peers>()
                    };
                    let bytes = || {
                        voters
                            .iter()
                            .map(|id| {
                                std::fs::metadata(options(*id).wal_path)
                                    .expect("WAL metadata")
                                    .len()
                            })
                            .sum::<u64>()
                    };
                    let mut peers = open();
                    let mut applied = Applied::new();
                    peers.get_mut(&1).unwrap().campaign().expect("campaign");
                    pump(&mut peers, &mut applied);
                    assert!(peers[&1].is_leader());
                    let initial = bytes();
                    let expected: Vec<_> =
                        (0..ENTRIES).map(|i| vec![i as u8; payload_bytes]).collect();
                    let mut cost = Cost::default();
                    for chunk in expected.chunks(batch) {
                        if grouped {
                            peers
                                .get_mut(&1)
                                .unwrap()
                                .propose_batch(chunk.to_vec())
                                .expect("propose batch");
                        } else {
                            for payload in chunk {
                                peers
                                    .get_mut(&1)
                                    .unwrap()
                                    .propose(payload.clone())
                                    .expect("propose");
                            }
                        }
                        let measured = pump(&mut peers, &mut applied);
                        cost.packets += measured.packets;
                        cost.packet_bytes += measured.packet_bytes;
                        cost.max_ready_us = cost.max_ready_us.max(measured.max_ready_us);
                    }
                    for id in &voters {
                        assert_eq!(applied.get(id), Some(&expected));
                    }
                    let growth = bytes().checked_sub(initial).expect("append-only WAL");
                    drop(peers);
                    let mut recovered = open();
                    let mut replayed = Applied::new();
                    pump(&mut recovered, &mut replayed);
                    for id in &voters {
                        assert_eq!(replayed.get(id), Some(&expected));
                    }
                    println!(
                        "{count},{payload_bytes},{batch},{},{ENTRIES},{growth},{:.4},{},{},{}",
                        if grouped { "grouped" } else { "individual" },
                        growth as f64 / (ENTRIES * payload_bytes) as f64,
                        cost.packets,
                        cost.packet_bytes,
                        cost.max_ready_us,
                    );
                }
            }
        }
    }
}

#[cfg(feature = "peer-runtime")]
fn main() {
    probe::run();
}

#[cfg(not(feature = "peer-runtime"))]
fn main() {
    eprintln!("enable --features peer-runtime to run this measurement");
    std::process::exit(2);
}
