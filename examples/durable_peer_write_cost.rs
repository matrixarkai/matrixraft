// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Durable WAL growth for independent peers exchanging encoded packets in process.
//! Run with --features peer-runtime. This does not measure network throughput.

#[cfg(feature = "peer-runtime")]
mod probe {
    use matrixraft::peer_runtime::{DurablePeer, PeerOptions};
    use std::collections::BTreeMap;

    type Peers = BTreeMap<u64, DurablePeer>;
    type Applied = BTreeMap<u64, Vec<Vec<u8>>>;

    fn pump(peers: &mut Peers, applied: &mut Applied) {
        for _ in 0..100 {
            let mut packets = Vec::new();
            for (id, peer) in peers.iter_mut() {
                packets.extend(
                    peer.process_ready(|_, payload| {
                        // Elections apply an empty no-op; proposals here are nonempty.
                        if !payload.is_empty() {
                            applied.entry(*id).or_default().push(payload.to_vec());
                        }
                        Ok(())
                    })
                    .expect("persist and apply"),
                );
            }
            if packets.is_empty() {
                return;
            }
            for packet in packets {
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
            "peers,payload_bytes,batch,proposals,wal_growth_bytes,cluster_bytes_per_payload_byte"
        );
        for count in [1u64, 3, 5, 9] {
            for payload_bytes in [64usize, 4096] {
                for batch in [1usize, 16] {
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
                    for chunk in expected.chunks(batch) {
                        for payload in chunk {
                            peers
                                .get_mut(&1)
                                .unwrap()
                                .propose(payload.clone())
                                .expect("propose");
                        }
                        pump(&mut peers, &mut applied);
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
                        "{count},{payload_bytes},{batch},{ENTRIES},{growth},{:.4}",
                        growth as f64 / (ENTRIES * payload_bytes) as f64
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
