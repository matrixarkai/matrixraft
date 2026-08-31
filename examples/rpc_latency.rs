// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Latency probe for the real TCP transport, over loopback.
//!
//! Every other probe in this directory measures in-process work.  This one
//! measures a Raft RPC as a peer actually experiences it: connect, encode,
//! write, wait, read, decode.
//!
//! Percentiles rather than a mean, because the costs being hunted here are
//! stalls -- a poll interval or a Nagle interaction shows up as a fat tail
//! while barely moving an average.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use matrixraft::{
    AppendEntriesRequest, ClusterRaftTransport, Config, LogEntry, LogId, Peer, RaftCluster,
    ReplicaRole, TcpRaftTransport, TcpRaftTransportServer, Transport,
};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn percentile(sorted_us: &[f64], q: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() - 1) as f64 * q).round() as usize;
    sorted_us[idx]
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(300);
    let payload_bytes: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(64);
    // Concurrent callers stand in for the other peers of a group all talking to
    // one node. The server accepts and serves connections inline on a single
    // thread, so if that serialises them the tail grows with this number while
    // the minimum stays put.
    let threads: usize = std::env::args()
        .nth(3)
        .and_then(|a| a.parse().ok())
        .unwrap_or(1);

    let cluster = Arc::new(Mutex::new(
        RaftCluster::new(3, Config::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster"),
    ));
    cluster.lock().expect("lock").start().expect("start");
    let handler = Arc::new(ClusterRaftTransport::new(Arc::clone(&cluster)));
    let mut server =
        TcpRaftTransportServer::start("127.0.0.1:0", handler).expect("start tcp server");

    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    let transport = TcpRaftTransport::new(peers);

    // Always index 1: the follower validates that entries are contiguous with
    // its log, and this probe is measuring transport latency, not replication
    // progress. Sending increasing indexes just makes every RPC fail
    // validation -- and a failure returns about a microsecond, which would look
    // like a spectacular result rather than a broken measurement.
    let request = |index: u64| AppendEntriesRequest {
        group_id: 3,
        term: 1,
        leader_id: 1,
        prev_log_id: None,
        entries: vec![LogEntry {
            log_id: LogId { term: 1, index },
            payload: vec![7u8; payload_bytes],
            is_command: true,
        }],
        leader_commit: 0,
        lease_epoch: 0,
    };

    // Warm up: the first RPC pays for lazily-created state on both sides.
    for _ in 0..10 {
        let _ = transport.append_entries(2, request(1));
    }

    // Count outcomes. A failing RPC returns in about a microsecond, which is
    // far too fast for a loopback round trip -- timing it would measure the
    // error path and report a spectacular latency that means nothing.
    let transport = Arc::new(transport);
    let collected: Vec<(Vec<f64>, Option<String>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let transport = Arc::clone(&transport);
                let request = &request;
                scope.spawn(move || {
                    let mut mine = Vec::with_capacity(iterations);
                    let mut failure: Option<String> = None;
                    for _ in 0..iterations {
                        let started = Instant::now();
                        let result = transport.append_entries(2, request(1));
                        let elapsed = started.elapsed().as_secs_f64() * 1e6;
                        match result {
                            Ok(_) => mine.push(elapsed),
                            Err(err) => {
                                if failure.is_none() {
                                    failure = Some(format!("{err:?}"));
                                }
                            }
                        }
                    }
                    (mine, failure)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("probe thread"))
            .collect()
    });
    let mut ok = 0usize;
    let mut failed: Option<String> = None;
    let mut samples = Vec::with_capacity(iterations * threads);
    for (mine, failure) in collected {
        ok += mine.len();
        samples.extend(mine);
        if failed.is_none() {
            failed = failure;
        }
    }
    let iterations = iterations * threads;
    println!("ok={ok}/{iterations}");
    if let Some(err) = failed {
        println!("  first failure: {err}");
    }
    if samples.is_empty() {
        println!("  no successful RPCs: nothing to measure");
        server.shutdown().expect("shutdown server");
        return;
    }
    server.shutdown().expect("shutdown server");

    samples.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    println!("rpcs={iterations}  payload_bytes={payload_bytes}  threads={threads}  (loopback)");
    println!(
        "  min {:>9.1} us   p50 {:>9.1}   p90 {:>9.1}   p99 {:>9.1}   max {:>9.1}   mean {:>9.1}",
        samples[0],
        percentile(&samples, 0.50),
        percentile(&samples, 0.90),
        percentile(&samples, 0.99),
        samples[samples.len() - 1],
        mean
    );
}
