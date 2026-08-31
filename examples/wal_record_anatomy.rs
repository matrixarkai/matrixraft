// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Anatomy probe: shows where the bytes in one WAL record go.
//!
//! `wal_amplification` reports that a 64-byte proposal costs ~598 WAL bytes.
//! This says which fields those bytes are, so an optimisation targets the part
//! that is actually large rather than the part that is easiest to see.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};
use serde_json::Value;

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn field_sizes(label: &str, json: &str) {
    let value: Value = serde_json::from_str(json).expect("record is json");
    println!("{label}: {} bytes total", json.len());
    let Value::Object(map) = &value else {
        return;
    };
    let mut rows: Vec<(String, usize)> = map
        .iter()
        .map(|(key, field)| {
            // key + quotes + colon + comma, plus the encoded value
            let encoded = serde_json::to_string(field).expect("field encodes");
            (key.clone(), key.len() + 4 + encoded.len())
        })
        .collect();
    rows.sort_by_key(|(_, size)| std::cmp::Reverse(*size));
    for (key, size) in rows {
        let share = size as f64 * 100.0 / json.len() as f64;
        println!("    {size:>6} B  {share:>5.1}%  {key}");
    }
    println!();
}

fn record_json(payload_bytes: usize, byte_value: u8) -> String {
    let mut cluster = RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("start");
    cluster.campaign(1, true).expect("campaign");
    cluster
        .propose(vec![byte_value; payload_bytes])
        .expect("propose");
    let record = cluster.wal_record_for(1).expect("wal record");
    serde_json::to_string(&record).expect("record encodes")
}

/// How often each top-level field actually changes from one record to the next.
///
/// A field that almost never changes is a candidate for the delta treatment
/// `entries` already gets: write it on the first record of a segment and omit
/// it afterwards while it is unchanged. A field that changes every time is not,
/// however large it looks in the anatomy above.
fn field_volatility(proposals: usize, payload_bytes: usize) {
    let mut cluster = RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("start");
    cluster.campaign(1, true).expect("campaign");

    let mut previous: Option<serde_json::Map<String, Value>> = None;
    let mut changes: std::collections::BTreeMap<String, usize> = Default::default();
    let mut sizes: std::collections::BTreeMap<String, usize> = Default::default();
    let mut compared = 0usize;

    for _ in 0..proposals {
        cluster
            .propose(vec![200u8; payload_bytes])
            .expect("propose");
        let record = cluster.wal_record_for(1).expect("wal record");
        let value = serde_json::to_value(&record).expect("record to value");
        let Value::Object(map) = value else { continue };
        for (key, field) in &map {
            let encoded = serde_json::to_string(field).expect("field encodes");
            sizes.insert(key.clone(), key.len() + 4 + encoded.len());
        }
        if let Some(prev) = &previous {
            compared += 1;
            for (key, field) in &map {
                if prev.get(key) != Some(field) {
                    *changes.entry(key.clone()).or_default() += 1;
                }
            }
        }
        previous = Some(map);
    }

    // `entries` here is the whole retained log, because `wal_record_for` builds
    // a full record. The WAL's own append path writes a delta instead, so treat
    // the `entries` row below as "changes every record", not as its real size.
    println!("field volatility over {compared} consecutive records (payload {payload_bytes} B)");
    let mut rows: Vec<(String, usize, usize)> = sizes
        .into_iter()
        .map(|(key, size)| {
            let changed = changes.get(&key).copied().unwrap_or(0);
            (key, size, changed)
        })
        .collect();
    rows.sort_by_key(|(_, size, _)| std::cmp::Reverse(*size));
    println!(
        "    {:>7}  {:>9}  {:>10}  field",
        "bytes", "changed", "rate"
    );
    let mut stable_bytes = 0usize;
    for (key, size, changed) in rows {
        let rate = changed as f64 * 100.0 / compared.max(1) as f64;
        if changed == 0 {
            stable_bytes += size;
        }
        println!("    {size:>7}  {changed:>9}  {rate:>9.1}%  {key}");
    }
    println!("    -> {stable_bytes} B per record never changed and could be omitted\n");
}

fn main() {
    // Byte value matters: serde_json writes each byte as a decimal number, so a
    // payload of 7s costs two characters per byte and a payload of 200s costs
    // four. Raw bytes are uniform in practice, so the high case is the honest
    // one to design against.
    for (label, payload_bytes, byte_value) in [
        ("empty payload", 0usize, 0u8),
        ("64 B payload of 0x07", 64, 7),
        ("64 B payload of 0xC8", 64, 200),
        ("4096 B payload of 0xC8", 4096, 200),
    ] {
        let json = record_json(payload_bytes, byte_value);
        field_sizes(label, &json);
        if payload_bytes > 0 {
            let ratio = json.len() as f64 / payload_bytes as f64;
            println!("    -> {ratio:.1}x amplification for this record\n");
        }
    }

    // Size alone does not say what is worth compressing. A big field that
    // changes every record has to be written every record.
    field_volatility(200, 64);
}
