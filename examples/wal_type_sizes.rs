// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What each field of a `WalRecord` costs before any payload is stored.

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, SnapshotMetadata, WalRecord,
    WalSegment,
};

fn main() {
    let rows: [(&str, usize); 9] = [
        ("WalRecord (whole)", std::mem::size_of::<WalRecord>()),
        ("  HardState", std::mem::size_of::<HardState>()),
        ("  Membership", std::mem::size_of::<Membership>()),
        (
            "  Option<SnapshotMetadata>",
            std::mem::size_of::<Option<SnapshotMetadata>>(),
        ),
        (
            "  SnapshotMetadata",
            std::mem::size_of::<SnapshotMetadata>(),
        ),
        (
            "  ApplySnapshotFence",
            std::mem::size_of::<ApplySnapshotFence>(),
        ),
        ("  Vec<LogEntry>", std::mem::size_of::<Vec<LogEntry>>()),
        ("LogEntry", std::mem::size_of::<LogEntry>()),
        ("WalSegment", std::mem::size_of::<WalSegment>()),
    ];
    for (name, size) in rows {
        println!("{name:<28} {size:>5} bytes");
    }
    println!();
    println!(
        "Option<Box<SnapshotMetadata>> would be {} bytes instead of {}",
        std::mem::size_of::<Option<Box<SnapshotMetadata>>>(),
        std::mem::size_of::<Option<SnapshotMetadata>>()
    );
    println!("LogId {} bytes", std::mem::size_of::<LogId>());
}
