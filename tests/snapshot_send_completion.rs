// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A snapshot send is complete when every chunk has gone out, not when enough
//! accepts have come back.
//!
//! `record_send_response` counted accepted responses and called the send
//! finished once the count reached the chunk count. A response does not say
//! which chunk it accepts, and a duplicated one is expected -- the early return
//! at the top of that function exists precisely to absorb an accept arriving
//! after the send has finished. Counting alone, duplicates reach the chunk
//! count while later chunks have never been transmitted.
//!
//! Measured before the fix: four accepts for two chunks completed a 4,096-byte
//! transfer having sent only offsets 0 and 1,024. `completed` is what tells a
//! leader the follower holds the snapshot, so the leader stops sending while
//! the follower waits for chunks nobody will send again.

use matrixraft::{
    InstallSnapshotResponse, LogId, RaftSnapshot, SnapshotLifecycle, SnapshotLifecycleConfig,
    SnapshotMetadata,
};

const CHUNK: u64 = 1024;
const PAYLOAD: usize = 4096;

fn snapshot() -> RaftSnapshot {
    let payload: Vec<u8> = (0..PAYLOAD as u32).map(|i| (i % 251) as u8).collect();
    RaftSnapshot {
        group_id: 9,
        meta: SnapshotMetadata {
            snapshot_id: "send-completion".to_string(),
            last_log_id: LogId { term: 3, index: 40 },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: payload.len() as u64,
        },
        payload,
    }
}

/// One chunk per tick, so an accept can be duplicated between chunks.
fn lifecycle() -> SnapshotLifecycle {
    SnapshotLifecycle::new(SnapshotLifecycleConfig {
        chunk_size: CHUNK,
        max_chunks_per_tick: 1,
        ..Default::default()
    })
    .expect("build the lifecycle")
}

fn accept(next_offset: u64) -> InstallSnapshotResponse {
    InstallSnapshotResponse {
        term: 3,
        accepted: true,
        next_offset,
        committed_index: 0,
        reason: "accepted".to_string(),
        throttled: false,
    }
}

#[test]
fn a_duplicated_accept_does_not_finish_a_send_that_has_chunks_left() {
    let mut life = lifecycle();
    let snapshot = snapshot();
    life.begin_send(&snapshot, 1, 3).expect("begin send");

    let mut sent = Vec::new();
    let mut rounds = 0;
    while life.status().sending && rounds < 20 {
        rounds += 1;
        let requests = life.poll_send_requests().expect("poll send requests");
        assert!(
            !requests.is_empty(),
            "the send is active but offered nothing"
        );
        for request in &requests {
            let through = request.chunk.offset + request.chunk.data.len() as u64;
            sent.push(request.chunk.offset);
            // The same accept twice, as a retried RPC delivers it.
            life.record_send_response(&accept(through))
                .expect("first accept");
            if life.status().sending {
                life.record_send_response(&accept(through))
                    .expect("the duplicate");
            }
        }
    }

    assert_eq!(
        sent,
        vec![0, 1024, 2048, 3072],
        "every chunk of the payload must go out before the send is complete"
    );
    assert!(life.status().completed);
    assert!(!life.status().sending);
}

#[test]
fn a_send_completes_on_the_last_chunk_and_not_before() {
    let mut life = lifecycle();
    let snapshot = snapshot();
    life.begin_send(&snapshot, 1, 3).expect("begin send");

    let expected_chunks = PAYLOAD / CHUNK as usize;
    for nth in 1..=expected_chunks {
        let requests = life.poll_send_requests().expect("poll send requests");
        assert_eq!(requests.len(), 1, "one chunk per tick was configured");
        let request = &requests[0];
        let through = request.chunk.offset + request.chunk.data.len() as u64;
        life.record_send_response(&accept(through)).expect("accept");

        if nth < expected_chunks {
            assert!(
                !life.status().completed,
                "the send cannot be complete after {nth} of {expected_chunks} chunks"
            );
            assert!(life.status().sending);
        }
    }
    assert!(life.status().completed, "the last chunk completes the send");
    assert!(!life.status().sending);
}

#[test]
fn an_accept_arriving_after_the_send_finished_is_absorbed() {
    // The behaviour the early return in `record_send_response` exists for, and
    // the reason duplicates cannot simply be rejected: a late accept is normal.
    let mut life = lifecycle();
    let snapshot = snapshot();
    life.begin_send(&snapshot, 1, 3).expect("begin send");

    while life.status().sending {
        for request in life.poll_send_requests().expect("poll send requests") {
            let through = request.chunk.offset + request.chunk.data.len() as u64;
            life.record_send_response(&accept(through)).expect("accept");
        }
    }
    assert!(life.status().completed);

    life.record_send_response(&accept(PAYLOAD as u64))
        .expect("a late accept is not an error");
    assert!(life.status().completed);
}
