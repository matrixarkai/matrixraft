// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;

use matrixraft::{UniqueIdGenerator, MATRIXRAFT_UNIQUE_ID_COUNTER_MASK};

#[test]
fn unique_id_generator_matches_matrixraft_packing() {
    let generator = UniqueIdGenerator::new(0x12, 0x3456);
    let id = generator.next_id();
    assert_eq!(id, 0x12000000345601);

    for offset in 1..1000 {
        assert_eq!(generator.next_id(), id + offset);
    }

    let decoded = UniqueIdGenerator::decode(id);
    assert_eq!(decoded.member_id, 0x12);
    assert_eq!(decoded.timestamp_millis, 0x3456);
    assert_eq!(decoded.counter, 1);
}

#[test]
fn unique_id_generator_masks_member_and_timestamp() {
    let generator = UniqueIdGenerator::new(0x1_0001, 0x1_0000_0000_0002);
    let decoded = UniqueIdGenerator::decode(generator.next_id());

    assert_eq!(decoded.member_id, 1);
    assert_eq!(decoded.timestamp_millis, 2);
    assert_eq!(decoded.counter, 1);
}

#[test]
fn unique_id_counter_rolls_into_timestamp_bits() {
    let generator = UniqueIdGenerator::new(7, 8);

    let first = generator.next_id();
    for _ in 0..MATRIXRAFT_UNIQUE_ID_COUNTER_MASK {
        generator.next_id();
    }
    let rolled = UniqueIdGenerator::decode(generator.next_id());

    assert_eq!(UniqueIdGenerator::decode(first).timestamp_millis, 8);
    assert_eq!(rolled.timestamp_millis, 9);
    assert_eq!(rolled.counter, 1);
}

#[test]
fn unique_id_generator_is_atomic_for_concurrent_request_ids() {
    let generator = Arc::new(UniqueIdGenerator::new(3, 4));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let generator = Arc::clone(&generator);
        handles.push(thread::spawn(move || {
            (0..128).map(|_| generator.next_id()).collect::<Vec<_>>()
        }));
    }

    let mut ids = BTreeSet::new();
    for handle in handles {
        for id in handle.join().expect("generator thread joins") {
            assert!(ids.insert(id), "duplicate id {id}");
        }
    }
    assert_eq!(ids.len(), 1024);
}
