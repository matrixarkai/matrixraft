// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use matrixraft::{RustRaftMailBox, RustRaftMailBoxFetchPolicy, RustRaftMailPriority};

#[test]
fn mailbox_try_send_applies_per_priority_high_watermark() {
    let mailbox = RustRaftMailBox::new(2);

    assert!(mailbox.try_send(RustRaftMailPriority::Normal, 1));
    assert!(mailbox.try_send(RustRaftMailPriority::Normal, 2));
    assert!(!mailbox.try_send(RustRaftMailPriority::Normal, 3));
    assert!(mailbox.try_send(RustRaftMailPriority::Urgent, 4));

    assert_eq!(mailbox.len(RustRaftMailPriority::Normal), 2);
    assert_eq!(mailbox.len(RustRaftMailPriority::Urgent), 1);
}

#[test]
fn mailbox_fetch_prioritizes_urgent_and_limits_included_lanes() {
    let mailbox = RustRaftMailBox::new(8);
    mailbox.send(RustRaftMailPriority::Slowly, "slow-1");
    mailbox.send(RustRaftMailPriority::Normal, "normal-1");
    mailbox.send(RustRaftMailPriority::Urgent, "urgent-1");
    mailbox.send(RustRaftMailPriority::Normal, "normal-2");
    mailbox.send(RustRaftMailPriority::Slowly, "slow-2");

    let fetched = mailbox.fetch(RustRaftMailBoxFetchPolicy {
        limit: 2,
        timeout_ms: 0,
        include_until: RustRaftMailPriority::Urgent,
    });
    assert_eq!(fetched, vec!["urgent-1", "normal-1"]);
    assert_eq!(mailbox.total_len(), 3);

    let fetched = mailbox.fetch(RustRaftMailBoxFetchPolicy {
        limit: 1,
        timeout_ms: 0,
        include_until: RustRaftMailPriority::Normal,
    });
    assert_eq!(fetched, vec!["normal-2"]);
    assert_eq!(mailbox.total_len(), 2);

    let fetched = mailbox.fetch(RustRaftMailBoxFetchPolicy {
        limit: 8,
        timeout_ms: 0,
        include_until: RustRaftMailPriority::Slowly,
    });
    assert_eq!(fetched, vec!["slow-1", "slow-2"]);
    assert!(mailbox.is_empty());
}

#[test]
fn mailbox_wait_and_send_unblocks_after_fetch() {
    let mailbox = Arc::new(RustRaftMailBox::new(1));
    assert!(mailbox.try_send(RustRaftMailPriority::Normal, 1));

    let sending = Arc::clone(&mailbox);
    let handle = thread::spawn(move || {
        sending.wait_and_send(RustRaftMailPriority::Normal, 2);
    });

    thread::sleep(Duration::from_millis(10));
    assert_eq!(mailbox.len(RustRaftMailPriority::Normal), 1);
    assert_eq!(
        mailbox.fetch(RustRaftMailBoxFetchPolicy {
            limit: 1,
            timeout_ms: 0,
            include_until: RustRaftMailPriority::Urgent,
        }),
        vec![1]
    );

    handle.join().expect("wait_and_send joins");
    assert_eq!(
        mailbox.fetch(RustRaftMailBoxFetchPolicy {
            limit: 1,
            timeout_ms: 0,
            include_until: RustRaftMailPriority::Urgent,
        }),
        vec![2]
    );
}

#[test]
fn mailbox_fetch_waits_for_timeout_or_arriving_mail() {
    let mailbox = RustRaftMailBox::<u64>::new(1);
    let start = Instant::now();
    let fetched = mailbox.fetch(RustRaftMailBoxFetchPolicy {
        limit: 1,
        timeout_ms: 5,
        include_until: RustRaftMailPriority::Urgent,
    });
    assert!(fetched.is_empty());
    assert!(start.elapsed() >= Duration::from_millis(5));

    let mailbox = Arc::new(RustRaftMailBox::new(1));
    let sending = Arc::clone(&mailbox);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        sending.send(RustRaftMailPriority::Urgent, 9);
    });

    assert_eq!(
        mailbox.fetch(RustRaftMailBoxFetchPolicy {
            limit: 1,
            timeout_ms: 100,
            include_until: RustRaftMailPriority::Urgent,
        }),
        vec![9]
    );
}
