// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{RustRaftByteQuotaLimiter, RustRaftRateLimiter};

#[test]
fn byte_quota_limiter_grants_rejects_and_refills_like_reference_raft_quota_gate() {
    let mut limiter = RustRaftByteQuotaLimiter::new(10);

    let first = limiter.reserve_bytes(7);
    assert!(first.allowed);
    assert_eq!(first.granted_bytes, 7);
    assert_eq!(first.available_before, 10);
    assert_eq!(first.available_after, 3);
    assert_eq!(first.reason, "granted");

    let rejected = limiter.reserve_bytes(4);
    assert!(!rejected.allowed);
    assert_eq!(rejected.reason, "quota_unavailable");
    assert_eq!(limiter.available_bytes(), 3);

    limiter.refill_bytes(5);
    assert_eq!(limiter.available_bytes(), 8);

    let oversized = limiter.reserve_bytes(11);
    assert!(!oversized.allowed);
    assert_eq!(oversized.reason, "request_exceeds_capacity");

    let stats = limiter.stats();
    assert_eq!(stats.grant_count, 1);
    assert_eq!(stats.rejection_count, 2);
    assert_eq!(stats.total_granted_bytes, 7);
    assert_eq!(stats.total_rejected_bytes, 15);
    assert_eq!(stats.total_refilled_bytes, 5);
}

#[test]
fn byte_quota_limiter_can_grant_partial_quota_like_matrixraft_streaming() {
    let mut limiter = RustRaftByteQuotaLimiter::with_available(10, 3);

    let partial = limiter.reserve_limited_bytes(8);
    assert!(partial.allowed);
    assert_eq!(partial.requested_bytes, 8);
    assert_eq!(partial.granted_bytes, 3);
    assert_eq!(partial.available_before, 3);
    assert_eq!(partial.available_after, 0);
    assert_eq!(partial.reason, "partial_granted");

    let blocked = limiter.reserve_limited_bytes(1);
    assert!(!blocked.allowed);
    assert_eq!(blocked.reason, "quota_unavailable");

    limiter.refill_bytes(10);
    let capped = limiter.reserve_limited_bytes(12);
    assert!(capped.allowed);
    assert_eq!(capped.granted_bytes, 10);
    assert_eq!(capped.reason, "partial_granted");

    let stats = limiter.stats();
    assert_eq!(stats.grant_count, 2);
    assert_eq!(stats.rejection_count, 1);
    assert_eq!(stats.total_granted_bytes, 13);
    assert_eq!(stats.total_rejected_bytes, 1);
}
