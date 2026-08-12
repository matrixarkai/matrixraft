// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ReferenceRaft-style byte quota and backpressure helpers.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftRateLimitDecision {
    pub allowed: bool,
    pub requested_bytes: u64,
    pub granted_bytes: u64,
    pub available_before: u64,
    pub available_after: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftRateLimiterStats {
    pub total_granted_bytes: u64,
    pub total_rejected_bytes: u64,
    pub total_refilled_bytes: u64,
    pub grant_count: u64,
    pub rejection_count: u64,
}

pub trait RustRaftRateLimiter {
    fn reserve_bytes(&mut self, requested_bytes: u64) -> RustRaftRateLimitDecision;
    fn reserve_limited_bytes(&mut self, requested_bytes: u64) -> RustRaftRateLimitDecision;
    fn refill_bytes(&mut self, bytes: u64);
    fn available_bytes(&self) -> u64;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftByteQuotaLimiter {
    capacity_bytes: u64,
    available_bytes: u64,
    stats: RustRaftRateLimiterStats,
}

impl RustRaftByteQuotaLimiter {
    pub fn new(capacity_bytes: u64) -> Self {
        Self::with_available(capacity_bytes, capacity_bytes)
    }

    pub fn with_available(capacity_bytes: u64, available_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            available_bytes: available_bytes.min(capacity_bytes),
            stats: RustRaftRateLimiterStats::default(),
        }
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub fn stats(&self) -> RustRaftRateLimiterStats {
        self.stats.clone()
    }

    fn decision(
        &self,
        allowed: bool,
        requested_bytes: u64,
        granted_bytes: u64,
        available_before: u64,
        available_after: u64,
        reason: &str,
    ) -> RustRaftRateLimitDecision {
        RustRaftRateLimitDecision {
            allowed,
            requested_bytes,
            granted_bytes,
            available_before,
            available_after,
            reason: reason.to_string(),
        }
    }
}

impl RustRaftRateLimiter for RustRaftByteQuotaLimiter {
    fn reserve_bytes(&mut self, requested_bytes: u64) -> RustRaftRateLimitDecision {
        let available_before = self.available_bytes;
        if requested_bytes == 0 {
            return self.decision(
                true,
                requested_bytes,
                0,
                available_before,
                available_before,
                "zero_request",
            );
        }
        if requested_bytes > self.capacity_bytes {
            self.stats.rejection_count = self.stats.rejection_count.saturating_add(1);
            self.stats.total_rejected_bytes = self
                .stats
                .total_rejected_bytes
                .saturating_add(requested_bytes);
            return self.decision(
                false,
                requested_bytes,
                0,
                available_before,
                available_before,
                "request_exceeds_capacity",
            );
        }
        if requested_bytes > self.available_bytes {
            self.stats.rejection_count = self.stats.rejection_count.saturating_add(1);
            self.stats.total_rejected_bytes = self
                .stats
                .total_rejected_bytes
                .saturating_add(requested_bytes);
            return self.decision(
                false,
                requested_bytes,
                0,
                available_before,
                available_before,
                "quota_unavailable",
            );
        }
        self.available_bytes -= requested_bytes;
        self.stats.grant_count = self.stats.grant_count.saturating_add(1);
        self.stats.total_granted_bytes = self
            .stats
            .total_granted_bytes
            .saturating_add(requested_bytes);
        self.decision(
            true,
            requested_bytes,
            requested_bytes,
            available_before,
            self.available_bytes,
            "granted",
        )
    }

    fn reserve_limited_bytes(&mut self, requested_bytes: u64) -> RustRaftRateLimitDecision {
        let available_before = self.available_bytes;
        if requested_bytes == 0 {
            return self.decision(
                true,
                requested_bytes,
                0,
                available_before,
                available_before,
                "zero_request",
            );
        }
        if self.capacity_bytes == 0 || self.available_bytes == 0 {
            self.stats.rejection_count = self.stats.rejection_count.saturating_add(1);
            self.stats.total_rejected_bytes = self
                .stats
                .total_rejected_bytes
                .saturating_add(requested_bytes);
            return self.decision(
                false,
                requested_bytes,
                0,
                available_before,
                available_before,
                "quota_unavailable",
            );
        }
        let granted_bytes = requested_bytes.min(self.available_bytes);
        self.available_bytes = self.available_bytes.saturating_sub(granted_bytes);
        self.stats.grant_count = self.stats.grant_count.saturating_add(1);
        self.stats.total_granted_bytes =
            self.stats.total_granted_bytes.saturating_add(granted_bytes);
        self.decision(
            true,
            requested_bytes,
            granted_bytes,
            available_before,
            self.available_bytes,
            if granted_bytes == requested_bytes {
                "granted"
            } else {
                "partial_granted"
            },
        )
    }

    fn refill_bytes(&mut self, bytes: u64) {
        let available_before = self.available_bytes;
        self.available_bytes = self
            .available_bytes
            .saturating_add(bytes)
            .min(self.capacity_bytes);
        self.stats.total_refilled_bytes = self
            .stats
            .total_refilled_bytes
            .saturating_add(self.available_bytes.saturating_sub(available_before));
    }

    fn available_bytes(&self) -> u64 {
        self.available_bytes
    }
}
