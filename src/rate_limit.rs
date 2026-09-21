// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BaselineRaft-style byte quota and backpressure helpers.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::DriverTickReceiver;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateLimitDecision {
    pub allowed: bool,
    pub requested_bytes: u64,
    pub granted_bytes: u64,
    pub available_before: u64,
    pub available_after: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateLimiterStats {
    pub total_granted_bytes: u64,
    pub total_rejected_bytes: u64,
    pub total_refilled_bytes: u64,
    pub grant_count: u64,
    pub rejection_count: u64,
}

pub trait RateLimiter {
    fn reserve_bytes(&mut self, requested_bytes: u64) -> RateLimitDecision;
    fn reserve_limited_bytes(&mut self, requested_bytes: u64) -> RateLimitDecision;
    fn refill_bytes(&mut self, bytes: u64);
    fn available_bytes(&self) -> u64;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteQuotaLimiter {
    capacity_bytes: u64,
    available_bytes: u64,
    stats: RateLimiterStats,
}

impl ByteQuotaLimiter {
    pub fn new(capacity_bytes: u64) -> Self {
        Self::with_available(capacity_bytes, capacity_bytes)
    }

    pub fn with_available(capacity_bytes: u64, available_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            available_bytes: available_bytes.min(capacity_bytes),
            stats: RateLimiterStats::default(),
        }
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub fn stats(&self) -> RateLimiterStats {
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
    ) -> RateLimitDecision {
        RateLimitDecision {
            allowed,
            requested_bytes,
            granted_bytes,
            available_before,
            available_after,
            reason: reason.to_string(),
        }
    }
}

impl RateLimiter for ByteQuotaLimiter {
    fn reserve_bytes(&mut self, requested_bytes: u64) -> RateLimitDecision {
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

    fn reserve_limited_bytes(&mut self, requested_bytes: u64) -> RateLimitDecision {
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

/// Puts a limiter's quota back on a cycle, driven by the driver's tick.
///
/// A [`ByteQuotaLimiter`] hands out bytes and never gets them back on its own —
/// `refill_bytes` is a method someone has to call. `check_cycle_sec` names how
/// often, and this is what spends it: register a refiller with a
/// [`crate::Driver`] at that interval and every tick tops the limiter back up to
/// capacity.
///
/// Without one, a configured limiter throttles once and then stays empty, which
/// looks like a snapshot that mysteriously stopped.
pub struct RateLimiterRefiller {
    limiter: Arc<Mutex<ByteQuotaLimiter>>,
    refills: AtomicU64,
    bytes_restored: AtomicU64,
}

impl std::fmt::Debug for RateLimiterRefiller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiterRefiller")
            .field("refills", &self.refills.load(Ordering::Relaxed))
            .field(
                "bytes_restored",
                &self.bytes_restored.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl RateLimiterRefiller {
    pub fn new(limiter: Arc<Mutex<ByteQuotaLimiter>>) -> Arc<Self> {
        Arc::new(Self {
            limiter,
            refills: AtomicU64::new(0),
            bytes_restored: AtomicU64::new(0),
        })
    }

    /// The limiter being refilled, for the caller that also spends from it.
    pub fn limiter(&self) -> Arc<Mutex<ByteQuotaLimiter>> {
        Arc::clone(&self.limiter)
    }

    /// Tops the limiter back up to capacity and reports the bytes restored.
    pub fn refill_now(&self) -> u64 {
        let restored = {
            let mut limiter = self.limiter.lock().expect("rate limiter mutex poisoned");
            let missing = limiter
                .capacity_bytes()
                .saturating_sub(limiter.available_bytes());
            if missing > 0 {
                limiter.refill_bytes(missing);
            }
            missing
        };
        self.refills.fetch_add(1, Ordering::Relaxed);
        self.bytes_restored.fetch_add(restored, Ordering::Relaxed);
        restored
    }

    pub fn refills(&self) -> u64 {
        self.refills.load(Ordering::Relaxed)
    }

    pub fn bytes_restored(&self) -> u64 {
        self.bytes_restored.load(Ordering::Relaxed)
    }
}

impl DriverTickReceiver for RateLimiterRefiller {
    fn fire_tick(&self) {
        self.refill_now();
    }
}
