// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A group's queue depth has to be the group's own.
//!
//! `MailChannel::overflow` asks whether the channel is full like this:
//!
//! ```text
//!     inner.selector_total_mail_count as usize > self.num_mail_limit
//! ```
//!
//! The limit is the channel's (`max_queue_depth`, one group's worth). The
//! count is the **selector's**, shared by every group that worker serves,
//! and it is a copy refreshed only when that channel is drained. So a
//! channel whose own queue is empty refuses mail because its neighbours were
//! busy -- and having refused it, it receives nothing, so it is never
//! drained, so the stale copy is never corrected. It stays shut.
//!
//! This is what a store of four thousand groups ran into. The pool gives
//! each worker its own selector, so the count crosses `max_queue_depth` when
//! a worker serves more groups than that number -- 1024 by default. Every
//! size measured agrees: 3072 groups over 4 shards (768 each) is fine, 4096
//! over 4 (1024 each) is not, 4096 over 8 (512 each) is fine, 8192 over 8
//! (1024 each) is not. In the failing runs all four workers sat parked in
//! `select` with nothing to do while the groups they owned went unticked and
//! stopped answering.

use std::sync::Arc;

use matrixraft::{ChannelSelector, MailChannel, MailPriority};

#[test]
fn a_channel_is_not_shut_by_how_busy_its_neighbours_were() {
    let selector: ChannelSelector<u64> = ChannelSelector::new();
    // One group's worth of depth, and more groups than that on this worker.
    let depth = 4;
    let channels: Vec<Arc<MailChannel<u64>>> = (1..=(depth as u64 + 2))
        .map(|id| MailChannel::new(id, depth))
        .collect();

    // One mail each. No channel is anywhere near its own depth of 4.
    for channel in &channels {
        selector
            .try_send_to_channel(Arc::clone(channel), MailPriority::Normal, 1)
            .expect("a channel holding nothing must accept its first mail");
    }

    // A worker drains them, which is when each one refreshes its copy of the
    // selector-wide total. By the last, that total has passed 4.
    for channel in &channels {
        let taken = channel.fetch(&selector);
        assert_eq!(taken.len(), 1, "each channel held exactly one mail");
    }

    // Every queue is now empty. Every one must accept mail again.
    let mut shut = Vec::new();
    for channel in &channels {
        if selector
            .try_send_to_channel(Arc::clone(channel), MailPriority::Normal, 2)
            .is_err()
        {
            shut.push(channel.replica_id());
        }
    }
    assert!(
        shut.is_empty(),
        "channels {shut:?} refused mail while their own queues were empty, because \
         the depth bound is comparing this channel's limit against the whole \
         selector's mail count"
    );
}

#[test]
fn a_group_that_fills_its_own_queue_is_still_refused() {
    // The control. The bound above must not be fixed by removing it: a group
    // that really is at its depth has to be refused, or a slow group grows
    // without limit.
    let selector: ChannelSelector<u64> = ChannelSelector::new();
    let depth = 4;
    let channel: Arc<MailChannel<u64>> = MailChannel::new(1, depth);

    for n in 0..depth {
        selector
            .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, n as u64)
            .unwrap_or_else(|_| panic!("mail {n} is within the depth of {depth}"));
    }
    assert_eq!(channel.queued_len(), depth, "all of them queued");

    assert!(
        selector
            .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, 99)
            .is_err(),
        "a channel at its own depth of {depth} accepted another mail, so the bound \
         is not bounding anything"
    );
}
