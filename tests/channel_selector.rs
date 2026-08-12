// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;

use matrixraft::{
    RustRaftChannelSelector, RustRaftChannelSelectorPolicy, RustRaftMailChannel,
    RustRaftMailPriority,
};

#[test]
fn channel_selector_selects_unique_active_channels_like_matrixraft() {
    let selector = RustRaftChannelSelector::<u64>::new();
    let channel_1 = RustRaftMailChannel::<u64>::new(1, 100);
    let channel_2 = RustRaftMailChannel::<u64>::new(2, 100);

    assert!(selector.fire(Arc::clone(&channel_1)));
    assert!(!selector.fire(Arc::clone(&channel_1)));
    assert!(selector.fire(Arc::clone(&channel_2)));

    let selection = selector.select(
        RustRaftChannelSelectorPolicy {
            limit: 1,
            timeout_ms: 0,
        },
        &[],
    );
    assert_eq!(selection.channels.len(), 1);
    assert_eq!(selection.channels[0].replica_id(), 1);
    assert!(selection.has_active_channels_left);

    let selection = selector.select(
        RustRaftChannelSelectorPolicy {
            limit: 8,
            timeout_ms: 0,
        },
        &[],
    );
    assert_eq!(selection.channels.len(), 1);
    assert_eq!(selection.channels[0].replica_id(), 2);
    assert!(!selection.has_active_channels_left);
}

#[test]
fn mail_channel_fetch_drains_priority_lanes_like_matrixraft() {
    let selector = RustRaftChannelSelector::new();
    let channel = RustRaftMailChannel::new(9, 100);

    selector.send_to_channel(Arc::clone(&channel), RustRaftMailPriority::Slowly, "slow");
    selector.send_to_channel(Arc::clone(&channel), RustRaftMailPriority::Normal, "normal");
    selector.send_to_channel(Arc::clone(&channel), RustRaftMailPriority::Urgent, "urgent");

    let selection = selector.select(
        RustRaftChannelSelectorPolicy {
            limit: 1,
            timeout_ms: 0,
        },
        &[],
    );
    assert_eq!(selection.channels[0].replica_id(), 9);

    let mails = selection.channels[0].fetch(&selector);
    assert_eq!(mails, vec!["urgent", "normal", "slow"]);
    assert_eq!(channel.queued_len(), 0);
    assert_eq!(selector.total_mail_count(), 3);
    assert_eq!(channel.selector_total_mail_count(), 3);
}

#[test]
fn channel_selector_delivers_global_mails_and_rearranged_inputs() {
    let selector = RustRaftChannelSelector::new();
    let channel = RustRaftMailChannel::<u64>::new(3, 100);

    selector.send_global(11);
    let selection = selector.select(
        RustRaftChannelSelectorPolicy {
            limit: 1,
            timeout_ms: 0,
        },
        &[Arc::clone(&channel)],
    );
    assert_eq!(selection.global_mails, vec![11]);
    assert_eq!(selection.channels.len(), 1);
    assert_eq!(selection.channels[0].replica_id(), 3);
}

#[test]
fn channel_selector_group_count_drives_channel_overflow_like_matrixraft() {
    let selector = RustRaftChannelSelector::new();
    let channel = RustRaftMailChannel::new(5, 2);

    selector
        .try_send_to_channel(Arc::clone(&channel), RustRaftMailPriority::Normal, 1)
        .expect("send one");
    selector
        .try_send_to_channel(Arc::clone(&channel), RustRaftMailPriority::Normal, 2)
        .expect("send two");
    selector
        .try_send_to_channel(Arc::clone(&channel), RustRaftMailPriority::Normal, 3)
        .expect("send three before selector observes overflow");

    let selection = selector.select(
        RustRaftChannelSelectorPolicy {
            limit: 1,
            timeout_ms: 0,
        },
        &[],
    );
    assert_eq!(selection.channels[0].fetch(&selector), vec![1, 2, 3]);
    assert!(selector
        .try_send_to_channel(Arc::clone(&channel), RustRaftMailPriority::Normal, 4)
        .is_err());
}
