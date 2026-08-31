// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;

use matrixraft::{ChannelSelector, ChannelSelectorPolicy, MailChannel, MailPriority};

#[test]
fn channel_selector_selects_unique_active_channels() {
    let selector = ChannelSelector::<u64>::new();
    let channel_1 = MailChannel::<u64>::new(1, 100);
    let channel_2 = MailChannel::<u64>::new(2, 100);

    assert!(selector.fire(Arc::clone(&channel_1)));
    assert!(!selector.fire(Arc::clone(&channel_1)));
    assert!(selector.fire(Arc::clone(&channel_2)));

    let selection = selector.select(
        ChannelSelectorPolicy {
            limit: 1,
            timeout_ms: 0,
        },
        &[],
    );
    assert_eq!(selection.channels.len(), 1);
    assert_eq!(selection.channels[0].replica_id(), 1);
    assert!(selection.has_active_channels_left);

    let selection = selector.select(
        ChannelSelectorPolicy {
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
fn mail_channel_fetch_drains_priority_lanes() {
    let selector = ChannelSelector::new();
    let channel = MailChannel::new(9, 100);

    selector.send_to_channel(Arc::clone(&channel), MailPriority::Slowly, "slow");
    selector.send_to_channel(Arc::clone(&channel), MailPriority::Normal, "normal");
    selector.send_to_channel(Arc::clone(&channel), MailPriority::Urgent, "urgent");

    let selection = selector.select(
        ChannelSelectorPolicy {
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
    let selector = ChannelSelector::new();
    let channel = MailChannel::<u64>::new(3, 100);

    selector.send_global(11);
    let selection = selector.select(
        ChannelSelectorPolicy {
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
fn channel_selector_group_count_drives_channel_overflow() {
    let selector = ChannelSelector::new();
    let channel = MailChannel::new(5, 2);

    selector
        .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, 1)
        .expect("send one");
    selector
        .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, 2)
        .expect("send two");
    selector
        .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, 3)
        .expect("send three before selector observes overflow");

    let selection = selector.select(
        ChannelSelectorPolicy {
            limit: 1,
            timeout_ms: 0,
        },
        &[],
    );
    assert_eq!(selection.channels[0].fetch(&selector), vec![1, 2, 3]);
    assert!(selector
        .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, 4)
        .is_err());
}
