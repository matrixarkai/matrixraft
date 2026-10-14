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
fn a_channels_depth_is_its_own_queue_not_the_selectors_total() {
    // RETARGETED. This was `channel_selector_group_count_drives_channel_overflow`,
    // and it asserted that a channel with a depth of 2 accepts a third mail
    // "before selector observes overflow" -- that the bound is the whole
    // selector's mail count, lagging, rather than this channel's own queue.
    //
    // That is what the code did, and it is wrong in both directions. It let
    // a channel exceed its depth, as the old assertion recorded. It also
    // refused mail to a channel whose queue was EMPTY whenever the other
    // groups on the same worker were busy -- and because the count each
    // channel compares against is a copy refreshed only when that channel is
    // drained, a channel that refused everything was never drained and never
    // corrected itself. It stayed shut for good.
    //
    // A store of 4096 groups over 4 workers ran into exactly that: more
    // groups on a worker than `max_queue_depth`, every worker then parked
    // with nothing to do, and the groups they owned unticked and unanswering.
    // `tests/channel_depth.rs` reproduces both halves directly.
    let selector = ChannelSelector::new();
    let channel = MailChannel::new(5, 2);

    selector
        .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, 1)
        .expect("send one");
    selector
        .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, 2)
        .expect("send two");
    assert!(
        selector
            .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, 3)
            .is_err(),
        "a channel at its depth of 2 took a third mail"
    );

    let selection = selector.select(
        ChannelSelectorPolicy {
            limit: 1,
            timeout_ms: 0,
        },
        &[],
    );
    assert_eq!(selection.channels[0].fetch(&selector), vec![1, 2]);

    // Drained, so it has room again.
    selector
        .try_send_to_channel(Arc::clone(&channel), MailPriority::Normal, 4)
        .expect("a drained channel has room");
}

#[test]
fn concurrent_fetches_on_one_channel_lose_no_mail() {
    // `fetch` is consume-then-drain under two separate lock holds, and a sender
    // firing the channel in between can hand it to a second worker, so two
    // fetches for the same channel can overlap. While `consume` assigned to
    // `buffered` rather than appending, the second one discarded whatever the
    // first had staged and not yet drained: mail that `send` had accepted,
    // gone, with no error anywhere.
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    let selector = Arc::new(ChannelSelector::<u64>::new());
    let channel = MailChannel::<u64>::new(1, usize::MAX);
    let sent = 5_000_u64;

    let received = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let fetchers: Vec<_> = (0..4)
        .map(|_| {
            let selector = Arc::clone(&selector);
            let channel = Arc::clone(&channel);
            let received = Arc::clone(&received);
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    let mails = channel.fetch(&selector);
                    received.fetch_add(mails.len(), Ordering::Relaxed);
                }
                // Whatever is left after the sender stopped.
                let mails = channel.fetch(&selector);
                received.fetch_add(mails.len(), Ordering::Relaxed);
            })
        })
        .collect();

    for n in 0..sent {
        channel.send(MailPriority::Normal, n);
        selector.fire(Arc::clone(&channel));
    }

    // Let the fetchers drain, then stop them and take a last pass each.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while received.load(Ordering::Relaxed) < sent as usize && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    done.store(true, Ordering::Relaxed);
    for fetcher in fetchers {
        fetcher.join().expect("fetcher");
    }

    assert_eq!(
        received.load(Ordering::Relaxed),
        sent as usize,
        "every mail accepted by send must come back out of fetch"
    );
    assert_eq!(channel.queued_len(), 0, "nothing should still be queued");
}

#[test]
fn channel_selector_checked_api_preserves_overflow_and_selection_contract() {
    let selector = ChannelSelector::new();
    let channel = MailChannel::new(7, 1);

    selector
        .send_global_checked("global")
        .expect("checked global send");
    assert!(selector
        .try_send_to_channel_checked(Arc::clone(&channel), MailPriority::Normal, "one")
        .expect("checked send")
        .is_ok());

    let selection = selector
        .select_checked(
            ChannelSelectorPolicy {
                limit: 1,
                timeout_ms: 0,
            },
            &[],
        )
        .expect("checked select");
    assert_eq!(selection.global_mails, vec!["global"]);
    assert_eq!(selection.channels[0].replica_id(), 7);
    assert_eq!(
        selection.channels[0]
            .fetch_checked(&selector)
            .expect("checked fetch"),
        vec!["one"]
    );
    assert_eq!(
        channel
            .selector_total_mail_count_checked()
            .expect("checked selector count"),
        1
    );

    channel
        .send_checked(MailPriority::Normal, "two")
        .expect("checked unbounded send two");
    channel
        .send_checked(MailPriority::Normal, "three")
        .expect("checked unbounded send three");
    assert!(selector
        .fire_checked(Arc::clone(&channel))
        .expect("checked fire"));
    let selection = selector
        .select_checked(
            ChannelSelectorPolicy {
                limit: 1,
                timeout_ms: 0,
            },
            &[],
        )
        .expect("checked select two");
    assert_eq!(
        selection.channels[0]
            .fetch_checked(&selector)
            .expect("checked fetch two and three"),
        vec!["two", "three"]
    );
    // RETARGETED, and this is the half of the test worth keeping an eye on. As
    // written it asserted that this send is REFUSED, with `queued_len` 0 and
    // `rejected_send_count` 1 beside it -- an EMPTY channel turning mail away.
    // That was true when `overflow` compared the SELECTOR'S total mail count
    // against this channel's limit: the count is a copy refreshed only when this
    // channel is drained, so a channel that refused everything was never drained
    // and never corrected itself. It stayed shut. See
    // `a_channels_depth_is_its_own_queue_not_the_selectors_total`, which is the
    // fix, and the 4096-group store that wedged on it.
    //
    // A drained channel has room, so it takes the mail. What the checked API has
    // to preserve is whatever the overflow contract IS, not the contract it had.
    assert!(selector
        .try_send_to_channel_checked(Arc::clone(&channel), MailPriority::Normal, "four")
        .expect("checked send after the channel was drained")
        .is_ok());
    let stats = channel
        .pressure_stats_checked()
        .expect("checked pressure stats");
    assert_eq!(stats.replica_id, 7);
    assert_eq!(stats.num_mail_limit, 1);
    assert_eq!(stats.queued_len, 1, "the mail it just took");
    assert_eq!(stats.max_depth, 2);
    assert_eq!(
        stats.rejected_send_count, 0,
        "nothing was refused, because the channel had been drained"
    );
    assert_eq!(channel.queued_len_checked().expect("checked queued len"), 1);

    // And the depth IS enforced, on its own queue: a second mail while that one
    // is still waiting is refused, and counted.
    assert!(selector
        .try_send_to_channel_checked(Arc::clone(&channel), MailPriority::Normal, "five")
        .expect("checked send at the limit")
        .is_err());
    assert_eq!(
        channel
            .pressure_stats_checked()
            .expect("checked pressure stats again")
            .rejected_send_count,
        1,
        "the refusal at its own limit is what should be counted"
    );
}

#[test]
fn mail_channel_checked_batch_send_rejects_oversized_bursts_before_queueing() {
    let selector = ChannelSelector::new();
    let channel = MailChannel::new(8, 2);

    assert!(channel
        .try_send_many_checked(MailPriority::Normal, vec!["one", "two", "three"])
        .expect("checked oversized burst")
        .is_err());
    assert_eq!(channel.queued_len_checked().expect("checked queued len"), 0);

    assert!(channel
        .try_send_many_checked(MailPriority::Normal, vec!["two", "three"])
        .expect("checked burst send")
        .is_ok());
    let stats = channel
        .pressure_stats_checked()
        .expect("checked pressure stats");
    assert_eq!(stats.replica_id, 8);
    assert_eq!(stats.num_mail_limit, 2);
    assert_eq!(stats.queued_len, 2);
    assert_eq!(stats.max_depth, 2);
    assert_eq!(stats.rejected_send_count, 3);
    assert!(selector
        .fire_checked(Arc::clone(&channel))
        .expect("checked fire"));
    let selection = selector
        .select_checked(
            ChannelSelectorPolicy {
                limit: 1,
                timeout_ms: 0,
            },
            &[],
        )
        .expect("checked select two");
    assert_eq!(
        selection.channels[0]
            .fetch_checked(&selector)
            .expect("checked fetch burst"),
        vec!["two", "three"]
    );
    assert_eq!(channel.queued_len_checked().expect("checked queued len"), 0);
}

#[test]
fn channel_selector_checked_batch_send_fires_only_bounded_batches() {
    let selector = ChannelSelector::new();
    let channel = MailChannel::new(9, 2);

    assert!(selector
        .try_send_many_to_channel_checked(
            Arc::clone(&channel),
            MailPriority::Normal,
            vec!["one", "two", "three"],
        )
        .expect("checked oversized selector burst")
        .is_err());
    assert_eq!(channel.queued_len_checked().expect("checked queued len"), 0);
    assert_eq!(
        selector
            .active_channel_count_checked()
            .expect("checked active count"),
        0
    );

    assert!(selector
        .try_send_many_to_channel_checked(
            Arc::clone(&channel),
            MailPriority::Normal,
            vec!["one", "two"],
        )
        .expect("checked selector batch send")
        .is_ok());

    let selection = selector
        .select_checked(
            ChannelSelectorPolicy {
                limit: 1,
                timeout_ms: 0,
            },
            &[],
        )
        .expect("checked select");
    assert_eq!(selection.channels[0].replica_id(), 9);
    assert_eq!(
        selection.channels[0]
            .fetch_checked(&selector)
            .expect("checked fetch"),
        vec!["one", "two"]
    );
}
