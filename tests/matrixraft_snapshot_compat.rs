// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    InstallSnapshotRequest, InstallSnapshotResponse, LogId, MatrixRaftOldSnapshotFinishState,
    MatrixRaftRateLimiterConfig, MatrixRaftRuntimeWiring, MatrixRaftSnapshotCreator,
    MatrixRaftSnapshotDownloader, MatrixRaftSnapshotLoader, MatrixRaftSnapshotSender, RaftSnapshot,
    SnapshotInstallState, SnapshotLifecycle, SnapshotLifecycleConfig, SnapshotMetadata,
};

fn snapshot(index: u64, payload: Vec<u8>) -> RaftSnapshot {
    RaftSnapshot {
        group_id: 77,
        meta: SnapshotMetadata {
            snapshot_id: format!("matrixraft-snapshot-{index}"),
            last_log_id: LogId { term: 3, index },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: payload.len() as u64,
        },
        payload,
    }
}

#[test]
fn matrixraft_snapshot_sender_downloader_cover_chunk_flow_and_finish_state() {
    let snapshot = snapshot(12, b"snapshot-payload-for-matrixraft".to_vec());
    let config = SnapshotLifecycleConfig {
        chunk_size: 8,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 8,
        max_retry_attempts: 2,
    };
    let mut sender = MatrixRaftSnapshotSender::new(config).expect("sender");
    let mut downloader = MatrixRaftSnapshotDownloader::default();

    sender.send(&snapshot, 3, 1).expect("begin send");
    assert!(sender.status().sending);
    let mut cancel_sender = MatrixRaftSnapshotSender::new(config).expect("cancel sender");
    cancel_sender
        .send(&snapshot, 3, 1)
        .expect("begin cancelable send");
    let canceled = cancel_sender.cancel();
    assert!(canceled.canceled);
    assert!(canceled.status_before.sending);
    assert!(!canceled.status_after.sending);
    assert!(!canceled.status_after.completed);

    let mut saw_finish = None;
    let mut sender_finish = None;
    while sender.status().sending {
        let requests = sender.poll_send_requests().expect("poll send requests");
        assert_eq!(requests.len(), 1);
        for request in requests {
            let result = downloader.download(request).expect("download chunk");
            let recorded_finish = sender
                .record_send_response(&result.response)
                .expect("record sender response");
            if let Some(finish) = result.finish {
                saw_finish = Some(finish);
                sender_finish = recorded_finish;
                assert_eq!(result.installed_snapshot.as_ref(), Some(&snapshot));
            } else {
                assert!(recorded_finish.is_none());
            }
        }
    }

    let finish = saw_finish.expect("snapshot finish");
    assert_eq!(
        finish.finish_state,
        MatrixRaftOldSnapshotFinishState::Received
    );
    assert_eq!(finish.snapshot_index, 12);
    assert_eq!(sender_finish, Some(finish));
    assert!(sender.status().completed);
    assert!(downloader.status().completed);
    assert_eq!(downloader.status().installed_index, 12);
}

#[test]
fn matrixraft_snapshot_creator_and_loader_expose_checkpoint_and_chunk_install_roles() {
    let snapshot = snapshot(7, b"creator-loader".to_vec());
    let creator = MatrixRaftSnapshotCreator::new(10, 2);
    let chunks = creator.checkpoint(&snapshot, 6).expect("checkpoint");
    assert!(chunks.len() > 1);

    let loader = MatrixRaftSnapshotLoader::new(10, 1);
    let mut install = SnapshotInstallState::new(snapshot.meta.clone());
    for chunk in chunks {
        loader
            .install_chunk(&mut install, chunk)
            .expect("loader install chunk");
    }
    assert!(install.complete);
    assert_eq!(
        install.finish(snapshot.group_id).expect("finish install"),
        snapshot
    );
}

#[test]
fn a_configured_send_limit_limits_an_ordinary_poll() {
    // The limit used to be reachable only by a caller that passed a limiter in
    // at every poll. A host that configured `snapshot_send_rate_limiter` and
    // called `poll_send_requests` -- which is the obvious call and the one the
    // other tests in this file use -- got no limit at all.
    let rate_limit = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 4,
        check_cycle_sec: 1,
    };
    let config = SnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 8,
        max_bytes_per_tick: 1 << 20,
        max_retry_attempts: 2,
    };
    let snapshot = snapshot(9, b"abcdefghijkl".to_vec());

    // The control: no limit configured, so one poll carries all three chunks.
    let mut unlimited = MatrixRaftSnapshotSender::new(config).expect("sender");
    unlimited.begin_send(&snapshot, 2, 1).expect("begin send");
    let all = unlimited.poll_send_requests().expect("poll");
    assert_eq!(all.len(), 3, "an unlimited sender hands over every chunk");
    assert!(unlimited.rate_limit_available_bytes().is_none());
    assert!(unlimited.rate_limit_stats().is_none());

    // And with the limit: one cycle is four bytes, which is one chunk.
    let mut limited =
        MatrixRaftSnapshotSender::with_rate_limit(config, &rate_limit).expect("sender");
    limited.begin_send(&snapshot, 2, 1).expect("begin send");
    assert_eq!(limited.rate_limit_available_bytes(), Some(4));

    let first = limited.poll_send_requests().expect("first poll");
    assert_eq!(first.len(), 1, "four bytes of quota is one four-byte chunk");
    assert_eq!(first[0].chunk.offset, 0);
    assert_eq!(limited.rate_limit_available_bytes(), Some(0));

    let blocked = limited.poll_send_requests().expect("blocked poll");
    assert!(
        blocked.is_empty(),
        "an exhausted quota must stop the transfer, not slow it"
    );

    // A limiter hands bytes out and never gets them back on its own, so the
    // refill is what makes the limit a rate rather than a one-off budget.
    assert_eq!(limited.refill_rate_limit(&rate_limit), 4);
    let resumed = limited.poll_send_requests().expect("resumed poll");
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].chunk.offset, 4);

    // The resumed poll emptied the quota again, so this restores the whole
    // cycle.
    assert_eq!(limited.refill_rate_limit(&rate_limit), 4);
    assert_eq!(limited.rate_limit_available_bytes(), Some(4));

    // And refilling a full quota restores nothing, which is what lets a host
    // call this on every tick without checking first.
    assert_eq!(limited.refill_rate_limit(&rate_limit), 0);
    assert_eq!(limited.rate_limit_available_bytes(), Some(4));
    let stats = limited
        .rate_limit_stats()
        .expect("a limited sender has stats");
    assert!(
        stats.total_granted_bytes >= 8,
        "granted {} bytes across two polls",
        stats.total_granted_bytes
    );

    // And a sender with no limit refills nothing rather than erroring.
    assert_eq!(unlimited.refill_rate_limit(&rate_limit), 0);
}

/// A wiring carrying a send limit and nothing else of interest.
///
/// Written out rather than defaulted: a wiring has no `Default` and should not,
/// because a `worker_num` of zero is not a runtime anyone configured. The point
/// of the literal is that it shows exactly which field carries a send limit.
fn wiring_with_send_limit(
    rate_limit: Option<MatrixRaftRateLimiterConfig>,
) -> MatrixRaftRuntimeWiring {
    MatrixRaftRuntimeWiring {
        group_id: 7,
        node_id: 1,
        creator_index: Some(0),
        store_id: 1,
        worker_num: 1,
        reader_num: 1,
        executor_num: 1,
        applier_num: 1,
        apply_max_batch_count: 8,
        snapshot_loader_num: 1,
        snapshot_downloader_num: 1,
        snapshot_creator_num: 1,
        snapshot_sender_num: 1,
        flexible_apply: false,
        heartbeat_merge: false,
        merge_heartbeat_interval_milli: 0,
        watched_address_resolver: false,
        transport: Default::default(),
        snapshot_send_rate_limiter: rate_limit,
        snapshot_download_rate_limiter: None,
        has_store_fsm: false,
        has_group_storage: false,
    }
}

#[test]
fn a_sender_built_from_the_wiring_carries_the_configured_limit() {
    // The route a host actually has: a limit configured on the context or the
    // creator reaches the wiring, and the wiring builds the sender. A plant
    // that dropped the limiter on this path passed the other test in this file,
    // which only exercises `with_rate_limit`.
    let rate_limit = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 4,
        check_cycle_sec: 1,
    };
    let config = SnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 8,
        max_bytes_per_tick: 1 << 20,
        max_retry_attempts: 2,
    };
    let snapshot = snapshot(9, b"abcdefghijkl".to_vec());

    let wired = wiring_with_send_limit(Some(rate_limit.clone()));
    assert_eq!(
        wired.snapshot_send_limiter().expect("a valid limit"),
        Some(matrixraft::ByteQuotaLimiter::new(4)),
        "the wiring turns the configured rate into one cycle of bytes"
    );

    let mut sender =
        MatrixRaftSnapshotSender::from_wiring(config, &wired).expect("sender from the wiring");
    assert_eq!(sender.rate_limit_available_bytes(), Some(4));
    sender.begin_send(&snapshot, 2, 1).expect("begin send");
    let first = sender.poll_send_requests().expect("poll");
    assert_eq!(
        first.len(),
        1,
        "the wired limit is four bytes, which is one chunk"
    );
    assert!(sender.poll_send_requests().expect("second poll").is_empty());

    // And a wiring with no limit gives a sender with no limit, which is what
    // that group asked for.
    let unwired = wiring_with_send_limit(None);
    assert_eq!(unwired.snapshot_send_limiter().expect("no limit"), None);
    let mut unlimited =
        MatrixRaftSnapshotSender::from_wiring(config, &unwired).expect("sender from the wiring");
    assert_eq!(unlimited.rate_limit_available_bytes(), None);
    unlimited.begin_send(&snapshot, 2, 1).expect("begin send");
    assert_eq!(unlimited.poll_send_requests().expect("poll").len(), 3);
}

#[test]
fn a_download_quota_declines_a_chunk_without_failing_the_send() {
    // The whole point of the `throttled` field: a receiver can say "not now"
    // without the sender treating it as a failure. Until it existed the only
    // way to decline was `accepted: false`, which spends the retry budget, so
    // a quota had to install every chunk and merely report `over_quota`.
    let rate_limit = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 4,
        check_cycle_sec: 1,
    };
    let config = SnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 1 << 20,
        max_retry_attempts: 2,
    };
    let snapshot = snapshot(9, b"abcdefghijkl".to_vec());

    let mut sender = MatrixRaftSnapshotSender::new(config).expect("sender");
    sender.begin_send(&snapshot, 2, 1).expect("begin send");

    let mut downloader =
        MatrixRaftSnapshotDownloader::with_rate_limit(config, &rate_limit).expect("downloader");

    // The first chunk fits the quota exactly and is installed.
    let first = sender.poll_send_requests().expect("poll").remove(0);
    let first_result = downloader.download(first).expect("download");
    assert!(first_result.accepted);
    assert!(!first_result.over_quota);
    assert!(!first_result.response.throttled);
    sender
        .record_send_response(&first_result.response)
        .expect("record the accepted chunk");

    // The second does not fit, and is DECLINED rather than installed.
    let second = sender.poll_send_requests().expect("poll").remove(0);
    let second_offset = second.chunk.offset;
    let second_result = downloader.download(second).expect("download");
    assert!(!second_result.accepted, "an over-quota chunk is declined");
    assert!(second_result.over_quota);
    assert!(
        second_result.response.throttled,
        "declining without throttled set would spend the senders retry budget"
    );
    assert!(second_result.installed_snapshot.is_none());
    assert_eq!(
        second_result.response.next_offset, second_offset,
        "the sender must rewind to the chunk that was declined, not past it"
    );

    // And the sender treats it as a pause, not a failure.
    sender
        .record_send_response(&second_result.response)
        .expect("a throttled response is not an error");
    let status = sender.status();
    assert_eq!(status.retry_count, 0, "a throttle is not a retry");
    assert_eq!(status.throttled_responses, 1);
    assert!(status.sending, "the send is still live");

    // The next poll re-sends the declined chunk, and after a refill it lands.
    assert_eq!(downloader.refill_rate_limit(&rate_limit), 4);
    let resent = sender.poll_send_requests().expect("poll").remove(0);
    assert_eq!(
        resent.chunk.offset, second_offset,
        "the rewind should re-send the declined chunk"
    );
    let resent_result = downloader.download(resent).expect("download");
    assert!(resent_result.accepted);
    assert!(!resent_result.response.throttled);
    sender
        .record_send_response(&resent_result.response)
        .expect("record");

    // An unlimited downloader never declines.
    let mut unlimited = MatrixRaftSnapshotDownloader::new(config).expect("downloader");
    assert_eq!(unlimited.rate_limit_available_bytes(), None);
    let mut lone = MatrixRaftSnapshotSender::new(config).expect("sender");
    lone.begin_send(&snapshot, 2, 1).expect("begin send");
    let chunk = lone.poll_send_requests().expect("poll").remove(0);
    let result = unlimited.download(chunk).expect("download");
    assert!(result.accepted);
    assert!(!result.over_quota);
    assert!(!result.response.throttled);
}

#[test]
fn more_throttles_than_the_retry_budget_do_not_abandon_the_send() {
    // The hazard that made the first version of this install every chunk:
    // `max_retry_attempts` defaults to 3 and a rejection spends one, so a
    // receiver pacing a transfer with four declines used to kill it. This is
    // the assertion that says it cannot.
    let config = SnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 1 << 20,
        max_retry_attempts: 2,
    };
    let snapshot = snapshot(9, b"abcdefghijkl".to_vec());
    let mut sender = MatrixRaftSnapshotSender::new(config).expect("sender");
    sender.begin_send(&snapshot, 2, 1).expect("begin send");

    let first = sender.poll_send_requests().expect("poll").remove(0);
    let offset = first.chunk.offset;
    let throttle = InstallSnapshotResponse {
        term: 0,
        accepted: false,
        next_offset: offset,
        committed_index: 0,
        reason: "snapshot download quota exhausted".to_string(),
        throttled: true,
    };

    // Five throttles against a budget of two.
    for _ in 0..5 {
        sender
            .record_send_response(&throttle)
            .expect("a throttle must never exhaust the budget");
    }
    let status = sender.status();
    assert_eq!(status.retry_count, 0);
    assert_eq!(status.throttled_responses, 5);
    assert!(
        status.sending,
        "the send must still be live after five throttles"
    );

    // A plain rejection still spends the budget and still abandons the send,
    // which is the behaviour a throttle is carved out of rather than replacing.
    let rejection = InstallSnapshotResponse {
        term: 0,
        accepted: false,
        next_offset: offset,
        committed_index: 0,
        reason: "no".to_string(),
        throttled: false,
    };
    sender
        .record_send_response(&rejection)
        .expect("first retry");
    sender
        .record_send_response(&rejection)
        .expect("second retry");
    let exhausted = sender.record_send_response(&rejection);
    assert!(
        exhausted.is_err(),
        "a budget of two must still be exhausted by three rejections, got {exhausted:?}"
    );
}

#[test]
fn a_downloader_built_from_the_wiring_carries_the_configured_limit() {
    let rate_limit = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 4,
        check_cycle_sec: 1,
    };
    let config = SnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 1 << 20,
        max_retry_attempts: 2,
    };

    let mut wired = wiring_with_send_limit(None);
    wired.snapshot_download_rate_limiter = Some(rate_limit.clone());
    assert_eq!(
        wired.snapshot_download_limiter().expect("a valid limit"),
        Some(matrixraft::ByteQuotaLimiter::new(4))
    );
    let downloader =
        MatrixRaftSnapshotDownloader::from_wiring(config, &wired).expect("from the wiring");
    assert_eq!(downloader.rate_limit_available_bytes(), Some(4));

    // And a wiring with no download limit gives a downloader with none, even
    // when a SEND limit is set -- the two are separate quotas.
    let mut send_only = wiring_with_send_limit(Some(rate_limit));
    send_only.snapshot_download_rate_limiter = None;
    let unlimited =
        MatrixRaftSnapshotDownloader::from_wiring(config, &send_only).expect("from the wiring");
    assert_eq!(unlimited.rate_limit_available_bytes(), None);
}

fn transfer_config() -> SnapshotLifecycleConfig {
    SnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 1 << 20,
        max_retry_attempts: 2,
    }
}

/// A sender part-way through a transfer, and the downloader receiving it.
fn mid_transfer() -> (
    MatrixRaftSnapshotSender,
    MatrixRaftSnapshotDownloader,
    RaftSnapshot,
) {
    let config = transfer_config();
    let snap = snapshot(21, b"abcdefghijkl".to_vec());
    let mut sender = MatrixRaftSnapshotSender::new(config).expect("sender");
    sender.begin_send(&snap, 2, 1).expect("begin send");
    let mut downloader = MatrixRaftSnapshotDownloader::new(config).expect("downloader");

    // One chunk in, so there is a partial install to cancel.
    let first = sender.poll_send_requests().expect("poll").remove(0);
    let result = downloader.download(first).expect("download");
    assert!(result.accepted);
    assert!(result.installed_snapshot.is_none());
    sender
        .record_send_response(&result.response)
        .expect("record");
    assert!(downloader.status().downloading);
    (sender, downloader, snap)
}

#[test]
fn a_cancelled_install_is_not_resurrected_by_the_next_chunk() {
    // The defect: `rollback_install` dropped the partial install, and
    // `install_request` starts a fresh one whenever it holds none -- so the
    // very next chunk to arrive quietly began the transfer again. A host
    // removing a replica, or refusing a snapshot it had decided against, had
    // no way to make the refusal stick.
    let (mut sender, mut downloader, _snap) = mid_transfer();

    assert!(
        downloader.cancel_install(),
        "a transfer was in progress, so the cancel should say so"
    );
    assert!(downloader.install_cancelled());
    let cancelled = downloader.status();
    assert!(!cancelled.downloading, "the partial install is gone");
    assert!(cancelled.install_cancelled);
    assert_eq!(cancelled.cancelled_installs, 1);
    assert_eq!(cancelled.refused_chunks, 0, "nothing refused yet");

    // The next chunk of the SAME snapshot is refused rather than installed.
    let next = sender.poll_send_requests().expect("poll").remove(0);
    let next_offset = next.chunk.offset;
    let refused = downloader.download(next).expect("download answers");
    assert!(
        !refused.accepted,
        "a cancelled install must not take a chunk"
    );
    assert!(
        !refused.response.throttled,
        "this is not a throttle: a throttle means later, and this means not at all"
    );
    assert!(refused.response.reason.contains("cancelled"));
    assert_eq!(refused.response.next_offset, next_offset);
    assert!(refused.installed_snapshot.is_none());
    assert!(!refused.over_quota);
    assert_eq!(downloader.status().refused_chunks, 1);
    assert!(!downloader.status().downloading);

    // And the resurrection route that mattered most: the first chunk of a
    // DIFFERENT snapshot, which is what starts a brand new install.
    let other = snapshot(99, b"wxyz".to_vec());
    let mut other_sender = MatrixRaftSnapshotSender::new(transfer_config()).expect("sender");
    other_sender.begin_send(&other, 2, 1).expect("begin send");
    let fresh = other_sender.poll_send_requests().expect("poll").remove(0);
    assert_eq!(fresh.chunk.offset, 0, "this is the chunk that begins one");
    let refused_fresh = downloader.download(fresh).expect("download answers");
    assert!(
        !refused_fresh.accepted,
        "a new snapshot must not start an install either"
    );
    assert_eq!(downloader.status().refused_chunks, 2);
    assert!(!downloader.status().downloading);

    // The sender finds out. A plain rejection spends the retry budget, so a
    // receiver that has stopped is not pushed a whole snapshot.
    sender
        .record_send_response(&refused.response)
        .expect("first retry");
    sender
        .record_send_response(&refused.response)
        .expect("second retry");
    let abandoned = sender.record_send_response(&refused.response);
    assert!(
        abandoned.is_err(),
        "a budget of two should be exhausted by three rejections, got {abandoned:?}"
    );
    assert!(!sender.status().sending, "and the send is abandoned");
}

#[test]
fn a_rollback_is_not_a_cancel() {
    // The difference, asserted rather than only documented: a rollback says
    // "that attempt was bad, send it again", so the next chunk legitimately
    // starts a fresh install. If this ever behaved like a cancel, a transient
    // bad chunk would stop a replica receiving snapshots at all.
    let (mut sender, mut downloader, snap) = mid_transfer();

    downloader.rollback();
    assert!(
        !downloader.install_cancelled(),
        "a rollback must not close the install"
    );
    let rolled = downloader.status();
    assert_eq!(rolled.rolled_back, 1);
    assert!(!rolled.downloading);

    // A rollback discards the partial, so the transfer restarts from the
    // beginning rather than resuming mid-stream: handing the installer a
    // chunk at offset 4 with nothing before it is an error. That is the
    // existing contract, not anything this change introduced, and the test
    // says so rather than asserting a continuation that never worked.
    let mid_stream = sender.poll_send_requests().expect("poll").remove(0);
    assert_ne!(mid_stream.chunk.offset, 0);
    assert!(
        downloader.download(mid_stream).is_err(),
        "a rollback rewinds the transfer; it does not resume one"
    );

    // Restarted from offset 0 it is accepted, which is the difference from a
    // cancel: there, even the first chunk of a new transfer is refused.
    let mut restarted = MatrixRaftSnapshotSender::new(transfer_config()).expect("sender");
    restarted.begin_send(&snap, 2, 1).expect("begin send");
    let first = restarted.poll_send_requests().expect("poll").remove(0);
    assert_eq!(first.chunk.offset, 0);
    let accepted = downloader.download(first).expect("download");
    assert!(
        accepted.accepted,
        "after a rollback a restarted transfer should be taken"
    );
    assert_eq!(
        downloader.status().refused_chunks,
        0,
        "and nothing was refused, because the install was never closed"
    );
    assert!(downloader.status().downloading);
}

#[test]
fn admit_install_lets_the_group_receive_again() {
    let (_sender, mut downloader, snap) = mid_transfer();
    downloader.cancel_install();
    assert!(downloader.install_cancelled());

    assert!(
        downloader.admit_install(),
        "it had been cancelled, so admitting should say so"
    );
    assert!(!downloader.install_cancelled());
    assert!(
        !downloader.admit_install(),
        "admitting an open install changes nothing and says so"
    );
    assert!(!downloader.status().install_cancelled);

    // A whole transfer now completes from the start.
    let mut fresh_sender = MatrixRaftSnapshotSender::new(transfer_config()).expect("sender");
    fresh_sender.begin_send(&snap, 2, 1).expect("begin send");
    let mut installed = None;
    for _ in 0..8 {
        let requests = fresh_sender.poll_send_requests().expect("poll");
        if requests.is_empty() {
            break;
        }
        for request in requests {
            let result = downloader.download(request).expect("download");
            assert!(result.accepted, "an admitted install takes chunks again");
            fresh_sender
                .record_send_response(&result.response)
                .expect("record");
            if let Some(snapshot) = result.installed_snapshot {
                installed = Some(snapshot);
            }
        }
        // Polling a finished send is an error rather than an empty answer, so
        // the loop stops on the snapshot and not on an empty poll.
        if installed.is_some() {
            break;
        }
    }
    let installed = installed.expect("the transfer should complete after admitting");
    assert_eq!(installed.meta.last_log_id.index, 21);
    assert_eq!(
        downloader.status().refused_chunks,
        0,
        "and nothing was refused along the way"
    );
}

#[test]
fn beginning_a_send_does_not_re_admit_a_cancelled_install() {
    // `begin_send` rebuilds the whole status, which is the quiet route by
    // which a cancel could be undone: both directions share one status
    // struct, and starting to SEND says nothing about whether this group will
    // RECEIVE.
    //
    // This has to be asserted on `SnapshotLifecycle` itself. The first
    // version used a `MatrixRaftSnapshotSender` and a
    // `MatrixRaftSnapshotDownloader`, which wrap **separate** lifecycles -- so
    // the send rebuilt a status that had never been cancelled, and a plant
    // that cleared the flag in `begin_send` survived the test untouched. It is
    // a host driving `SnapshotLifecycle` directly that can reach both.
    let config = transfer_config();
    let mut lifecycle = SnapshotLifecycle::new(config).expect("lifecycle");

    assert!(!lifecycle.cancel_install(), "nothing was installing");
    assert!(lifecycle.install_cancelled());
    assert!(lifecycle.status().install_cancelled);

    // The same lifecycle now starts sending a snapshot out.
    let outgoing = snapshot(31, b"mnopqrst".to_vec());
    lifecycle
        .begin_send(&outgoing, 2, 1)
        .expect("a cancelled install must not block a send");
    assert!(lifecycle.status().sending);

    // Both the flag and the behaviour have to survive it.
    assert!(
        lifecycle.install_cancelled(),
        "beginning a send must not reopen the install"
    );
    assert!(
        lifecycle.status().install_cancelled,
        "and the status must not report it as reopened either -- a host \
         watching this would otherwise believe chunks were being taken"
    );

    let chunk = lifecycle.poll_send_requests().expect("poll").remove(0);
    let refused = lifecycle.install_request(InstallSnapshotRequest {
        group_id: chunk.group_id,
        term: chunk.term,
        leader_id: chunk.leader_id,
        chunk: chunk.chunk,
    });
    assert!(
        refused.is_err(),
        "a chunk must still be refused after a send began, got {refused:?}"
    );
    assert_eq!(lifecycle.status().refused_chunks, 1);

    // And admitting still works afterwards, so the send has not wedged it.
    assert!(lifecycle.admit_install());
    assert!(!lifecycle.status().install_cancelled);
}

#[test]
fn a_refused_chunk_is_not_charged_to_the_quota() {
    // Order matters: a chunk this node will not install should not count
    // against the bytes it is willing to install, or a cancelled replica
    // would exhaust the quota of a node that never took a byte.
    let rate_limit = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 4,
        check_cycle_sec: 1,
    };
    let config = transfer_config();
    let snap = snapshot(41, b"abcdefgh".to_vec());
    let mut sender = MatrixRaftSnapshotSender::new(config).expect("sender");
    sender.begin_send(&snap, 2, 1).expect("begin send");
    let mut downloader =
        MatrixRaftSnapshotDownloader::with_rate_limit(config, &rate_limit).expect("downloader");

    downloader.cancel_install();
    let before = downloader.rate_limit_available_bytes();
    let chunk = sender.poll_send_requests().expect("poll").remove(0);
    let refused = downloader.download(chunk).expect("download answers");
    assert!(!refused.accepted);
    assert!(!refused.over_quota, "it was refused, not over quota");
    assert_eq!(
        downloader.rate_limit_available_bytes(),
        before,
        "a refused chunk must not spend the install quota"
    );
    assert_eq!(downloader.status().refused_chunks, 1);
}

#[test]
fn cancelling_an_idle_install_says_nothing_was_in_progress() {
    // The return value is about the transfer, not about the flag: a host
    // cancelling pre-emptively should be able to tell that from stopping one
    // in flight, because only the latter means a peer is now wasting its time.
    let mut downloader = MatrixRaftSnapshotDownloader::new(transfer_config()).expect("downloader");
    assert!(
        !downloader.cancel_install(),
        "nothing was installing, so there was nothing to stop"
    );
    assert!(
        downloader.install_cancelled(),
        "but the install is closed all the same"
    );
    assert_eq!(downloader.status().cancelled_installs, 0);
    assert!(downloader.status().install_cancelled);
}
