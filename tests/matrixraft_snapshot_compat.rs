// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    InstallSnapshotResponse, LogId, MatrixRaftOldSnapshotFinishState, MatrixRaftRateLimiterConfig,
    MatrixRaftRuntimeWiring, MatrixRaftSnapshotCreator, MatrixRaftSnapshotDownloader,
    MatrixRaftSnapshotLoader, MatrixRaftSnapshotSender, RaftSnapshot, SnapshotInstallState,
    SnapshotLifecycleConfig, SnapshotMetadata,
};

fn snapshot(index: u64, payload: Vec<u8>) -> RaftSnapshot {
    RaftSnapshot {
        group_id: 77,
        meta: SnapshotMetadata {
            snapshot_id: format!("matrixraft-snapshot-{index}"),
            last_log_id: LogId { term: 3, index },
            membership: vec![1, 2, 3],
            members: Vec::new(),
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
