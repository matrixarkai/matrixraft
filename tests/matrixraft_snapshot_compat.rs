// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    LogId, MatrixRaftOldSnapshotFinishState, MatrixRaftRateLimiterConfig,
    MatrixRaftSnapshotCreator, MatrixRaftSnapshotDownloader, MatrixRaftSnapshotLoader,
    MatrixRaftSnapshotSender, RaftSnapshot, SnapshotInstallState, SnapshotLifecycleConfig,
    SnapshotMetadata,
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
