// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    MatrixRaftOldSnapshotFinishState, MatrixRaftSnapshotCreator, MatrixRaftSnapshotDownloader,
    MatrixRaftSnapshotLoader, MatrixRaftSnapshotSender, RaftSnapshot, RaftSnapshotInstallState,
    RaftSnapshotLifecycleConfig, RustRaftLogId, RustRaftSnapshotMeta,
};

fn snapshot(index: u64, payload: Vec<u8>) -> RaftSnapshot {
    RaftSnapshot {
        group_id: 77,
        meta: RustRaftSnapshotMeta {
            snapshot_id: format!("matrixraft-snapshot-{index}"),
            last_log_id: RustRaftLogId { term: 3, index },
            membership: vec![1, 2, 3],
            members: Vec::new(),
        },
        payload,
    }
}

#[test]
fn matrixraft_snapshot_sender_downloader_cover_chunk_flow_and_finish_state() {
    let snapshot = snapshot(12, b"snapshot-payload-for-matrixraft".to_vec());
    let config = RaftSnapshotLifecycleConfig {
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
    let mut install = RaftSnapshotInstallState::new(snapshot.meta.clone());
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
