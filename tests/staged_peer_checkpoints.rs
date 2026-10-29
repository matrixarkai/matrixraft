// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]
use matrixraft::peer_runtime::{DurablePeer, PeerOptions};
use std::{
    cell::Cell,
    io::{self, Read},
    sync::mpsc,
};
fn options(root: &std::path::Path, name: &str) -> PeerOptions {
    PeerOptions {
        group_id: 17,
        node_id: 1,
        voters: vec![1],
        wal_path: root.join(name),
    }
}
fn elected(options: PeerOptions) -> DurablePeer {
    let mut peer = DurablePeer::open(options).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    assert!(peer.is_leader());
    peer
}
fn apply(peer: &mut DurablePeer, value: u8) {
    peer.propose(vec![value]).unwrap();
    let mut seen = false;
    peer.process_ready(|_, bytes| {
        if !bytes.is_empty() {
            assert_eq!(bytes, [value]);
            seen = true;
        }
        Ok(())
    })
    .unwrap();
    assert!(seen);
}
#[test]
fn background_capture_preserves_writes_committed_after_its_position() {
    let dir = tempfile::tempdir().unwrap();
    let opts = options(dir.path(), "peer.wal");
    let mut peer = elected(opts.clone());
    apply(&mut peer, 7);
    let ticket = peer.prepare_checkpoint().unwrap();
    let position = ticket.position();
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    struct Paused {
        started: Option<mpsc::Sender<()>>,
        resume: mpsc::Receiver<()>,
        body: io::Cursor<Vec<u8>>,
    }
    impl Read for Paused {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if let Some(tx) = self.started.take() {
                tx.send(()).unwrap();
                self.resume.recv().unwrap();
            }
            self.body.read(out)
        }
    }
    let worker = std::thread::spawn(move || {
        ticket
            .capture(
                &mut Paused {
                    started: Some(started_tx),
                    resume: resume_rx,
                    body: io::Cursor::new(vec![7; 1024 * 1024]),
                },
                1024 * 1024,
            )
            .unwrap()
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    apply(&mut peer, 9);
    assert!(peer.applied_index() > position.index);
    let applied = peer.applied_index();
    resume_tx.send(()).unwrap();
    let prepared = worker.join().unwrap();
    let mut other = elected(options(dir.path(), "other.wal"));
    assert!(other.install_checkpoint(&prepared).is_err());
    assert!(peer.install_checkpoint(&prepared).unwrap());
    assert_eq!(peer.applied_index(), applied);
    assert!(!peer.install_checkpoint(&prepared).unwrap());
    drop(prepared);
    apply(&mut peer, 11);
    let applied = peer.applied_index();
    drop(peer);
    let mut peer = DurablePeer::open(opts).unwrap();
    let state = Cell::new(0);
    let restored = Cell::new(false);
    peer.process_ready_with_snapshot_reader(
        |_, bytes| {
            if !bytes.is_empty() {
                assert!(restored.get());
                state.set(bytes[0]);
            }
            Ok(())
        },
        |id, reader, length| {
            assert_eq!(id, position);
            assert_eq!(length, 1024 * 1024);
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).unwrap();
            assert!(bytes.iter().all(|b| *b == 7));
            state.set(7);
            restored.set(true);
            Ok(())
        },
    )
    .unwrap();
    assert!(restored.get());
    assert_eq!(state.get(), 11);
    assert_eq!(peer.applied_index(), applied);
}
#[test]
fn capture_holds_storage_lock_and_dropped_results_remove_unpublished_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let opts = options(dir.path(), "peer.wal");
    let peer = elected(opts.clone());
    let ticket = peer.prepare_checkpoint().unwrap();
    drop(peer);
    assert!(DurablePeer::open(opts.clone()).is_err());
    let prepared = ticket
        .capture(&mut io::Cursor::new(vec![3; 4096]), 4096)
        .unwrap();
    assert!(DurablePeer::open(opts.clone()).is_err());
    let payloads = dir.path().join("peer.wal.payloads");
    assert_eq!(std::fs::read_dir(&payloads).unwrap().count(), 1);
    drop(prepared);
    assert_eq!(std::fs::read_dir(payloads).unwrap().count(), 0);
    assert!(DurablePeer::open(opts).is_ok());
}
#[test]
fn newer_checkpoint_supersedes_a_completed_worker_without_losing_state() {
    let dir = tempfile::tempdir().unwrap();
    let opts = options(dir.path(), "peer.wal");
    let mut peer = elected(opts.clone());
    apply(&mut peer, 3);
    let prepared = peer
        .prepare_checkpoint()
        .unwrap()
        .capture(&mut io::Cursor::new([3]), 1)
        .unwrap();
    apply(&mut peer, 5);
    peer.checkpoint_from_reader(&mut io::Cursor::new([5]), 1)
        .unwrap();
    assert!(!peer.install_checkpoint(&prepared).unwrap());
    drop(prepared);
    drop(peer);
    let mut peer = DurablePeer::open(opts).unwrap();
    let mut value = [0];
    peer.process_ready_with_snapshot_reader(
        |_, _| Ok(()),
        |_, reader, _| {
            reader.read_exact(&mut value).unwrap();
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(value, [5]);
}

#[test]
fn busy_peer_keeps_prepared_capture_retryable_and_bad_readers_leave_no_payload() {
    let dir = tempfile::tempdir().unwrap();
    let opts = options(dir.path(), "peer.wal");
    let mut peer = elected(opts);
    apply(&mut peer, 3);
    assert!(peer
        .prepare_checkpoint()
        .unwrap()
        .capture(&mut io::Cursor::new([3]), 2)
        .is_err());
    assert!(peer
        .prepare_checkpoint()
        .unwrap()
        .capture(&mut io::Cursor::new([3, 4]), 1)
        .is_err());
    let prepared = peer
        .prepare_checkpoint()
        .unwrap()
        .capture(&mut io::Cursor::new([3]), 1)
        .unwrap();
    peer.propose(vec![5]).unwrap();
    assert!(peer.install_checkpoint(&prepared).is_err());
    peer.process_ready(|_, _| Ok(())).unwrap();
    assert!(peer.install_checkpoint(&prepared).unwrap());
    assert_eq!(
        std::fs::read_dir(dir.path().join("peer.wal.payloads"))
            .unwrap()
            .count(),
        1
    );
}
