// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]
use matrixraft::{
    peer_runtime::{ApplyDecision, DurablePeer, MembershipChange, PeerOptions, PeerTuning},
    RaftError,
};
use std::{cell::RefCell, collections::BTreeMap};

struct Cluster {
    root: tempfile::TempDir,
    peers: BTreeMap<u64, DurablePeer>,
    values: BTreeMap<u64, Vec<u8>>,
    restores: BTreeMap<u64, usize>,
}
impl Cluster {
    fn options(&self, id: u64) -> PeerOptions {
        PeerOptions {
            group_id: 31,
            node_id: id,
            voters: vec![1, 2, 3],
            wal_path: self.root.path().join(format!("peer-{id}.wal")),
        }
    }
    fn new() -> Self {
        let mut cluster = Self {
            root: tempfile::tempdir().unwrap(),
            peers: BTreeMap::new(),
            values: BTreeMap::new(),
            restores: BTreeMap::new(),
        };
        for id in 1..=3 {
            let peer = DurablePeer::open_with_membership_and_tuning(
                cluster.options(id),
                vec![1, 2, 3, 4],
                PeerTuning {
                    max_apply_bytes: Some(128),
                    ..PeerTuning::default()
                },
            )
            .unwrap();
            cluster.peers.insert(id, peer);
        }
        cluster.drive(None, None);
        cluster.peers.get_mut(&1).unwrap().campaign().unwrap();
        cluster.drive(None, None);
        assert!(cluster.peers[&1].is_leader());
        cluster
    }
    fn drive(&mut self, blocked: Option<u64>, isolated: Option<u64>) {
        // Fixed rounds let consensus settle without treating a deferred application
        // as idle or repeatedly entering an unbounded host drain loop.
        for _ in 0..64 {
            let mut packets = Vec::new();
            for (id, peer) in &mut self.peers {
                let values = RefCell::new(self.values.remove(id).unwrap_or_default());
                let mut restores = 0;
                let outgoing = peer
                    .process_ready_retryable_with_snapshot_reader(
                        |_, data| {
                            if blocked == Some(*id) && !data.is_empty() {
                                return Ok(ApplyDecision::Retry);
                            }
                            values.borrow_mut().extend_from_slice(data);
                            Ok(ApplyDecision::Applied)
                        },
                        |_, reader, _| {
                            let mut restored = Vec::new();
                            reader
                                .read_to_end(&mut restored)
                                .map_err(|e| RaftError::Storage(e.to_string()))?;
                            *values.borrow_mut() = restored;
                            restores += 1;
                            Ok(())
                        },
                    )
                    .unwrap();
                self.values.insert(*id, values.into_inner());
                *self.restores.entry(*id).or_default() += restores;
                if isolated != Some(*id) {
                    packets.extend(outgoing.into_iter().filter(|p| isolated != Some(p.to)));
                }
            }
            for packet in packets {
                if let Some(peer) = self.peers.get_mut(&packet.to) {
                    peer.step(&packet.bytes).unwrap();
                }
            }
        }
    }
    fn propose(&mut self, values: std::ops::RangeInclusive<u8>) -> u64 {
        self.peers
            .get_mut(&1)
            .unwrap()
            .propose_batch(values.map(|value| vec![value]).collect())
            .unwrap()
            .last()
            .unwrap()
            .index
    }
}

#[test]
fn borrowed_batches_reduce_callback_count_without_changing_positions_or_values() {
    let root = tempfile::tempdir().unwrap();
    let mut outcomes = Vec::new();
    let mut counts = Vec::new();
    for batched in [false, true] {
        let options = PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.path().join(format!("peer-{batched}.wal")),
        };
        let mut peer = DurablePeer::open(options).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let values: Vec<_> = (0..32u8).map(|value| vec![value; 32]).collect();
        let ids = peer.propose_batch(values.clone()).unwrap();
        let mut applied = Vec::new();
        let mut calls = 0;
        if batched {
            peer.process_ready_batch(|batch| {
                calls += 1;
                assert!(!batch.is_empty());
                assert_eq!(batch.iter().len(), batch.len());
                assert!(batch.get(batch.len()).is_none());
                assert_eq!(
                    batch.iter().next_back().unwrap().position,
                    batch.get(batch.len() - 1).unwrap().position
                );
                for entry in batch.iter() {
                    applied.push((entry.position, entry.payload.to_vec()));
                }
                Ok(batch.len())
            })
            .unwrap();
        } else {
            peer.process_ready(|id, data| {
                calls += 1;
                applied.push((id, data.to_vec()));
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(peer.applied_index(), ids.last().unwrap().index);
        assert_eq!(
            applied.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
            ids
        );
        assert_eq!(
            applied
                .iter()
                .map(|(_, value)| value.clone())
                .collect::<Vec<_>>(),
            values
        );
        counts.push(calls);
        outcomes.push(applied);
    }
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(counts, vec![32, 1]);
}

#[test]
fn batch_prefix_consumption_defers_only_the_suffix_and_keeps_reads_ordered() {
    for limit in [None, Some(64)] {
        let root = tempfile::tempdir().unwrap();
        let options = PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.path().join("peer.wal"),
        };
        let mut peer = DurablePeer::open_with_tuning(
            options.clone(),
            PeerTuning {
                max_apply_bytes: limit,
                ..PeerTuning::default()
            },
        )
        .unwrap();
        peer.campaign().unwrap();
        for _ in 0..20 {
            if !peer.has_ready() {
                break;
            }
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        let ids = peer
            .propose_batch((1..=10u8).map(|value| vec![value]).collect())
            .unwrap();
        let mut values = Vec::new();
        for _ in 0..20 {
            peer.process_ready_batch(|batch| {
                let take = (2 - values.len()).min(batch.len());
                for entry in batch.iter().take(take) {
                    values.extend_from_slice(entry.payload);
                }
                Ok(take)
            })
            .unwrap();
            if values.len() == 2 {
                break;
            }
        }
        assert_eq!(values, vec![1, 2]);
        assert_eq!(peer.applied_index(), ids[1].index);
        let read = peer.begin_read().unwrap();
        let tail = peer.propose(vec![11]).unwrap();
        let mut attempts = 0;
        for _ in 0..20 {
            peer.process_ready_batch(|batch| {
                attempts += 1;
                assert_eq!(batch.get(0).unwrap().position, ids[2]);
                Ok(0)
            })
            .unwrap();
        }
        assert!(attempts > 0 && attempts <= 20);
        assert_eq!(peer.commit_index(), tail.index);
        assert_eq!(peer.applied_index(), ids[1].index);
        assert!(!peer.poll_read(read).unwrap());
        for _ in 0..100 {
            if !peer.has_ready() {
                break;
            }
            peer.process_ready_batch(|batch| {
                let take = batch.len().min(3);
                for entry in batch.iter().take(take) {
                    values.extend_from_slice(entry.payload);
                }
                Ok(take)
            })
            .unwrap();
        }
        assert_eq!(values, (1..=11).collect::<Vec<_>>());
        assert_eq!(peer.applied_index(), tail.index);
        assert!(peer.poll_read(read).unwrap());
        drop(peer);
        let mut peer = DurablePeer::open(options).unwrap();
        let mut replay = Vec::new();
        peer.process_ready_batch(|batch| {
            for entry in batch.iter() {
                replay.extend_from_slice(entry.payload);
            }
            Ok(batch.len())
        })
        .unwrap();
        assert_eq!(replay, values);
    }
}

#[test]
fn invalid_batch_progress_and_errors_stop_without_marking_the_batch_applied() {
    for invalid_count in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let options = PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.path().join("peer.wal"),
        };
        let mut peer = DurablePeer::open(options.clone()).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let before = peer.applied_index();
        peer.propose_batch(vec![vec![1], vec![2]]).unwrap();
        assert!(peer
            .process_ready_batch(|batch| if invalid_count {
                Ok(batch.len() + 1)
            } else {
                Err(RaftError::Storage("batch failed".into()))
            })
            .is_err());
        assert_eq!(peer.applied_index(), before);
        assert!(!peer.has_ready());
        assert!(peer.process_ready(|_, _| Ok(())).is_err());
        drop(peer);
        let mut peer = DurablePeer::open(options).unwrap();
        let mut replay = Vec::new();
        peer.process_ready(|_, data| {
            replay.extend_from_slice(data);
            Ok(())
        })
        .unwrap();
        assert_eq!(replay, vec![1, 2]);
    }
}

#[test]
fn batch_replay_splits_at_membership_and_does_not_skip_a_deferred_prefix() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(None, None);
    let change = cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .propose_membership(MembershipChange::AddLearner(4))
        .unwrap();
    cluster.drive(None, None);
    cluster.propose(4..=6);
    cluster.drive(None, None);
    let options = cluster.options(2);
    cluster.peers.remove(&2);
    let mut peer = DurablePeer::open_with_membership(options, vec![1, 2, 3, 4]).unwrap();
    let mut values = Vec::new();
    peer.process_ready_batch(|batch| {
        assert_eq!(
            batch.iter().next_back().unwrap().position.index,
            change.index - 1
        );
        for entry in batch.iter().take(batch.len() - 1) {
            values.extend_from_slice(entry.payload);
        }
        Ok(batch.len() - 1)
    })
    .unwrap();
    assert_eq!(values, vec![1, 2]);
    assert!(!peer.membership().learners.contains(&4));
    let mut starts = Vec::new();
    peer.process_ready_batch(|batch| {
        starts.push(batch.get(0).unwrap().position.index);
        for entry in batch.iter() {
            values.extend_from_slice(entry.payload);
        }
        Ok(batch.len())
    })
    .unwrap();
    assert_eq!(starts, vec![change.index - 1, change.index + 1]);
    assert_eq!(values, (1..=6).collect::<Vec<_>>());
    assert!(peer.membership().learners.contains(&4));
}

#[test]
fn batch_application_restores_the_checkpoint_before_its_suffix() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(None, None);
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(vec![1, 2, 3])
        .unwrap();
    cluster.propose(4..=6);
    cluster.drive(None, None);
    let options = cluster.options(1);
    cluster.peers.remove(&1);
    let mut peer = DurablePeer::open_with_membership(options, vec![1, 2, 3, 4]).unwrap();
    let values = RefCell::new(Vec::new());
    let restored = std::cell::Cell::new(0);
    peer.process_ready_batch_with_snapshot_reader(
        |batch| {
            assert_eq!(restored.get(), 1);
            for entry in batch.iter() {
                values.borrow_mut().extend_from_slice(entry.payload);
            }
            Ok(batch.len())
        },
        |_, reader, _| {
            reader
                .read_to_end(&mut values.borrow_mut())
                .map_err(|e| RaftError::Storage(e.to_string()))?;
            restored.set(restored.get() + 1);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(restored.get(), 1);
    assert_eq!(values.into_inner(), (1..=6).collect::<Vec<_>>());
}

#[test]
fn retry_preserves_partial_progress_and_read_order_without_poisoning_the_peer() {
    for bytes in [None, Some(32)] {
        let root = tempfile::tempdir().unwrap();
        let options = PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.path().join("peer.wal"),
        };
        let mut peer = DurablePeer::open_with_tuning(
            options.clone(),
            PeerTuning {
                max_apply_bytes: bytes,
                ..PeerTuning::default()
            },
        )
        .unwrap();
        peer.campaign().unwrap();
        for _ in 0..20 {
            if !peer.has_ready() {
                break;
            }
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        let ids = peer
            .propose_batch((1..=8u8).map(|value| vec![value]).collect())
            .unwrap();
        let mut values = Vec::new();
        let mut attempts = Vec::new();
        for _ in 0..20 {
            peer.process_ready_retryable(|id, data| {
                if data == [4] {
                    attempts.push(id.index);
                    return Ok(ApplyDecision::Retry);
                }
                values.extend_from_slice(data);
                Ok(ApplyDecision::Applied)
            })
            .unwrap();
        }
        assert_eq!(values, vec![1, 2, 3]);
        assert!(!attempts.is_empty());
        assert!(
            attempts.len() <= 20,
            "Retry cannot busy-loop within a Ready call"
        );
        assert!(attempts.iter().all(|index| *index == ids[3].index));
        assert_eq!(peer.applied_index(), ids[2].index);
        assert_eq!(peer.commit_index(), ids[7].index);
        assert!(peer.has_ready());
        assert!(peer.prepare_checkpoint().is_err());
        assert!(peer.checkpoint(vec![1, 2, 3]).is_err());
        let token = peer.begin_read().unwrap();
        let tail = peer.propose(vec![9]).unwrap();
        for _ in 0..10 {
            peer.process_ready_retryable(|_, _| Ok(ApplyDecision::Retry))
                .unwrap();
        }
        assert_eq!(peer.commit_index(), tail.index);
        assert!(!peer.poll_read(token).unwrap());
        for _ in 0..20 {
            if !peer.has_ready() {
                break;
            }
            peer.process_ready_retryable(|_, data| {
                values.extend_from_slice(data);
                Ok(ApplyDecision::Applied)
            })
            .unwrap();
        }
        assert_eq!(values, (1..=9).collect::<Vec<_>>());
        assert_eq!(peer.applied_index(), tail.index);
        assert!(peer.poll_read(token).unwrap());
        let failed = peer.propose(vec![55]).unwrap();
        let mut failed_call = false;
        for _ in 0..10 {
            if peer
                .process_ready_retryable(|_, _| {
                    Err(RaftError::Storage("application failed".into()))
                })
                .is_err()
            {
                failed_call = true;
                break;
            }
        }
        assert!(failed_call);
        assert!(peer.applied_index() < failed.index);
        assert!(!peer.has_ready());
        drop(peer);
        let mut peer = DurablePeer::open(options).unwrap();
        let mut replay = Vec::new();
        peer.process_ready(|_, data| {
            replay.extend_from_slice(data);
            Ok(())
        })
        .unwrap();
        values.push(55);
        assert_eq!(replay, values);
    }
}

#[test]
fn a_deferred_follower_still_supplies_durable_quorum_acknowledgments() {
    let mut cluster = Cluster::new();
    let last = cluster.propose(1..=8);
    cluster.drive(Some(2), None);
    assert_eq!(cluster.peers[&2].commit_index(), last);
    assert!(cluster.peers[&2].applied_index() < last);
    assert!(cluster.values[&2].is_empty());
    let last = cluster.propose(9..=9);
    cluster.drive(Some(2), Some(3));
    assert_eq!(
        cluster.peers[&1].commit_index(),
        last,
        "the leader needs the deferred voter's durable acknowledgment"
    );
    assert_eq!(cluster.peers[&2].commit_index(), last);
    assert!(cluster.values[&2].is_empty());
    cluster.drive(None, None);
    for _ in 0..2 {
        cluster.peers.get_mut(&1).unwrap().tick().unwrap();
        cluster.drive(None, None);
    }
    for id in 1..=3 {
        assert_eq!(cluster.values[&id], (1..=9).collect::<Vec<_>>());
    }
}

#[test]
fn deferred_work_replays_after_restart_and_holds_membership_recovery_closed() {
    let mut cluster = Cluster::new();
    let last = cluster.propose(1..=8);
    cluster.drive(Some(2), None);
    let options = cluster.options(2);
    cluster.peers.remove(&2);
    let mut peer = DurablePeer::open_with_membership_and_tuning(
        options,
        vec![1, 2, 3, 4],
        PeerTuning {
            max_apply_bytes: Some(32),
            ..PeerTuning::default()
        },
    )
    .unwrap();
    for _ in 0..8 {
        peer.process_ready_retryable(|_, data| {
            Ok(if data.is_empty() {
                ApplyDecision::Applied
            } else {
                ApplyDecision::Retry
            })
        })
        .unwrap();
        assert!(peer.applied_index() < last);
        assert!(peer.campaign().is_err());
        assert!(peer.tick().is_err());
    }
    let mut values = Vec::new();
    for _ in 0..100 {
        if !peer.has_ready() {
            break;
        }
        peer.process_ready_retryable(|_, data| {
            values.extend_from_slice(data);
            Ok(ApplyDecision::Applied)
        })
        .unwrap();
    }
    assert!(!peer.has_ready());
    assert_eq!(peer.applied_index(), last);
    assert_eq!(values, (1..=8).collect::<Vec<_>>());
    peer.tick().unwrap();
}

#[test]
fn committed_membership_waits_behind_a_deferred_application_prefix() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(Some(2), None);
    let change = cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .propose_membership(MembershipChange::AddLearner(4))
        .unwrap();
    cluster.drive(Some(2), None);
    assert_eq!(cluster.peers[&1].commit_index(), change.index);
    assert!(!cluster.peers[&2].membership().learners.contains(&4));
    assert!(cluster.peers[&1].membership().learners.contains(&4));
    cluster.drive(None, None);
    assert!(cluster.peers[&2].membership().learners.contains(&4));
    assert_eq!(cluster.values[&2], vec![1, 2, 3]);
}

#[test]
fn a_newer_checkpoint_supersedes_deferred_entries_before_suffix_application() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(Some(2), None);
    cluster.propose(4..=9);
    cluster.drive(Some(2), Some(2));
    let image = cluster.values[&1].clone();
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(image)
        .unwrap();
    let last = cluster.propose(10..=10);
    cluster.drive(Some(2), None);
    for _ in 0..4 {
        cluster.peers.get_mut(&1).unwrap().tick().unwrap();
        cluster.drive(Some(2), None);
    }
    assert_eq!(cluster.restores[&2], 1);
    assert_eq!(cluster.values[&2], (1..=9).collect::<Vec<_>>());
    assert_eq!(cluster.peers[&2].commit_index(), last);
    assert!(cluster.peers[&2].applied_index() < last);
    cluster.drive(None, None);
    assert_eq!(cluster.values[&2], (1..=10).collect::<Vec<_>>());
    assert_eq!(cluster.peers[&2].applied_index(), last);
}
