// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]
use matrixraft::{
    peer_runtime::{ApplyDecision, DurablePeer, PeerOptions, PeerTuning, RestoreDecision},
    RaftError,
};
use prost::Message as _;
use std::collections::BTreeMap;

struct Cluster {
    root: tempfile::TempDir,
    peers: BTreeMap<u64, DurablePeer>,
    values: BTreeMap<u64, Vec<u8>>,
}
impl Cluster {
    fn options(&self, id: u64) -> PeerOptions {
        PeerOptions {
            group_id: 71,
            node_id: id,
            voters: vec![1, 2, 3],
            wal_path: self.root.path().join(format!("{id}.wal")),
        }
    }
    fn tuning() -> PeerTuning {
        PeerTuning {
            max_apply_bytes: Some(1),
            max_log_cache_bytes: Some(512),
            max_pending_reads: 4,
            ..PeerTuning::default()
        }
    }
    fn new() -> Self {
        Self::create(false)
    }
    fn create(dynamic_membership: bool) -> Self {
        let mut cluster = Self {
            root: tempfile::tempdir().unwrap(),
            peers: BTreeMap::new(),
            values: BTreeMap::new(),
        };
        for id in 1..=3 {
            let peer = if dynamic_membership {
                DurablePeer::open_with_membership_and_tuning(
                    cluster.options(id),
                    vec![1, 2, 3, 4],
                    Self::tuning(),
                )
                .unwrap()
            } else {
                DurablePeer::open_with_tuning(cluster.options(id), Self::tuning()).unwrap()
            };
            cluster.peers.insert(id, peer);
        }
        cluster.pump(None);
        cluster.peers.get_mut(&1).unwrap().campaign().unwrap();
        cluster.pump(None);
        assert!(cluster.peers[&1].is_leader());
        cluster
    }
    fn pump(&mut self, retry: Option<u64>) {
        for _ in 0..1000 {
            let mut packets = Vec::new();
            for (id, peer) in &mut self.peers {
                let values = self.values.entry(*id).or_default();
                packets.extend(
                    peer.process_ready_retryable(|_, bytes| {
                        if retry == Some(*id) {
                            return Ok(ApplyDecision::Retry);
                        }
                        if let Some(value) = bytes.first() {
                            values.push(*value);
                        }
                        Ok(ApplyDecision::Applied)
                    })
                    .unwrap(),
                );
            }
            if packets.is_empty()
                && self
                    .peers
                    .iter()
                    .all(|(id, peer)| retry == Some(*id) || !peer.has_ready())
            {
                return;
            }
            for packet in packets {
                if let Some(peer) = self.peers.get_mut(&packet.to) {
                    peer.step(&packet.bytes).unwrap();
                }
            }
        }
        panic!("shutdown fixture did not settle");
    }
    fn commit_waiting(&mut self) -> u64 {
        let ids = self
            .peers
            .get_mut(&1)
            .unwrap()
            .propose_batch((1..=16).map(|n| vec![n; 64]).collect())
            .unwrap();
        self.pump(Some(1));
        let last = ids.last().unwrap().index;
        assert_eq!(self.peers[&1].commit_index(), last);
        assert!(self.peers[&1].applied_index() < last);
        last
    }
}

#[derive(prost::Message)]
struct Envelope {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    group: u64,
    #[prost(bytes = "vec", tag = "3")]
    message: Vec<u8>,
}

#[test]
fn shutdown_drains_the_captured_fence_with_retries_and_rejects_new_work() {
    let mut cluster = Cluster::new();
    let fence = cluster.commit_waiting();
    let mut peer = cluster.peers.remove(&1).unwrap();
    let vote_before = std::fs::read(&cluster.options(1).wal_path).unwrap();
    assert_eq!(peer.begin_shutdown().unwrap(), fence);
    assert_eq!(peer.begin_shutdown().unwrap(), fence);
    assert!(!peer.is_leader());
    assert!(peer.propose(vec![99]).is_err());
    assert!(peer.propose_batch(vec![vec![99]]).is_err());
    assert!(peer.begin_read().is_err());
    assert!(peer.campaign().is_err());
    assert!(peer.force_campaign().is_err());
    assert!(peer.tick().is_err());
    assert!(peer.prepare_checkpoint().is_err());
    assert!(peer.enable_async_log_reads(1).is_err());
    let heartbeat = raft_core::eraftpb::Message {
        from: 2,
        to: 1,
        term: peer.term(),
        msg_type: raft_core::eraftpb::MessageType::MsgHeartbeat as i32,
        ..Default::default()
    };
    let bytes = Envelope {
        version: 1,
        group: 71,
        message: heartbeat.encode_to_vec(),
    }
    .encode_to_vec();
    assert!(peer.step(&bytes).is_err());
    for _ in 0..3 {
        assert!(peer
            .process_ready_retryable(|_, _| Ok(ApplyDecision::Retry))
            .unwrap()
            .is_empty());
        assert!(!peer.poll_shutdown().unwrap());
    }
    let mut values = Vec::new();
    while !peer.poll_shutdown().unwrap() {
        assert!(peer.has_ready());
        assert!(peer
            .process_ready(|_, bytes| {
                if let Some(value) = bytes.first() {
                    values.push(*value);
                }
                Ok(())
            })
            .unwrap()
            .is_empty());
    }
    assert_eq!(values, (1..=16).collect::<Vec<_>>());
    assert_eq!(peer.applied_index(), fence);
    assert!(!peer.has_ready());
    assert_eq!(
        std::fs::read(&cluster.options(1).wal_path).unwrap(),
        vote_before,
        "local shutdown must not invent a new durable term or vote"
    );
    assert!(peer
        .process_ready(|_, _| panic!("stopped peer cannot apply"))
        .is_err());
    assert!(
        DurablePeer::open(cluster.options(1)).is_err(),
        "stopping must retain exclusive storage ownership until drop"
    );
    drop(peer);
    let mut peer = DurablePeer::open_with_tuning(cluster.options(1), Cluster::tuning()).unwrap();
    let mut replay = Vec::new();
    while peer.has_ready() {
        peer.process_ready(|_, bytes| {
            if let Some(value) = bytes.first() {
                replay.push(*value);
            }
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(replay, values);
}

#[test]
fn immediate_stop_cancels_application_without_erasing_the_committed_tail() {
    let mut cluster = Cluster::new();
    let fence = cluster.commit_waiting();
    let mut peer = cluster.peers.remove(&1).unwrap();
    let applied = peer.applied_index();
    peer.stop();
    peer.stop();
    assert_eq!(peer.applied_index(), applied);
    assert!(applied < fence);
    assert!(!peer.has_ready());
    assert!(peer.poll_shutdown().is_err());
    assert!(peer.begin_shutdown().is_err());
    assert!(peer
        .process_ready(|_, _| panic!("immediate stop must not drain"))
        .is_err());
    drop(peer);
    let mut peer = DurablePeer::open_with_tuning(cluster.options(1), Cluster::tuning()).unwrap();
    let mut replay = Vec::new();
    while peer.has_ready() {
        peer.process_ready(|_, bytes| {
            if let Some(value) = bytes.first() {
                replay.push(*value);
            }
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(replay, (1..=16).collect::<Vec<_>>());
    assert_eq!(peer.applied_index(), fence);
    cluster.values.insert(1, replay);
    peer.force_campaign().unwrap();
    cluster.peers.insert(1, peer);
    cluster.pump(None);
    assert!(
        cluster.peers[&1].is_leader(),
        "reopening starts a fresh local lifecycle"
    );
}

#[test]
fn shutdown_does_not_force_an_uncommitted_proposal_or_truncate_its_log() {
    let mut cluster = Cluster::new();
    let mut peer = cluster.peers.remove(&1).unwrap();
    let id = peer.propose(vec![99]).unwrap();
    let packets = peer.process_ready(|_, _| Ok(())).unwrap();
    assert!(!packets.is_empty());
    assert!(peer.commit_index() < id.index);
    let fence = peer.commit_index();
    let before = std::fs::read(&cluster.options(1).wal_path).unwrap();
    assert_eq!(peer.begin_shutdown().unwrap(), fence);
    assert!(peer.poll_shutdown().unwrap());
    assert_eq!(std::fs::read(&cluster.options(1).wal_path).unwrap(), before);
    drop(peer);
    let mut peer = DurablePeer::open(cluster.options(1)).unwrap();
    peer.process_ready(|_, bytes| {
        assert!(bytes.is_empty());
        Ok(())
    })
    .unwrap();
    let entries = peer
        .capture_log_reader()
        .unwrap()
        .entries(id.index, id.index + 1, None)
        .unwrap();
    assert_eq!(entries[0].data, vec![99]);
    assert_eq!(peer.commit_index(), fence);
}

#[test]
fn shutdown_waits_for_file_checkpoint_restore_and_its_committed_suffix() {
    let root = tempfile::tempdir().unwrap();
    let options = PeerOptions {
        group_id: 71,
        node_id: 1,
        voters: vec![1],
        wal_path: root.path().join("1.wal"),
    };
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    let image = vec![7; 128 * 1024];
    peer.propose(image.clone()).unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.checkpoint_from_reader(&mut std::io::Cursor::new(&image), image.len() as u64)
        .unwrap();
    peer.propose(vec![8; 32]).unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    let fence = peer.commit_index();
    drop(peer);
    let mut peer = DurablePeer::open_with_tuning(options, Cluster::tuning()).unwrap();
    assert_eq!(peer.applied_index(), 0);
    assert_eq!(peer.begin_shutdown().unwrap(), fence);
    for _ in 0..3 {
        assert!(peer
            .process_ready_batch_with_restore(
                |_| panic!("restore must precede suffix"),
                |_, reader, length| {
                    assert_eq!(length, image.len() as u64);
                    let mut first = [0; 16];
                    reader.read_exact(&mut first).unwrap();
                    assert_eq!(first, [7; 16]);
                    Ok(RestoreDecision::Retry)
                }
            )
            .unwrap()
            .is_empty());
        assert!(!peer.poll_shutdown().unwrap());
    }
    let mut restored = Vec::new();
    let mut suffix = Vec::new();
    while !peer.poll_shutdown().unwrap() {
        peer.process_ready_batch_with_restore(
            |batch| {
                for entry in batch.iter() {
                    suffix.push(entry.payload.to_vec());
                }
                Ok(batch.len())
            },
            |_, reader, _| {
                reader.read_to_end(&mut restored).unwrap();
                Ok(RestoreDecision::Restored)
            },
        )
        .unwrap();
    }
    assert_eq!(restored, image);
    assert_eq!(suffix, vec![vec![8; 32]]);
    assert_eq!(peer.applied_index(), fence);
}

#[test]
fn shutdown_can_be_aborted_but_application_failure_is_not_completion() {
    for fail in [false, true] {
        let mut cluster = Cluster::new();
        let fence = cluster.commit_waiting();
        let mut peer = cluster.peers.remove(&1).unwrap();
        peer.begin_shutdown().unwrap();
        if fail {
            assert!(peer
                .process_ready(|_, _| Err(RaftError::Storage("application unavailable".into())))
                .is_err());
        }
        peer.stop();
        assert!(peer.applied_index() < fence);
        assert!(peer.poll_shutdown().is_err());
        assert!(peer.begin_shutdown().is_err());
    }
}

#[test]
fn empty_shutdown_is_idempotent_and_does_not_write_lifecycle_state() {
    let root = tempfile::tempdir().unwrap();
    let options = PeerOptions {
        group_id: 71,
        node_id: 1,
        voters: vec![1],
        wal_path: root.path().join("1.wal"),
    };
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    let before = std::fs::read(&options.wal_path).unwrap();
    assert!(peer.poll_shutdown().is_err());
    assert_eq!(peer.begin_shutdown().unwrap(), 0);
    assert!(peer.poll_shutdown().unwrap());
    assert_eq!(peer.begin_shutdown().unwrap(), 0);
    assert!(!peer.has_ready());
    peer.stop();
    assert!(peer.poll_shutdown().unwrap());
    assert_eq!(std::fs::read(&options.wal_path).unwrap(), before);
}

#[test]
fn remaining_voters_elect_and_commit_after_a_peer_stops() {
    let mut cluster = Cluster::new();
    let fence = cluster.commit_waiting();
    let mut stopped = cluster.peers.remove(&1).unwrap();
    stopped.stop();
    let mut leader = None;
    for _ in 0..100 {
        for peer in cluster.peers.values_mut() {
            peer.tick().unwrap();
        }
        cluster.pump(None);
        leader = cluster
            .peers
            .iter()
            .find_map(|(id, peer)| peer.is_leader().then_some(*id));
        if leader.is_some() {
            break;
        }
    }
    let leader = leader.unwrap();
    cluster
        .peers
        .get_mut(&leader)
        .unwrap()
        .propose(vec![99])
        .unwrap();
    cluster.pump(None);
    let expected: Vec<_> = (1..=16).chain(std::iter::once(99)).collect();
    for id in [2, 3] {
        assert_eq!(cluster.values[&id], expected);
    }
    assert_eq!(stopped.commit_index(), fence);
    assert!(!stopped.is_leader());
    assert!(!stopped.has_ready());
}

#[test]
fn graceful_shutdown_applies_committed_membership_during_local_draining() {
    use matrixraft::peer_runtime::MembershipChange;
    let mut cluster = Cluster::create(true);
    cluster.peers.get_mut(&1).unwrap().propose(vec![7]).unwrap();
    cluster.pump(Some(1));
    cluster.peers.get_mut(&2).unwrap().force_campaign().unwrap();
    cluster.pump(Some(1));
    assert!(cluster.peers[&2].is_leader());
    let log = cluster
        .peers
        .get_mut(&2)
        .unwrap()
        .propose_membership(MembershipChange::AddLearner(4))
        .unwrap();
    cluster.pump(Some(1));
    let mut peer = cluster.peers.remove(&1).unwrap();
    assert_eq!(peer.commit_index(), log.index);
    assert!(peer.applied_index() < log.index);
    assert_eq!(peer.begin_shutdown().unwrap(), log.index);
    while !peer.poll_shutdown().unwrap() {
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    assert_eq!(peer.membership().learners, vec![4]);
    drop(peer);
    let mut peer = DurablePeer::open_with_membership_and_tuning(
        cluster.options(1),
        vec![1, 2, 3, 4],
        Cluster::tuning(),
    )
    .unwrap();
    peer.begin_shutdown().unwrap();
    while !peer.poll_shutdown().unwrap() {
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    assert_eq!(peer.membership().learners, vec![4]);
}

#[test]
fn shutdown_persists_and_restores_an_already_accepted_checkpoint() {
    use raft_core::eraftpb::{Message, MessageType};
    let mut cluster = Cluster::new();
    let mut lagging = cluster.peers.remove(&3).unwrap();
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .propose_batch((1..=16).map(|n| vec![n; 64]).collect())
        .unwrap();
    cluster.pump(None);
    let image: Vec<_> = (1..=16).collect();
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(image.clone())
        .unwrap();
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .report_unreachable(3)
        .unwrap();
    let response = Message {
        from: 3,
        to: 1,
        term: lagging.term(),
        msg_type: MessageType::MsgHeartbeatResponse as i32,
        ..Default::default()
    };
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .step(
            &Envelope {
                version: 1,
                group: 71,
                message: response.encode_to_vec(),
            }
            .encode_to_vec(),
        )
        .unwrap();
    let packets = cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .process_ready(|_, _| Ok(()))
        .unwrap();
    let snapshot = packets
        .into_iter()
        .find(|packet| {
            if packet.to != 3 {
                return false;
            }
            let envelope = Envelope::decode(packet.bytes.as_slice()).unwrap();
            Message::decode(envelope.message.as_slice())
                .unwrap()
                .get_msg_type()
                == MessageType::MsgSnapshot
        })
        .unwrap();
    lagging.step(&snapshot.bytes).unwrap();
    let fence = lagging.commit_index();
    assert!(lagging.applied_index() < fence);
    assert_eq!(lagging.begin_shutdown().unwrap(), fence);
    assert!(lagging
        .process_ready_retryable_with_restore(
            |_, _| panic!("unexpected suffix"),
            |_, _, _| Ok(RestoreDecision::Retry)
        )
        .unwrap()
        .is_empty());
    assert!(!lagging.poll_shutdown().unwrap());
    let mut restored = Vec::new();
    while !lagging.poll_shutdown().unwrap() {
        lagging
            .process_ready_with_snapshot(
                |_, _| Ok(()),
                |_, bytes| {
                    restored = bytes.to_vec();
                    Ok(())
                },
            )
            .unwrap();
    }
    assert_eq!(restored, image);
    drop(lagging);
    let mut peer = DurablePeer::open(cluster.options(3)).unwrap();
    peer.process_ready_with_snapshot(
        |_, _| Ok(()),
        |_, bytes| {
            assert_eq!(bytes, image);
            Ok(())
        },
    )
    .unwrap();
}
