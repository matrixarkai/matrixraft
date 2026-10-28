// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]
use matrixraft::{
    peer_runtime::{DurablePeer, PeerOptions},
    RaftError,
};
use prost::Message as _;
use std::{collections::BTreeMap, path::PathBuf};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "matrixraft-peer-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn options(&self, id: u64, voters: Vec<u64>) -> PeerOptions {
        PeerOptions {
            group_id: 17,
            node_id: id,
            voters,
            wal_path: self.0.join(format!("peer-{id}.wal")),
        }
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
type Applied = BTreeMap<u64, Vec<(u64, Vec<u8>)>>;
fn pump(peers: &mut BTreeMap<u64, DurablePeer>, applied: &mut Applied, isolated: Option<u64>) {
    for _ in 0..100 {
        let mut packets = Vec::new();
        for (id, peer) in peers.iter_mut() {
            let id = *id;
            let output = peer
                .process_ready(|log, payload| {
                    applied
                        .entry(id)
                        .or_default()
                        .push((log.index, payload.to_vec()));
                    Ok(())
                })
                .unwrap();
            packets.extend(
                output
                    .into_iter()
                    .filter(|packet| isolated != Some(id) && isolated != Some(packet.to)),
            );
        }
        if packets.is_empty() {
            return;
        }
        for packet in packets {
            peers
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
        }
    }
    panic!("peer message exchange did not settle");
}
fn peers(dir: &Directory) -> BTreeMap<u64, DurablePeer> {
    (1..=3)
        .map(|id| {
            (
                id,
                DurablePeer::open(dir.options(id, vec![1, 2, 3])).unwrap(),
            )
        })
        .collect()
}
#[test]
fn isolated_peer_cannot_elect_or_commit_without_received_votes() {
    let dir = Directory::new();
    let mut peer = DurablePeer::open(dir.options(1, vec![1, 2, 3])).unwrap();
    peer.campaign().unwrap();
    let mut applied = 0;
    let packets = peer
        .process_ready(|_, _| {
            applied += 1;
            Ok(())
        })
        .unwrap();
    assert!(!packets.is_empty());
    assert!(!peer.is_leader());
    assert_eq!(peer.commit_index(), 0);
    assert_eq!(applied, 0);
    assert!(peer.propose(b"uncommitted".to_vec()).is_err());
}
#[test]
fn real_message_quorum_commits_and_replays_after_restart() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    assert!(peers[&1].is_leader());
    let log = peers
        .get_mut(&1)
        .unwrap()
        .propose(b"binary outcome".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, Some(1));
    assert!(peers[&1].commit_index() < log.index);
    assert!(!applied[&1].iter().any(|(_, p)| p == b"binary outcome"));
    for _ in 0..3 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    for id in 1..=3 {
        assert!(applied[&id]
            .iter()
            .any(|(index, p)| *index == log.index && p == b"binary outcome"));
    }
    let expected = applied[&1].clone();
    drop(peers.remove(&1));
    applied.remove(&1);
    peers.insert(1, DurablePeer::open(dir.options(1, vec![1, 2, 3])).unwrap());
    pump(&mut peers, &mut applied, None);
    assert_eq!(applied[&1], expected);
}
#[test]
fn majority_replaces_an_isolated_leaders_uncommitted_tail() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    peers
        .get_mut(&1)
        .unwrap()
        .propose(b"orphan".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, Some(1));
    let mut replacement = None;
    for _ in 0..100 {
        for peer in peers.values_mut() {
            peer.tick().unwrap();
        }
        pump(&mut peers, &mut applied, Some(1));
        replacement = (2..=3).find(|id| peers[id].is_leader());
        if replacement.is_some() {
            break;
        }
    }
    let leader = replacement.expect("reachable majority elects a leader");
    peers
        .get_mut(&leader)
        .unwrap()
        .propose(b"majority".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, Some(1));
    for _ in 0..10 {
        peers.get_mut(&leader).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    for id in 1..=3 {
        assert!(!applied[&id].iter().any(|(_, p)| p == b"orphan"));
        assert!(applied[&id].iter().any(|(_, p)| p == b"majority"));
        assert_eq!(applied[&id], applied[&leader]);
    }
}
#[test]
fn durable_identity_lock_and_packet_routing_are_enforced() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1, 2, 3]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    assert!(DurablePeer::open(options.clone()).is_err());
    peer.campaign().unwrap();
    let packet = peer.process_ready(|_, _| Ok(())).unwrap().remove(0);
    assert!(peer.step(&packet.bytes).is_err());
    drop(peer);
    let mut wrong = options.clone();
    wrong.group_id += 1;
    assert!(DurablePeer::open(wrong).is_err());
    let mut wrong = options.clone();
    wrong.voters = vec![1, 2];
    assert!(DurablePeer::open(wrong).is_err());
    assert!(DurablePeer::open(options).is_ok());
}
#[test]
fn application_failure_stops_peer_and_committed_data_replays() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.propose(b"retry after restart".to_vec()).unwrap();
    assert!(peer
        .process_ready(|_, payload| if payload.is_empty() {
            Ok(())
        } else {
            Err(RaftError::Storage("apply failed".into()))
        })
        .is_err());
    assert!(!peer.is_leader());
    assert!(peer.tick().is_err());
    assert!(peer.propose(b"later".to_vec()).is_err());
    drop(peer);
    let mut peer = DurablePeer::open(options).unwrap();
    let mut payloads = Vec::new();
    peer.process_ready(|_, p| {
        payloads.push(p.to_vec());
        Ok(())
    })
    .unwrap();
    assert!(payloads.iter().any(|p| p == b"retry after restart"));
}

#[test]
fn quorum_reads_wait_for_messages_without_growing_any_wal() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    peers
        .get_mut(&1)
        .unwrap()
        .propose(b"visible".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, None);
    for _ in 0..3 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    assert_eq!(applied[&1], applied[&2]);
    assert_eq!(applied[&1], applied[&3]);
    let before = applied.clone();
    let files: Vec<_> = (1..=3)
        .map(|id| std::fs::read(dir.options(id, vec![1, 2, 3]).wal_path).unwrap())
        .collect();
    let token = peers.get_mut(&1).unwrap().begin_read().unwrap();
    assert!(peers.get_mut(&1).unwrap().begin_read().is_err());
    assert!(peers.get_mut(&2).unwrap().begin_read().is_err());
    pump(&mut peers, &mut applied, Some(1));
    assert!(!peers.get_mut(&1).unwrap().poll_read(token).unwrap());
    assert!(peers.get_mut(&1).unwrap().poll_read(token + 1).is_err());
    for _ in 0..3 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    assert!(peers.get_mut(&1).unwrap().poll_read(token).unwrap());
    assert!(peers.get_mut(&1).unwrap().poll_read(token).is_err());
    for _ in 0..64 {
        let next = peers.get_mut(&1).unwrap().begin_read().unwrap();
        assert_ne!(next, token);
        assert!(!peers.get_mut(&1).unwrap().poll_read(next).unwrap());
        pump(&mut peers, &mut applied, None);
        assert!(peers.get_mut(&1).unwrap().poll_read(next).unwrap());
    }
    assert_eq!(applied, before);
    for (id, bytes) in (1..=3).zip(files) {
        assert_eq!(
            std::fs::read(dir.options(id, vec![1, 2, 3]).wal_path).unwrap(),
            bytes
        );
    }
}
#[test]
fn read_confirmation_is_invalid_after_leadership_changes() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    let token = peers.get_mut(&1).unwrap().begin_read().unwrap();
    // Even a completed confirmation cannot survive a later leadership change.
    pump(&mut peers, &mut applied, None);
    let mut replacement = None;
    for _ in 0..100 {
        for peer in peers.values_mut() {
            peer.tick().unwrap();
        }
        pump(&mut peers, &mut applied, Some(1));
        replacement = (2..=3).find(|id| peers[id].is_leader());
        if replacement.is_some() {
            break;
        }
    }
    let leader = replacement.unwrap();
    peers
        .get_mut(&leader)
        .unwrap()
        .propose(b"new term".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, None);
    for _ in 0..3 {
        peers.get_mut(&leader).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    assert!(!peers[&1].is_leader());
    assert_eq!(peers[&1].term(), peers[&leader].term());
    assert!(peers.get_mut(&1).unwrap().poll_read(token).is_err());
    let token = peers.get_mut(&leader).unwrap().begin_read().unwrap();
    pump(&mut peers, &mut applied, None);
    assert!(peers.get_mut(&leader).unwrap().poll_read(token).unwrap());
}
#[test]
fn read_confirmation_requires_successful_local_application() {
    let dir = Directory::new();
    let mut peer = DurablePeer::open(dir.options(1, vec![1])).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.propose(b"must apply".to_vec()).unwrap();
    let token = peer.begin_read().unwrap();
    assert!(!peer.poll_read(token).unwrap());
    assert!(peer
        .process_ready(|_, payload| {
            if payload.is_empty() {
                Ok(())
            } else {
                Err(RaftError::Storage("injected apply failure".into()))
            }
        })
        .is_err());
    assert!(peer.poll_read(token).is_err());
    assert!(peer.begin_read().is_err());
}

#[derive(Default)]
struct CheckpointApp {
    value: u64,
    restored: usize,
}
#[derive(Clone, PartialEq, prost::Message)]
struct TestPacket {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    group: u64,
    #[prost(bytes = "vec", tag = "3")]
    message: Vec<u8>,
}
fn pump_checkpoints(
    peers: &mut BTreeMap<u64, DurablePeer>,
    apps: &mut BTreeMap<u64, CheckpointApp>,
    isolated: Option<u64>,
) {
    pump_checkpoint_packets(peers, apps, isolated, None);
}
fn pump_checkpoint_packets(
    peers: &mut BTreeMap<u64, DurablePeer>,
    apps: &mut BTreeMap<u64, CheckpointApp>,
    isolated: Option<u64>,
    drop_kind: Option<raft_core::eraftpb::MessageType>,
) -> usize {
    let mut dropped = 0;
    for _ in 0..100 {
        let mut packets = Vec::new();
        for (id, peer) in peers.iter_mut() {
            let app = std::cell::RefCell::new(apps.entry(*id).or_default());
            let output = peer
                .process_ready_with_snapshot(
                    |_, bytes| {
                        if !bytes.is_empty() {
                            app.borrow_mut().value = u64::from_be_bytes(bytes.try_into().unwrap());
                        }
                        Ok(())
                    },
                    |_, bytes| {
                        let mut app = app.borrow_mut();
                        app.value = u64::from_be_bytes(bytes.try_into().unwrap());
                        app.restored += 1;
                        Ok(())
                    },
                )
                .unwrap();
            packets.extend(output.into_iter().filter(|p| {
                let packet = TestPacket::decode(p.bytes.as_slice()).unwrap();
                let message =
                    raft_core::eraftpb::Message::decode(packet.message.as_slice()).unwrap();
                if drop_kind == Some(message.get_msg_type())
                    && (message.to == 3 || message.from == 3)
                {
                    dropped += 1;
                    return false;
                }
                isolated != Some(*id) && isolated != Some(p.to)
            }));
        }
        if packets.is_empty() {
            return dropped;
        }
        for packet in packets {
            peers
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
        }
    }
    panic!("checkpoint exchange did not settle");
}
#[test]
fn checkpoint_catches_up_a_lagging_voter_and_all_peers_recover() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut apps = BTreeMap::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    for value in 1u64..=32 {
        peers
            .get_mut(&1)
            .unwrap()
            .propose(value.to_be_bytes().to_vec())
            .unwrap();
        pump_checkpoints(&mut peers, &mut apps, Some(3));
    }
    assert_eq!(apps[&1].value, 32);
    assert_eq!(apps[&3].value, 0);
    let path = dir.options(1, vec![1, 2, 3]).wal_path;
    let before = std::fs::metadata(&path).unwrap().len();
    peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(32u64.to_be_bytes().to_vec())
        .unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() < before);
    assert!(
        DurablePeer::open(dir.options(1, vec![1, 2, 3])).is_err(),
        "replacement WAL must remain exclusively locked"
    );
    for _ in 0..10 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump_checkpoints(&mut peers, &mut apps, None);
    }
    assert_eq!(apps[&3].value, 32);
    assert_eq!(apps[&3].restored, 1);
    peers
        .get_mut(&1)
        .unwrap()
        .propose(33u64.to_be_bytes().to_vec())
        .unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    for _ in 0..3 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump_checkpoints(&mut peers, &mut apps, None);
    }
    drop(peers);
    apps.clear();
    let mut peers = self::peers(&dir);
    pump_checkpoints(&mut peers, &mut apps, None);
    for id in 1..=3 {
        assert_eq!(apps[&id].value, 33);
    }
    assert_eq!(apps[&1].restored, 1);
    assert_eq!(apps[&3].restored, 1);
}
#[test]
fn checkpoint_preserves_an_uncommitted_suffix_without_applying_it_on_restart() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut apps = BTreeMap::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    peers
        .get_mut(&1)
        .unwrap()
        .propose(1u64.to_be_bytes().to_vec())
        .unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    let undecided = peers
        .get_mut(&1)
        .unwrap()
        .propose(999u64.to_be_bytes().to_vec())
        .unwrap();
    pump_checkpoints(&mut peers, &mut apps, Some(1));
    peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(1u64.to_be_bytes().to_vec())
        .unwrap();
    drop(peers);
    apps.clear();
    let mut peers = self::peers(&dir);
    pump_checkpoints(&mut peers, &mut apps, Some(1));
    assert_eq!(apps[&1].value, 1);
    assert!(peers[&1].commit_index() < undecided.index);
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    assert_eq!(apps[&1].value, 999);
}
#[test]
fn checkpoint_publication_failure_preserves_the_old_wal_and_stops_the_peer() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.propose(7u64.to_be_bytes().to_vec()).unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    let before = std::fs::read(&options.wal_path).unwrap();
    assert!(peer
        .checkpoint(vec![0; matrixraft::peer_runtime::MAX_CHECKPOINT_BYTES + 1])
        .is_err());
    assert!(peer.is_leader());
    let temporary = options.wal_path.with_extension("checkpoint.tmp");
    std::fs::create_dir(&temporary).unwrap();
    assert!(peer.checkpoint(7u64.to_be_bytes().to_vec()).is_err());
    assert!(!peer.is_leader());
    assert!(peer.tick().is_err());
    assert_eq!(std::fs::read(&options.wal_path).unwrap(), before);
    drop(peer);
    let mut reopened = DurablePeer::open(options).unwrap();
    let mut seen = false;
    reopened
        .process_ready(|_, bytes| {
            seen |= bytes == 7u64.to_be_bytes();
            Ok(())
        })
        .unwrap();
    assert!(seen);
}
#[test]
fn checkpoint_recovery_requires_restoration_and_refuses_every_truncated_prefix() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.checkpoint(4u64.to_be_bytes().to_vec()).unwrap();
    drop(peer);
    let full = std::fs::read(&options.wal_path).unwrap();
    for cut in 0..full.len() {
        std::fs::write(&options.wal_path, &full[..cut]).unwrap();
        assert!(DurablePeer::open(options.clone()).is_err(), "cut {cut}");
        assert_eq!(std::fs::read(&options.wal_path).unwrap(), full[..cut]);
    }
    for index in 0..full.len() {
        let mut damaged = full.clone();
        damaged[index] ^= 1;
        std::fs::write(&options.wal_path, &damaged).unwrap();
        assert!(DurablePeer::open(options.clone()).is_err(), "byte {index}");
        assert_eq!(std::fs::read(&options.wal_path).unwrap(), damaged);
    }
    std::fs::write(&options.wal_path, &full).unwrap();
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    assert!(peer.process_ready(|_, _| Ok(())).is_err());
    assert!(peer.tick().is_err());
    drop(peer);
    let mut peer = DurablePeer::open(options).unwrap();
    assert!(peer
        .process_ready_with_snapshot(
            |_, _| Ok(()),
            |_, _| Err(RaftError::Storage("restore failed".into()))
        )
        .is_err());
    assert!(peer.tick().is_err());
}

#[test]
fn repeated_checkpoints_bound_history_and_missing_wal_is_not_reinitialized() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    for value in 1u64..=64 {
        peer.propose(value.to_be_bytes().to_vec()).unwrap();
        peer.process_ready_with_snapshot(|_, _| Ok(()), |_, _| Ok(()))
            .unwrap();
        peer.checkpoint(value.to_be_bytes().to_vec()).unwrap();
        assert!(std::fs::metadata(&options.wal_path).unwrap().len() < 256);
    }
    drop(peer);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    let mut restored = 0;
    peer.process_ready_with_snapshot(
        |_, _| Ok(()),
        |_, bytes| {
            restored = u64::from_be_bytes(bytes.try_into().unwrap());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(restored, 64);
    drop(peer);
    std::fs::remove_file(&options.wal_path).unwrap();
    assert!(DurablePeer::open(options.clone()).is_err());
    assert!(!options.wal_path.exists());
}

#[test]
fn lost_snapshots_and_acknowledgments_are_retried_without_reapplying_state() {
    for kind in [
        raft_core::eraftpb::MessageType::MsgSnapshot,
        raft_core::eraftpb::MessageType::MsgAppendResponse,
    ] {
        let dir = Directory::new();
        let mut peers = peers(&dir);
        let mut apps = BTreeMap::new();
        peers.get_mut(&1).unwrap().campaign().unwrap();
        pump_checkpoints(&mut peers, &mut apps, None);
        peers
            .get_mut(&1)
            .unwrap()
            .propose(32u64.to_be_bytes().to_vec())
            .unwrap();
        pump_checkpoints(&mut peers, &mut apps, Some(3));
        peers
            .get_mut(&1)
            .unwrap()
            .checkpoint(32u64.to_be_bytes().to_vec())
            .unwrap();
        let mut dropped = 0;
        for _ in 0..24 {
            peers.get_mut(&1).unwrap().tick().unwrap();
            dropped += pump_checkpoint_packets(&mut peers, &mut apps, None, Some(kind));
        }
        assert!(dropped > 0);
        if kind == raft_core::eraftpb::MessageType::MsgSnapshot {
            assert_eq!(apps[&3].value, 0);
        }
        for _ in 0..24 {
            peers.get_mut(&1).unwrap().tick().unwrap();
            pump_checkpoints(&mut peers, &mut apps, None);
        }
        assert_eq!(apps[&3].value, 32);
        assert_eq!(apps[&3].restored, 1);
        peers
            .get_mut(&1)
            .unwrap()
            .propose(33u64.to_be_bytes().to_vec())
            .unwrap();
        pump_checkpoints(&mut peers, &mut apps, None);
        for _ in 0..3 {
            peers.get_mut(&1).unwrap().tick().unwrap();
            pump_checkpoints(&mut peers, &mut apps, None);
        }
        assert_eq!(apps[&3].value, 33);
    }
}
