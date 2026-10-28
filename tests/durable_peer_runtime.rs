// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]
use matrixraft::{
    peer_runtime::{DurablePeer, PeerOptions},
    RaftError,
};
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
