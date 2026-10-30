// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]

use matrixraft::peer_runtime::{DurablePeer, PeerOptions};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

#[derive(Serialize, Deserialize)]
enum Request {
    Campaign,
    Propose(Vec<u8>),
    Step(Vec<u8>),
    Inspect,
}
#[derive(Serialize, Deserialize)]
struct Reply {
    packets: Vec<(u64, Vec<u8>)>,
    applied: Vec<Vec<u8>>,
}

fn send<T: Serialize>(stream: &mut TcpStream, value: &T) {
    let bytes = serde_json::to_vec(value).unwrap();
    assert!(bytes.len() <= 8 * 1024 * 1024);
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&bytes).unwrap();
}
fn receive<T: serde::de::DeserializeOwned>(stream: &mut TcpStream) -> T {
    let mut size = [0; 4];
    stream.read_exact(&mut size).unwrap();
    let size = u32::from_be_bytes(size) as usize;
    assert!(size <= 8 * 1024 * 1024);
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
fn timeouts(stream: &TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
}

#[test]
#[ignore = "subprocess entry point for loopback transport test"]
fn peer_transport_child() {
    let Some(root) = std::env::var_os("MATRIXRAFT_PROCESS_TEST_DIR") else {
        return;
    };
    let id: u64 = std::env::var("MATRIXRAFT_PROCESS_TEST_ID")
        .unwrap()
        .parse()
        .unwrap();
    let count: u64 = std::env::var("MATRIXRAFT_PROCESS_TEST_COUNT")
        .unwrap()
        .parse()
        .unwrap();
    let root = Path::new(&root);
    let mut peer = DurablePeer::open(PeerOptions {
        group_id: 31,
        node_id: id,
        voters: (1..=count).collect(),
        wal_path: root.join(format!("{id}.wal")),
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let pending = root.join(format!("{id}.pending"));
    std::fs::write(&pending, address).unwrap();
    std::fs::rename(pending, root.join(format!("{id}.addr"))).unwrap();
    let mut applied = Vec::new();
    for connection in listener.incoming() {
        let mut stream = connection.unwrap();
        timeouts(&stream);
        match receive(&mut stream) {
            Request::Campaign => peer.campaign().unwrap(),
            Request::Propose(bytes) => {
                peer.propose(bytes).unwrap();
            }
            Request::Step(bytes) => peer.step(&bytes).unwrap(),
            Request::Inspect => {}
        }
        let packets = peer
            .process_ready(|_, bytes| {
                if !bytes.is_empty() {
                    applied.push(bytes.to_vec());
                }
                Ok(())
            })
            .unwrap()
            .into_iter()
            .map(|packet| (packet.to, packet.bytes))
            .collect();
        send(
            &mut stream,
            &Reply {
                packets,
                applied: applied.clone(),
            },
        );
    }
}

struct Voter {
    child: Child,
    address: String,
}
impl Drop for Voter {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Voter {
    fn start(root: &Path, id: u64, count: u64) -> Self {
        let address_file = root.join(format!("{id}.addr"));
        if address_file.exists() {
            std::fs::remove_file(&address_file).unwrap();
        }
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "peer_transport_child",
                "--ignored",
                "--nocapture",
            ])
            .env("MATRIXRAFT_PROCESS_TEST_DIR", root)
            .env("MATRIXRAFT_PROCESS_TEST_ID", id.to_string())
            .env("MATRIXRAFT_PROCESS_TEST_COUNT", count.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut voter = Self {
            child,
            address: String::new(),
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            assert!(
                voter.child.try_wait().unwrap().is_none(),
                "child exited during startup"
            );
            if let Ok(address) = std::fs::read_to_string(&address_file) {
                voter.address = address;
                return voter;
            }
            assert!(Instant::now() < deadline, "child startup timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn request(&self, request: Request) -> Reply {
        let mut stream =
            TcpStream::connect_timeout(&self.address.parse().unwrap(), Duration::from_secs(5))
                .unwrap();
        timeouts(&stream);
        send(&mut stream, &request);
        receive(&mut stream)
    }
}
fn deliver(voters: &[Voter], packets: Vec<(u64, Vec<u8>)>) {
    let mut queue: VecDeque<_> = packets.into();
    for _ in 0..10_000 {
        let Some((to, bytes)) = queue.pop_front() else {
            return;
        };
        assert!((1..=voters.len() as u64).contains(&to));
        queue.extend(
            voters[to as usize - 1]
                .request(Request::Step(bytes))
                .packets,
        );
    }
    panic!("packet exchange did not settle");
}

#[test]
fn independent_tcp_voters_commit_and_recover_after_process_restart() {
    for count in [3, 5] {
        let root = tempfile::tempdir().unwrap();
        let start = || {
            (1..=count)
                .map(|id| Voter::start(root.path(), id, count))
                .collect::<Vec<_>>()
        };
        let voters = start();
        deliver(&voters, voters[0].request(Request::Campaign).packets);
        let expected: Vec<_> = (0..32u8).map(|value| vec![value; 4096]).collect();
        for bytes in &expected {
            deliver(
                &voters,
                voters[0].request(Request::Propose(bytes.clone())).packets,
            );
        }
        for voter in &voters {
            assert_eq!(voter.request(Request::Inspect).applied, expected);
        }
        // Kill every process without a graceful flush; acknowledged WAL writes must survive.
        drop(voters);
        let recovered = start();
        for voter in &recovered {
            assert_eq!(voter.request(Request::Inspect).applied, expected);
        }
    }
}
