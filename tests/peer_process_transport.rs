// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]

use matrixraft::peer_runtime::{
    DurablePeer, MembershipChange, PeerOptions, PeerProposalOptions, PeerRole, PeerRoles,
    PeerTuning,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
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
    ProposeBatch(Vec<Vec<u8>>),
    ProposeGroup(Vec<Vec<u8>>),
    Step(Vec<u8>),
    StepBatch(Vec<Vec<u8>>),
    Inspect,
    Tick,
    AddWitness(u64),
    ProposeCommand(Vec<u8>),
    WitnessCampaignForbidden,
    WitnessVoting(bool),
    ForceCampaign,
}
#[derive(Serialize, Deserialize)]
struct Reply {
    packets: Vec<(u64, Vec<u8>)>,
    applied: Vec<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    membership: Option<ProcessMembership>,
}

#[derive(Serialize, Deserialize)]
struct ProcessMembership {
    voters: Vec<u64>,
    outgoing: Vec<u64>,
    witnesses: Vec<u64>,
    witness_voting: bool,
    nonvoting_witnesses: Vec<u64>,
    leader: bool,
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
    let roles = std::env::var("MATRIXRAFT_PROCESS_TEST_ROLES").as_deref() == Ok("1");
    let options = PeerOptions {
        group_id: 31,
        node_id: id,
        voters: (1..=if roles { 3 } else { count }).collect(),
        wal_path: root.join(format!("{id}.wal")),
    };
    let mut peer = if roles {
        DurablePeer::open_with_roles_and_tuning(
            options,
            (1..=count).collect(),
            PeerRoles {
                local: if id >= 3 {
                    PeerRole::Witness
                } else {
                    PeerRole::Data
                },
                bootstrap_witnesses: vec![3],
            },
            PeerTuning {
                bootstrap_witness_voting_disabled: std::env::var(
                    "MATRIXRAFT_PROCESS_TEST_BOOTSTRAP_DATA_ONLY",
                )
                .as_deref()
                    == Ok("1"),
                ..Default::default()
            },
        )
    } else {
        DurablePeer::open(options)
    }
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
        let request: Request = receive(&mut stream);
        let inspect = matches!(&request, Request::Inspect);
        match request {
            Request::Campaign => peer.campaign().unwrap(),
            Request::Propose(bytes) => {
                peer.propose(bytes).unwrap();
            }
            Request::ProposeBatch(entries) => {
                for bytes in entries {
                    peer.propose(bytes).unwrap();
                }
            }
            Request::Step(bytes) => peer.step(&bytes).unwrap(),
            Request::ProposeGroup(entries) => {
                peer.propose_batch(entries).unwrap();
            }
            Request::StepBatch(packets) => {
                for bytes in packets {
                    peer.step(&bytes).unwrap();
                }
            }
            Request::Inspect => {}
            Request::Tick => peer.tick().unwrap(),
            Request::AddWitness(id) => {
                peer.propose_membership_auto(MembershipChange::AddWitness(id))
                    .unwrap();
            }
            Request::ProposeCommand(bytes) => {
                peer.propose_with_options(
                    PeerProposalOptions {
                        durable_command: true,
                        ..Default::default()
                    },
                    bytes,
                )
                .unwrap();
            }
            Request::WitnessCampaignForbidden => {
                assert!(peer.campaign().is_err());
            }
            Request::WitnessVoting(enabled) => {
                peer.propose_membership_auto(MembershipChange::SetWitnessVoting(enabled))
                    .unwrap();
            }
            Request::ForceCampaign => peer.force_campaign().unwrap(),
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
                applied: if inspect { applied.clone() } else { Vec::new() },
                membership: (inspect && roles).then(|| {
                    let membership = peer.membership();
                    ProcessMembership {
                        voters: membership.voters,
                        outgoing: membership.outgoing_voters,
                        witnesses: membership.witnesses,
                        witness_voting: membership.witness_voting,
                        nonvoting_witnesses: membership.nonvoting_witnesses,
                        leader: peer.is_leader(),
                    }
                }),
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
        Self::start_inner(root, id, count, false)
    }
    fn start_inner(root: &Path, id: u64, count: u64, roles: bool) -> Self {
        Self::start_with_bootstrap(root, id, count, roles, false)
    }
    fn start_with_bootstrap(
        root: &Path,
        id: u64,
        count: u64,
        roles: bool,
        bootstrap_disabled: bool,
    ) -> Self {
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
            .env(
                "MATRIXRAFT_PROCESS_TEST_ROLES",
                if roles { "1" } else { "0" },
            )
            .env(
                "MATRIXRAFT_PROCESS_TEST_BOOTSTRAP_DATA_ONLY",
                if bootstrap_disabled { "1" } else { "0" },
            )
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

fn deliver_batched(voters: &[Voter], mut packets: Vec<(u64, Vec<u8>)>) {
    let mut delivered = 0;
    while !packets.is_empty() {
        let mut destinations = BTreeMap::<u64, Vec<Vec<u8>>>::new();
        for (to, bytes) in std::mem::take(&mut packets) {
            assert!((1..=voters.len() as u64).contains(&to));
            destinations.entry(to).or_default().push(bytes);
        }
        for (to, pending) in destinations {
            for batch in pending.chunks(16) {
                delivered += batch.len();
                assert!(delivered <= 10_000, "packet exchange did not settle");
                packets.extend(
                    voters[to as usize - 1]
                        .request(Request::StepBatch(batch.to_vec()))
                        .packets,
                );
            }
        }
    }
}

#[test]
fn independent_tcp_witnesses_join_filter_payloads_and_recover_roles_and_commands() {
    let root = tempfile::tempdir().unwrap();
    let start = || {
        (1..=4)
            .map(|id| Voter::start_inner(root.path(), id, 4, true))
            .collect::<Vec<_>>()
    };
    let voters = start();
    for voter in &voters {
        deliver(&voters, voter.request(Request::Inspect).packets);
    }
    deliver(&voters, voters[0].request(Request::Campaign).packets);
    deliver(&voters, voters[0].request(Request::AddWitness(4)).packets);
    for _ in 0..25 {
        deliver(&voters, voters[0].request(Request::Tick).packets);
    }
    let ordinary = b"tcp-ordinary-payload-not-kept-by-witness".to_vec();
    let command = b"tcp-durable-command-kept-by-witness".to_vec();
    deliver(
        &voters,
        voters[0]
            .request(Request::Propose(ordinary.clone()))
            .packets,
    );
    deliver(
        &voters,
        voters[0]
            .request(Request::ProposeCommand(command.clone()))
            .packets,
    );
    for _ in 0..4 {
        deliver(&voters, voters[0].request(Request::Tick).packets);
    }
    let verify = |voters: &[Voter]| {
        for (index, voter) in voters.iter().enumerate() {
            let reply = voter.request(Request::Inspect);
            let membership = reply.membership.unwrap();
            assert_eq!(membership.voters, vec![1, 2, 3, 4]);
            assert!(membership.outgoing.is_empty());
            assert_eq!(membership.witnesses, vec![3, 4]);
            assert_eq!(
                reply.applied,
                if index >= 2 {
                    vec![command.clone()]
                } else {
                    vec![ordinary.clone(), command.clone()]
                }
            );
        }
    };
    verify(&voters);
    for id in [3, 4] {
        let bytes = std::fs::read(root.path().join(format!("{id}.wal"))).unwrap();
        assert!(!bytes.windows(ordinary.len()).any(|part| part == ordinary));
        assert!(bytes.windows(command.len()).any(|part| part == command));
        assert!(voters[id as usize - 1]
            .request(Request::WitnessCampaignForbidden)
            .packets
            .is_empty());
    }
    drop(voters);
    let voters = start();
    verify(&voters);
}

#[test]
fn independent_tcp_nonvoting_witnesses_replicate_but_do_not_commit_or_elect() {
    let root = tempfile::tempdir().unwrap();
    let start = || {
        (1..=4)
            .map(|id| Voter::start_inner(root.path(), id, 4, true))
            .collect::<Vec<_>>()
    };
    let voters = start();
    for voter in &voters {
        deliver(&voters, voter.request(Request::Inspect).packets);
    }
    deliver(&voters, voters[0].request(Request::Campaign).packets);
    deliver(&voters, voters[0].request(Request::AddWitness(4)).packets);
    for _ in 0..25 {
        deliver(&voters, voters[0].request(Request::Tick).packets);
    }
    deliver(
        &voters,
        voters[0].request(Request::WitnessVoting(false)).packets,
    );
    for _ in 0..25 {
        deliver(&voters, voters[0].request(Request::Tick).packets);
    }
    let exchange_except = |voters: &[Voter], packets: Vec<(u64, Vec<u8>)>, blocked: u64| {
        let mut queue: VecDeque<_> = packets.into();
        for _ in 0..10000 {
            let Some((to, bytes)) = queue.pop_front() else {
                return;
            };
            if to != blocked {
                queue.extend(
                    voters[to as usize - 1]
                        .request(Request::Step(bytes))
                        .packets,
                );
            }
        }
        panic!("filtered packet exchange did not settle");
    };
    let ordinary = b"ordinary-data-needs-data-votes".to_vec();
    let command = b"command-needs-data-votes".to_vec();
    exchange_except(
        &voters,
        voters[0]
            .request(Request::Propose(ordinary.clone()))
            .packets,
        2,
    );
    exchange_except(
        &voters,
        voters[0]
            .request(Request::ProposeCommand(command.clone()))
            .packets,
        2,
    );
    assert!(voters[0].request(Request::Inspect).applied.is_empty());
    for _ in 0..5 {
        deliver(&voters, voters[0].request(Request::Tick).packets);
    }
    let verify = |voters: &[Voter]| {
        for (index, voter) in voters.iter().enumerate() {
            let reply = voter.request(Request::Inspect);
            let membership = reply.membership.unwrap();
            assert_eq!(membership.voters, vec![1, 2]);
            assert_eq!(membership.witnesses, vec![3, 4]);
            assert_eq!(membership.nonvoting_witnesses, vec![3, 4]);
            assert!(!membership.witness_voting);
            assert!(membership.outgoing.is_empty());
            assert_eq!(
                reply.applied,
                if index >= 2 {
                    vec![command.clone()]
                } else {
                    vec![ordinary.clone(), command.clone()]
                }
            );
        }
    };
    verify(&voters);
    drop(voters);
    let voters = start();
    verify(&voters);
    exchange_except(
        &voters,
        voters[1].request(Request::ForceCampaign).packets,
        1,
    );
    assert!(
        !voters[1]
            .request(Request::Inspect)
            .membership
            .unwrap()
            .leader
    );
}

#[test]
fn independent_tcp_bootstrap_data_only_voting_uses_only_data_election_votes() {
    let root = tempfile::tempdir().unwrap();
    let start = || {
        (1..=4)
            .map(|id| Voter::start_with_bootstrap(root.path(), id, 4, true, true))
            .collect::<Vec<_>>()
    };
    let voters = start();
    for voter in &voters {
        let membership = voter.request(Request::Inspect).membership.unwrap();
        assert!(!membership.witness_voting);
        assert_eq!(membership.voters, vec![1, 2]);
        assert_eq!(membership.nonvoting_witnesses, vec![3]);
    }
    let requests = voters[0].request(Request::ForceCampaign).packets;
    assert!(!requests.is_empty());
    assert!(requests.iter().all(|packet| packet.0 == 2));
    assert!(
        !voters[0]
            .request(Request::Inspect)
            .membership
            .unwrap()
            .leader
    );
    deliver(&voters, requests);
    assert!(
        voters[0]
            .request(Request::Inspect)
            .membership
            .unwrap()
            .leader
    );
    let ordinary = b"bootstrap-ordinary-data".to_vec();
    let command = b"bootstrap-durable-command".to_vec();
    deliver(
        &voters,
        voters[0]
            .request(Request::Propose(ordinary.clone()))
            .packets,
    );
    deliver(
        &voters,
        voters[0]
            .request(Request::ProposeCommand(command.clone()))
            .packets,
    );
    for _ in 0..4 {
        deliver(&voters, voters[0].request(Request::Tick).packets);
    }
    drop(voters);
    let voters = start();
    for (index, voter) in voters.iter().enumerate() {
        let reply = voter.request(Request::Inspect);
        let membership = reply.membership.unwrap();
        assert!(!membership.witness_voting);
        assert_eq!(membership.voters, vec![1, 2]);
        assert_eq!(membership.nonvoting_witnesses, vec![3]);
        assert_eq!(
            reply.applied,
            match index {
                0 | 1 => vec![ordinary.clone(), command.clone()],
                2 => vec![command.clone()],
                _ => Vec::new(),
            }
        );
    }
}

fn run_durable_batch(
    count: u64,
    payload_bytes: usize,
    batch_size: usize,
    receive_batch: bool,
    multi_entry: bool,
) -> u64 {
    let root = tempfile::tempdir().unwrap();
    let start = || {
        (1..=count)
            .map(|id| Voter::start(root.path(), id, count))
            .collect::<Vec<_>>()
    };
    let voters = start();
    let exchange = |voters: &[Voter], packets| {
        if receive_batch {
            deliver_batched(voters, packets)
        } else {
            deliver(voters, packets)
        }
    };
    exchange(&voters, voters[0].request(Request::Campaign).packets);
    let wal_bytes = || {
        (1..=count)
            .map(|id| {
                std::fs::metadata(root.path().join(format!("{id}.wal")))
                    .unwrap()
                    .len()
            })
            .sum::<u64>()
    };
    let before = wal_bytes();
    let expected: Vec<_> = (0..32u8).map(|value| vec![value; payload_bytes]).collect();
    for entries in expected.chunks(batch_size) {
        let request = if multi_entry {
            Request::ProposeGroup(entries.to_vec())
        } else if batch_size == 1 {
            Request::Propose(entries[0].clone())
        } else {
            Request::ProposeBatch(entries.to_vec())
        };
        exchange(&voters, voters[0].request(request).packets);
    }
    for voter in &voters {
        assert_eq!(voter.request(Request::Inspect).applied, expected);
    }
    let growth = wal_bytes() - before;
    // Kill every process without a graceful flush; acknowledged WAL writes must survive.
    drop(voters);
    let recovered = start();
    for voter in &recovered {
        assert_eq!(voter.request(Request::Inspect).applied, expected);
    }
    growth
}

#[test]
fn independent_tcp_voters_commit_and_recover_after_process_restart() {
    // Logical WAL growth only: the loopback router is not a throughput benchmark.
    for count in [3, 5, 9] {
        for payload_bytes in [64, 4096] {
            let single = run_durable_batch(count, payload_bytes, 1, false, false);
            let batched = run_durable_batch(count, payload_bytes, 16, false, false);
            let receive_batched = run_durable_batch(count, payload_bytes, 16, true, false);
            let grouped = run_durable_batch(count, payload_bytes, 16, true, true);
            println!(
                "voters={count} payload={payload_bytes} single_wal={single} leader_batched_wal={batched} all_batched_wal={receive_batched} grouped_wal={grouped}"
            );
            assert!(
                batched < single,
                "batching did not reduce aggregate WAL growth"
            );
            assert!(
                receive_batched < batched,
                "receive batching did not reduce aggregate WAL growth"
            );
            assert!(grouped <= receive_batched);
        }
    }
}
