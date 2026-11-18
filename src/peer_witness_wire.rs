// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

const METADATA_CONTEXT: &[u8] = b"\0W";

pub(super) fn is_metadata(entry_type: i32, context: &[u8]) -> bool {
    entry_type == EntryType::EntryNormal as i32 && context == METADATA_CONTEXT
}

pub(super) fn valid_entry_role(
    identity: &Option<Arc<witness_identity::Identity>>,
    entry_type: i32,
    data: &[u8],
    context: &[u8],
) -> bool {
    let Some(identity) = identity else {
        return true;
    };
    if identity.local_witness()
        && entry_type == EntryType::EntryNormal as i32
        && !commands::is_command_context(entry_type, context)
    {
        data.is_empty() && (context.is_empty() || is_metadata(entry_type, context))
    } else {
        !is_metadata(entry_type, context)
    }
}

pub(super) fn trailer_len(identity: &witness_identity::Identity) -> usize {
    let body = 4 + identity.wire_bootstrap().len();
    1 + prost::encoding::encoded_len_varint(body as u64) + body
}

fn wrap(
    identity: &witness_identity::Identity,
    witness: bool,
    bytes: &mut Vec<u8>,
) -> Result<(), RaftError> {
    if bytes.first() != Some(&8)
        || !bytes
            .get(1)
            .is_some_and(|version| (1..=7).contains(version))
    {
        return Err(invalid("unsupported witness packet protocol"));
    }
    if bytes.len().saturating_add(trailer_len(identity)) > LIMIT {
        return Err(invalid("witness packet exceeds size limit"));
    }
    let protocol = u32::from(bytes[1]);
    bytes[1] = 8;
    prost::encoding::encode_key(4, prost::encoding::WireType::LengthDelimited, &mut *bytes);
    prost::encoding::encode_varint((4 + identity.wire_bootstrap().len()) as u64, &mut *bytes);
    prost::encoding::uint32::encode(1, &protocol, &mut *bytes);
    prost::encoding::uint32::encode(2, &if witness { 2 } else { 1 }, &mut *bytes);
    bytes.extend_from_slice(identity.wire_bootstrap());
    Ok(())
}

impl DurablePeer {
    pub(super) fn local_witness(&self) -> bool {
        self.store
            .witness_identity
            .as_ref()
            .is_some_and(|identity| identity.local_witness())
    }

    pub(super) fn witness_node(&self, id: u64) -> bool {
        self.witness_commit
            .as_ref()
            .is_some_and(|policy| policy.contains(id))
    }

    pub(super) fn thin_witness_message(&self, message: &mut Message) {
        if self.store.witness_identity.is_some() && self.witness_node(message.to) {
            for entry in &mut message.entries {
                if entry.entry_type == EntryType::EntryNormal as i32 && !commands::is_command(entry)
                {
                    if (entry.data.is_empty() && entry.context.is_empty())
                        || is_metadata(entry.entry_type, &entry.context)
                    {
                        continue;
                    }
                    entry.data.clear();
                    entry.context.clear();
                    if entry.data.capacity() > entry.context.capacity() {
                        std::mem::swap(&mut entry.data, &mut entry.context);
                    }
                    entry.context.extend_from_slice(METADATA_CONTEXT);
                }
            }
        }
    }

    pub(super) fn encode_witness_message(&self, message: &Message) -> Result<Vec<u8>, RaftError> {
        let version = if message.entries.iter().any(commands::is_command) {
            7
        } else if message.get_msg_type() == MessageType::MsgSnapshot && !message.context.is_empty()
        {
            4
        } else {
            1
        };
        let Some(identity) = &self.store.witness_identity else {
            return if version == 1 {
                encode_packet(self.options.group_id, message)
            } else {
                encode_envelope(version, self.options.group_id, message)
            };
        };
        let mut bytes = encode_envelope_reserved(
            version,
            self.options.group_id,
            message,
            trailer_len(identity),
        )?;
        wrap(identity, self.witness_node(message.to), &mut bytes)?;
        Ok(bytes)
    }

    pub(super) fn wrap_witness_packets(&self, packets: &mut [PeerPacket]) -> Result<(), RaftError> {
        if let Some(identity) = &self.store.witness_identity {
            for packet in packets {
                wrap(identity, self.witness_node(packet.to), &mut packet.bytes)?;
            }
        }
        Ok(())
    }

    pub(super) fn decode_witness_input<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<(u32, u64, &'a [u8]), RaftError> {
        let Some(identity) = &self.store.witness_identity else {
            return decode_canonical_packet(bytes)
                .ok_or_else(|| invalid("invalid peer packet encoding"));
        };
        let (version, group, message, mut tail) = decode_packet_prefix(bytes)
            .ok_or_else(|| invalid("invalid witness packet encoding"))?;
        if version != 8 || tail.first() != Some(&0x22) {
            return Err(invalid(
                "witness-enabled groups require role-marked packets",
            ));
        }
        tail = &tail[1..];
        let length = take_canonical_varint(&mut tail)
            .ok_or_else(|| invalid("invalid witness role length"))?;
        if length != tail.len() as u64 || length > 128 || tail.first() != Some(&8) {
            return Err(invalid("invalid witness role trailer"));
        }
        tail = &tail[1..];
        let protocol =
            take_canonical_varint(&mut tail).ok_or_else(|| invalid("invalid witness protocol"))?;
        if !(1..=7).contains(&protocol) || tail.first() != Some(&0x10) {
            return Err(invalid("invalid witness protocol"));
        }
        tail = &tail[1..];
        let role = take_canonical_varint(&mut tail)
            .ok_or_else(|| invalid("invalid witness target role"))?;
        if role != if identity.local_witness() { 2 } else { 1 } || tail != identity.wire_bootstrap()
        {
            return Err(invalid("witness target or bootstrap identity mismatch"));
        }
        Ok((protocol as u32, group, message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct State {
        applied: BTreeMap<u64, Vec<Vec<u8>>>,
        restored: BTreeMap<u64, Vec<Vec<u8>>>,
    }

    fn open(directory: &Path, id: u64, witnesses: &[u64], tuning: PeerTuning) -> DurablePeer {
        open_role(directory, id, witnesses, witnesses.contains(&id), tuning)
    }

    fn open_role(
        directory: &Path,
        id: u64,
        witnesses: &[u64],
        local_witness: bool,
        tuning: PeerTuning,
    ) -> DurablePeer {
        DurablePeer::open_with_roles_and_tuning(
            PeerOptions {
                group_id: 83,
                node_id: id,
                voters: vec![1, 2, 3],
                wal_path: directory.join(format!("{id}.wal")),
            },
            vec![1, 2, 3, 4],
            PeerRoles {
                local: if local_witness {
                    PeerRole::Witness
                } else {
                    PeerRole::Data
                },
                bootstrap_witnesses: witnesses.to_vec(),
            },
            tuning,
        )
        .unwrap()
    }

    fn group(witnesses: &[u64], tuning: PeerTuning) -> (tempfile::TempDir, Vec<DurablePeer>) {
        let directory = tempfile::tempdir().unwrap();
        let peers = (1..=4)
            .map(|id| open(directory.path(), id, witnesses, tuning))
            .collect();
        (directory, peers)
    }

    fn dynamic_group(
        bootstrap: &[u64],
        spare_witness: bool,
        tuning: PeerTuning,
    ) -> (tempfile::TempDir, Vec<DurablePeer>) {
        let directory = tempfile::tempdir().unwrap();
        let peers = (1..=4)
            .map(|id| {
                open_role(
                    directory.path(),
                    id,
                    bootstrap,
                    bootstrap.contains(&id) || (id == 4 && spare_witness),
                    tuning,
                )
            })
            .collect();
        (directory, peers)
    }

    fn add_witness(peers: &mut [DurablePeer], state: &mut State) {
        peers[0]
            .propose_membership_auto(MembershipChange::AddWitness(4))
            .unwrap();
        drive(peers, state, None);
        ticks(peers, state, 0, None, 25);
        for peer in peers {
            assert_eq!(
                peer.membership().voters,
                vec![1, 2, 3, 4],
                "peer {}",
                peer.node_id()
            );
            assert!(peer.membership().outgoing_voters.is_empty());
            assert!(peer.witness_node(4));
        }
    }

    #[test]
    fn dynamic_witness_addition_uses_public_roles_and_explicit_joint_consensus() {
        let (_directory, mut peers) = dynamic_group(&[], true, PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        assert!(peers[0].witness_commit.is_none());
        peers[0]
            .propose_membership(MembershipChange::AddWitness(4))
            .unwrap();
        drive(&mut peers, &mut state, None);
        assert_eq!(peers[0].membership().outgoing_voters, vec![1, 2, 3]);
        assert_eq!(peers[0].membership().witnesses, vec![4]);
        peers[0]
            .propose_membership(MembershipChange::LeaveJoint)
            .unwrap();
        drive(&mut peers, &mut state, None);
        peers[0]
            .propose(b"ordinary-after-joining".to_vec())
            .unwrap();
        drive(&mut peers, &mut state, None);
        let command = peers[0]
            .propose_with_options(
                PeerProposalOptions {
                    durable_command: true,
                    ..Default::default()
                },
                b"joined-command".to_vec(),
            )
            .unwrap();
        drive(&mut peers, &mut state, None);
        assert_eq!(state.applied[&4], vec![b"joined-command".to_vec()]);
        assert_eq!(peers[3].applied_index(), command.index);
        let bytes = std::fs::read(&peers[3].options.wal_path).unwrap();
        assert!(!bytes
            .windows(b"ordinary-after-joining".len())
            .any(|part| part == b"ordinary-after-joining"));
        assert!(peers[3].campaign().is_err());
        assert!(peers[0].transfer_leadership(4).is_err());
    }

    #[test]
    fn dynamic_witnesses_restore_every_checkpoint_format_and_survive_all_peer_restart() {
        for form in 0..3 {
            let tuning = PeerTuning {
                max_checkpoint_sends: (form != 0).then_some(1),
                ..Default::default()
            };
            let (directory, mut peers) = dynamic_group(&[3], true, tuning);
            let mut state = State::default();
            elect(&mut peers, &mut state);
            peers[0].propose(b"before-witness-join".to_vec()).unwrap();
            drive(&mut peers, &mut state, None);
            let image = vec![
                6;
                if form == 1 {
                    INLINE_CHECKPOINT_BYTES + 19
                } else {
                    65536
                }
            ];
            if form == 2 {
                peers[0]
                    .checkpoint_from_reader(&mut std::io::Cursor::new(&image), image.len() as u64)
                    .unwrap();
            } else {
                peers[0].checkpoint(image.clone()).unwrap();
            }
            peers[0]
                .propose_membership_auto(MembershipChange::AddWitness(4))
                .unwrap();
            drive(&mut peers, &mut state, None);
            assert!(peers[0].membership_checkpoint_needed());
            if form == 2 {
                peers[0]
                    .checkpoint_from_reader(&mut std::io::Cursor::new(&image), image.len() as u64)
                    .unwrap();
            } else {
                peers[0].checkpoint(image.clone()).unwrap();
            }
            ticks(&mut peers, &mut state, 0, None, 25);
            assert_eq!(state.restored[&4], vec![image]);
            let command = peers[0]
                .propose_with_options(
                    PeerProposalOptions {
                        durable_command: true,
                        ..Default::default()
                    },
                    b"dynamic-command".to_vec(),
                )
                .unwrap();
            drive(&mut peers, &mut state, None);
            drop(peers);
            let mut peers: Vec<_> = (1..=4)
                .map(|id| open_role(directory.path(), id, &[3], id >= 3, tuning))
                .collect();
            let mut state = State::default();
            drive(&mut peers, &mut state, None);
            for peer in &peers {
                assert_eq!(peer.membership().witnesses, vec![3, 4]);
                assert_eq!(peer.membership().voters, vec![1, 2, 3, 4]);
                assert!(peer.membership().outgoing_voters.is_empty());
                assert_eq!(peer.applied_index(), command.index);
            }
            assert_eq!(state.applied[&4], vec![b"dynamic-command".to_vec()]);
        }
    }

    #[test]
    fn removed_witness_roles_survive_flushed_compaction_and_can_rejoin_only_as_witnesses() {
        let tuning = PeerTuning::default();
        let (directory, mut peers) = dynamic_group(&[3], true, tuning);
        let mut state = State::default();
        elect(&mut peers, &mut state);
        add_witness(&mut peers, &mut state);
        peers[0]
            .propose_membership_auto(MembershipChange::Remove(4))
            .unwrap();
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 25);
        for peer in &mut peers[..3] {
            let floor = peer.applied_index();
            peer.set_flushed_index(floor).unwrap();
            peer.trim_log_to(floor).unwrap();
        }
        drop(peers.remove(0));
        peers.insert(0, open_role(directory.path(), 1, &[3], false, tuning));
        drive(&mut peers, &mut state, None);
        peers[0].force_campaign().unwrap();
        drive(&mut peers, &mut state, None);
        assert!(peers[0].is_leader());
        assert_eq!(peers[0].membership().witnesses, vec![3]);
        assert!(peers[0]
            .propose_membership(MembershipChange::AddLearner(4))
            .is_err());
        assert!(peers[0]
            .propose_membership(MembershipChange::AddLearnerAuto(4))
            .is_err());
        peers[0].checkpoint(b"rejoin-image".to_vec()).unwrap();
        peers[0]
            .propose_membership_auto(MembershipChange::AddWitness(4))
            .unwrap();
        drive(&mut peers, &mut state, None);
        assert!(peers[0].membership_checkpoint_needed());
        peers[0].checkpoint(b"rejoin-image".to_vec()).unwrap();
        ticks(&mut peers, &mut state, 0, None, 25);
        for peer in &peers {
            assert_eq!(peer.membership().witnesses, vec![3, 4]);
        }
        assert!(peers[0]
            .propose_membership(MembershipChange::Remove(1))
            .is_ok());
    }

    #[test]
    fn retired_data_roles_cannot_be_reinterpreted_after_restart_and_trim() {
        let tuning = PeerTuning::default();
        let (directory, mut peers) = dynamic_group(&[3], false, tuning);
        let mut state = State::default();
        elect(&mut peers, &mut state);
        peers[0]
            .propose_membership(MembershipChange::AddLearnerAuto(4))
            .unwrap();
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 25);
        peers[0]
            .propose_membership_auto(MembershipChange::Remove(4))
            .unwrap();
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 25);
        let floor = peers[0].applied_index();
        peers[0].set_flushed_index(floor).unwrap();
        peers[0].trim_log_to(floor).unwrap();
        drop(peers.remove(0));
        peers.insert(0, open_role(directory.path(), 1, &[3], false, tuning));
        drive(&mut peers, &mut state, None);
        peers[0].force_campaign().unwrap();
        drive(&mut peers, &mut state, None);
        let before = peers[0].raw.raft.raft_log.last_index();
        assert!(peers[0]
            .propose_membership(MembershipChange::AddWitness(4))
            .is_err());
        assert_eq!(peers[0].raw.raft.raft_log.last_index(), before);
    }

    #[test]
    fn uncommitted_witness_addition_does_not_change_roles_after_leader_replacement() {
        let (_directory, mut peers) = dynamic_group(&[3], true, PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        peers[0]
            .propose_membership_auto(MembershipChange::AddWitness(4))
            .unwrap();
        drive(&mut peers, &mut state, Some(1));
        assert!(!peers[0].witness_node(4));
        peers[1].force_campaign().unwrap();
        drive(&mut peers, &mut state, Some(1));
        ticks(&mut peers, &mut state, 1, Some(1), 25);
        drive(&mut peers, &mut state, None);
        for peer in &peers {
            assert!(!peer.witness_node(4));
            assert_eq!(peer.membership().voters, vec![1, 2, 3]);
            assert_eq!(peer.membership().witnesses, vec![3]);
        }
    }

    #[test]
    fn automatic_witness_receipts_complete_only_after_committed_joint_exit() {
        let tuning = PeerTuning {
            max_pending_proposal_receipts: Some(4),
            max_inflight_apply_tasks: Some(2),
            ..Default::default()
        };
        let (_directory, mut peers) = dynamic_group(&[3], true, tuning);
        let mut state = State::default();
        elect(&mut peers, &mut state);
        let receipt = peers[0]
            .propose_membership_auto_tracked(MembershipChange::AddWitness(4), None)
            .unwrap();
        assert_eq!(receipt.status(), PeerProposalStatus::Pending);
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 25);
        assert_eq!(
            peers[0].poll_proposal(&receipt).unwrap(),
            PeerProposalStatus::Applied
        );
        assert!(peers[0].membership().outgoing_voters.is_empty());
        assert_eq!(peers[0].membership().witnesses, vec![3, 4]);
    }

    #[test]
    fn snapshots_cannot_forget_or_reassign_retired_replica_roles_before_advancing_term() {
        let (_directory, mut peers) = dynamic_group(&[3], true, PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        add_witness(&mut peers, &mut state);
        peers[0]
            .propose_membership_auto(MembershipChange::Remove(4))
            .unwrap();
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 25);
        let term = peers[0].term();
        let length = peers[0].journal.length;
        let conf = peers[0].store.initial_state().unwrap().conf_state;
        for reassign in [false, true] {
            let mut history = peers[0].roles.as_ref().unwrap().as_ref().clone();
            history.witnesses.remove(&4);
            if reassign {
                history.data.insert(4);
            }
            let policy = membership_roles::encode(&BTreeSet::new(), Some(&history));
            membership_roles::decode(
                &policy,
                &conf,
                &peers[0].store.permitted,
                &peers[0].store.witness_identity,
            )
            .unwrap();
            let message = Message {
                from: 2,
                to: 1,
                term: term + 1,
                msg_type: MessageType::MsgSnapshot as i32,
                context: policy,
                snapshot: Some(Snapshot {
                    data: b"replacement-image".to_vec(),
                    metadata: Some(SnapshotMetadata {
                        index: peers[0].commit_index() + 1,
                        term,
                        conf_state: Some(conf.clone()),
                    }),
                }),
                ..Default::default()
            };
            let packet = peers[1].encode_witness_message(&message).unwrap();
            assert!(peers[0].step(&packet).is_err());
            assert_eq!(peers[0].term(), term);
            assert_eq!(peers[0].journal.length, length);
            assert!(!peers[0].failed);
        }
    }

    #[test]
    fn staged_images_keep_capture_time_roles_and_replay_later_witness_additions() {
        let (directory, mut peers) = dynamic_group(&[3], true, PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        peers[0].propose(b"before-role-capture".to_vec()).unwrap();
        drive(&mut peers, &mut state, None);
        let capture = peers[0].prepare_checkpoint().unwrap();
        let image = b"frozen-role-image";
        add_witness(&mut peers, &mut state);
        peers[0]
            .propose_with_options(
                PeerProposalOptions {
                    durable_command: true,
                    ..Default::default()
                },
                b"captured-role-command".to_vec(),
            )
            .unwrap();
        drive(&mut peers, &mut state, None);
        let expected = peers[0].membership();
        let prepared = capture
            .capture(&mut image.as_slice(), image.len() as u64)
            .unwrap();
        assert!(peers[0].install_checkpoint(&prepared).unwrap());
        assert_eq!(peers[0].membership(), expected);
        assert!(peers[0].membership_checkpoint_needed());
        drop(prepared);
        drop(peers.remove(0));
        peers.insert(
            0,
            open_role(directory.path(), 1, &[3], false, PeerTuning::default()),
        );
        state.applied.insert(1, Vec::new());
        drive(&mut peers, &mut state, None);
        assert_eq!(peers[0].membership(), expected);
        assert_eq!(state.restored[&1], vec![image.to_vec()]);
        assert_eq!(state.applied[&1], vec![b"captured-role-command".to_vec()]);
    }

    fn voting(peers: &mut [DurablePeer], state: &mut State, leader: usize, enabled: bool) {
        peers[leader]
            .propose_membership_auto(MembershipChange::SetWitnessVoting(enabled))
            .unwrap();
        drive(peers, state, None);
        ticks(peers, state, leader, None, 25);
        for peer in &peers[..3] {
            assert_eq!(peer.membership().witness_voting, enabled);
            assert!(peer.membership().outgoing_voters.is_empty());
            assert_eq!(peer.membership().witnesses, vec![3]);
            assert_eq!(
                peer.membership().nonvoting_witnesses,
                if enabled { vec![] } else { vec![3] }
            );
            assert!(peer.membership().learners.is_empty());
        }
    }

    #[test]
    fn nonvoting_witnesses_keep_replication_but_cannot_replace_a_data_commit_vote() {
        let (_directory, mut peers) = group(&[3], PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        voting(&mut peers, &mut state, 0, false);
        assert!(peers[0]
            .raw
            .raft
            .prs()
            .conf()
            .to_conf_state()
            .learners
            .contains(&3));
        let before = peers[0].commit_index();
        let proposal = peers[0].propose(b"data-quorum-required".to_vec()).unwrap();
        drive(&mut peers, &mut state, Some(2));
        assert_eq!(
            peers[0].raw.raft.prs().get(3).unwrap().matched,
            proposal.index
        );
        assert_eq!(peers[0].commit_index(), before);
        assert!(state.applied[&3].is_empty());
        ticks(&mut peers, &mut state, 0, None, 4);
        assert_eq!(peers[0].applied_index(), proposal.index);
        let command = peers[0]
            .propose_with_options(
                PeerProposalOptions {
                    durable_command: true,
                    ..Default::default()
                },
                b"nonvoting-command".to_vec(),
            )
            .unwrap();
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 4);
        assert_eq!(peers[2].applied_index(), command.index);
        assert_eq!(state.applied[&3], vec![b"nonvoting-command".to_vec()]);
    }

    #[test]
    fn witness_election_votes_are_excluded_until_voting_is_reenabled_by_consensus() {
        let (_directory, mut peers) = group(&[3], PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        voting(&mut peers, &mut state, 0, false);
        peers[1].force_campaign().unwrap();
        let requests = peers[1].process_ready(|_, _| Ok(())).unwrap();
        assert!(!requests.is_empty());
        assert!(requests.iter().all(|packet| packet.to == 1));
        assert!(!peers[1].is_leader());
        for packet in requests {
            peers[0].step(&packet.bytes).unwrap();
        }
        drive(&mut peers, &mut state, None);
        assert!(peers[1].is_leader());
        voting(&mut peers, &mut state, 1, true);
        assert!(!peers[1]
            .raw
            .raft
            .prs()
            .conf()
            .to_conf_state()
            .learners
            .contains(&3));
        peers[0].force_campaign().unwrap();
        drive(&mut peers, &mut state, Some(2));
        assert!(peers[0].is_leader());
        assert_eq!(peers[2].store.initial_state().unwrap().hard_state.vote, 1);
    }

    #[test]
    fn voting_receipts_wait_for_real_new_majorities_before_joint_exit() {
        let tuning = PeerTuning {
            max_pending_proposal_receipts: Some(4),
            ..Default::default()
        };
        let (_directory, mut peers) = group(&[3], tuning);
        let mut state = State::default();
        elect(&mut peers, &mut state);
        let receipt = peers[0]
            .propose_membership_auto_tracked(MembershipChange::SetWitnessVoting(false), None)
            .unwrap();
        drive(&mut peers, &mut state, Some(1));
        assert!(peers[0].membership().witness_voting);
        assert_eq!(receipt.status(), PeerProposalStatus::Pending);
        let timeout = peers[0].raw.raft.election_timeout();
        ticks(&mut peers, &mut state, 0, Some(2), timeout * 2);
        assert!(!peers[0].membership().witness_voting);
        assert_eq!(peers[0].membership().voters, vec![1, 2]);
        assert_eq!(peers[0].membership().outgoing_voters, vec![1, 2, 3]);
        assert_eq!(receipt.status(), PeerProposalStatus::Pending);
        ticks(&mut peers, &mut state, 0, None, 5);
        assert_eq!(receipt.status(), PeerProposalStatus::Applied);
        assert!(peers[0].membership().outgoing_voters.is_empty());
    }

    #[test]
    fn disabled_voting_survives_checkpoint_formats_flushed_compaction_and_restart() {
        for form in 0..3 {
            let tuning = PeerTuning {
                max_checkpoint_sends: (form != 0).then_some(1),
                ..Default::default()
            };
            let (directory, mut peers) = dynamic_group(&[3], true, tuning);
            let mut state = State::default();
            elect(&mut peers, &mut state);
            voting(&mut peers, &mut state, 0, false);
            let image = vec![
                7;
                if form == 1 {
                    INLINE_CHECKPOINT_BYTES + 29
                } else {
                    65536
                }
            ];
            let capture = |peer: &mut DurablePeer| {
                if form == 2 {
                    peer.checkpoint_from_reader(
                        &mut std::io::Cursor::new(&image),
                        image.len() as u64,
                    )
                    .unwrap();
                } else {
                    peer.checkpoint(image.clone()).unwrap();
                }
            };
            capture(&mut peers[0]);
            peers[0]
                .propose_membership_auto(MembershipChange::AddWitness(4))
                .unwrap();
            drive(&mut peers, &mut state, None);
            assert!(peers[0].membership_checkpoint_needed());
            capture(&mut peers[0]);
            ticks(&mut peers, &mut state, 0, None, 25);
            assert_eq!(state.restored[&4], vec![image.clone()]);
            let command = peers[0]
                .propose_with_options(
                    PeerProposalOptions {
                        durable_command: true,
                        ..Default::default()
                    },
                    b"voting-recovery-command".to_vec(),
                )
                .unwrap();
            drive(&mut peers, &mut state, None);
            ticks(&mut peers, &mut state, 0, None, 5);
            drop(peers);
            let mut peers: Vec<_> = (1..=4)
                .map(|id| open_role(directory.path(), id, &[3], id >= 3, tuning))
                .collect();
            let mut state = State::default();
            drive(&mut peers, &mut state, None);
            assert_eq!(state.restored[&1], vec![image.clone()]);
            assert_eq!(state.restored[&4], vec![image.clone()]);
            for peer in &mut peers {
                assert!(!peer.membership().witness_voting);
                assert_eq!(peer.membership().nonvoting_witnesses, vec![3, 4]);
                let floor = peer.applied_index();
                peer.set_flushed_index(floor).unwrap();
                peer.trim_log_to(floor).unwrap();
            }
            drop(peers);
            let mut peers: Vec<_> = (1..=4)
                .map(|id| open_role(directory.path(), id, &[3], id >= 3, tuning))
                .collect();
            let mut state = State::default();
            drive(&mut peers, &mut state, None);
            for peer in &peers {
                assert!(!peer.membership().witness_voting);
                assert_eq!(peer.membership().nonvoting_witnesses, vec![3, 4]);
                assert_eq!(peer.membership().voters, vec![1, 2]);
                assert_eq!(peer.applied_index(), command.index);
            }
        }
    }

    #[test]
    fn policy_without_bootstrap_witnesses_governs_later_nonvoting_joins_and_removal() {
        let (_directory, mut peers) = dynamic_group(&[], true, PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        peers[0]
            .propose_membership_auto(MembershipChange::SetWitnessVoting(false))
            .unwrap();
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 5);
        peers[0]
            .propose_membership_auto(MembershipChange::AddWitness(4))
            .unwrap();
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 5);
        for peer in &peers {
            assert!(!peer.membership().witness_voting);
            assert_eq!(peer.membership().voters, vec![1, 2, 3]);
            assert_eq!(peer.membership().nonvoting_witnesses, vec![4]);
            assert!(peer.membership().learners.is_empty());
        }
        peers[0]
            .propose_membership(MembershipChange::Remove(4))
            .unwrap();
        drive(&mut peers, &mut state, None);
        assert!(peers[0].membership().witnesses.is_empty());
        assert!(peers[0]
            .propose_membership(MembershipChange::AddLearner(4))
            .is_err());
        peers[0]
            .propose_membership_auto(MembershipChange::SetWitnessVoting(true))
            .unwrap();
        drive(&mut peers, &mut state, None);
        assert!(peers[0].membership().witness_voting);
    }

    #[test]
    fn staged_capture_replays_later_voting_policy_and_keeps_it_through_a_flushed_floor() {
        let tuning = PeerTuning::default();
        let (directory, mut peers) = group(&[3], tuning);
        let mut state = State::default();
        elect(&mut peers, &mut state);
        peers[0].propose(b"before-voting-capture".to_vec()).unwrap();
        drive(&mut peers, &mut state, None);
        let ticket = peers[0].prepare_checkpoint().unwrap();
        voting(&mut peers, &mut state, 0, false);
        peers[0]
            .propose_with_options(
                PeerProposalOptions {
                    durable_command: true,
                    ..Default::default()
                },
                b"staged-voting-command".to_vec(),
            )
            .unwrap();
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 4);
        let expected = peers[0].membership();
        let image = b"staged-voting-image";
        let prepared = ticket
            .capture(&mut image.as_slice(), image.len() as u64)
            .unwrap();
        assert!(peers[0].install_checkpoint(&prepared).unwrap());
        assert_eq!(peers[0].membership(), expected);
        assert_eq!(
            &peers[0].store.checkpoint_promotions.lock().unwrap()[..2],
            &[8, 2]
        );
        assert!(peers[0].membership_checkpoint_needed());
        drop(prepared);
        drop(peers.remove(0));
        peers.insert(0, open(directory.path(), 1, &[3], tuning));
        state.applied.insert(1, Vec::new());
        drive(&mut peers, &mut state, None);
        assert_eq!(peers[0].membership(), expected);
        assert_eq!(state.restored[&1], vec![image.to_vec()]);
        assert_eq!(state.applied[&1], vec![b"staged-voting-command".to_vec()]);
        let floor = peers[0].applied_index();
        peers[0].set_flushed_index(floor).unwrap();
        peers[0].trim_log_to(floor).unwrap();
        drop(peers.remove(0));
        peers.insert(0, open(directory.path(), 1, &[3], tuning));
        drive(&mut peers, &mut state, None);
        assert_eq!(peers[0].membership(), expected);
    }

    #[test]
    fn an_uncommitted_voting_change_is_not_resurrected_after_leader_replacement() {
        let tuning = PeerTuning {
            max_pending_proposal_receipts: Some(4),
            ..Default::default()
        };
        let (_directory, mut peers) = group(&[3], tuning);
        let mut state = State::default();
        elect(&mut peers, &mut state);
        let receipt = peers[0]
            .propose_membership_auto_tracked(MembershipChange::SetWitnessVoting(false), None)
            .unwrap();
        drive(&mut peers, &mut state, Some(1));
        assert!(peers[0].membership().witness_voting);
        assert_eq!(receipt.status(), PeerProposalStatus::Pending);
        peers[1].force_campaign().unwrap();
        drive(&mut peers, &mut state, Some(1));
        ticks(&mut peers, &mut state, 1, Some(1), 25);
        assert!(peers[1].is_leader());
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 1, None, 4);
        assert_eq!(receipt.status(), PeerProposalStatus::LeadershipLost);
        for peer in &peers[..3] {
            assert!(peer.membership().witness_voting);
            assert_eq!(peer.membership().voters, vec![1, 2, 3]);
            assert!(peer.membership().nonvoting_witnesses.is_empty());
        }
    }

    #[test]
    fn bootstrap_data_only_voting_commits_without_any_witness_response() {
        let tuning = PeerTuning {
            bootstrap_witness_voting_disabled: true,
            ..Default::default()
        };
        let (_directory, mut peers) = group(&[2, 3], tuning);
        peers[0].campaign().unwrap();
        peers[0].process_ready(|_, _| Ok(())).unwrap();
        assert!(peers[0].is_leader());
        assert_eq!(peers[0].membership().voters, vec![1]);
        assert_eq!(peers[0].membership().nonvoting_witnesses, vec![2, 3]);
        assert!(!peers[0].membership().witness_voting);
        let position = peers[0].propose(b"bootstrap-data-only".to_vec()).unwrap();
        let mut applied = Vec::new();
        peers[0]
            .process_ready(|_, bytes| {
                if !bytes.is_empty() {
                    applied.push(bytes.to_vec());
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(applied, vec![b"bootstrap-data-only".to_vec()]);
        assert_eq!(peers[0].applied_index(), position.index);
        for id in [2, 3] {
            assert_eq!(peers[0].raw.raft.prs().get(id).unwrap().matched, 0);
            assert_eq!(
                peers[id as usize - 1]
                    .store
                    .initial_state()
                    .unwrap()
                    .hard_state
                    .vote,
                0
            );
        }
    }

    #[test]
    fn bootstrap_voting_identity_is_fenced_and_does_not_overwrite_later_committed_policy() {
        let tuning = PeerTuning {
            bootstrap_witness_voting_disabled: true,
            ..Default::default()
        };
        let (directory, mut peers) = group(&[2, 3], tuning);
        let mut state = State::default();
        peers[0].campaign().unwrap();
        drive(&mut peers, &mut state, None);
        peers[0]
            .propose_membership_auto(MembershipChange::SetWitnessVoting(true))
            .unwrap();
        drive(&mut peers, &mut state, None);
        ticks(&mut peers, &mut state, 0, None, 25);
        assert!(peers[0].membership().witness_voting);
        let floor = peers[0].applied_index();
        peers[0].set_flushed_index(floor).unwrap();
        peers[0].trim_log_to(floor).unwrap();
        drop(peers.remove(0));
        assert!(DurablePeer::open_with_roles(
            PeerOptions {
                group_id: 83,
                node_id: 1,
                voters: vec![1, 2, 3],
                wal_path: directory.path().join("1.wal")
            },
            vec![1, 2, 3, 4],
            PeerRoles {
                local: PeerRole::Data,
                bootstrap_witnesses: vec![2, 3]
            },
        )
        .is_err());
        peers.insert(0, open(directory.path(), 1, &[2, 3], tuning));
        drive(&mut peers, &mut state, None);
        assert!(peers[0].membership().witness_voting);
        assert_eq!(peers[0].membership().voters, vec![1, 2, 3]);
        let wrong_directory = tempfile::tempdir().unwrap();
        let mut wrong = open(wrong_directory.path(), 2, &[2, 3], PeerTuning::default());
        let before = wrong.journal.length;
        let packet = peers[0]
            .encode_witness_message(&Message {
                from: 1,
                to: 2,
                term: peers[0].term() + 1,
                msg_type: MessageType::MsgHeartbeat as i32,
                ..Default::default()
            })
            .unwrap();
        assert!(wrong.step(&packet).is_err());
        assert_eq!(wrong.term(), 0);
        assert_eq!(wrong.journal.length, before);
    }

    fn drive(peers: &mut [DurablePeer], state: &mut State, blocked: Option<u64>) {
        let deadline = Instant::now() + Duration::from_secs(10);
        for _ in 0..16384 {
            let mut packets = Vec::new();
            for peer in &mut *peers {
                let id = peer.node_id();
                let applied = state.applied.entry(id).or_default();
                let restored = state.restored.entry(id).or_default();
                let ready = if peer.apply_pipeline.is_some() {
                    peer.process_ready_async_apply(|_, reader, length| {
                        let mut image = Vec::new();
                        reader.read_to_end(&mut image).unwrap();
                        assert_eq!(image.len() as u64, length);
                        restored.push(image);
                        Ok(RestoreDecision::Restored)
                    })
                } else {
                    peer.process_ready_with_snapshot(
                        |_, value| {
                            if !value.is_empty() {
                                applied.push(value.to_vec());
                            }
                            Ok(())
                        },
                        |_, image| {
                            restored.push(image.to_vec());
                            Ok(())
                        },
                    )
                }
                .unwrap();
                if peer.apply_pipeline.is_some() {
                    for task in peer.take_application_tasks().unwrap() {
                        let (entries, completion) = std::thread::spawn(move || {
                            let entries = task
                                .entries()
                                .iter()
                                .map(|entry| {
                                    (entry.payload.to_vec(), entry.is_metadata, entry.is_command)
                                })
                                .collect::<Vec<_>>();
                            (entries, task.finish())
                        })
                        .join()
                        .unwrap();
                        for (payload, metadata, command) in entries {
                            assert!(!metadata || (payload.is_empty() && !command));
                            assert!(peer.local_witness() || !metadata);
                            assert!(!peer.local_witness() || payload.is_empty() || command);
                            if !payload.is_empty() {
                                applied.push(payload);
                            }
                        }
                        peer.complete_application_task(completion).unwrap();
                    }
                }
                for packet in ready {
                    assert_eq!(packet.bytes.get(..2), Some([8, 8].as_slice()));
                    packets.push((id, packet));
                }
                if peer.local_witness() {
                    assert_eq!(peer.raw.raft.state, StateRole::Follower);
                }
            }
            if packets.is_empty()
                && peers
                    .iter()
                    .all(|p| !p.has_ready() && !p.checkpoint_receive_pending())
            {
                return;
            }
            if packets.is_empty() {
                std::thread::sleep(Duration::from_millis(1));
            }
            for (from, packet) in packets {
                if blocked == Some(from) || blocked == Some(packet.to) {
                    continue;
                }
                let receiver = peers.iter_mut().find(|p| p.node_id() == packet.to).unwrap();
                let (protocol, _, payload) = receiver.decode_witness_input(&packet.bytes).unwrap();
                if matches!(protocol, 1 | 4 | 7)
                    && receiver.raw.raft.prs().get(from).is_none()
                    && matches!(
                        Message::decode(payload).unwrap().get_msg_type(),
                        MessageType::MsgAppendResponse
                            | MessageType::MsgHeartbeatResponse
                            | MessageType::MsgRequestVoteResponse
                            | MessageType::MsgRequestPreVoteResponse
                    )
                {
                    continue;
                }
                receiver.step(&packet.bytes).unwrap();
            }
            assert!(Instant::now() < deadline, "witness peers did not settle");
        }
        panic!("witness peers exceeded bounded work");
    }

    fn ticks(
        peers: &mut [DurablePeer],
        state: &mut State,
        leader: usize,
        blocked: Option<u64>,
        count: usize,
    ) {
        for _ in 0..count {
            peers[leader].tick().unwrap();
            drive(peers, state, blocked);
        }
    }

    fn elect(peers: &mut [DurablePeer], state: &mut State) {
        peers[0].campaign().unwrap();
        drive(peers, state, None);
        assert!(peers[0].is_leader());
        assert_eq!(peers[2].store.initial_state().unwrap().hard_state.vote, 1);
    }

    fn unfiltered(sender: &DurablePeer, message: &Message, witness: bool) -> Vec<u8> {
        let identity = sender.store.witness_identity.as_ref().unwrap();
        let mut bytes =
            encode_envelope_reserved(1, sender.options.group_id, message, trailer_len(identity))
                .unwrap();
        wrap(identity, witness, &mut bytes).unwrap();
        bytes
    }

    #[test]
    fn envelope_borrows_payloads_and_uses_reserved_space_for_every_protocol() {
        let (_directory, peers) = group(&[3], PeerTuning::default());
        let identity = peers[0].store.witness_identity.as_ref().unwrap();
        let message = Message {
            from: 1,
            to: 2,
            term: 1,
            msg_type: MessageType::MsgAppend as i32,
            entries: vec![Entry {
                data: vec![7; 4 * 1024 * 1024],
                index: 1,
                term: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        for protocol in 1..=7 {
            let mut bytes =
                encode_envelope_reserved(protocol, 83, &message, trailer_len(identity)).unwrap();
            let pointer = bytes.as_ptr();
            let capacity = bytes.capacity();
            wrap(identity, false, &mut bytes).unwrap();
            assert_eq!(bytes.as_ptr(), pointer);
            assert_eq!(bytes.capacity(), capacity);
            let (version, group, borrowed) = peers[1].decode_witness_input(&bytes).unwrap();
            assert_eq!((version, group), (protocol, 83));
            assert!(borrowed.as_ptr() >= bytes.as_ptr());
            assert!(borrowed.as_ptr_range().end <= bytes.as_ptr_range().end);
            assert_eq!(Message::decode(borrowed).unwrap(), message);
            assert!(decode_canonical_packet(&bytes).is_none());
            assert!(peers[2].decode_witness_input(&bytes).is_err());
        }
        let mut oversized = encode_packet(83, &message).unwrap();
        oversized.resize(LIMIT, 0);
        assert!(wrap(identity, false, &mut oversized).is_err());
        assert_eq!(oversized[1], 1);
    }

    #[test]
    fn ordinary_entries_are_metadata_only_but_commands_vote_failover_and_replay_are_real() {
        let (directory, mut peers) = group(&[3], PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        let before = peers[0].commit_index();
        let ordinary = b"ordinary-payload-not-stored-on-witness".to_vec();
        let proposal = peers[0].propose(ordinary.clone()).unwrap();
        drive(&mut peers, &mut state, Some(2));
        assert_eq!(peers[0].commit_index(), before);
        assert_eq!(
            peers[0].raw.raft.prs().get(3).unwrap().matched,
            proposal.index
        );
        assert_eq!(peers[0].closest_follower(), Some(2));
        assert!(peers[0].transfer_leadership(3).is_err());
        peers[0].report_unreachable(3).unwrap();
        let timeout = peers[0].raw.raft.election_timeout();
        ticks(&mut peers, &mut state, 0, Some(2), timeout * 2);
        assert_eq!(peers[0].applied_index(), proposal.index);
        assert!(state.applied[&3].is_empty());
        let command = b"durable-command-on-witness".to_vec();
        let command_id = peers[0]
            .propose_with_options(
                PeerProposalOptions {
                    durable_command: true,
                    ..Default::default()
                },
                command.clone(),
            )
            .unwrap();
        drive(&mut peers, &mut state, Some(2));
        assert_eq!(peers[2].applied_index(), command_id.index);
        assert_eq!(state.applied[&3], vec![command.clone()]);
        let wire = std::fs::read(&peers[2].options.wal_path).unwrap();
        assert!(!wire.windows(ordinary.len()).any(|bytes| bytes == ordinary));
        assert!(wire.windows(command.len()).any(|bytes| bytes == command));
        let entries = peers[2]
            .capture_log_reader()
            .unwrap()
            .entries(proposal.index, command_id.index + 1, None)
            .unwrap();
        assert!(entries[0].data.is_empty());
        assert!(application_entry(&entries[0]).is_metadata);
        assert!(!application_entry(&entries[1]).is_metadata);
        assert!(!application_entry(&Entry::default()).is_metadata);
        assert_eq!(entries[1].data, command);
        assert!(commands::is_command(&entries[1]));
        ticks(&mut peers, &mut state, 0, None, timeout * 2);
        assert_eq!(state.applied[&2], vec![ordinary, command.clone()]);
        let removed = peers.remove(2);
        drop(removed);
        peers.insert(2, open(directory.path(), 3, &[3], PeerTuning::default()));
        state.applied.insert(3, Vec::new());
        drive(&mut peers, &mut state, None);
        assert_eq!(state.applied[&3], vec![command]);
        peers[1].force_campaign().unwrap();
        drive(&mut peers, &mut state, Some(1));
        assert!(peers[1].is_leader());
        assert_eq!(peers[2].store.initial_state().unwrap().hard_state.vote, 2);
        ticks(&mut peers, &mut state, 1, Some(1), timeout * 2);
        let next = peers[1]
            .propose_with_options(
                PeerProposalOptions {
                    durable_command: true,
                    ..Default::default()
                },
                b"next-command".to_vec(),
            )
            .unwrap();
        drive(&mut peers, &mut state, Some(1));
        assert_eq!(peers[2].applied_index(), next.index);
        assert_eq!(state.applied[&3].last().unwrap(), b"next-command");
    }

    #[test]
    fn witness_never_campaigns_or_accepts_handoff_but_votes_after_leader_expiry() {
        let (_directory, mut peers) = group(&[3], PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        assert!(peers[2].campaign().is_err());
        assert!(peers[2].force_campaign().is_err());
        let timeout = Message {
            from: 1,
            to: 3,
            term: peers[0].term(),
            msg_type: MessageType::MsgTimeoutNow as i32,
            ..Default::default()
        };
        let packet = peers[0].encode_witness_message(&timeout).unwrap();
        peers[2].step(&packet).unwrap();
        peers[2].set_automatic_elections(true).unwrap();
        for _ in 0..100 {
            peers[2].tick().unwrap();
            assert!(peers[2].process_ready(|_, _| Ok(())).unwrap().is_empty());
            assert_eq!(peers[2].raw.raft.state, StateRole::Follower);
        }
        assert_eq!(peers[2].leader_id(), None);
        peers[1].campaign().unwrap();
        drive(&mut peers, &mut state, Some(1));
        assert!(peers[1].is_leader());
        assert_eq!(peers[2].store.initial_state().unwrap().hard_state.vote, 2);
    }

    #[test]
    fn host_workers_receive_metadata_flags_and_replay_commands_after_witness_restart() {
        let tuning = PeerTuning {
            max_inflight_apply_tasks: Some(2),
            max_apply_bytes: Some(128),
            ..Default::default()
        };
        let (directory, mut peers) = group(&[3], tuning);
        let mut state = State::default();
        elect(&mut peers, &mut state);
        peers[0].propose(b"ordinary-worker-data".to_vec()).unwrap();
        drive(&mut peers, &mut state, None);
        let command = peers[0]
            .propose_with_options(
                PeerProposalOptions {
                    durable_command: true,
                    ..Default::default()
                },
                b"worker-command".to_vec(),
            )
            .unwrap();
        drive(&mut peers, &mut state, None);
        assert_eq!(
            state.applied[&1],
            vec![b"ordinary-worker-data".to_vec(), b"worker-command".to_vec()]
        );
        assert_eq!(state.applied[&2], state.applied[&1]);
        assert_eq!(state.applied[&3], vec![b"worker-command".to_vec()]);
        assert_eq!(peers[2].applied_index(), command.index);
        drop(peers.remove(2));
        peers.insert(2, open(directory.path(), 3, &[3], tuning));
        state.applied.insert(3, Vec::new());
        drive(&mut peers, &mut state, None);
        assert_eq!(state.applied[&3], vec![b"worker-command".to_vec()]);
        assert_eq!(peers[2].applied_index(), command.index);
    }

    #[test]
    fn malformed_roles_payloads_and_witness_leaders_cannot_change_consensus_state() {
        let (_directory, mut peers) = group(&[3], PeerTuning::default());
        let message = Message {
            from: 1,
            to: 3,
            term: 99,
            msg_type: MessageType::MsgHeartbeat as i32,
            ..Default::default()
        };
        let mut invalid = vec![
            encode_packet(83, &message).unwrap(),
            unfiltered(&peers[0], &message, false),
        ];
        let valid = unfiltered(&peers[0], &message, true);
        let (_, _, _, role_tail) = decode_packet_prefix(&valid).unwrap();
        let prefix_length = valid.len() - role_tail.len();
        for body in [
            vec![],
            vec![8, 0, 16, 2],
            vec![8, 8, 16, 2],
            vec![8, 1, 16, 0],
            vec![8, 1, 16, 3],
            vec![16, 2, 8, 1],
            vec![8, 1, 16, 2, 26, 1, 2],
            vec![8, 1, 16, 2, 26, 1, 3, 32, 1],
            vec![8, 0x81, 0, 16, 2, 26, 1, 3],
        ] {
            let mut packet = valid[..prefix_length].to_vec();
            prost::encoding::bytes::encode(4, &body, &mut packet);
            invalid.push(packet);
        }
        let mut unknown = valid.clone();
        prost::encoding::uint32::encode(5, &1, &mut unknown);
        invalid.push(unknown);
        let ordinary = Message {
            entries: vec![Entry {
                index: 1,
                term: 99,
                data: b"not-metadata".to_vec(),
                ..Default::default()
            }],
            msg_type: MessageType::MsgAppend as i32,
            ..message.clone()
        };
        invalid.push(unfiltered(&peers[0], &ordinary, true));
        let snapshot = Message {
            msg_type: MessageType::MsgSnapshot as i32,
            snapshot: Some(Snapshot {
                data: b"image".to_vec(),
                metadata: Some(SnapshotMetadata {
                    index: 1,
                    term: 1,
                    conf_state: Some(ConfState {
                        voters: vec![3],
                        ..Default::default()
                    }),
                }),
            }),
            ..message.clone()
        };
        invalid.push(unfiltered(&peers[0], &snapshot, true));
        let length = peers[2].journal.length;
        for packet in invalid {
            assert!(peers[2].step(&packet).is_err());
            assert_eq!(peers[2].term(), 0);
            assert_eq!(peers[2].journal.length, length);
            assert!(!peers[2].failed);
        }
        for kind in [
            MessageType::MsgRequestVote,
            MessageType::MsgRequestPreVote,
            MessageType::MsgAppend,
            MessageType::MsgHeartbeat,
            MessageType::MsgSnapshot,
        ] {
            let forged = Message {
                from: 3,
                to: 1,
                term: 99,
                msg_type: kind as i32,
                ..Default::default()
            };
            let packet = unfiltered(&peers[2], &forged, false);
            assert!(peers[0].step(&packet).is_err());
            assert_eq!(peers[0].term(), 0);
        }
        let full_data_target = Message {
            to: 2,
            entries: vec![Entry {
                index: 1,
                term: 99,
                context: METADATA_CONTEXT.to_vec(),
                ..Default::default()
            }],
            ..ordinary
        };
        let packet = unfiltered(&peers[0], &full_data_target, false);
        assert!(peers[1].step(&packet).is_err());
        assert_eq!(peers[1].term(), 0);
        peers[2].step(&valid).unwrap();
        assert_eq!(peers[2].term(), 99);
    }

    #[test]
    fn checkpoint_formats_restore_witnesses_and_preserve_command_suffixes_after_restart() {
        for form in 0..3 {
            let tuning = PeerTuning {
                max_checkpoint_sends: (form != 0).then_some(1),
                ..Default::default()
            };
            let (directory, mut peers) = group(&[3], tuning);
            let mut state = State::default();
            elect(&mut peers, &mut state);
            peers[0].propose(b"before-image".to_vec()).unwrap();
            drive(&mut peers, &mut state, Some(3));
            let image = vec![
                9;
                if form == 1 {
                    INLINE_CHECKPOINT_BYTES + 17
                } else {
                    65536
                }
            ];
            if form == 2 {
                peers[0]
                    .checkpoint_from_reader(&mut std::io::Cursor::new(&image), image.len() as u64)
                    .unwrap();
            } else {
                peers[0].checkpoint(image.clone()).unwrap();
            }
            ticks(&mut peers, &mut state, 0, None, 25);
            assert_eq!(state.restored[&3], vec![image.clone()]);
            let command = peers[0]
                .propose_with_options(
                    PeerProposalOptions {
                        durable_command: true,
                        ..Default::default()
                    },
                    b"after-image-command".to_vec(),
                )
                .unwrap();
            drive(&mut peers, &mut state, None);
            assert_eq!(peers[2].applied_index(), command.index);
            let removed = peers.remove(2);
            drop(removed);
            peers.insert(2, open(directory.path(), 3, &[3], tuning));
            state.applied.insert(3, Vec::new());
            state.restored.insert(3, Vec::new());
            drive(&mut peers, &mut state, None);
            assert_eq!(state.restored[&3], vec![image]);
            assert_eq!(state.applied[&3], vec![b"after-image-command".to_vec()]);
        }
    }

    #[test]
    fn role_invalid_journal_batches_cannot_install_a_valid_prefix_or_hard_state() {
        let (_directory, peers) = group(&[3], PeerTuning::default());
        for peer in [&peers[1], &peers[2]] {
            let entries = vec![
                Entry {
                    index: 1,
                    term: 1,
                    ..Default::default()
                },
                Entry {
                    index: 2,
                    term: 1,
                    data: if peer.local_witness() {
                        b"ordinary".to_vec()
                    } else {
                        Vec::new()
                    },
                    context: if peer.local_witness() {
                        Vec::new()
                    } else {
                        METADATA_CONTEXT.to_vec()
                    },
                    ..Default::default()
                },
            ];
            let hard_state = HardState {
                term: 1,
                vote: 1,
                commit: 2,
            };
            let mut record = batch(&peer.options, Some(&hard_state), &entries);
            record.permitted = peer.store.permitted.clone();
            record.witness_identity =
                witness_identity::bytes(&peer.store.witness_identity).to_vec();
            let before = peer.store.initial_state().unwrap();
            let last = peer.store.last_index().unwrap();
            assert!(install_recovered_entries(&peer.store, record.clone()).is_err());
            assert_eq!(peer.store.last_index().unwrap(), last);
            let after = peer.store.initial_state().unwrap();
            assert_eq!(after.hard_state, before.hard_state);
            assert_eq!(after.conf_state, before.conf_state);
            assert!(install_decoded(
                &peer.store,
                &record,
                SnapshotSource::Journal,
                &entries,
                Some(hard_state)
            )
            .is_err());
            assert_eq!(peer.store.last_index().unwrap(), last);
            let after = peer.store.initial_state().unwrap();
            assert_eq!(after.hard_state, before.hard_state);
            assert_eq!(after.conf_state, before.conf_state);
        }
    }

    #[test]
    fn automatic_learner_checkpoint_protocols_coexist_with_witness_replicas() {
        for form in 0..3 {
            let tuning = PeerTuning {
                max_checkpoint_sends: (form != 0).then_some(1),
                ..Default::default()
            };
            let (_directory, mut peers) = group(&[3], tuning);
            let mut state = State::default();
            elect(&mut peers, &mut state);
            peers[0]
                .propose_membership(MembershipChange::AddLearnerAuto(4))
                .unwrap();
            drive(&mut peers, &mut state, Some(4));
            assert_eq!(peers[0].membership().automatic_learners, vec![4]);
            let image = vec![
                7;
                if form == 1 {
                    INLINE_CHECKPOINT_BYTES + 17
                } else {
                    65536
                }
            ];
            if form == 2 {
                peers[0]
                    .checkpoint_from_reader(&mut std::io::Cursor::new(&image), image.len() as u64)
                    .unwrap();
            } else {
                peers[0].checkpoint(image.clone()).unwrap();
            }
            ticks(&mut peers, &mut state, 0, None, 25);
            assert_eq!(state.restored[&4], vec![image]);
            for peer in &peers {
                assert_eq!(peer.membership().voters, vec![1, 2, 3, 4]);
                assert!(peer.membership().outgoing_voters.is_empty());
                assert!(peer.membership().automatic_learners.is_empty());
            }
        }
    }

    #[test]
    fn membership_cannot_remove_the_only_data_voter_or_reinterpret_a_witness() {
        let (_directory, mut peers) = group(&[2, 3], PeerTuning::default());
        let mut state = State::default();
        elect(&mut peers, &mut state);
        let before = peers[0].raw.raft.raft_log.last_index();
        assert!(peers[0]
            .propose_membership(MembershipChange::Remove(1))
            .is_err());
        assert_eq!(peers[0].raw.raft.raft_log.last_index(), before);
        peers[0]
            .propose_membership(MembershipChange::Remove(2))
            .unwrap();
        drive(&mut peers, &mut state, None);
        peers[0]
            .propose_membership(MembershipChange::LeaveJoint)
            .unwrap();
        drive(&mut peers, &mut state, None);
        assert!(peers[0]
            .propose_membership(MembershipChange::AddLearner(2))
            .is_err());
        assert!(peers[0]
            .propose_membership(MembershipChange::AddLearnerAuto(2))
            .is_err());
        assert!(peers[0]
            .propose_membership(MembershipChange::Promote(2))
            .is_err());
        assert_eq!(peers[0].membership().voters, vec![1, 3]);
    }
}
