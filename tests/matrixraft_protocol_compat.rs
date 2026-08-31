// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    AppendEntriesRequest, AppendEntriesResponse, HardState, InstallSnapshotRequest,
    InstallSnapshotResponse, LogEntry, LogId, MatrixRaftAdminCommand, MatrixRaftAdminCommandType,
    MatrixRaftAdminStatus, MatrixRaftAppendEntriesRequest, MatrixRaftAppendEntriesResponse,
    MatrixRaftConfState, MatrixRaftEntry, MatrixRaftEntryType, MatrixRaftHardState,
    MatrixRaftLeaseRequest, MatrixRaftLeaseResponse, MatrixRaftMemberId, MatrixRaftMessage,
    MatrixRaftMessageType, MatrixRaftMetadata, MatrixRaftOldSnapshotFinish,
    MatrixRaftOldSnapshotFinishState, MatrixRaftPropose, MatrixRaftRequireSnapshot,
    MatrixRaftSnapshotDesc, MatrixRaftSnapshotProgress, Membership, MembershipOperation, Peer,
    ReadIndexRequest, ReplicaRole, SnapshotChunk, SnapshotMetadata, SnapshotState,
    StorageApplyFence, VoteResponse,
};

fn peer(node_id: u64, role: ReplicaRole, auto_promote: bool) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 71_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 72_000 + node_id),
        role,
        auto_promote,
    }
}

#[test]
fn matrixraft_protocol_metadata_shapes_round_trip_to_matrixraft_types() {
    let voter = peer(1, ReplicaRole::Voter, false);
    let learner = peer(2, ReplicaRole::Learner, true);
    let witness = peer(3, ReplicaRole::Witness, false);

    let voter_member = MatrixRaftMemberId::from(&voter);
    assert_eq!(voter_member.conf_state, MatrixRaftConfState::Voter);
    assert_eq!(voter_member.to_peer(), voter);
    let learner_member = MatrixRaftMemberId::from(&learner);
    assert_eq!(learner_member.conf_state, MatrixRaftConfState::Learner);
    assert!(learner_member.auto_promote);
    assert_eq!(
        MatrixRaftConfState::from(witness.role),
        MatrixRaftConfState::Witness
    );

    let snapshot_meta = SnapshotMetadata {
        snapshot_id: "snap-9".to_string(),
        last_log_id: LogId { term: 4, index: 9 },
        membership: vec![1, 2, 3],
        members: vec![voter.clone(), learner.clone(), witness.clone()],
    };
    let desc = MatrixRaftSnapshotDesc::from_snapshot_meta(&snapshot_meta);
    assert_eq!(desc.index, 9);
    assert_eq!(desc.term, 4);
    assert_eq!(desc.members.len(), 3);
    assert_eq!(desc.version, 1);
    assert_eq!(desc.to_snapshot_meta("snap-9"), snapshot_meta);

    let hard_state = HardState {
        current_term: 4,
        voted_for: Some(1),
        committed: Some(LogId { term: 4, index: 9 }),
    };
    let matrixraft_hard = MatrixRaftHardState::from(&hard_state);
    assert_eq!(matrixraft_hard.current_term, 4);
    assert_eq!(matrixraft_hard.voted_for, Some(1));

    let metadata = MatrixRaftMetadata::from_hard_state_and_membership(
        &hard_state,
        &Membership {
            group_id: 44,
            voters: vec![1],
            learners: vec![2],
            witnesses: vec![3],
            epoch: 8,
        },
        &[voter.clone(), learner.clone(), witness.clone()],
    );
    assert_eq!(metadata.current_term, 4);
    assert_eq!(metadata.committed_index, 9);
    assert_eq!(metadata.members.len(), 3);
    assert_eq!(
        metadata.initial_state.as_ref().map(|state| state.index),
        Some(9)
    );
    assert_eq!(metadata.version, 2);
}

#[test]
fn matrixraft_protocol_message_shapes_cover_entries_lease_snapshot_and_admin_payloads() {
    let entries = vec![
        LogEntry {
            log_id: LogId { term: 5, index: 10 },
            payload: b"set-a".to_vec(),
            is_command: true,
        },
        LogEntry {
            log_id: LogId { term: 5, index: 11 },
            payload: b"meta".to_vec(),
            is_command: false,
        },
    ];
    let request = AppendEntriesRequest {
        group_id: 99,
        term: 5,
        leader_id: 1,
        prev_log_id: Some(LogId { term: 4, index: 9 }),
        entries: entries.clone(),
        leader_commit: 11,
        lease_epoch: 77,
    };
    let matrixraft_append = MatrixRaftAppendEntriesRequest::from(&request);
    assert_eq!(matrixraft_append.prev_term, 4);
    assert_eq!(matrixraft_append.prev_index, 9);
    assert_eq!(matrixraft_append.entries.len(), 2);
    assert_eq!(matrixraft_append.entries[0].bytes_size, 5);
    assert_eq!(
        matrixraft_append.entries[0].entry_type,
        MatrixRaftEntryType::Normal
    );
    assert_eq!(
        matrixraft_append.entries[1].entry_type,
        MatrixRaftEntryType::Meta
    );

    let message = MatrixRaftMessage::append_entries(1, 2, &request);
    assert_eq!(
        message.message_type,
        MatrixRaftMessageType::AppendEntriesRequest
    );
    assert_eq!(message.from, Some(1));
    assert_eq!(message.to, Some(2));
    assert_eq!(message.term, Some(5));
    assert_eq!(message.committed_index, Some(11));
    assert_eq!(message.bytes_size, 9);
    let encoded_message = message.to_wire_bytes().expect("encode MatrixRaft message");
    assert_eq!(
        MatrixRaftMessage::from_wire_bytes(&encoded_message).expect("decode MatrixRaft message"),
        message
    );
    assert_eq!(
        message.wire_size().expect("message wire size"),
        encoded_message.len() as u64
    );
    let sized_message = message.clone().with_wire_size().expect("stamp wire size");
    assert_eq!(
        sized_message.bytes_size,
        sized_message.wire_size().expect("sized message wire size")
    );
    assert_eq!(
        message
            .append_entries_request
            .as_ref()
            .expect("append entries")
            .entries
            .len(),
        2
    );
    let lease_request_message = MatrixRaftMessage::append_entries_lease_request(
        1,
        2,
        &request,
        MatrixRaftLeaseRequest { epoch_id: 88 },
    );
    assert_eq!(
        lease_request_message.message_type,
        MatrixRaftMessageType::AppendEntriesRequest
    );
    assert_eq!(
        lease_request_message
            .lease_request
            .as_ref()
            .expect("lease request")
            .epoch_id,
        88
    );
    assert!(lease_request_message.append_entries_request.is_some());

    let vote_response_message = MatrixRaftMessage::vote_response(
        2,
        1,
        VoteResponse {
            term: 6,
            vote_granted: true,
            reason: "vote_granted".to_string(),
        },
        false,
    );
    assert_eq!(
        vote_response_message.message_type,
        MatrixRaftMessageType::VoteResponse
    );
    assert_eq!(vote_response_message.from, Some(2));
    assert_eq!(vote_response_message.to, Some(1));
    assert_eq!(vote_response_message.term, Some(6));
    assert!(
        vote_response_message
            .vote_response
            .as_ref()
            .expect("vote response")
            .vote_granted
    );
    let pre_vote_response_message = MatrixRaftMessage::vote_response(
        2,
        1,
        VoteResponse {
            term: 6,
            vote_granted: true,
            reason: "pre_vote_granted".to_string(),
        },
        true,
    );
    assert_eq!(
        pre_vote_response_message.message_type,
        MatrixRaftMessageType::PreVoteResponse
    );

    let pre_vote_message = MatrixRaftMessage::pre_vote(2, 1);
    assert_eq!(
        pre_vote_message.message_type,
        MatrixRaftMessageType::PreVote
    );
    assert_eq!(pre_vote_message.from, Some(2));
    assert_eq!(pre_vote_message.to, Some(1));
    assert!(pre_vote_message.vote_request.is_none());
    assert!(pre_vote_message.vote_response.is_none());

    let response = AppendEntriesResponse {
        term: 5,
        success: false,
        match_index: 0,
        rejection_hint: Some(8),
        rejected_index: Some(10),
        require_snapshot: Some(7),
        snapshot_state: SnapshotState::NotReady,
        lease_confirmation_epoch: 0,
        lease_duration_ms: 0,
    };
    let matrixraft_response = MatrixRaftAppendEntriesResponse::from(&response);
    assert!(!matrixraft_response.received);
    assert_eq!(matrixraft_response.matched_index, None);
    assert_eq!(matrixraft_response.rejected_hint, Some(8));
    assert_eq!(matrixraft_response.rejected_index, Some(10));

    let response_message = MatrixRaftMessage::append_entries_response(2, 1, &response);
    assert_eq!(
        response_message.message_type,
        MatrixRaftMessageType::AppendEntriesResponse
    );
    assert_eq!(response_message.from, Some(2));
    assert_eq!(response_message.to, Some(1));
    assert_eq!(response_message.term, Some(5));
    assert_eq!(
        response_message
            .append_entries_response
            .as_ref()
            .expect("append response")
            .rejected_hint,
        Some(8)
    );
    assert_eq!(
        response_message
            .require_snapshot
            .as_ref()
            .expect("snapshot requirement")
            .required_index,
        7
    );
    assert_eq!(
        response_message
            .snapshot_state
            .expect("snapshot state on response"),
        SnapshotState::NotReady
    );
    let lease_response_message = MatrixRaftMessage::append_entries_lease_response(
        2,
        1,
        &response,
        MatrixRaftLeaseResponse {
            max_met_epoch_id: 89,
            duration_ms: 250,
        },
    );
    assert_eq!(
        lease_response_message.message_type,
        MatrixRaftMessageType::AppendEntriesResponse
    );
    assert_eq!(
        lease_response_message
            .lease_response
            .as_ref()
            .expect("lease response")
            .max_met_epoch_id,
        89
    );
    assert_eq!(
        lease_response_message
            .lease_response
            .as_ref()
            .expect("lease response")
            .duration_ms,
        250
    );
    assert!(lease_response_message.append_entries_response.is_some());

    let install_response = InstallSnapshotResponse {
        term: 6,
        accepted: true,
        next_offset: 4096,
        committed_index: 12,
        reason: "snapshot_chunk_accepted".to_string(),
    };
    let install_response_message =
        MatrixRaftMessage::install_snapshot_response(2, 1, install_response.clone());
    assert_eq!(
        install_response_message.message_type,
        MatrixRaftMessageType::InstallSnapshotResponse
    );
    assert_eq!(install_response_message.from, Some(2));
    assert_eq!(install_response_message.to, Some(1));
    assert_eq!(install_response_message.term, Some(6));
    assert_eq!(install_response_message.committed_index, Some(12));
    assert_eq!(
        install_response_message
            .install_snapshot_response
            .as_ref()
            .expect("install snapshot response"),
        &install_response
    );

    let install_request = InstallSnapshotRequest {
        group_id: 99,
        term: 6,
        leader_id: 1,
        chunk: SnapshotChunk {
            meta: SnapshotMetadata {
                snapshot_id: "snap-12".to_string(),
                last_log_id: LogId { term: 6, index: 12 },
                membership: vec![1, 2, 3],
                members: Vec::new(),
            },
            offset: 0,
            data: b"snapshot-state".to_vec(),
            done: true,
        },
    };
    let install_request_message =
        MatrixRaftMessage::install_snapshot(1, 2, install_request.clone());
    assert_eq!(
        install_request_message.message_type,
        MatrixRaftMessageType::InstallSnapshotRequest
    );
    assert_eq!(install_request_message.from, Some(1));
    assert_eq!(install_request_message.to, Some(2));
    assert_eq!(install_request_message.term, Some(6));
    assert_eq!(install_request_message.committed_index, Some(12));
    assert_eq!(
        install_request_message
            .install_snapshot_request
            .as_ref()
            .expect("install snapshot request"),
        &install_request
    );

    let read_index_request = ReadIndexRequest {
        group_id: 99,
        requester_id: 2,
        min_commit_index: 11,
        allow_lease_read: false,
    };
    let read_index_message = MatrixRaftMessage::read_index(2, 1, read_index_request.clone());
    assert_eq!(
        read_index_message.message_type,
        MatrixRaftMessageType::ReadIndexRequest
    );
    assert_eq!(read_index_message.from, Some(2));
    assert_eq!(read_index_message.to, Some(1));
    assert_eq!(read_index_message.committed_index, Some(11));
    assert_eq!(
        read_index_message
            .read_index_request
            .as_ref()
            .expect("read-index request"),
        &read_index_request
    );

    let snapshot_progress = MatrixRaftSnapshotProgress {
        remote_receiving: true,
        elapsed_since_last_receiving_ms: 20,
        send_timeout_ms: 100,
    };
    let snapshot_progress_message =
        MatrixRaftMessage::snapshot_progress(2, 1, snapshot_progress.clone());
    assert_eq!(
        snapshot_progress_message.message_type,
        MatrixRaftMessageType::SnapshotProgress
    );
    assert_eq!(snapshot_progress_message.from, Some(2));
    assert_eq!(snapshot_progress_message.to, Some(1));
    assert_eq!(
        snapshot_progress_message
            .snapshot_progress
            .as_ref()
            .expect("snapshot progress"),
        &snapshot_progress
    );

    let catch_up_message = MatrixRaftMessage::catch_up_peer(1, 5);
    assert_eq!(
        catch_up_message.message_type,
        MatrixRaftMessageType::CatchUpPeer
    );
    assert_eq!(catch_up_message.from, Some(1));
    assert_eq!(catch_up_message.to, Some(5));
    assert!(catch_up_message.propose.is_none());
    assert!(catch_up_message.config_change.is_none());
    assert!(catch_up_message.read_index_request.is_none());

    let promote_message = MatrixRaftMessage::promote_peer(1, 5);
    assert_eq!(
        promote_message.message_type,
        MatrixRaftMessageType::PromotePeer
    );
    assert_eq!(promote_message.from, Some(1));
    assert_eq!(promote_message.to, Some(5));
    assert!(!promote_message.auto_promote);
    assert!(promote_message.config_change.is_none());

    let auto_promote_message = MatrixRaftMessage::auto_promote_learner(1, 6);
    assert_eq!(
        auto_promote_message.message_type,
        MatrixRaftMessageType::AutoPromoteLearner
    );
    assert_eq!(auto_promote_message.from, Some(1));
    assert_eq!(auto_promote_message.to, Some(6));
    assert!(auto_promote_message.auto_promote);
    assert!(auto_promote_message.config_change.is_none());

    let network_error_message = MatrixRaftMessage::network_error(1, 2);
    assert_eq!(
        network_error_message.message_type,
        MatrixRaftMessageType::NetworkError
    );
    assert_eq!(network_error_message.from, Some(1));
    assert_eq!(network_error_message.to, Some(2));
    assert!(network_error_message.propose.is_none());
    assert!(network_error_message.config_change.is_none());

    let config_entry = MatrixRaftEntry {
        entry_type: MatrixRaftEntryType::ConfigChange,
        term: 6,
        index: 12,
        propose: None,
        config_change: Some(matrixraft::MatrixRaftConfigChange {
            request_id: Some(123),
            change_type: matrixraft::MatrixRaftConfigChangeType::AddNode,
            member_id: 4,
            raft_addr: "127.0.0.1:71004".to_string(),
            snapshot_addr: "127.0.0.1:72004".to_string(),
            old_members: Vec::new(),
            conf_state: MatrixRaftConfState::Learner,
            auto_promote: true,
        }),
        memberships: Vec::new(),
        request_id: 123,
        bytes_size: 0,
    };
    let log_entry = config_entry.to_log_entry();
    assert_eq!(log_entry.log_id.index, 12);
    assert!(!log_entry.is_command);
    assert!(!log_entry.payload.is_empty());

    let normal_entry = MatrixRaftEntry {
        entry_type: MatrixRaftEntryType::Normal,
        term: 6,
        index: 13,
        propose: Some(MatrixRaftPropose {
            request_id: Some(124),
            data: b"write".to_vec(),
            context: b"ctx".to_vec(),
            is_command: true,
        }),
        config_change: None,
        memberships: Vec::new(),
        request_id: 124,
        bytes_size: 5,
    };
    assert_eq!(normal_entry.to_log_entry().payload, b"write");

    let propose_message = MatrixRaftMessage::propose(
        1,
        2,
        MatrixRaftPropose {
            request_id: Some(125),
            data: b"route-write".to_vec(),
            context: b"ctx".to_vec(),
            is_command: true,
        },
    );
    assert_eq!(propose_message.message_type, MatrixRaftMessageType::Propose);
    assert_eq!(propose_message.from, Some(1));
    assert_eq!(propose_message.to, Some(2));
    assert_eq!(propose_message.bytes_size, 11);
    assert_eq!(
        propose_message
            .propose
            .as_ref()
            .expect("propose payload")
            .data,
        b"route-write"
    );

    let config_change_message = MatrixRaftMessage::config_change(
        1,
        2,
        matrixraft::MatrixRaftConfigChange {
            request_id: Some(126),
            change_type: matrixraft::MatrixRaftConfigChangeType::AddNode,
            member_id: 7,
            raft_addr: "127.0.0.1:71007".to_string(),
            snapshot_addr: "127.0.0.1:72007".to_string(),
            old_members: Vec::new(),
            conf_state: MatrixRaftConfState::Learner,
            auto_promote: true,
        },
    );
    assert_eq!(
        config_change_message.message_type,
        MatrixRaftMessageType::ConfigChange
    );
    assert_eq!(config_change_message.from, Some(1));
    assert_eq!(config_change_message.to, Some(2));
    let config_change = config_change_message
        .config_change
        .as_ref()
        .expect("config change payload");
    assert_eq!(config_change.member_id, 7);
    assert_eq!(config_change.conf_state, MatrixRaftConfState::Learner);
    assert!(config_change.auto_promote);

    let membership_operation = MembershipOperation::AddVoter(peer(8, ReplicaRole::Learner, true));
    let membership_operation_message =
        MatrixRaftMessage::membership_operation(1, 2, membership_operation.clone());
    assert_eq!(
        membership_operation_message.message_type,
        MatrixRaftMessageType::MembershipOperation
    );
    assert_eq!(membership_operation_message.from, Some(1));
    assert_eq!(membership_operation_message.to, Some(2));
    assert_eq!(
        membership_operation_message
            .membership_operation
            .as_ref()
            .expect("membership operation"),
        &membership_operation
    );
    assert!(membership_operation_message.config_change.is_none());

    let lease_request = MatrixRaftLeaseRequest { epoch_id: 88 };
    let lease_response = MatrixRaftLeaseResponse {
        max_met_epoch_id: 89,
        duration_ms: 250,
    };
    assert_eq!(lease_request.epoch_id, 88);
    assert_eq!(lease_response.duration_ms, 250);

    let require_snapshot = MatrixRaftRequireSnapshot { required_index: 7 };
    let old_finish = MatrixRaftOldSnapshotFinish {
        finish_state: MatrixRaftOldSnapshotFinishState::ChecksumError,
        snapshot_index: 6,
    };
    assert_eq!(require_snapshot.required_index, 7);
    assert_eq!(old_finish.snapshot_index, 6);

    let command = MatrixRaftAdminCommand {
        command_type: MatrixRaftAdminCommandType::TransferLeader,
        request_id: Some(9),
        node_id: Some(1),
        transferee_id: Some(2),
        forced_campaign: false,
        status: Some(MatrixRaftAdminStatus {
            success: true,
            tips: b"ok".to_vec(),
        }),
        snapshot_state: Some(SnapshotState::Received),
        snapshot_id: Some("snapshot-12".to_string()),
        applied_index: Some(11),
        log_index: Some(10),
        entry: Some(MatrixRaftEntry {
            term: 1,
            index: 10,
            entry_type: MatrixRaftEntryType::Normal,
            propose: Some(MatrixRaftPropose {
                request_id: Some(10),
                data: b"future-entry".to_vec(),
                context: b"reorder".to_vec(),
                is_command: true,
            }),
            config_change: None,
            memberships: Vec::new(),
            request_id: 10,
            bytes_size: 12,
        }),
        first_index: Some(1),
        last_index: Some(11),
        prohibits_election: Some(false),
        apply_task_rejected: false,
        stabled_config_change_index: Some(12),
        ignore_witness: Some(true),
        snapshot_peer_id: Some(3),
        snapshot_index: Some(12),
        snapshot_total_chunks: Some(4),
        snapshot_bytes: Some(1024),
        snapshot_done: false,
        storage_fence: Some(StorageApplyFence {
            group_id: 77,
            node_id: 1,
            committed_index: 11,
            applied_index: 11,
            durable_applied_index: 11,
            storage_flushed_index: 11,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        }),
        acknowledgements: vec![1, 2],
        lease_valid: Some(true),
        lease_epoch: Some(77),
        lease_duration_ms: Some(25),
        elapsed_ms: Some(5),
        healthy: Some(true),
        reason: Some("fatal".to_string()),
    };
    assert_eq!(
        command.command_type,
        MatrixRaftAdminCommandType::TransferLeader
    );
    assert_eq!(command.transferee_id, Some(2));
    assert_eq!(command.node_id, Some(1));
    assert_eq!(command.log_index, Some(10));
    assert_eq!(command.snapshot_id.as_deref(), Some("snapshot-12"));
    assert_eq!(command.status.as_ref().unwrap().tips, b"ok");

    let step_down = MatrixRaftAdminCommand::step_down(Some(2));
    assert_eq!(step_down.command_type, MatrixRaftAdminCommandType::StepDown);
    assert_eq!(step_down.transferee_id, Some(2));

    let explicit_campaign = MatrixRaftAdminCommand::campaign(3, true);
    assert_eq!(
        explicit_campaign.command_type,
        MatrixRaftAdminCommandType::Election
    );
    assert_eq!(explicit_campaign.node_id, Some(3));
    assert!(explicit_campaign.forced_campaign);

    let complete_transfer = MatrixRaftAdminCommand::complete_leader_transfer();
    assert_eq!(
        complete_transfer.command_type,
        MatrixRaftAdminCommandType::CompleteLeaderTransfer
    );

    let abort_transfer = MatrixRaftAdminCommand::abort_leader_transfer("operator abort");
    assert_eq!(
        abort_transfer.command_type,
        MatrixRaftAdminCommandType::AbortLeaderTransfer
    );
    assert_eq!(abort_transfer.reason.as_deref(), Some("operator abort"));

    let partition_peer = MatrixRaftAdminCommand::partition_peer(2);
    assert_eq!(
        partition_peer.command_type,
        MatrixRaftAdminCommandType::PartitionPeer
    );
    assert_eq!(partition_peer.snapshot_peer_id, Some(2));
    let heal_peer = MatrixRaftAdminCommand::heal_peer(2);
    assert_eq!(heal_peer.command_type, MatrixRaftAdminCommandType::HealPeer);
    assert_eq!(heal_peer.snapshot_peer_id, Some(2));

    let healthy_peer = MatrixRaftAdminCommand::set_node_healthy(2, false);
    assert_eq!(
        healthy_peer.command_type,
        MatrixRaftAdminCommandType::SetNodeHealthy
    );
    assert_eq!(healthy_peer.node_id, Some(2));
    assert_eq!(healthy_peer.healthy, Some(false));

    let fatal = MatrixRaftAdminCommand::fire_fatal_event(1, "disk stalled");
    assert_eq!(
        fatal.command_type,
        MatrixRaftAdminCommandType::FireFatalEvent
    );
    assert_eq!(fatal.node_id, Some(1));
    assert_eq!(fatal.reason.as_deref(), Some("disk stalled"));

    let reorder_entry = MatrixRaftEntry {
        term: 1,
        index: 12,
        entry_type: MatrixRaftEntryType::Normal,
        propose: Some(MatrixRaftPropose {
            request_id: Some(12),
            data: b"future".to_vec(),
            context: Vec::new(),
            is_command: true,
        }),
        config_change: None,
        memberships: Vec::new(),
        request_id: 12,
        bytes_size: 6,
    };
    let out_of_order =
        MatrixRaftAdminCommand::receive_out_of_order_append(2, reorder_entry.clone());
    assert_eq!(
        out_of_order.command_type,
        MatrixRaftAdminCommandType::ReceiveOutOfOrderAppend
    );
    assert_eq!(out_of_order.node_id, Some(2));
    assert_eq!(out_of_order.entry, Some(reorder_entry));

    let expire_reorder = MatrixRaftAdminCommand::expire_peer_reorder_queue(2);
    assert_eq!(
        expire_reorder.command_type,
        MatrixRaftAdminCommandType::ExpirePeerReorderQueue
    );
    assert_eq!(expire_reorder.node_id, Some(2));

    let ready = MatrixRaftAdminCommand::snapshot_ready("snapshot-12", true);
    assert_eq!(
        ready.command_type,
        MatrixRaftAdminCommandType::SnapshotReady
    );
    assert_eq!(ready.snapshot_id.as_deref(), Some("snapshot-12"));
    assert!(ready.status.as_ref().expect("ready status").success);

    let applied = MatrixRaftAdminCommand::snapshot_applied("snapshot-12");
    assert_eq!(
        applied.command_type,
        MatrixRaftAdminCommandType::SnapshotApplied
    );
    assert_eq!(applied.snapshot_id.as_deref(), Some("snapshot-12"));

    let set_lease = MatrixRaftAdminCommand::set_leader_lease_valid(false);
    assert_eq!(
        set_lease.command_type,
        MatrixRaftAdminCommandType::SetLeaderLeaseValid
    );
    assert_eq!(set_lease.lease_valid, Some(false));

    let leader_confirmation =
        MatrixRaftAdminCommand::receive_leader_lease_confirmation(2, 100, Some(15));
    assert_eq!(
        leader_confirmation.command_type,
        MatrixRaftAdminCommandType::ReceiveLeaderLeaseConfirmation
    );
    assert_eq!(leader_confirmation.node_id, Some(2));
    assert_eq!(leader_confirmation.lease_epoch, Some(100));
    assert_eq!(leader_confirmation.lease_duration_ms, Some(15));

    let tick_leader = MatrixRaftAdminCommand::tick_leader_lease(15);
    assert_eq!(
        tick_leader.command_type,
        MatrixRaftAdminCommandType::TickLeaderLease
    );
    assert_eq!(tick_leader.elapsed_ms, Some(15));

    let follower_lease = MatrixRaftAdminCommand::receive_follower_lease(101);
    assert_eq!(
        follower_lease.command_type,
        MatrixRaftAdminCommandType::ReceiveFollowerLease
    );
    assert_eq!(follower_lease.lease_epoch, Some(101));

    let tick_follower = MatrixRaftAdminCommand::tick_follower_lease(20);
    assert_eq!(
        tick_follower.command_type,
        MatrixRaftAdminCommandType::TickFollowerLease
    );
    assert_eq!(tick_follower.elapsed_ms, Some(20));

    let begin_send = MatrixRaftAdminCommand::begin_snapshot_send(2, "snapshot-13", 13, 3);
    assert_eq!(
        begin_send.command_type,
        MatrixRaftAdminCommandType::BeginSnapshotSend
    );
    assert_eq!(begin_send.snapshot_peer_id, Some(2));
    assert_eq!(begin_send.snapshot_id.as_deref(), Some("snapshot-13"));
    assert_eq!(begin_send.snapshot_index, Some(13));
    assert_eq!(begin_send.snapshot_total_chunks, Some(3));
    let encoded_command = begin_send
        .to_wire_bytes()
        .expect("encode MatrixRaft admin command");
    assert_eq!(
        MatrixRaftAdminCommand::from_wire_bytes(&encoded_command)
            .expect("decode MatrixRaft admin command"),
        begin_send
    );
    assert_eq!(
        begin_send.wire_size().expect("admin wire size"),
        encoded_command.len() as u64
    );

    let sent = MatrixRaftAdminCommand::record_snapshot_chunk_sent(2, 512);
    assert_eq!(
        sent.command_type,
        MatrixRaftAdminCommandType::RecordSnapshotChunkSent
    );
    assert_eq!(sent.snapshot_peer_id, Some(2));
    assert_eq!(sent.snapshot_bytes, Some(512));

    let retry = MatrixRaftAdminCommand::retry_snapshot_chunk(2);
    assert_eq!(
        retry.command_type,
        MatrixRaftAdminCommandType::RetrySnapshotChunk
    );

    let ack = MatrixRaftAdminCommand::acknowledge_snapshot_chunk(2);
    assert_eq!(
        ack.command_type,
        MatrixRaftAdminCommandType::AcknowledgeSnapshotChunk
    );

    let cancel = MatrixRaftAdminCommand::cancel_snapshot_send(2);
    assert_eq!(
        cancel.command_type,
        MatrixRaftAdminCommandType::CancelSnapshotSend
    );

    let begin_install = MatrixRaftAdminCommand::begin_snapshot_install(2, "snapshot-14", 14, 2);
    assert_eq!(
        begin_install.command_type,
        MatrixRaftAdminCommandType::BeginSnapshotInstall
    );
    assert_eq!(begin_install.snapshot_total_chunks, Some(2));

    let receive = MatrixRaftAdminCommand::receive_snapshot_chunk(2, 128, true);
    assert_eq!(
        receive.command_type,
        MatrixRaftAdminCommandType::ReceiveSnapshotChunk
    );
    assert_eq!(receive.snapshot_bytes, Some(128));
    assert!(receive.snapshot_done);

    let rollback = MatrixRaftAdminCommand::rollback_snapshot_install(2);
    assert_eq!(
        rollback.command_type,
        MatrixRaftAdminCommandType::RollbackSnapshotInstall
    );

    let synced = MatrixRaftAdminCommand::synced(Some(1), Some(12), 11);
    assert_eq!(synced.command_type, MatrixRaftAdminCommandType::Synced);
    assert_eq!(synced.first_index, Some(1));
    assert_eq!(synced.last_index, Some(12));
    assert_eq!(synced.stabled_config_change_index, Some(11));

    let apply_result = MatrixRaftAdminCommand::applied(1, 12, true);
    assert_eq!(
        apply_result.command_type,
        MatrixRaftAdminCommandType::Applied
    );
    assert_eq!(apply_result.node_id, Some(1));
    assert_eq!(apply_result.applied_index, Some(12));
    assert!(apply_result.apply_task_rejected);

    let apply_inflight = MatrixRaftAdminCommand::apply_task_inflight(1, 12);
    assert_eq!(
        apply_inflight.command_type,
        MatrixRaftAdminCommandType::ApplyTaskInflight
    );
    assert_eq!(apply_inflight.node_id, Some(1));
    assert_eq!(apply_inflight.applied_index, Some(12));

    let replicated = MatrixRaftAdminCommand::replicated(2, false);
    assert_eq!(
        replicated.command_type,
        MatrixRaftAdminCommandType::Replicated
    );
    assert_eq!(replicated.node_id, Some(2));
    assert!(
        !replicated
            .status
            .as_ref()
            .expect("replicated status")
            .success
    );

    let compact = MatrixRaftAdminCommand::compact_logs_through(8);
    assert_eq!(
        compact.command_type,
        MatrixRaftAdminCommandType::CompactLogsThrough
    );
    assert_eq!(compact.log_index, Some(8));

    let fence = StorageApplyFence {
        group_id: 77,
        node_id: 1,
        committed_index: 12,
        applied_index: 12,
        durable_applied_index: 12,
        storage_flushed_index: 12,
        installed_snapshot_index: 0,
        first_retained_log_index: 1,
    };
    let fenced_compact = MatrixRaftAdminCommand::compact_logs_with_storage_fence(9, fence.clone());
    assert_eq!(
        fenced_compact.command_type,
        MatrixRaftAdminCommandType::CompactLogsWithStorageFence
    );
    assert_eq!(fenced_compact.log_index, Some(9));
    assert_eq!(fenced_compact.storage_fence, Some(fence));

    let checkpoint = MatrixRaftAdminCommand::checkpoint_snapshot(1, "checkpoint-12");
    assert_eq!(
        checkpoint.command_type,
        MatrixRaftAdminCommandType::CheckpointSnapshot
    );
    assert_eq!(checkpoint.node_id, Some(1));
    assert_eq!(checkpoint.snapshot_id.as_deref(), Some("checkpoint-12"));

    let witness_quorum = MatrixRaftAdminCommand::witness_quorum([1, 2, 5]);
    assert_eq!(
        witness_quorum.command_type,
        MatrixRaftAdminCommandType::WitnessQuorum
    );
    assert_eq!(witness_quorum.acknowledgements, vec![1, 2, 5]);

    let release_memory = MatrixRaftAdminCommand::release_memory();
    assert_eq!(
        release_memory.command_type,
        MatrixRaftAdminCommandType::ReleaseMemory
    );
}
