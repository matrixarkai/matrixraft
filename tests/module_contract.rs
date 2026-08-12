// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    cluster::{RaftCluster, RustRaftConsensus, RustRaftReadIndexRequest},
    config::{RaftConfig, RustRaftConfig},
    membership::{
        JointConsensusMembership, RaftMembershipExecutor, RaftMembershipOperation, RustRaftPeer,
        RustRaftReplicaRole,
    },
    metrics::rustraft_metric_names,
    node::{RaftNodeRuntime, RustRaftNodeOptions},
    readiness::{
        rustraft_open_source_surface, rustraft_parity_report, rustraft_public_api_contract,
        rustraft_standalone_readiness_report, rustraft_temporalstore_adapter_shape,
        RustRaftReadinessSnapshot,
    },
    snapshot::{
        PersistentRaftSnapshotStoreOptions, RaftSnapshot, RustRaftApplySnapshotFence,
        RustRaftSnapshotMeta,
    },
    status::{rustraft_cluster_status_report, RaftHealthStatus},
    transport::{AppendEntriesRequest, ReadIndexRequest, RustRaftTransport, VoteRequest},
    wal::{PersistentRaftWalOptions, RaftHardState, RaftWalRecord},
};

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 23_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 24_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn module_cluster() -> RaftCluster {
    RaftCluster::new(
        707,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("module cluster")
}

#[test]
fn public_modules_expose_temporalstore_consumption_boundary() {
    let mut cluster = module_cluster();
    RustRaftConsensus::start(&mut cluster).expect("start through cluster module");
    let log_id =
        RustRaftConsensus::propose(&mut cluster, b"module-write".to_vec(), Default::default())
            .expect("propose through cluster module");
    assert_eq!(log_id.index, 2);

    let read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 707,
            requester_id: 1,
            min_commit_index: 2,
            allow_lease_read: true,
        })
        .expect("read index through cluster module");
    assert!(read.safe);
    assert!(read.lease_read);
    assert!(cluster.lease_read_eligible(1, 2).expect("lease eligible"));

    cluster
        .campaign(2, false)
        .expect("campaign/pre-vote surface");
    cluster.transfer_leader(1).expect("leader transfer surface");

    let mut executor = RaftMembershipExecutor::new();
    executor
        .execute(
            &mut cluster,
            RaftMembershipOperation::AddLearner(peer(4, RustRaftReplicaRole::Voter)),
        )
        .expect("add learner through membership module");
    assert!(cluster.membership().learners.contains(&4));
    cluster
        .set_node_healthy(4, false)
        .expect("isolate learner after immediate catch-up");
    let catchup_log_id = cluster
        .propose(b"module-write-after-add".to_vec())
        .expect("write after add");
    cluster.compact_logs_through(catchup_log_id.index);
    cluster
        .set_node_healthy(4, true)
        .expect("restore learner for snapshot");

    let snapshot = RaftSnapshot {
        group_id: 707,
        meta: RustRaftSnapshotMeta {
            snapshot_id: "module-contract-catchup".to_string(),
            last_log_id: catchup_log_id.clone(),
            membership: vec![1, 2, 3, 4],
            members: Vec::new(),
        },
        payload: b"snapshot".to_vec(),
    };
    cluster
        .install_snapshot_to(
            4,
            snapshot,
            RustRaftApplySnapshotFence {
                applied_index: catchup_log_id.index,
                commit_index: catchup_log_id.index,
                installed_snapshot_index: catchup_log_id.index,
                first_retained_log_index: catchup_log_id.index + 1,
            },
        )
        .expect("snapshot catch-up through snapshot module");

    executor
        .execute_all(
            &mut cluster,
            vec![
                RaftMembershipOperation::Promote(4),
                RaftMembershipOperation::AddWitness(peer(5, RustRaftReplicaRole::Voter)),
                RaftMembershipOperation::Remove(3),
            ],
        )
        .expect("membership workflow through module boundary");
    let membership = cluster.membership();
    assert!(membership.voters.contains(&4));
    assert!(membership.witnesses.contains(&5));
    assert!(!membership.voters.contains(&3));

    let report = rustraft_cluster_status_report(
        cluster.group_id,
        cluster.leader_id(),
        cluster.leader_transfer_state(),
        vec![cluster.status(1).expect("node status")],
    );
    assert_eq!(report.health, RaftHealthStatus::Healthy);
    assert_eq!(report.leader_id, Some(1));

    let readiness = RustRaftReadinessSnapshot {
        rustraft_leader_write_authority_present: true,
        rustraft_operator_observability_present: true,
        rustraft_rpc_transport_contract_present: true,
        rustraft_log_retention_snapshot_trigger_present: true,
        rustraft_apply_snapshot_fence_present: true,
        raft_storage_apply_fence_present: true,
        rustraft_snapshot_floor_log_matching_present: true,
        rustraft_snapshot_tail_catchup_present: true,
        rustraft_compacted_entry_rejection_present: true,
        rustraft_metaserver_snapshot_floor_election_present: true,
        learner_catchup_promotion_present: true,
        metaserver_membership_workflow_present: true,
    };
    assert!(rustraft_parity_report(&readiness).ready);
    assert!(!rustraft_metric_names().append_latency_ms.is_empty());
}

#[test]
fn standalone_readiness_report_covers_non_temporalstore_embedding_status() {
    let report = rustraft_standalone_readiness_report();
    assert!(report.standalone, "{:?}", report.missing);
    assert_eq!(
        report.production_status,
        matrixraft::RustRaftProductionStatus::ProductionReady
    );
    assert!(report.missing.is_empty());

    let expected = [
        "node_lifecycle",
        "replication",
        "election_pre_vote",
        "membership",
        "wal_recovery",
        "snapshots",
        "read_index_lease_read",
        "status_metrics_readiness",
    ];
    let actual = report
        .capabilities
        .iter()
        .map(|capability| capability.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert!(report
        .capabilities
        .iter()
        .all(|capability| capability.ready));
    assert!(report
        .capabilities
        .iter()
        .all(|capability| !capability.evidence.is_empty()));
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("RaftNodeRuntime")));
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("AppendEntriesRequest")));
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("PersistentRaftWalOptions")));
    assert!(report
        .evidence
        .iter()
        .all(|item| !item.contains("TemporalStore")));

    let api = rustraft_public_api_contract();
    assert!(api
        .compatibility_reports
        .contains(&"rustraft_standalone_readiness_report".to_string()));
    let surface = rustraft_open_source_surface();
    assert!(surface
        .compatibility_reports
        .contains(&"rustraft_standalone_readiness_report".to_string()));
}

#[test]
fn public_modules_export_runtime_storage_wal_snapshot_and_transport_types() {
    let _node_options = std::mem::size_of::<RustRaftNodeOptions>();
    let _node_runtime = std::mem::size_of::<RaftNodeRuntime>();
    let _config = RustRaftConfig::default();
    let _joint = std::mem::size_of::<JointConsensusMembership>();

    let wal_options =
        PersistentRaftWalOptions::new(std::env::temp_dir().join("rustraft-module-wal"));
    assert!(wal_options.validate().is_ok());
    let _hard_state = std::mem::size_of::<RaftHardState>();
    let _wal_record = std::mem::size_of::<RaftWalRecord>();

    let snapshot_options =
        PersistentRaftSnapshotStoreOptions::new(std::env::temp_dir().join("rustraft-module-snap"));
    assert!(snapshot_options.chunk_size > 0);

    let _append = std::mem::size_of::<AppendEntriesRequest>();
    let _vote = std::mem::size_of::<VoteRequest>();
    let _read_index_alias = std::mem::size_of::<ReadIndexRequest>();
    let _transport = std::mem::size_of::<&dyn RustRaftTransport>();
}

#[test]
fn open_source_surface_names_modules_examples_reports_and_adapter_boundary() {
    let api = rustraft_public_api_contract();
    for module in [
        "node",
        "cluster",
        "membership",
        "wal",
        "snapshot",
        "transport",
        "status",
        "metrics",
        "readiness",
    ] {
        assert!(api.public_modules.contains(&module.to_string()));
    }
    assert!(api
        .embedding_examples
        .contains(&"examples/open_source_surface.rs".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"RustRaftBenchmarkRunner".to_string()));
    assert!(api
        .compatibility_reports
        .contains(&"rustraft_production_readiness_report".to_string()));

    let surface = rustraft_open_source_surface();
    assert_eq!(surface.crate_name, "rustraft");
    assert!(surface.public_modules.contains(&"wal".to_string()));
    assert!(surface.embedding_docs.contains(&"README.md".to_string()));
    assert!(surface
        .reference_raft_parity_matrix
        .contains(&"leader_election".to_string()));
    assert!(surface
        .benchmark_harness_interface
        .contains(&"rustraft_run_reference_raft_parity_benchmark".to_string()));
    assert!(surface
        .compatibility_reports
        .contains(&"rustraft_public_api_contract".to_string()));
    assert!(surface
        .temporalstore_adapter_boundary
        .iter()
        .any(|item| item.contains("TemporalStore command codecs")));
    let adapter_shape = rustraft_temporalstore_adapter_shape();
    assert_eq!(adapter_shape.node_field, "node");
    assert!(adapter_shape.node_runtime_type.contains("RaftNodeRuntime"));
    assert!(adapter_shape
        .temporalstore_owned
        .contains(&"apply semantics".to_string()));
}
