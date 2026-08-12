// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};

pub const RUSTRAFT_FAULT_MIN_SCENARIO_RUNTIME_MS: u64 = 1_000;
pub const RUSTRAFT_FAULT_MIN_CLIENT_OPERATION_COUNT: u64 = 1;
pub const RUSTRAFT_FAULT_MIN_INJECTED_FAULT_COUNT: u64 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftFaultScenario {
    PacketLossMajority,
    PartitionHeal,
    SlowWalFsync,
    SnapshotDuringMembershipChange,
    LeaderTransferUnderLoad,
    FollowerRejoinCompactedLogs,
    RollingRestartJointConsensus,
}

impl RustRaftFaultScenario {
    pub fn id(self) -> &'static str {
        match self {
            Self::PacketLossMajority => "packet_loss_majority",
            Self::PartitionHeal => "partition_heal",
            Self::SlowWalFsync => "slow_wal_fsync",
            Self::SnapshotDuringMembershipChange => "snapshot_during_membership_change",
            Self::LeaderTransferUnderLoad => "leader_transfer_under_load",
            Self::FollowerRejoinCompactedLogs => "follower_rejoin_compacted_logs",
            Self::RollingRestartJointConsensus => "rolling_restart_joint_consensus",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftFaultScenarioRequirement {
    pub scenario: RustRaftFaultScenario,
    pub required_for_production: bool,
    pub baseline_raft_reference: String,
    pub acceptance: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftFaultScenarioEvidence {
    pub scenario: RustRaftFaultScenario,
    pub process_path_observed: bool,
    #[serde(default)]
    pub spawned_process_count: u64,
    #[serde(default)]
    pub observed_process_ids: Vec<u64>,
    #[serde(default)]
    pub scenario_runtime_ms: u64,
    #[serde(default)]
    pub client_operation_count: u64,
    #[serde(default)]
    pub injected_fault_count: u64,
    pub independent_wal_dirs_observed: bool,
    pub independent_snapshot_dirs_observed: bool,
    pub safety_observed: bool,
    pub recovery_observed: bool,
    pub metrics_observed: bool,
    #[serde(default)]
    pub observed_acceptance: Vec<String>,
    #[serde(default)]
    pub report_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftFaultScenarioResult {
    pub scenario: RustRaftFaultScenario,
    pub ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftFaultHarnessReadinessReport {
    pub ready: bool,
    pub required_scenarios: Vec<RustRaftFaultScenarioRequirement>,
    pub results: Vec<RustRaftFaultScenarioResult>,
    pub missing: Vec<String>,
}

pub fn rustraft_baseline_raft_fault_scenarios() -> Vec<RustRaftFaultScenarioRequirement> {
    vec![
        RustRaftFaultScenarioRequirement {
            scenario: RustRaftFaultScenario::PacketLossMajority,
            required_for_production: true,
            baseline_raft_reference: "majority continues while the minority rejects stale reads/writes".to_string(),
            acceptance: vec![
                "majority_commit_observed".to_string(),
                "minority_write_rejected".to_string(),
                "minority_stale_read_rejected".to_string(),
            ],
        },
        RustRaftFaultScenarioRequirement {
            scenario: RustRaftFaultScenario::PartitionHeal,
            required_for_production: true,
            baseline_raft_reference: "partitioned peer heals, reconciles divergent tail, catches up, and only then becomes read eligible".to_string(),
            acceptance: vec![
                "partition_isolated_peer_observed".to_string(),
                "heal_detected".to_string(),
                "divergent_tail_truncated_or_reconciled".to_string(),
                "healed_follower_caught_up".to_string(),
                "read_eligible_after_heal_catchup".to_string(),
            ],
        },
        RustRaftFaultScenarioRequirement {
            scenario: RustRaftFaultScenario::SlowWalFsync,
            required_for_production: true,
            baseline_raft_reference: "slow WAL fsync activates backpressure without losing committed writes".to_string(),
            acceptance: vec![
                "fsync_pressure_observed".to_string(),
                "backpressure_observed".to_string(),
                "committed_write_survived_restart".to_string(),
            ],
        },
        RustRaftFaultScenarioRequirement {
            scenario: RustRaftFaultScenario::SnapshotDuringMembershipChange,
            required_for_production: true,
            baseline_raft_reference: "snapshot floor and membership generation remain consistent across restart".to_string(),
            acceptance: vec![
                "snapshot_floor_matches_membership_generation".to_string(),
                "restart_replay_preserves_joint_state".to_string(),
            ],
        },
        RustRaftFaultScenarioRequirement {
            scenario: RustRaftFaultScenario::LeaderTransferUnderLoad,
            required_for_production: true,
            baseline_raft_reference: "leader transfer under active writes commits each accepted write exactly once".to_string(),
            acceptance: vec![
                "transfer_timeout_enforced".to_string(),
                "exact_once_commit_ids_observed".to_string(),
                "final_leader_has_committed_entries".to_string(),
            ],
        },
        RustRaftFaultScenarioRequirement {
            scenario: RustRaftFaultScenario::FollowerRejoinCompactedLogs,
            required_for_production: true,
            baseline_raft_reference: "rejoining follower installs snapshot, replays retained tail, and becomes read-eligible only after catch-up".to_string(),
            acceptance: vec![
                "compacted_log_rejected".to_string(),
                "snapshot_installed".to_string(),
                "tail_replayed".to_string(),
                "read_eligible_after_catchup".to_string(),
            ],
        },
        RustRaftFaultScenarioRequirement {
            scenario: RustRaftFaultScenario::RollingRestartJointConsensus,
            required_for_production: true,
            baseline_raft_reference: "pending joint consensus survives rolling restarts and completes or rolls back safely".to_string(),
            acceptance: vec![
                "joint_state_recovered".to_string(),
                "safe_completion_or_rollback_observed".to_string(),
            ],
        },
    ]
}

pub fn rustraft_fault_harness_readiness_report(
    evidence: &[RustRaftFaultScenarioEvidence],
) -> RustRaftFaultHarnessReadinessReport {
    let required_scenarios = rustraft_baseline_raft_fault_scenarios();
    let results = required_scenarios
        .iter()
        .map(|requirement| {
            let scenario_evidence = evidence
                .iter()
                .find(|item| item.scenario == requirement.scenario);
            let missing = rustraft_fault_scenario_missing(requirement, scenario_evidence);
            RustRaftFaultScenarioResult {
                scenario: requirement.scenario,
                ready: missing.is_empty(),
                missing,
            }
        })
        .collect::<Vec<_>>();
    let missing = results
        .iter()
        .flat_map(|result| {
            result
                .missing
                .iter()
                .map(move |field| format!("{}:{field}", result.scenario.id()))
        })
        .collect::<Vec<_>>();
    RustRaftFaultHarnessReadinessReport {
        ready: missing.is_empty(),
        required_scenarios,
        results,
        missing,
    }
}

fn rustraft_fault_scenario_missing(
    requirement: &RustRaftFaultScenarioRequirement,
    evidence: Option<&RustRaftFaultScenarioEvidence>,
) -> Vec<String> {
    let Some(evidence) = evidence else {
        return vec!["evidence_missing".to_string()];
    };
    let mut missing = Vec::new();
    if !evidence.process_path_observed {
        missing.push("process_path_observed".to_string());
    }
    if evidence.spawned_process_count < 3 {
        missing.push("spawned_process_count_at_least_3".to_string());
    }
    let distinct_process_ids = evidence
        .observed_process_ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if distinct_process_ids.len() < 3 {
        missing.push("distinct_process_ids_at_least_3".to_string());
    }
    if evidence.scenario_runtime_ms < RUSTRAFT_FAULT_MIN_SCENARIO_RUNTIME_MS {
        missing.push(format!(
            "scenario_runtime_ms_at_least_{}",
            RUSTRAFT_FAULT_MIN_SCENARIO_RUNTIME_MS
        ));
    }
    if evidence.client_operation_count < RUSTRAFT_FAULT_MIN_CLIENT_OPERATION_COUNT {
        missing.push(format!(
            "client_operation_count_at_least_{}",
            RUSTRAFT_FAULT_MIN_CLIENT_OPERATION_COUNT
        ));
    }
    if evidence.injected_fault_count < RUSTRAFT_FAULT_MIN_INJECTED_FAULT_COUNT {
        missing.push(format!(
            "injected_fault_count_at_least_{}",
            RUSTRAFT_FAULT_MIN_INJECTED_FAULT_COUNT
        ));
    }
    if !evidence.independent_wal_dirs_observed {
        missing.push("independent_wal_dirs_observed".to_string());
    }
    if !evidence.independent_snapshot_dirs_observed {
        missing.push("independent_snapshot_dirs_observed".to_string());
    }
    if !evidence.safety_observed {
        missing.push("safety_observed".to_string());
    }
    if !evidence.recovery_observed {
        missing.push("recovery_observed".to_string());
    }
    if !evidence.metrics_observed {
        missing.push("metrics_observed".to_string());
    }
    for required in &requirement.acceptance {
        if !evidence
            .observed_acceptance
            .iter()
            .any(|observed| observed == required)
        {
            missing.push(format!("acceptance:{required}"));
        }
    }
    if evidence
        .report_path
        .as_deref()
        .unwrap_or_default()
        .is_empty()
    {
        missing.push(
            match requirement.scenario {
                RustRaftFaultScenario::LeaderTransferUnderLoad => "exact_once_report_path",
                _ => "process_fault_report_path",
            }
            .to_string(),
        );
    }
    missing
}
