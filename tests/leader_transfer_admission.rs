// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{matrixraft_leader_transfer_admission, LeaderTransferAdmissionKind, ReplicaRole};

#[test]
fn leader_transfer_admission_reports_each_decision() {
    let cases = [
        (
            1,
            None,
            Some(ReplicaRole::Voter),
            LeaderTransferAdmissionKind::IgnoredSelf,
            "ignored_self_transfer",
        ),
        (
            2,
            None,
            None,
            LeaderTransferAdmissionKind::IgnoredUnknownPeer,
            "ignored_unknown_transferee",
        ),
        (
            2,
            None,
            Some(ReplicaRole::Learner),
            LeaderTransferAdmissionKind::IgnoredIneligiblePeer,
            "ignored_ineligible_transferee_Learner",
        ),
        (
            2,
            Some(2),
            Some(ReplicaRole::Voter),
            LeaderTransferAdmissionKind::Duplicate,
            "duplicate_transfer_in_progress",
        ),
        (
            2,
            Some(3),
            Some(ReplicaRole::Voter),
            LeaderTransferAdmissionKind::Replaced,
            "replaced_transfer_in_progress",
        ),
        (
            2,
            None,
            Some(ReplicaRole::Voter),
            LeaderTransferAdmissionKind::Accepted,
            "accepted_transfer",
        ),
    ];

    for (transferee_id, current, role, kind, reason) in cases {
        let admission = matrixraft_leader_transfer_admission(1, transferee_id, current, role);
        assert_eq!(admission.kind, kind);
        assert_eq!(admission.transferee_id, transferee_id);
        assert_eq!(admission.previous_transferee_id, current);
        assert_eq!(admission.reason, reason);
    }
}
