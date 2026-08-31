// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    AdminCommand, ApplyTask, FlushTask, FlushTaskDesc, HardState, LogEntry, LogId,
    MailBoxFetchPolicy, MailPriority, Message, ReadTask, Scheduler, SchedulerTask,
};

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: format!("entry-{index}").into_bytes(),
        is_command: true,
    }
}

#[test]
fn scheduler_fetch_preserves_priority_and_fifo_order_mailbox() {
    let scheduler = Scheduler::new(8);
    scheduler.schedule(
        MailPriority::Slowly,
        SchedulerTask::Read(ReadTask {
            target_id: 2,
            from_index: 1,
            to_index: 5,
            limit_bytes: 0,
        }),
    );
    scheduler.schedule(
        MailPriority::Normal,
        SchedulerTask::Message(Message::Admin {
            command: AdminCommand::TriggerSnapshot,
        }),
    );
    scheduler.schedule(
        MailPriority::Urgent,
        SchedulerTask::Apply(ApplyTask {
            entries: vec![entry(1)],
            snapshot: None,
        }),
    );
    scheduler.schedule(
        MailPriority::Normal,
        SchedulerTask::Message(Message::Admin {
            command: AdminCommand::ReleaseMemory,
        }),
    );

    let fetched = scheduler.fetch(MailBoxFetchPolicy {
        limit: 3,
        timeout_ms: 0,
        include_until: MailPriority::Urgent,
    });
    assert_eq!(fetched.len(), 3);
    assert!(matches!(fetched[0], SchedulerTask::Apply(_)));
    assert!(matches!(
        fetched[1],
        SchedulerTask::Message(Message::Admin {
            command: AdminCommand::TriggerSnapshot
        })
    ));
    assert!(matches!(
        fetched[2],
        SchedulerTask::Message(Message::Admin {
            command: AdminCommand::ReleaseMemory
        })
    ));
    assert_eq!(scheduler.queued_tasks(), 1);
}

#[test]
fn scheduler_try_schedule_applies_high_watermark_per_priority() {
    let scheduler = Scheduler::new(1);
    let first = SchedulerTask::Apply(ApplyTask {
        entries: vec![entry(1)],
        snapshot: None,
    });
    let second = SchedulerTask::Apply(ApplyTask {
        entries: vec![entry(2)],
        snapshot: None,
    });

    assert!(scheduler.try_schedule(MailPriority::Normal, first).is_ok());
    assert!(scheduler
        .try_schedule(MailPriority::Normal, second)
        .is_err());
    assert!(scheduler
        .try_schedule(
            MailPriority::Urgent,
            SchedulerTask::Apply(ApplyTask {
                entries: vec![entry(3)],
                snapshot: None,
            })
        )
        .is_ok());
}

#[test]
fn scheduler_records_apply_results_and_step_down_signals() {
    let scheduler = Scheduler::new(4);

    scheduler.send_apply_result(12, false);
    scheduler.send_apply_result(13, true);
    scheduler.step_down(None);
    scheduler.step_down(Some(3));

    let apply_results = scheduler.drain_apply_results();
    assert_eq!(apply_results.len(), 2);
    assert_eq!(apply_results[0].applied_index, 12);
    assert!(!apply_results[0].rejected);
    assert_eq!(apply_results[1].applied_index, 13);
    assert!(apply_results[1].rejected);
    assert!(scheduler.drain_apply_results().is_empty());

    let step_downs = scheduler.drain_step_downs();
    assert_eq!(step_downs.len(), 2);
    assert_eq!(step_downs[0].transferee, None);
    assert_eq!(step_downs[1].transferee, Some(3));
    assert!(scheduler.drain_step_downs().is_empty());
}

#[test]
fn scheduler_validates_flush_task_ranges_and_metadata() {
    let valid = FlushTask {
        desc: FlushTaskDesc {
            first_index: Some(1),
            last_index: Some(2),
            unstable_config_change_index: None,
            delay_apply_task: None,
        },
        committed_index: 2,
        should_flush_meta: true,
        members: vec![1, 2, 3],
        hard_state: Some(HardState {
            current_term: 1,
            voted_for: Some(1),
            committed: Some(LogId { term: 1, index: 2 }),
        }),
        entries: vec![entry(1), entry(2)],
    };
    Scheduler::validate_flush_task(&valid).expect("valid flush task");

    let mut missing_meta = valid.clone();
    missing_meta.hard_state = None;
    assert!(Scheduler::validate_flush_task(&missing_meta).is_err());

    let mut bad_range = valid;
    bad_range.desc.last_index = Some(3);
    assert!(Scheduler::validate_flush_task(&bad_range).is_err());
}
