// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

fn locations(segments: &[Segment]) -> Vec<(u64, u64, u64)> {
    segments
        .iter()
        .map(|s| (s.start, s.end, s.offset))
        .collect()
}

#[test]
fn captured_ranges_match_replayed_prefixes_after_overwrites() {
    let ranges = [(1, 4), (5, 8), (9, 12), (7, 10), (11, 16), (3, 6), (7, 18)];
    let mut index = LogIndex::default();
    index.append_records(
        ranges
            .iter()
            .enumerate()
            .map(|(i, &(start, end))| RecordMeta {
                start,
                end,
                offset: i as u64 * 64,
                end_offset: (i as u64 + 1) * 64,
            })
            .collect(),
    );
    index.scanned = ranges.len() as u64 * 64;
    for prefix in 0..=ranges.len() {
        let length = prefix as u64 * 64;
        let expected = LogIndex::segments_through(&index.records, length);
        for low in 1..=19 {
            for high in low..=20 {
                let wanted: Vec<_> = expected
                    .iter()
                    .filter(|s| s.end >= low && s.start < high)
                    .map(|s| (s.start, s.end, s.offset))
                    .collect();
                let actual = index.range_plan(length, low, high);
                assert_eq!(
                    locations(&actual),
                    wanted,
                    "prefix={prefix} range={low}..{high}"
                );
            }
        }
    }
}

#[test]
fn an_overwrite_in_a_later_batch_does_not_change_earlier_captures() {
    let mut index = LogIndex::default();
    index.append_records(vec![RecordMeta {
        start: 1,
        end: 4,
        offset: 0,
        end_offset: 64,
    }]);
    index.append_records(vec![
        RecordMeta {
            start: 5,
            end: 8,
            offset: 64,
            end_offset: 128,
        },
        RecordMeta {
            start: 9,
            end: 12,
            offset: 128,
            end_offset: 192,
        },
        RecordMeta {
            start: 7,
            end: 10,
            offset: 192,
            end_offset: 256,
        },
    ]);
    index.scanned = 256;
    assert_eq!(index.first_overwrite_record, Some(3));
    assert_eq!(
        locations(&index.range_plan(192, 7, 12)),
        [(5, 8, 64), (9, 12, 128)]
    );
    assert_eq!(locations(&index.range_plan(256, 7, 12)), [(7, 10, 192)]);
    // Captures can include records with no entries between the indexed records.
    assert_eq!(locations(&index.range_plan(130, 5, 12)), [(5, 8, 64)]);
}
