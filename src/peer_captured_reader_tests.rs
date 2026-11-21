// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[test]
fn captured_prefix_large_journal_keeps_serving_exact_small_reads() {
    let entries: Vec<_> = (1..=4096)
        .map(|index| Entry {
            index,
            term: 1,
            data: vec![index as u8; 32],
            ..Default::default()
        })
        .collect();
    let records: Vec<_> = entries
        .iter()
        .map(|entry| record(std::slice::from_ref(entry)))
        .collect();
    let prefix = records[..2048]
        .iter()
        .map(|r| 24 + r.encode_to_vec().len() as u64)
        .sum();
    let (root, mut long) = fixture(&records, 4096);
    let mut short = PeerLogReader {
        file: File::open(root.path().join("log")).unwrap(),
        length: prefix,
        first: 1,
        last: 2048,
        group: 17,
        node: 1,
        voters: vec![1],
        permitted: Vec::new(),
        index: Arc::clone(&long.index),
        witness_identity: None,
    };
    assert_eq!(
        long.entries(4096, 4097, None).unwrap(),
        [entries[4095].clone()]
    );
    for expected in entries.iter().skip(1799).take(32) {
        let index = expected.index;
        let actual = short.entries(index, index + 1, Some(0)).unwrap();
        assert_eq!(actual.as_slice(), std::slice::from_ref(expected));
        assert_eq!(
            actual,
            short.scan_entries(index, index + 1, Some(0)).unwrap()
        );
    }
    assert!(short.entries(2049, 2050, None).is_err());
}

#[test]
fn captured_prefix_before_an_overwrite_keeps_its_original_entries() {
    let batches = [
        entries_of(1, 4, 1),
        entries_of(5, 8, 1),
        entries_of(4, 12, 2),
    ];
    let records: Vec<_> = batches.iter().map(|entries| record(entries)).collect();
    let prefix = records[..2]
        .iter()
        .map(|r| 24 + r.encode_to_vec().len() as u64)
        .sum();
    let (root, mut long) = fixture(&records, 12);
    let mut short = PeerLogReader {
        file: File::open(root.path().join("log")).unwrap(),
        length: prefix,
        first: 1,
        last: 8,
        group: 17,
        node: 1,
        voters: vec![1],
        permitted: Vec::new(),
        index: Arc::clone(&long.index),
        witness_identity: None,
    };
    assert_eq!(long.entries(4, 5, None).unwrap()[0].term, 2);
    assert_eq!(short.index.lock().unwrap().first_overwrite_record, Some(2));
    for low in 1..=8 {
        for high in low + 1..=9 {
            for budget in [None, Some(0), Some(1), Some(64)] {
                let actual = short.entries(low, high, budget).unwrap();
                assert!(actual.iter().all(|e| e.term == 1));
                assert_eq!(actual, short.scan_entries(low, high, budget).unwrap());
            }
        }
    }
}
