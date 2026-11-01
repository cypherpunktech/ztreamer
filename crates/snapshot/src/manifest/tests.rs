use serde_json::Value;
use zakura_chain::{ironwood, orchard, subtree::NoteCommitmentSubtreeIndex};
use ztreamer_chain::{codec::TreeSizes, test_utils::hash};

use super::*;
use crate::{
    manifest_entry::{BlobDigest, Frontiers},
    pool::{SubtreeNode, SubtreeRoot},
    test_utils::entries_from_genesis,
};

/// A manifest with two entries. The first entry has frontiers and subtree roots in all three
/// pools.
fn two_entry_manifest() -> Manifest {
    let add = TreeSizes { sapling: 70_000, orchard: 3, ironwood: 0 };
    let mut entries = entries_from_genesis(&[add, add]);
    entries[0].frontiers = Frontiers {
        sapling: vec![0x01, 0xab, 0x00],
        orchard: vec![0xcd; 40],
        ironwood: Vec::new(),
    };
    entries[0].subtree_roots = vec![
        SubtreeRoot {
            index: NoteCommitmentSubtreeIndex(0),
            root: SubtreeNode::Sapling(sapling_crypto::Node::from_bytes([1; 32]).unwrap()),
        },
        SubtreeRoot {
            index: NoteCommitmentSubtreeIndex(1),
            root: SubtreeNode::Orchard(orchard::tree::Node::try_from([2; 32]).unwrap()),
        },
        SubtreeRoot {
            index: NoteCommitmentSubtreeIndex(2),
            root: SubtreeNode::Ironwood(ironwood::tree::Node::try_from([3; 32]).unwrap()),
        },
    ];
    entries[1].frontiers = entries[0].frontiers.clone();
    entries[0].blob_digest = BlobDigest([7; 32]);
    entries[0].blob_len = 121_034_512;
    Manifest::new("Mainnet", hash(0), entries).unwrap()
}

/// The JSON form of [`two_entry_manifest`], to break one rule at a time.
fn json_form() -> ManifestJson<'static> {
    let manifest = two_entry_manifest();
    let json = ManifestJson::from(&manifest);
    ManifestJson {
        network: Cow::Owned(json.network.into_owned()),
        entries: Cow::Owned(json.entries.into_owned()),
        ..json
    }
}

#[test]
fn manifest_round_trips_through_json() {
    let manifest = two_entry_manifest();
    let json = serde_json::to_string(&manifest).unwrap();

    assert_eq!(Manifest::from_json(&json).unwrap(), manifest);
    assert_eq!(manifest.sealed_through(), 1_999);
}

#[test]
fn json_uses_the_field_names_and_encodings_of_the_plan() {
    let manifest = two_entry_manifest();
    let json: Value = serde_json::to_value(&manifest).unwrap();
    let entry = &json["entries"][0];

    assert_eq!(json["genesis_hash"], manifest.genesis_hash().to_string());
    assert_eq!(entry["end_hash"], manifest.entries()[0].end_hash.to_string());
    assert_eq!(entry["end_tree_sizes"]["sapling"], 70_000);
    assert_eq!(entry["end_tree_sizes"]["orchard"], 3);
    assert_eq!(entry["end_tree_sizes"]["ironwood"], 0);
    assert_eq!(entry["sapling_frontier"], "01ab00");
    assert_eq!(entry["ironwood_frontier"], "");
    assert_eq!(entry["subtree_roots"][0]["pool"], "sapling");
    assert_eq!(entry["subtree_roots"][2]["pool"], "ironwood");
    assert_eq!(entry["subtree_roots"][1]["index"], 1);
    assert_eq!(entry["subtree_roots"][1]["root"], "02".repeat(32));
    assert_eq!(entry["blob_digest"], "07".repeat(32));
    assert_eq!(entry["blob_len"], 121_034_512);
    assert_eq!(entry["epoch_end"], false);
    assert_eq!(json["entries"][1]["epoch_end"], true);
}

/// Twelve rows with the flags of the epoch rule: the rule closes an epoch at row 9 only.
#[test]
fn contiguous_entries_stop_at_the_first_gap_and_close_the_last_epoch() {
    let mut rows = entries_from_genesis(&[TreeSizes::default(); 12]);
    rows[11].epoch_end = false;
    rows.remove(7);

    let manifest = Manifest::from_entries_to_gap("Mainnet", hash(0), rows.clone()).unwrap();
    let prefix = Manifest::from_entries_to_gap("Mainnet", hash(0), rows[..7].to_vec()).unwrap();

    assert_eq!(manifest, prefix);
    assert_eq!(manifest.sealed_through(), 6_999);
    let flags: Vec<_> = manifest.entries().iter().map(|entry| entry.epoch_end).collect();
    assert_eq!(flags, [false, false, false, false, false, false, true]);
}

#[test]
fn contiguous_entries_keep_the_epoch_ends_of_the_rule_before_the_last_entry() {
    let rows = entries_from_genesis(&[TreeSizes::default(); 12]);
    let mut forced = rows.clone();
    forced[11].epoch_end = false;

    let manifest = Manifest::from_entries_to_gap("Mainnet", hash(0), forced).unwrap();

    assert_eq!(manifest.entries(), rows);
    let epochs: Vec<_> = manifest.epochs().map(|epoch| epoch.end_height()).collect();
    assert_eq!(epochs, [9_999, 11_999]);
}

#[test]
fn contiguous_entries_that_do_not_start_at_genesis_are_refused() {
    let rows = entries_from_genesis(&[TreeSizes::default(); 3]);

    let error = Manifest::from_entries_to_gap("Mainnet", hash(0), rows[1..].to_vec()).unwrap_err();

    assert!(matches!(error, ManifestError::Empty), "{error:?}");
}

#[test]
fn serde_checks_the_rules() {
    let mut json = serde_json::to_value(two_entry_manifest()).unwrap();
    json["sealed_through"] = Value::from(2_999);

    let error = serde_json::from_value::<Manifest>(json).unwrap_err();

    assert!(error.to_string().contains("sealed_through 2999"), "{error}");
}

#[test]
fn a_subtree_root_that_is_not_a_field_element_is_refused() {
    let mut json = serde_json::to_value(two_entry_manifest()).unwrap();
    json["entries"][0]["subtree_roots"][1]["root"] = Value::from("ff".repeat(32));

    let error = Manifest::from_json(&json.to_string()).unwrap_err();

    assert!(matches!(error, ManifestError::Json(_)), "{error:?}");
}

/// Each row breaks one rule of [`two_entry_manifest`]. `Manifest::try_from` must refuse the
/// JSON form with the expected error.
#[test]
fn json_that_breaks_a_rule_is_refused() {
    type IsExpected = fn(&ManifestError) -> bool;
    type BreakRule = fn(&mut ManifestJson<'static>);
    let cases: [(&str, BreakRule, IsExpected); 11] = [
        (
            "entries out of order",
            |manifest| manifest.entries.to_mut().swap(0, 1),
            |error| matches!(error, ManifestError::Start { index: 0, start_height: 1_000 }),
        ),
        (
            "a gap",
            |manifest| {
                manifest.entries = Cow::Owned(entries_from_genesis(&[TreeSizes::default(); 3]));
                manifest.entries.to_mut().remove(1);
            },
            |error| matches!(error, ManifestError::Start { index: 1, start_height: 2_000 }),
        ),
        (
            "a wrong range length",
            |manifest| manifest.entries.to_mut()[1].end_height += 1,
            |error| matches!(error, ManifestError::RangeLength { index: 1 }),
        ),
        (
            "a shrinking tree",
            |manifest| manifest.entries.to_mut()[1].end_tree_sizes.orchard = 2,
            |error| matches!(error, ManifestError::TreeSizes { index: 1 }),
        ),
        (
            "a wrong version",
            |manifest| manifest.manifest_version = 2,
            |error| matches!(error, ManifestError::Version { version: 2 }),
        ),
        (
            "sealed_through after the last entry",
            |manifest| manifest.sealed_through = 2_999,
            |error| {
                matches!(
                    error,
                    ManifestError::SealedThrough { sealed_through: 2_999, last_end: 1_999 }
                )
            },
        ),
        (
            "no entries",
            |manifest| {
                manifest.sealed_through = 2_999;
                manifest.entries.to_mut().clear();
            },
            |error| matches!(error, ManifestError::Empty),
        ),
        (
            "a pool that becomes inactive",
            |manifest| manifest.entries.to_mut()[1].frontiers.orchard.clear(),
            |error| matches!(error, ManifestError::PoolInactive { index: 1 }),
        ),
        (
            "an epoch end before the epoch closes",
            |manifest| manifest.entries.to_mut()[0].epoch_end = true,
            |error| matches!(error, ManifestError::EpochEnd { index: 0, expected: false }),
        ),
        (
            "no epoch end where the epoch closes",
            |manifest| {
                manifest.entries = Cow::Owned(entries_from_genesis(&[TreeSizes::default(); 12]));
                manifest.sealed_through = 11_999;
                manifest.entries.to_mut()[9].epoch_end = false;
            },
            |error| matches!(error, ManifestError::EpochEnd { index: 9, expected: true }),
        ),
        // The last entry of the manifest always closes an epoch.
        (
            "no epoch end at the last entry",
            |manifest| {
                manifest.entries = Cow::Owned(entries_from_genesis(&[TreeSizes::default(); 12]));
                manifest.sealed_through = 11_999;
                manifest.entries.to_mut()[11].epoch_end = false;
            },
            |error| matches!(error, ManifestError::EpochEnd { index: 11, expected: true }),
        ),
    ];

    for (case, break_rule, expected) in cases {
        let mut manifest = json_form();
        break_rule(&mut manifest);

        let error = Manifest::try_from(manifest).unwrap_err();

        assert!(expected(&error), "{case}: {error:?}");
    }
}
