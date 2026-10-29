use std::fs;

use chrono::DateTime;
use ed25519_dalek::SigningKey;
use serde_json::Value;
use tempfile::TempDir;
use ztreamer_chain::test_utils::record;
use ztreamer_index::test_utils::{populate, range_meta_row, sealed_range_blob};
use ztreamer_snapshot::layout::{Latest, RangePath, SignatureFile};

use super::*;
use crate::test_utils::index_with_rows;

/// 2026-10-26T00:00:00Z.
const NOW: i64 = 1_792_972_800;

/// The rows that the filler writes for three ranges with no commitments: no range closes an
/// epoch.
const ALL_ROWS: [(u32, bool); 3] = [(0, false), (1_000, false), (2_000, false)];

fn export(index: &Index, out: &TempDir, days: i64) -> SnapshotExport {
    let now = DateTime::from_timestamp(NOW + days * 86_400, 0).unwrap();
    SnapshotExport::run(index, &Network::Mainnet, now, &writer(out)).unwrap()
}

fn signing_key() -> SigningKey {
    SigningKey::from_bytes(&[7; 32])
}

fn writer(out: &TempDir) -> SnapshotWriter {
    SnapshotWriter::new(out.path(), signing_key())
}

/// `entries` with `epoch_end` on the last entry, as the export writes them.
fn closed(mut entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
    entries.last_mut().unwrap().epoch_end = true;
    entries
}

#[test]
fn export_writes_content_addressed_ranges_a_manifest_and_latest() {
    let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (index, entries) = index_with_rows(dir.path(), 3, &ALL_ROWS);

    let report = export(&index, &out, 0);

    let manifest_path = "manifests/2026-10-26T000000Z.json";
    assert_eq!(
        report,
        SnapshotExport {
            manifest: manifest_path.to_owned(),
            ranges: 3,
            written: 3,
            missing_rows: 0
        }
    );
    for entry in &entries {
        let start = entry.start_height;
        let name = format!("{start}-{}.bin", hex::encode(&entry.blob_digest.0[..4]));
        assert_eq!(RangePath::from(entry).as_str(), format!("ranges/{name}"));
        let file = fs::read(out.path().join("ranges").join(name)).unwrap();
        assert_eq!(file, sealed_range_blob(&index, start));
    }

    let bytes = fs::read(out.path().join(manifest_path)).unwrap();
    let manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
    let expected =
        Manifest::new("Mainnet", Network::Mainnet.genesis_hash(), closed(entries.clone())).unwrap();
    assert_eq!(manifest, expected);
    assert_eq!(manifest.sealed_through(), 2_999);
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    let entry = &json["entries"][0];
    assert_eq!(entry["end_hash"], entries[0].end_hash.to_string());
    assert_eq!(entry["sapling_frontier"], "01ab00");
    assert_eq!(entry["ironwood_frontier"], "");
    assert_eq!(entry["subtree_roots"][0]["pool"], "orchard");
    assert_eq!(entry["subtree_roots"][0]["root"], "02".repeat(32));
    assert_eq!(entry["blob_digest"], hex::encode(entries[0].blob_digest.0));
    assert_eq!(entry["end_tree_sizes"]["sapling"], 0);

    let json: Value =
        serde_json::from_slice(&fs::read(out.path().join("latest.json")).unwrap()).unwrap();
    assert!(json.get("signature").is_some());
    let latest: Latest = serde_json::from_value(json["latest"].clone()).unwrap();
    assert_eq!(latest, Latest { manifest: manifest_path.to_owned(), sealed_through: 2_999 });
    assert!(!out.path().join("latest.json.sig").exists());
    assert!(out.path().join(format!("{manifest_path}.sig")).exists());
}

#[test]
fn a_second_export_writes_no_range_file_again() {
    let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (index, entries) = index_with_rows(dir.path(), 3, &ALL_ROWS);
    export(&index, &out, 0);
    // The export does not read a file with the name and the length of its entry. A file
    // with another length is written again.
    let [same_length, short] = [&entries[0], &entries[1]].map(|entry| {
        let path = out.path().join(RangePath::from(entry).as_str());
        let mut bytes = fs::read(&path).unwrap();
        bytes[100] ^= 1;
        (path, bytes)
    });
    fs::write(&same_length.0, &same_length.1).unwrap();
    fs::write(&short.0, &short.1[..10]).unwrap();

    let report = export(&index, &out, 1);

    assert_eq!((report.ranges, report.written), (3, 1));
    assert_eq!(fs::read(&same_length.0).unwrap(), same_length.1);
    assert_eq!(fs::read(&short.0).unwrap(), sealed_range_blob(&index, 1_000));
    assert_eq!(report.manifest, "manifests/2026-10-27T000000Z.json");
    assert_eq!(fs::read_dir(out.path().join("ranges")).unwrap().count(), 3);
    // Two manifests, each with its signature.
    assert_eq!(fs::read_dir(out.path().join("manifests")).unwrap().count(), 4);

    // An export in the same second does not replace the manifest.
    let now = DateTime::from_timestamp(NOW + 86_400, 0).unwrap();
    let error = SnapshotExport::run(&index, &Network::Mainnet, now, &writer(&out)).unwrap_err();
    assert!(error.to_string().contains("never replaced"), "{error}");
}

#[test]
fn signatures_verify_with_the_signing_key() {
    let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (index, _) = index_with_rows(dir.path(), 3, &ALL_ROWS);
    let key = signing_key().verifying_key();

    let report = export(&index, &out, 0);

    let latest = fs::read(out.path().join(Latest::PATH)).unwrap();
    let latest = Latest::from_file(&latest, &key).unwrap();
    assert_eq!(latest.manifest, "manifests/2026-10-26T000000Z.json");
    // `latest.json` holds its own signature.
    assert!(!out.path().join(SignatureFile::path_of(Latest::PATH)).exists());
    let signature = fs::read(out.path().join(SignatureFile::path_of(&report.manifest)));
    let signature = SignatureFile::try_from(&signature.unwrap()[..]).unwrap();
    let bytes = fs::read(out.path().join(&report.manifest)).unwrap();
    signature.verify(&bytes, &key).unwrap();
    assert!(signature.verify(b"other bytes", &key).is_err());
}

#[test]
fn export_stops_at_the_first_range_without_a_row() {
    let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (index, entries) = index_with_rows(dir.path(), 3, &[(0, false), (2_000, false)]);

    let report = export(&index, &out, 0);

    assert_eq!((report.ranges, report.missing_rows), (1, 2));
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(out.path().join(&report.manifest)).unwrap()).unwrap();
    assert_eq!(manifest.entries(), closed(entries[..1].to_vec()));
    assert_eq!(manifest.sealed_through(), 999);
}

/// Twelve ranges with no commitments: the filler closes an epoch at range 9 only. The
/// export keeps ranges 10 and 11 and closes the second epoch at range 11.
#[test]
fn export_closes_the_last_epoch_at_the_last_range() {
    let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let rows: Vec<_> = (0..12).map(|range| (range * RANGE_SIZE, range == 9)).collect();
    let (index, entries) = index_with_rows(dir.path(), 12, &rows);

    let report = export(&index, &out, 0);

    assert_eq!((report.ranges, report.missing_rows), (12, 0));
    let json = fs::read_to_string(out.path().join(report.manifest)).unwrap();
    let manifest = Manifest::from_json(&json).unwrap();
    assert_eq!(manifest.entries(), closed(entries));
    assert_eq!(manifest.sealed_through(), 11_999);
    let epochs: Vec<_> = manifest.epochs().map(|epoch| epoch.end_height()).collect();
    assert_eq!(epochs, [9_999, 11_999]);
    assert!(!range_meta_row(&index, 11_000).entry.epoch_end);
}

#[test]
fn rows_stop_at_the_filled_through_height() {
    let dir = tempfile::tempdir().unwrap();
    let rows = [(0, false), (1_000, false), (3_000, false)];
    let (index, entries) = index_with_rows(dir.path(), 4, &rows);
    let filled_through = index.latest().unwrap().range_meta_filled_through().unwrap();

    let rows = Rows::read(&index).unwrap();

    assert_eq!(filled_through, Some(1_999));
    assert_eq!(rows.entries.last().map(|entry| entry.end_height), filled_through);
    assert_eq!(rows.entries, entries[..2]);
    assert_eq!(rows.missing, 2);
}

#[test]
fn an_index_without_a_row_at_genesis_is_refused() {
    let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (index, _) = index_with_rows(dir.path(), 3, &[(1_000, false)]);
    let now = DateTime::from_timestamp(NOW, 0).unwrap();

    let error = SnapshotExport::run(&index, &Network::Mainnet, now, &writer(&out)).unwrap_err();

    assert!(error.to_string().contains("no range_meta row"), "{error}");
    assert!(!out.path().join("latest.json").exists());
}

#[test]
fn an_index_with_no_sealed_range_is_refused() {
    let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let network = Network::Mainnet;
    let index =
        Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", network.genesis_hash()).unwrap();
    let records: Vec<_> = (0..10).map(|height| record(height, Vec::new())).collect();
    populate(&index, &records, None);
    let now = DateTime::from_timestamp(NOW, 0).unwrap();

    let error = SnapshotExport::run(&index, &network, now, &writer(&out)).unwrap_err();

    assert!(error.to_string().contains("no sealed range"), "{error}");
}
