//! `ztreamerd export-snapshot`: writes the sealed ranges of an index as a snapshot that a replica
//! can import.
//!
//! [`ztreamer_verify::snapshot`] gives the layout of the snapshot directory. The manifest is
//! `manifests/<time>.json`, where `<time>` is UTC, for example `2026-10-26T143005Z`.
//!
//! The export reads the chain from the identity row of the index. It starts at genesis and stops
//! at the first sealed range without a `range_meta` row. The last range of a manifest always
//! closes an epoch, so the export sets `epoch_end` on the last entry. The row in the index keeps
//! its flag.
//!
//! [`SnapshotWriter`] writes the files. A daily export into the same directory writes only the
//! new ranges. It never replaces a manifest.

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use eyre::{Result, WrapErr, bail, ensure, eyre};
use tracing::{info, warn};
use zakura_chain::parameters::Network;
use zeroize::Zeroizing;
use ztreamer_indexer::index::{Index, RANGE_SIZE, range_meta::ManifestEntry};
use ztreamer_verify::{manifest::Manifest, snapshot::SnapshotWriter};

/// Writes the sealed ranges of an index as a snapshot. The index is the one of the global
/// `--index-dir` and `--index-map-size` flags.
#[derive(Debug, clap::Args)]
pub(crate) struct ExportArgs {
    /// Snapshot directory. An export into the same directory writes only the new ranges.
    #[arg(long, value_name = "DIR")]
    out: PathBuf,

    /// Ed25519 key as a raw 32-byte seed. It signs the manifest and `latest.json`.
    #[arg(long, value_name = "FILE")]
    signing_key: PathBuf,
}

impl ExportArgs {
    /// Exports `index` and logs what the export wrote.
    pub(crate) fn run(self, index: &Index) -> Result<()> {
        // Read the key before the potentially lengthy export.
        let signer = Signer::from_file(&self.signing_key)?;
        let identity = index.identity()?;
        let network: Network = identity
            .network
            .parse()
            .wrap_err_with(|| format!("the index is for network {}", identity.network))?;
        ensure!(
            network.genesis_hash() == identity.genesis_hash,
            "the index has genesis {}, not the genesis of {network}",
            identity.genesis_hash
        );
        let writer = SnapshotWriter::new(self.out, signer.0);
        ExportReport::export(index, &network, Utc::now(), &writer)?.log();
        Ok(())
    }
}

/// What one export wrote.
#[derive(Debug, Eq, PartialEq)]
struct ExportReport {
    /// The path of the manifest, relative to the snapshot directory.
    manifest: String,
    /// The ranges in the manifest.
    ranges: usize,
    /// The range files written. The other files were present with the correct digest.
    written: usize,
    /// The sealed ranges from the first range without a `range_meta` row to the sealed tip.
    missing_rows: u32,
}

impl ExportReport {
    /// Writes the `range_meta` entries of `index` from genesis as a snapshot with `writer`.
    /// Returns what the export wrote.
    fn export(
        index: &Index,
        network: &Network,
        now: DateTime<Utc>,
        writer: &SnapshotWriter,
    ) -> Result<Self> {
        let Rows { mut entries, missing: missing_rows } = Rows::read(index)?;
        let last = entries
            .last_mut()
            .ok_or_else(|| eyre!("the index has no range_meta row for the range at genesis"))?;
        // `ztreamer_verify::epoch::Epochs` closes an epoch at the last entry of a manifest.
        last.epoch_end = true;
        let manifest = Manifest::new(network.to_string(), network.genesis_hash(), entries)
            .wrap_err("the range_meta rows do not make a valid manifest")?;
        let exported = writer.export(index, &manifest, now)?;
        Ok(Self {
            manifest: exported.manifest,
            ranges: manifest.entries().len(),
            written: exported.written,
            missing_rows,
        })
    }

    fn log(&self) {
        let Self { manifest, ranges, written, missing_rows } = self;
        info!(%manifest, ranges, written, "exported snapshot");
        if *missing_rows > 0 {
            warn!(
                missing_rows,
                "export stopped at the first sealed range without a range_meta row; \
                 run the daemon in embedded mode to fill the rows"
            );
        }
    }
}

/// The `range_meta` entries from genesis to the first sealed range without a row.
#[derive(Debug)]
struct Rows {
    entries: Vec<ManifestEntry>,
    /// The sealed ranges from the first range without a row to the sealed tip.
    missing: u32,
}

impl Rows {
    fn read(index: &Index) -> Result<Self> {
        let Some(sealed_through) = index.state()?.sealed_through() else {
            bail!("the index has no sealed range");
        };
        let sealed = sealed_through / RANGE_SIZE + 1;
        let entries = (0..sealed)
            .map_while(|range| index.range_meta(range * RANGE_SIZE).transpose())
            .map(|meta| meta.map(|meta| meta.entry))
            .collect::<Result<Vec<_>, _>>()?;
        let found = u32::try_from(entries.len()).expect("at most `sealed` entries");
        Ok(Self { entries, missing: sealed - found })
    }
}

/// The Ed25519 key that signs the manifest and `latest.json`.
#[derive(Debug)]
struct Signer(SigningKey);

impl Signer {
    /// Reads a key file that holds the raw 32-byte seed. The seed goes straight into a buffer
    /// that is zeroed on drop, with no other copy in memory.
    fn from_file(path: &Path) -> Result<Self> {
        let read = || format!("read signing key {}", path.display());
        let mut file = File::open(path).wrap_err_with(read)?;
        let len = file.metadata().wrap_err_with(read)?.len();
        ensure!(len == 32, "signing key {} has {len} bytes, not a 32-byte seed", path.display());
        let mut seed = Zeroizing::new([0; 32]);
        file.read_exact(seed.as_mut()).wrap_err_with(read)?;
        Ok(Self(SigningKey::from_bytes(&seed)))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::Value;
    use tempfile::TempDir;
    use zakura_chain::{orchard, subtree::NoteCommitmentSubtreeIndex};
    use ztreamer_indexer::{
        index::range_meta::{Frontiers, RangeMeta, SubtreeNode, SubtreeRoot, VerifyState},
        test_utils::{populate, record},
    };
    use ztreamer_verify::snapshot::{Latest, RangePath, SignatureFile};

    use super::*;

    /// 2026-10-26T00:00:00Z.
    const NOW: i64 = 1_792_972_800;

    /// An index with `ranges` sealed ranges and a `range_meta` row for each `(start, epoch_end)`
    /// in `rows`. Returns the index and the entries of the rows.
    fn indexed(dir: &TempDir, ranges: u32, rows: &[(u32, bool)]) -> (Index, Vec<ManifestEntry>) {
        let network = Network::Mainnet;
        let index = Index::open(
            dir.path(),
            512 * 1024 * 1024,
            &network.to_string(),
            network.genesis_hash(),
        )
        .unwrap();
        let records: Vec<_> =
            (0..ranges * RANGE_SIZE + 10).map(|height| record(height, Vec::new())).collect();
        populate(&index, &records, Some(ranges * RANGE_SIZE - 1));
        let entries = rows
            .iter()
            .map(|&(start, epoch_end)| {
                let blob = index.read_sealed_range_bytes(start).unwrap().unwrap();
                let range = &records[start as usize..][..RANGE_SIZE as usize];
                let entry = ManifestEntry {
                    frontiers: Frontiers {
                        sapling: vec![0x01, 0xab, 0x00],
                        orchard: Vec::new(),
                        ironwood: Vec::new(),
                    },
                    subtree_roots: vec![SubtreeRoot {
                        index: NoteCommitmentSubtreeIndex(1),
                        root: SubtreeNode::Orchard(orchard::tree::Node::try_from([2; 32]).unwrap()),
                    }],
                    epoch_end,
                    ..ManifestEntry::from_records_and_blob(range, &blob)
                };
                let meta = RangeMeta {
                    entry: entry.clone(),
                    state: VerifyState::Verified,
                    verified_by: None,
                    verified_at: None,
                    attested: None,
                    sampled_heights: Vec::new(),
                };
                index.put_range_meta(start, &meta).unwrap();
                entry
            })
            .collect();
        (index, entries)
    }

    fn export(index: &Index, out: &TempDir, days: i64) -> ExportReport {
        let now = DateTime::from_timestamp(NOW + days * 86_400, 0).unwrap();
        ExportReport::export(index, &Network::Mainnet, now, &writer(out)).unwrap()
    }

    fn writer(out: &TempDir) -> SnapshotWriter {
        SnapshotWriter::new(out.path(), signer().0)
    }

    /// Writes the test key to a key file in `dir`.
    fn key_file(dir: &Path) -> PathBuf {
        let path = dir.join("key");
        fs::write(&path, [7; 32]).unwrap();
        path
    }

    fn signer() -> Signer {
        let dir = tempfile::tempdir().unwrap();
        Signer::from_file(&key_file(dir.path())).unwrap()
    }

    /// The rows that the filler writes for three ranges with no commitments: no range closes an
    /// epoch.
    const ALL_ROWS: [(u32, bool); 3] = [(0, false), (1_000, false), (2_000, false)];

    /// `entries` with `epoch_end` on the last entry, as the export writes them.
    fn closed(mut entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
        entries.last_mut().unwrap().epoch_end = true;
        entries
    }

    #[test]
    fn export_writes_content_addressed_ranges_a_manifest_and_latest() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, entries) = indexed(&dir, 3, &ALL_ROWS);

        let report = export(&index, &out, 0);

        let manifest_path = "manifests/2026-10-26T000000Z.json";
        assert_eq!(
            report,
            ExportReport {
                manifest: manifest_path.to_owned(),
                ranges: 3,
                written: 3,
                missing_rows: 0,
            }
        );
        for entry in &entries {
            let start = entry.start_height;
            let name = format!("{start}-{}.bin", hex::encode(&entry.blob_digest.0[..4]));
            assert_eq!(RangePath::from(entry).as_str(), format!("ranges/{name}"));
            let file = fs::read(out.path().join("ranges").join(name)).unwrap();
            assert_eq!(file, index.read_sealed_range_bytes(start).unwrap().unwrap());
        }

        let bytes = fs::read(out.path().join(manifest_path)).unwrap();
        let manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
        let expected =
            Manifest::new("Mainnet", Network::Mainnet.genesis_hash(), closed(entries.clone()))
                .unwrap();
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
        assert_eq!(
            latest,
            Latest {
                manifest: manifest_path.to_owned(),
                sealed_through: 2_999,
                published_at: "2026-10-26T00:00:00Z".to_owned(),
                public_key: signer().0.verifying_key().to_bytes(),
            }
        );
        assert!(!out.path().join("latest.json.sig").exists());
        assert!(out.path().join(format!("{manifest_path}.sig")).exists());
    }

    #[test]
    fn a_second_export_writes_no_range_file_again() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, entries) = indexed(&dir, 3, &ALL_ROWS);
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
        assert_eq!(
            fs::read(&short.0).unwrap(),
            index.read_sealed_range_bytes(1_000).unwrap().unwrap()
        );
        assert_eq!(report.manifest, "manifests/2026-10-27T000000Z.json");
        assert_eq!(fs::read_dir(out.path().join("ranges")).unwrap().count(), 3);
        // Two manifests, each with its signature.
        assert_eq!(fs::read_dir(out.path().join("manifests")).unwrap().count(), 4);

        // An export in the same second does not replace the manifest.
        let now = DateTime::from_timestamp(NOW + 86_400, 0).unwrap();
        let error =
            ExportReport::export(&index, &Network::Mainnet, now, &writer(&out)).unwrap_err();
        assert!(error.to_string().contains("never replaced"), "{error}");
    }

    #[test]
    fn signatures_verify_with_the_signing_key() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, _) = indexed(&dir, 3, &ALL_ROWS);
        let key = signer().0.verifying_key();

        let report = export(&index, &out, 0);

        let latest = fs::read(out.path().join(Latest::PATH)).unwrap();
        let latest = Latest::from_file(&latest, &key).unwrap();
        assert_eq!(latest.public_key, key.to_bytes());
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
    fn a_key_file_that_is_not_a_32_byte_seed_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        fs::write(&path, [7; 31]).unwrap();

        let error = Signer::from_file(&path).unwrap_err();

        assert!(error.to_string().contains("has 31 bytes"), "{error}");
    }

    #[test]
    fn export_stops_at_the_first_range_without_a_row() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, entries) = indexed(&dir, 3, &[(0, false), (2_000, false)]);

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
        let (index, entries) = indexed(&dir, 12, &rows);

        let report = export(&index, &out, 0);

        assert_eq!((report.ranges, report.missing_rows), (12, 0));
        let json = fs::read_to_string(out.path().join(report.manifest)).unwrap();
        let manifest = Manifest::from_json(&json).unwrap();
        assert_eq!(manifest.entries(), closed(entries));
        assert_eq!(manifest.sealed_through(), 11_999);
        let epochs: Vec<_> = manifest.epochs().map(|epoch| epoch.end_height()).collect();
        assert_eq!(epochs, [9_999, 11_999]);
        assert!(!index.range_meta(11_000).unwrap().unwrap().entry.epoch_end);
    }

    #[test]
    fn an_index_without_a_row_at_genesis_is_refused() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, _) = indexed(&dir, 3, &[(1_000, false)]);
        let now = DateTime::from_timestamp(NOW, 0).unwrap();

        let error =
            ExportReport::export(&index, &Network::Mainnet, now, &writer(&out)).unwrap_err();

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

        let error = ExportReport::export(&index, &network, now, &writer(&out)).unwrap_err();

        assert!(error.to_string().contains("no sealed range"), "{error}");
    }

    #[test]
    fn the_export_reads_the_chain_from_the_index() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, _) = indexed(&dir, 3, &ALL_ROWS);
        let args = ExportArgs { out: out.path().to_owned(), signing_key: key_file(dir.path()) };

        args.run(&index).unwrap();

        let json: Value =
            serde_json::from_slice(&fs::read(out.path().join(Latest::PATH)).unwrap()).unwrap();
        let manifest =
            fs::read_to_string(out.path().join(json["latest"]["manifest"].as_str().unwrap()));
        let manifest = Manifest::from_json(&manifest.unwrap()).unwrap();
        assert_eq!(manifest.network(), "Mainnet");
        assert_eq!(manifest.genesis_hash(), Network::Mainnet.genesis_hash());
    }

    #[test]
    fn an_export_without_a_readable_key_writes_nothing() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, _) = indexed(&dir, 3, &ALL_ROWS);
        let args =
            ExportArgs { out: out.path().to_owned(), signing_key: dir.path().join("missing") };

        let error = args.run(&index).unwrap_err();

        assert!(error.to_string().contains("read signing key"), "{error}");
        assert_eq!(fs::read_dir(out.path()).unwrap().count(), 0);
    }
}
