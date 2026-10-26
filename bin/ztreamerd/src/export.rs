//! `ztreamerd export-snapshot`: writes the sealed ranges of an index as a snapshot that a replica
//! can import.
//!
//! The layout of the snapshot directory:
//!
//! ```text
//! latest.json                         manifest path, sealed height, publish time, public key
//! latest.json.sig                     Ed25519 signature of latest.json, hex
//! manifests/<time>.json               one entry per sealed range; <time> is UTC, for example
//!                                     2026-10-26T143005Z
//! manifests/<time>.json.sig           Ed25519 signature of the manifest, hex
//! ranges/<start>-<digest prefix>.bin  range blob; the prefix is 8 hex digits of its BLAKE3 digest
//! ```
//!
//! The export starts at genesis and stops at the first sealed range without a `range_meta` row.
//! The last range of a manifest always closes an epoch, so the export sets `epoch_end` on the
//! last entry. The row in the index keeps its flag.
//! A range file that exists with the correct digest is not written again. Thus a daily export
//! writes only the new ranges. A manifest is never replaced: each export writes a manifest with
//! a new name, and `latest.json` points at the newest.

use std::{
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, SecondsFormat, Utc};
use ed25519_dalek::{Signer as _, SigningKey};
use eyre::{Result, WrapErr, bail, ensure, eyre};
use serde::{Deserialize, Serialize};
use serde_with::{hex::Hex, serde_as};
use tracing::{info, warn};
use zakura_chain::parameters::Network;
use ztreamer_indexer::index::{Index, RANGE_SIZE, range_meta::ManifestEntry};
use ztreamer_verify::manifest::Manifest;

use crate::DEFAULT_MAP_SIZE;

/// Bytes per write when a range blob goes to disk.
const WRITE_CHUNK: usize = 1024 * 1024;

/// Writes the sealed ranges of an index as a snapshot.
#[derive(Debug, clap::Args)]
pub(crate) struct ExportArgs {
    /// Directory containing the LMDB compact index.
    #[arg(long, value_name = "DIR")]
    index_dir: PathBuf,

    /// The chain of the index.
    #[arg(long, default_value = "Mainnet")]
    network: Network,

    /// Snapshot directory. An export into the same directory writes only the new ranges.
    #[arg(long, value_name = "DIR")]
    out: PathBuf,

    /// Ed25519 key as a raw 32-byte seed. Without it, the export writes no signatures.
    #[arg(long, value_name = "FILE")]
    signing_key: Option<PathBuf>,
}

impl ExportArgs {
    /// Exports the index and logs what the export wrote.
    pub(crate) fn run(self) -> Result<()> {
        // Read the key before the potentially lengthy export.
        let signer = self.signing_key.as_deref().map(Signer::from_file).transpose()?;
        if signer.is_none() {
            warn!("no --signing-key: the snapshot has no signatures, and a replica refuses it");
        }
        ensure!(
            self.index_dir.join("data.mdb").is_file(),
            "{} contains no index",
            self.index_dir.display()
        );
        let index = Index::open(
            &self.index_dir,
            DEFAULT_MAP_SIZE,
            &self.network.to_string(),
            self.network.genesis_hash(),
        )?;
        SnapshotDir::new(self.out)
            .export(&index, &self.network, Utc::now(), signer.as_ref())?
            .log();
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
    fn log(&self) {
        let Self { manifest, ranges, written, missing_rows } = self;
        info!(%manifest, ranges, written, "exported snapshot");
        if *missing_rows > 0 {
            warn!(
                missing_rows,
                "export stopped at the first sealed range without a range_meta row; \
                 run the daemon with --fill-range-meta"
            );
        }
    }
}

/// A snapshot directory.
#[derive(Debug)]
struct SnapshotDir {
    root: PathBuf,
}

impl SnapshotDir {
    const fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Writes the range files, then the manifest, then `latest.json`. Thus `latest.json` points
    /// only at files that are present. Signs the manifest and `latest.json` if `signer` is set.
    fn export(
        &self,
        index: &Index,
        network: &Network,
        now: DateTime<Utc>,
        signer: Option<&Signer>,
    ) -> Result<ExportReport> {
        let Rows { mut entries, missing: missing_rows } = Rows::read(index)?;
        let last = entries
            .last_mut()
            .ok_or_else(|| eyre!("the index has no range_meta row for the range at genesis"))?;
        // `ztreamer_verify::epoch::Epochs` closes an epoch at the last entry of a manifest.
        last.epoch_end = true;
        let manifest = Manifest::builder(network.to_string(), network.genesis_hash())
            .entries(entries)
            .build()
            .wrap_err("the range_meta rows do not make a valid manifest")?;
        let manifest_path = format!("manifests/{}.json", now.format("%Y-%m-%dT%H%M%SZ"));
        ensure!(
            !self.root.join(&manifest_path).exists(),
            "{manifest_path} exists, and a manifest is never replaced"
        );

        for dir in ["ranges", "manifests"] {
            fs::create_dir_all(self.root.join(dir))?;
        }
        let written = manifest
            .entries()
            .iter()
            .map(|entry| self.put_range(index, entry))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|&wrote| wrote)
            .count();

        self.put_signed(&manifest_path, &serde_json::to_vec(&manifest)?, signer)?;
        let latest = Latest {
            manifest: manifest_path.clone(),
            sealed_through: manifest.sealed_through(),
            published_at: now.to_rfc3339_opts(SecondsFormat::Secs, true),
            public_key: signer.map(Signer::public_key),
        };
        self.put_signed("latest.json", &serde_json::to_vec(&latest)?, signer)?;

        Ok(ExportReport {
            manifest: manifest_path,
            ranges: manifest.entries().len(),
            written,
            missing_rows,
        })
    }

    /// Writes the blob of `entry` to its content-addressed file, and checks it against the
    /// digest and the length in `entry`. Returns `false` if the file is present with the
    /// correct digest.
    fn put_range(&self, index: &Index, entry: &ManifestEntry) -> Result<bool> {
        let start = entry.start_height;
        let path = self
            .root
            .join(format!("ranges/{start}-{}.bin", hex::encode(&entry.blob_digest.0[..4])));
        if path.is_file() {
            let present = blake3::Hasher::new().update_reader(File::open(&path)?)?.finalize();
            if present == entry.blob_digest.0 {
                return Ok(false);
            }
            warn!(path = %path.display(), "range file has the wrong digest; writing it again");
        }
        let blob = index
            .read_sealed_range_bytes(start)?
            .ok_or_else(|| eyre!("sealed range {start} is not in the index"))?;
        let staged = Staged::write(path, &blob)?;
        if staged.digest != entry.blob_digest.0 || blob.len() as u64 != entry.blob_len {
            fs::remove_file(&staged.temp)?;
            bail!("the blob of sealed range {start} does not match its range_meta row");
        }
        staged.commit()?;
        Ok(true)
    }

    /// Writes `bytes` to `relative`, and its signature to `relative` + `.sig` if `signer` is set.
    fn put_signed(&self, relative: &str, bytes: &[u8], signer: Option<&Signer>) -> Result<()> {
        let path = self.root.join(relative);
        Staged::write(path.clone(), bytes)?.commit()?;
        if let Some(signer) = signer {
            Staged::write(Signer::signature_path(&path), signer.sign(bytes).as_bytes())?
                .commit()?;
        }
        Ok(())
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
        let sealed = index.state()?.sealed_through().map_or(0, |height| height / RANGE_SIZE + 1);
        let entries = (0..sealed)
            .map_while(|range| index.range_meta(range * RANGE_SIZE).transpose())
            .map(|meta| meta.map(|meta| meta.entry))
            .collect::<Result<Vec<_>, _>>()?;
        let found = u32::try_from(entries.len()).expect("at most `sealed` entries");
        Ok(Self { entries, missing: sealed - found })
    }
}

/// A file written beside its final path. It replaces the final path on [`Staged::commit`].
#[derive(Debug)]
struct Staged {
    temp: PathBuf,
    path: PathBuf,
    /// The BLAKE3 digest of the bytes written.
    digest: blake3::Hash,
}

impl Staged {
    /// Writes `bytes` in chunks to a temporary file beside `path`, and hashes each chunk as it
    /// goes to disk.
    fn write(path: PathBuf, bytes: &[u8]) -> io::Result<Self> {
        let mut temp = path.as_os_str().to_owned();
        temp.push(".tmp");
        let temp = PathBuf::from(temp);
        let mut file = File::create(&temp)?;
        let mut hasher = blake3::Hasher::new();
        for chunk in bytes.chunks(WRITE_CHUNK) {
            file.write_all(chunk)?;
            hasher.update(chunk);
        }
        file.sync_all()?;
        Ok(Self { temp, path, digest: hasher.finalize() })
    }

    fn commit(self) -> io::Result<()> {
        fs::rename(&self.temp, &self.path)
    }
}

/// The Ed25519 key that signs the manifest and `latest.json`.
#[derive(Debug)]
struct Signer(SigningKey);

impl Signer {
    /// Reads a key file that holds the raw 32-byte seed.
    fn from_file(path: &Path) -> Result<Self> {
        let bytes =
            fs::read(path).wrap_err_with(|| format!("read signing key {}", path.display()))?;
        let seed = <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| {
            eyre!("signing key {} has {} bytes, not a 32-byte seed", path.display(), bytes.len())
        })?;
        Ok(Self(SigningKey::from_bytes(&seed)))
    }

    /// The signature of `bytes` as a `.sig` file holds it: 64 bytes, hex.
    fn sign(&self, bytes: &[u8]) -> String {
        hex::encode(self.0.sign(bytes).to_bytes())
    }

    fn public_key(&self) -> [u8; 32] {
        self.0.verifying_key().to_bytes()
    }

    /// The path of the signature of the file at `path`: `path` with `.sig` appended.
    fn signature_path(path: &Path) -> PathBuf {
        let mut signature = path.as_os_str().to_owned();
        signature.push(".sig");
        PathBuf::from(signature)
    }
}

/// `latest.json`: the pointer to the current manifest.
#[serde_as]
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Latest {
    /// The path of the manifest, relative to the snapshot directory.
    manifest: String,
    /// The last height of the last range in the manifest.
    sealed_through: u32,
    /// The export time, RFC 3339 in UTC.
    published_at: String,
    /// The key that signed the manifest and this file. Absent if the export is not signed.
    #[serde_as(as = "Option<Hex>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    public_key: Option<[u8; 32]>,
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::{Signature, VerifyingKey};
    use serde_json::Value;
    use tempfile::TempDir;
    use zakura_chain::{orchard, subtree::NoteCommitmentSubtreeIndex};
    use ztreamer_indexer::{
        index::range_meta::{
            BlobDigest, Frontiers, RangeMeta, SubtreeNode, SubtreeRoot, VerifyState,
        },
        test_utils::{populate, record},
    };

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
                let last = &records[start as usize + 999];
                let entry = ManifestEntry {
                    range_version: 1,
                    start_height: start,
                    end_height: start + 999,
                    end_hash: last.hash,
                    end_tree_sizes: last.end_tree_sizes,
                    frontiers: Frontiers {
                        sapling: vec![0x01, 0xab, 0x00],
                        orchard: Vec::new(),
                        ironwood: Vec::new(),
                    },
                    subtree_roots: vec![SubtreeRoot {
                        index: NoteCommitmentSubtreeIndex(1),
                        root: SubtreeNode::Orchard(orchard::tree::Node::try_from([2; 32]).unwrap()),
                    }],
                    blob_digest: BlobDigest(*blake3::hash(&blob).as_bytes()),
                    blob_len: blob.len() as u64,
                    epoch_end,
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

    fn export(index: &Index, out: &TempDir, days: i64, signer: Option<&Signer>) -> ExportReport {
        let now = DateTime::from_timestamp(NOW + days * 86_400, 0).unwrap();
        SnapshotDir::new(out.path().to_owned())
            .export(index, &Network::Mainnet, now, signer)
            .unwrap()
    }

    fn signer(dir: &TempDir) -> Signer {
        let path = dir.path().join("key");
        fs::write(&path, [7; 32]).unwrap();
        Signer::from_file(&path).unwrap()
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

        let report = export(&index, &out, 0, None);

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
            let file = fs::read(out.path().join("ranges").join(name)).unwrap();
            assert_eq!(file, index.read_sealed_range_bytes(start).unwrap().unwrap());
        }

        let bytes = fs::read(out.path().join(manifest_path)).unwrap();
        let manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
        let expected = Manifest::builder("Mainnet", Network::Mainnet.genesis_hash())
            .entries(closed(entries.clone()))
            .build()
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

        let latest: Latest =
            serde_json::from_slice(&fs::read(out.path().join("latest.json")).unwrap()).unwrap();
        assert_eq!(
            latest,
            Latest {
                manifest: manifest_path.to_owned(),
                sealed_through: 2_999,
                published_at: "2026-10-26T00:00:00Z".to_owned(),
                public_key: None,
            }
        );
        assert!(!out.path().join("latest.json.sig").exists());
        assert!(!out.path().join(format!("{manifest_path}.sig")).exists());
    }

    #[test]
    fn the_written_manifest_passes_manifest_validation() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, entries) = indexed(&dir, 3, &ALL_ROWS);

        let report = export(&index, &out, 0, None);

        let json = fs::read_to_string(out.path().join(report.manifest)).unwrap();
        // `from_json` checks every rule of the manifest.
        let manifest = Manifest::from_json(&json).unwrap();
        assert_eq!(manifest.entries(), closed(entries));
    }

    #[test]
    fn a_second_export_writes_no_range_file_again() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, _) = indexed(&dir, 3, &ALL_ROWS);
        export(&index, &out, 0, None);

        let report = export(&index, &out, 1, None);

        assert_eq!((report.ranges, report.written), (3, 0));
        assert_eq!(report.manifest, "manifests/2026-10-27T000000Z.json");
        assert_eq!(fs::read_dir(out.path().join("ranges")).unwrap().count(), 3);
        assert_eq!(fs::read_dir(out.path().join("manifests")).unwrap().count(), 2);

        // An export in the same second does not replace the manifest.
        let now = DateTime::from_timestamp(NOW + 86_400, 0).unwrap();
        let error = SnapshotDir::new(out.path().to_owned())
            .export(&index, &Network::Mainnet, now, None)
            .unwrap_err();
        assert!(error.to_string().contains("never replaced"), "{error}");
    }

    #[test]
    fn signatures_verify_with_the_public_key_in_latest() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, _) = indexed(&dir, 3, &ALL_ROWS);
        let signer = signer(&dir);

        let report = export(&index, &out, 0, Some(&signer));

        let latest_bytes = fs::read(out.path().join("latest.json")).unwrap();
        let latest: Latest = serde_json::from_slice(&latest_bytes).unwrap();
        let key = VerifyingKey::from_bytes(&latest.public_key.unwrap()).unwrap();
        assert_eq!(key.to_bytes(), signer.public_key());
        for path in [out.path().join(report.manifest), out.path().join("latest.json")] {
            let signature = fs::read_to_string(Signer::signature_path(&path)).unwrap();
            let signature = <[u8; 64]>::try_from(hex::decode(signature).unwrap()).unwrap();
            let bytes = fs::read(&path).unwrap();
            key.verify_strict(&bytes, &Signature::from_bytes(&signature)).unwrap();
            assert!(key.verify_strict(b"other bytes", &Signature::from_bytes(&signature)).is_err());
        }
        assert_eq!(latest.manifest, "manifests/2026-10-26T000000Z.json");
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

        let report = export(&index, &out, 0, None);

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

        let report = export(&index, &out, 0, None);

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

        let error = SnapshotDir::new(out.path().to_owned())
            .export(&index, &Network::Mainnet, now, None)
            .unwrap_err();

        assert!(error.to_string().contains("no range_meta row"), "{error}");
        assert!(!out.path().join("latest.json").exists());
    }
}
