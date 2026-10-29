//! `ztreamerd export-snapshot`: writes the sealed ranges of an index as a snapshot that a replica
//! can import.
//!
//! `ztreamer_snapshot::layout` gives the layout of the snapshot directory. The manifest is
//! `manifests/<time>.json`, where `<time>` is UTC, for example `2026-10-26T143005Z`.
//!
//! The export reads the chain from the identity row of the index. [`SnapshotExport`] does the
//! export.

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use chrono::Utc;
use ed25519_dalek::SigningKey;
use eyre::{Result, WrapErr, ensure};
use zakura_chain::parameters::Network;
use zeroize::Zeroizing;
use ztreamer_index::index::Index;
use ztreamer_replicator::{SnapshotExport, snapshot_writer::SnapshotWriter};

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
        SnapshotExport::run(index, &network, Utc::now(), &writer)?.log();
        Ok(())
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
    use ztreamer_replicator::test_utils::index_with_rows;
    use ztreamer_snapshot::{layout::Latest, manifest::Manifest};

    use super::*;

    /// The rows that the filler writes for three ranges with no commitments.
    const ALL_ROWS: [(u32, bool); 3] = [(0, false), (1_000, false), (2_000, false)];

    /// Writes the test key to a key file in `dir`.
    fn key_file(dir: &Path) -> PathBuf {
        let path = dir.join("key");
        fs::write(&path, [7; 32]).unwrap();
        path
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
    fn the_export_reads_the_chain_from_the_index() {
        let (dir, out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (index, _) = index_with_rows(dir.path(), 3, &ALL_ROWS);
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
        let (index, _) = index_with_rows(dir.path(), 3, &ALL_ROWS);
        let args =
            ExportArgs { out: out.path().to_owned(), signing_key: dir.path().join("missing") };

        let error = args.run(&index).unwrap_err();

        assert!(error.to_string().contains("read signing key"), "{error}");
        assert_eq!(fs::read_dir(out.path()).unwrap().count(), 0);
    }
}
