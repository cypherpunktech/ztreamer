//! `ztreamerd export-snapshot`: writes the sealed ranges of an index as a snapshot that a replica
//! can import.
//!
//! [`ztreamer_verify::snapshot`] gives the layout of the snapshot directory. The manifest is
//! `manifests/<time>.json`, where `<time>` is UTC, for example `2026-10-26T143005Z`.
//!
//! The export reads the chain from the identity row of the index. It starts at genesis and stops
//! at the first sealed range without a `range_meta` row. [`Manifest::from_contiguous_entries`]
//! makes the manifest of the rows: it sets `epoch_end` on the last entry, because the last range
//! of a manifest always closes an epoch. A peer source makes its manifest in the same way. The
//! row in the index keeps its flag.
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
use eyre::{Result, WrapErr, bail, ensure};
use tracing::{info, warn};
use zakura_chain::parameters::Network;
use zeroize::Zeroizing;
use ztreamer_chain::codec::RANGE_SIZE;
use ztreamer_indexer::{index::Index, manifest_entry::ManifestEntry};
use ztreamer_verify::{manifest::Manifest, snapshot_writer::SnapshotWriter};

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
        let Rows { entries, missing: missing_rows } = Rows::read(index)?;
        ensure!(!entries.is_empty(), "the index has no range_meta row for the range at genesis");
        let manifest =
            Manifest::from_contiguous_entries(network.to_string(), network.genesis_hash(), entries)
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
        let index_snapshot = index.latest()?;
        let Some(sealed_through) = index_snapshot.state().sealed_through() else {
            bail!("the index has no sealed range");
        };
        let sealed = sealed_through / RANGE_SIZE + 1;
        let entries = (0..sealed)
            .map_while(|range| index_snapshot.range_meta(range * RANGE_SIZE).transpose())
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
mod tests;
