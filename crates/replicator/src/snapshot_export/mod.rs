//! The export of the sealed ranges of an index as a snapshot that a replica can import.
//!
//! The export starts at genesis and stops at the first sealed range without a `range_meta` row.
//! [`Manifest::from_contiguous_entries`] makes the manifest of the rows: it sets `epoch_end` on
//! the last entry, because the last range of a manifest always closes an epoch. A peer source
//! makes its manifest in the same way. The row in the index keeps its flag.
//!
//! [`SnapshotWriter`] writes the files. A daily export into the same directory writes only the
//! new ranges. It never replaces a manifest.

use chrono::{DateTime, Utc};
use tracing::{info, warn};
use zakura_chain::parameters::Network;
use ztreamer_chain::codec::RANGE_SIZE;
use ztreamer_index::index::{Index, IndexError};
use ztreamer_snapshot::{
    manifest::{Manifest, ManifestError},
    manifest_entry::ManifestEntry,
};

use crate::snapshot_writer::{SnapshotWriteError, SnapshotWriter};

/// Why an export stopped.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotExportError {
    /// The index read failed.
    #[error(transparent)]
    Index(#[from] IndexError),
    /// The index has no sealed range.
    #[error("the index has no sealed range")]
    NoSealedRange,
    /// The range at genesis has no `range_meta` row.
    #[error("the index has no range_meta row for the range at genesis")]
    NoGenesisRow,
    /// The rows do not make a valid manifest.
    #[error("the range_meta rows do not make a valid manifest")]
    Manifest(#[source] ManifestError),
    /// The write of the snapshot directory failed.
    #[error(transparent)]
    Write(#[from] SnapshotWriteError),
}

/// What one export wrote.
#[derive(Debug, Eq, PartialEq)]
pub struct SnapshotExport {
    /// The path of the manifest, relative to the snapshot directory.
    pub manifest: String,
    /// The ranges in the manifest.
    pub ranges: usize,
    /// The range files written. The other files were present with the correct digest.
    pub written: usize,
    /// The sealed ranges from the first range without a `range_meta` row to the sealed tip.
    pub missing_rows: u32,
}

impl SnapshotExport {
    /// Writes the `range_meta` entries of `index` from genesis as a snapshot with `writer`.
    /// Returns what the export wrote.
    pub fn run(
        index: &Index,
        network: &Network,
        now: DateTime<Utc>,
        writer: &SnapshotWriter,
    ) -> Result<Self, SnapshotExportError> {
        let Rows { entries, missing: missing_rows } = Rows::read(index)?;
        if entries.is_empty() {
            return Err(SnapshotExportError::NoGenesisRow);
        }
        let manifest =
            Manifest::from_contiguous_entries(network.to_string(), network.genesis_hash(), entries)
                .map_err(SnapshotExportError::Manifest)?;
        let exported = writer.export(index, &manifest, now)?;
        Ok(Self {
            manifest: exported.manifest,
            ranges: manifest.entries().len(),
            written: exported.written,
            missing_rows,
        })
    }

    /// Logs what the export wrote. It warns when the export stopped before the sealed tip.
    pub fn log(&self) {
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
    fn read(index: &Index) -> Result<Self, SnapshotExportError> {
        let index_snapshot = index.latest()?;
        let Some(sealed_through) = index_snapshot.state().sealed_through() else {
            return Err(SnapshotExportError::NoSealedRange);
        };
        let sealed = sealed_through / RANGE_SIZE + 1;
        let found =
            index_snapshot.range_meta_filled_through()?.map_or(0, |end| end / RANGE_SIZE + 1);
        let entries = index_snapshot
            .range_metas_from(0)?
            .into_iter()
            .take(found as usize)
            .map(|meta| meta.entry)
            .collect();
        Ok(Self { entries, missing: sealed - found })
    }
}

#[cfg(test)]
mod tests;
