//! The source side of replication: what a ztreamer does so that replicas can import its index.
//!
//! [`RangeMetaFiller`] writes the `range_meta` row of each sealed range from the answers of a
//! node. [`ReplicatorService`] serves the sealed ranges that have a row to replicas over gRPC.
//! [`SnapshotExport`] exports them as a snapshot directory with
//! [`snapshot_writer::SnapshotWriter`]. The replica side is
//! `ztreamer-import`; the format both sides use is `ztreamer-snapshot`.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod range_meta_fill;
mod replicator;
mod snapshot_export;
pub mod snapshot_writer;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use range_meta_fill::RangeMetaFiller;
pub use replicator::{MAX_RANGE_STREAMS, ReplicatorService};
pub use snapshot_export::{SnapshotExport, SnapshotExportError};
