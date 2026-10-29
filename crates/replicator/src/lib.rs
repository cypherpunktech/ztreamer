//! The source side of replication: what a ztreamer does so that replicas can import its index.
//!
//! [`RangeMetaFiller`] writes the `range_meta` row of each sealed range from the answers of a
//! node. [`ReplicatorService`] serves the sealed ranges that have a row to replicas over gRPC.
//! [`snapshot_writer::SnapshotWriter`] exports them as a snapshot directory. The replica side is
//! `ztreamer-import`; the format both sides use is `ztreamer-snapshot`.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod range_meta_fill;
mod replicator;
pub mod snapshot_writer;

pub use range_meta_fill::RangeMetaFiller;
pub use replicator::{MAX_RANGE_STREAMS, ReplicatorService};
