//! Verification of imported ranges against a snapshot manifest.
//!
//! A replica imports sealed ranges from a snapshot, not from a node. The snapshot publishes a
//! [`Manifest`](manifest::Manifest) with one entry per range. The replica verifies the ranges in
//! [epochs](epoch::Epoch): the start frontier of an epoch is the frontier that the previous epoch
//! attested, each frontier in the manifest inside the epoch is a checkpoint, and the node attests
//! the end roots of the epoch with one call.
//!
//! The crate also has the source side of replication. [`ReplicatorService`] serves the sealed
//! ranges of an index to replicas over gRPC. [`RangeMetaFiller`] writes the `range_meta` row of
//! each sealed range from the answers of a node.
//!
//! This crate depends on `ztreamer-indexer` and `ztreamer-service`. Neither depends on this
//! crate. The indexer has no serde.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod attest;
pub mod epoch;
pub mod fetch;
pub mod importer;
pub mod manifest;
pub mod range;
mod range_meta_fill;
mod replicator;
pub mod sample;
pub mod snapshot;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use range_meta_fill::RangeMetaFiller;
pub use replicator::{MAX_RANGE_STREAMS, ReplicatorService};
