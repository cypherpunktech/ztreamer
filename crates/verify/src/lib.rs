//! Verification of imported ranges against a snapshot manifest.
//!
//! A replica imports sealed ranges from a snapshot, not from a node. The snapshot publishes a
//! [`Manifest`](manifest::Manifest) with one entry per range. The replica verifies the ranges in
//! [epochs](epoch::Epoch): the start frontier of an epoch is the frontier that the previous epoch
//! attested, each frontier in the manifest inside the epoch is a checkpoint, and the node attests
//! the end roots of the epoch with one call.
//!
//! This crate depends on `ztreamer-indexer`. The indexer does not depend on this crate, and it
//! has no serde.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod attest;
pub mod epoch;
pub mod fetch;
pub mod importer;
pub mod manifest;
pub mod range;
pub mod sample;
pub mod snapshot;
#[cfg(test)]
mod test_utils;
