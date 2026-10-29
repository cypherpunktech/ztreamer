//! The replica side of replication: fetch the sealed ranges of a source, verify them against a
//! node, and install them in an index.
//!
//! A replica imports sealed ranges from a snapshot or a peer, not from a node. The source
//! publishes a [`Manifest`](ztreamer_snapshot::manifest::Manifest) with one entry per range. The
//! replica verifies the ranges in [epochs](ztreamer_snapshot::epoch::Epoch): the start frontier of
//! an epoch is the frontier that the previous epoch attested, each frontier in the manifest inside
//! the epoch is a checkpoint, and the node attests the end roots of the epoch with one call.
//!
//! The source side is `ztreamer-replicator`; the format both sides use is `ztreamer-snapshot`.
//! This crate does not depend on the wallet service.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod attest;
pub mod fetch;
pub mod importer;
pub mod proof;
pub mod range;
pub mod sample;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
