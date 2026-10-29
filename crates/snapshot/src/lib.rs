//! The snapshot format: what a snapshot source publishes and a replica reads.
//!
//! - [`manifest_entry`]: what a manifest says about one sealed range,
//!   [`ManifestEntry`](manifest_entry::ManifestEntry), and its Replicator wire conversion.
//! - [`pool`]: the note commitment pools and their subtree and tree roots.
//! - [`epoch`]: the epoch rule, which groups the entries of a manifest into verification epochs.
//! - [`manifest`]: the JSON manifest of a snapshot.
//! - [`layout`]: the files of a snapshot directory and their signatures.
//!
//! The crate has no I/O. The source writes the files with `ztreamer_replicator::snapshot_writer`,
//! and the replica reads them with `ztreamer_import::fetch`.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod epoch;
pub mod layout;
pub mod manifest;
pub mod manifest_entry;
pub mod pool;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
