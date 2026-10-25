//! Compact block parsing, historical ingestion, LMDB storage, and canonical-head reconciliation.

// Public items are not fully documented yet. Remove this once every `pub` item has a doc comment.
#![allow(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod codec;
pub mod frontier;
pub mod head;
pub mod index;
pub mod ingest;
pub mod parser;
pub mod pipeline;
mod protobuf;
pub mod source;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
pub mod types;

pub type Digest = [u8; 32];
pub type EphemeralKey = [u8; 32];
pub type Ciphertext = [u8; 52];
