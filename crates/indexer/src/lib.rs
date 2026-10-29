//! Historical ingestion, LMDB storage, and canonical-head reconciliation of compact blocks.

// Public items are not fully documented yet. Remove this once every `pub` item has a doc comment.
#![allow(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod head;
pub mod index;
pub mod ingest;
pub mod pipeline;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
