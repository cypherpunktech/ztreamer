//! LMDB storage of compact blocks: the index tables, its reads and writes, and the ordered write
//! batches that fill it.

// Public items are not fully documented yet. Remove this once every `pub` item has a doc comment.
#![allow(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod index;
pub mod ingest;
mod metrics;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
