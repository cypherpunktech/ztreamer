//! The compact block model: the compact block and transaction types, the parser that builds
//! them from a Zakura block, their protobuf conversion, and the byte codecs of one block and of
//! a [`RANGE_SIZE`](codec::RANGE_SIZE)-block range.
//!
//! The crate has no I/O. The index stores the bytes of [`codec`], and the services read them.

// Public items are not fully documented yet. Remove this once every `pub` item has a doc comment.
#![allow(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod codec;
pub mod parser;
mod protobuf;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
pub mod types;

pub type Digest = [u8; 32];
pub type EphemeralKey = [u8; 32];
pub type Ciphertext = [u8; 52];
