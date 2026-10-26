//! Shared CompactTxStreamer behavior with gRPC and Zakura p2p transport adapters.

// Public items are not fully documented yet. Remove this once every `pub` item has a doc comment.
#![allow(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod backend;
pub mod grpc;
pub mod p2p;
mod project;
pub mod range_meta_fill;
pub mod remote;
pub mod replicate;
mod service;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use backend::{BackendError, InProcessBackend, StateBackend};
pub use range_meta_fill::RangeMetaFiller;
pub use remote::RemoteBackend;
pub use replicate::ReplicatorService;
pub use service::{
    CompactService, HeadFollowerConfig, HeadFollowerError, Readiness, ServingSnapshot,
    SnapshotError,
};

mod telemetry;
