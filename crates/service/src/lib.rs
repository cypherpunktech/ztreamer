//! Shared CompactTxStreamer behavior with a gRPC transport adapter. The service reads the chain
//! state through a [`StateBackend`]: `ztreamer_node::InProcessBackend` for the embedded node, or
//! [`RemoteBackend`] for a remote node over JSON-RPC. `ztreamer_node` also has the Zakura p2p
//! transport.
//!
//! The crate also has the Replicator gRPC service ([`ReplicatorService`]), which serves sealed
//! ranges to replicas, and the [`RangeMetaFiller`], which writes the `range_meta` rows.

// Public items are not fully documented yet. Remove this once every `pub` item has a doc comment.
#![allow(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod backend;
pub mod grpc;
mod project;
mod range_meta_fill;
pub mod remote;
mod replicate;
mod service;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use backend::{BackendError, StateBackend};
pub use range_meta_fill::RangeMetaFiller;
pub use remote::RemoteBackend;
pub use replicate::{MAX_RANGE_STREAMS, ReplicatorService};
pub use service::{
    CompactService, HeadFollowerConfig, HeadFollowerError, NodeVersion, Readiness, RpcStream,
    ServingSnapshot, SnapshotError,
};

mod telemetry;
