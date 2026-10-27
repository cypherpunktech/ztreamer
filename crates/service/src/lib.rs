//! Shared CompactTxStreamer behavior with gRPC and Zakura p2p transport adapters. The service
//! reads the chain state through a [`StateBackend`]: [`InProcessBackend`] for the embedded node,
//! or [`RemoteBackend`] for a remote node over JSON-RPC.
//!
//! The crate also has the Replicator gRPC service ([`ReplicatorService`]), which serves sealed
//! ranges to replicas, and the [`RangeMetaFiller`], which writes the `range_meta` rows.

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
