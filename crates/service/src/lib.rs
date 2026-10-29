//! Shared CompactTxStreamer behavior with a gRPC transport adapter. The service reads the chain
//! state through a [`StateBackend`](ztreamer_backend::StateBackend):
//! `ztreamer_node::InProcessBackend` for the embedded node, or
//! [`RemoteBackend`](remote::RemoteBackend) for a remote node over JSON-RPC. `ztreamer_node` also
//! has the Zakura p2p transport.

// Public items are not fully documented yet. Remove this once every `pub` item has a doc comment.
#![allow(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod compact;
mod grpc;
mod project;
pub mod remote;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use compact::{CompactService, NodeVersion};

mod telemetry;
