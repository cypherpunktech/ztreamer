//! The chain-state seam: everything that ztreamer reads from a node and not from the compact
//! index.
//!
//! The compact index answers block and range requests. Everything else, from transparent address
//! lookups to tree states to the mempool, comes from a node. `ztreamer_service::CompactService`
//! and the `range_meta` filler of `ztreamer_replicator` read the node through a [`StateBackend`].
//! `ztreamer_embedded::InProcessBackend` reaches the embedded Zakura in-process, through its
//! `ReadStateService` and the mempool handle. `ztreamer_remote::RemoteBackend` reaches a
//! remote node over JSON-RPC.
//!
//! The trait speaks in `zakura_chain` types, not in `ReadResponse` variants and not in the wire
//! encodings of the light wallet protocol. The service does the wire conversion. A remote
//! backend parses the node's RPC answers into the same types, which also validates them.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod answers;
mod error;
mod state_backend;

pub use answers::{AddressUtxo, MinedTransaction, TreeState};
pub use error::BackendError;
pub use state_backend::{StateBackend, Subtrees};
