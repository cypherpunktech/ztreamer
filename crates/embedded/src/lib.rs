//! The embedded mode of Ztreamer: the in-process Zakura client, its block sources and its
//! [`StateBackend`](ztreamer_backend::StateBackend). The Zakura p2p transport of the wallet
//! service is `ztreamer_p2p`.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod backend;
mod client;
mod error;
mod finalized;
mod head;

pub use backend::InProcessBackend;
pub use client::NodeClient;
pub use error::NodeError;
pub use finalized::FinalizedDb;
