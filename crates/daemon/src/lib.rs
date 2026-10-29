//! The ztreamer daemon: one node, an embedded Zakura node or a remote zakura node, through the
//! fixed stages of the daemon: boot, historical, catch up, and serve.
//!
//! `ztreamerd` makes a [`DaemonConfig`] from the command line and calls [`run_daemon`].

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod config;
mod daemon;
mod embedded;
mod index_pass;
mod launcher;
mod lifecycle;
mod metrics_endpoint;
mod node;
mod remote;
mod replicate;
mod tasks;
#[cfg(test)]
mod test_utils;

pub use config::{DaemonConfig, IndexConfig, Mode, ServerConfig, SyncConfig, TlsFiles};
pub use daemon::run_daemon;
pub use remote::RemoteConfig;
pub use replicate::ReplicaSource;
