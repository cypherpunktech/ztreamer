//! Sources over a remote zakura node, for a `ztreamerd` that runs without an embedded node.

mod backend;
mod head;

pub use backend::{RemoteBackend, RemoteConnectError, TipPoller};
pub use head::RemoteHeadSource;
