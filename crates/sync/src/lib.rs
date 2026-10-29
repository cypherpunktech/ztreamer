//! Keep the index at the node tip: the parallel historical pass over a finalized source, and the
//! head follower, which follows the canonical head of a node, recovers reorgs and publishes the
//! serving snapshot.

// Public items are not fully documented yet. Remove this once every `pub` item has a doc comment.
#![allow(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod head;
pub mod pipeline;
