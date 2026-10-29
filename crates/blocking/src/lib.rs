//! Work on the Tokio blocking thread pool.
//!
//! [`BlockingTask`] is the join rule for blocking work that must keep a panic of the closure.
//! A panic in the closure continues on the caller. A closure that the runtime does not run gives
//! [`BlockingTaskCancelled`]. Other `spawn_blocking` sites of the workspace still join with
//! their own rule. `JoinError::into_panic` panics on a cancel, thus it gives no
//! error for the caller to return.
//!
//! The blocking pool cancels a closure that starts after the pool stopped. When a runtime
//! stops, a worker can still poll a task after the pool stopped. If that task starts a
//! `BlockingTask`, its join gives [`BlockingTaskCancelled`].

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod blocking_task;

pub use blocking_task::{BlockingTask, BlockingTaskCancelled};
