//! Work on the Tokio blocking thread pool.
//!
//! [`BlockingTask`] is the join rule for the blocking work of the workspace. A panic in the
//! closure continues on the caller. A closure that the runtime does not run gives
//! [`BlockingTaskCancelled`]. `JoinError::into_panic` panics on a cancel, thus it gives no
//! error for the caller to return.
//!
//! The blocking pool cancels a closure that starts after the pool stopped. When a runtime
//! stops, a worker can still poll a task after the pool stopped. If that task starts a
//! `BlockingTask`, its join gives [`BlockingTaskCancelled`].

use std::panic;

use tokio::task::JoinHandle;

/// A closure that runs on the Tokio blocking thread pool.
///
/// ```
/// use ztreamer_indexer::blocking::{BlockingTask, BlockingTaskCancelled};
///
/// # async fn run() -> Result<(), BlockingTaskCancelled> {
/// let sum = BlockingTask::spawn(|| (1..=10).sum::<u32>()).join().await?;
/// assert_eq!(sum, 55);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct BlockingTask<T>(JoinHandle<T>);

/// The runtime did not run a [`BlockingTask`], because its blocking pool stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the blocking pool stopped before the blocking task ran")]
pub struct BlockingTaskCancelled;

impl<T: Send + 'static> BlockingTask<T> {
    /// Starts `work` on the blocking thread pool of the current runtime.
    ///
    /// # Panics
    ///
    /// Panics if the caller is not in the context of a Tokio runtime.
    pub fn spawn(work: impl FnOnce() -> T + Send + 'static) -> Self {
        Self(tokio::task::spawn_blocking(work))
    }

    /// Waits for the result of the closure. If the closure panicked, the panic continues on
    /// the caller.
    pub async fn join(self) -> Result<T, BlockingTaskCancelled> {
        self.0.await.map_err(|error| match error.try_into_panic() {
            Ok(payload) => panic::resume_unwind(payload),
            Err(_) => BlockingTaskCancelled,
        })
    }
}

impl<U: Send + 'static, E: From<BlockingTaskCancelled> + Send + 'static>
    BlockingTask<Result<U, E>>
{
    /// [`Self::join`] for a closure that returns a `Result`. A cancel goes into the error type of
    /// the closure.
    pub async fn join_flattened(self) -> Result<U, E> {
        self.join().await?
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::{Builder, Runtime};

    use super::*;

    fn current_thread_runtime() -> Runtime {
        Builder::new_current_thread().build().unwrap()
    }

    #[tokio::test]
    async fn join_gives_the_result_of_the_closure() {
        assert_eq!(BlockingTask::spawn(|| 7).join().await.unwrap(), 7);
    }

    #[tokio::test]
    #[should_panic(expected = "the closure panicked")]
    async fn a_panic_in_the_closure_continues_on_the_caller() {
        let _ = BlockingTask::spawn(|| panic!("the closure panicked")).join().await;
    }

    /// The blocking pool of `stopped` refuses the closure. A runtime that still polls its
    /// tasks gets an error, not a panic.
    #[test]
    fn a_closure_that_starts_after_the_pool_stopped_gives_cancelled() {
        let stopped = current_thread_runtime();
        let handle = stopped.handle().clone();
        stopped.shutdown_background();
        let task = {
            let _context = handle.enter();
            BlockingTask::spawn(|| ())
        };

        let caller = current_thread_runtime();
        assert_eq!(caller.block_on(task.join()).unwrap_err(), BlockingTaskCancelled);
    }

    #[test]
    fn join_flattened_puts_a_cancel_in_the_error_of_the_closure() {
        let stopped = current_thread_runtime();
        let handle = stopped.handle().clone();
        stopped.shutdown_background();
        let task = {
            let _context = handle.enter();
            BlockingTask::spawn(|| Ok::<_, BlockingTaskCancelled>(7))
        };

        let caller = current_thread_runtime();
        assert_eq!(caller.block_on(task.join_flattened()), Err(BlockingTaskCancelled));
        assert_eq!(caller.block_on(spawn_on(&caller, || Ok(7)).join_flattened()), Ok(7));
    }

    fn spawn_on<T: Send + 'static>(
        runtime: &Runtime,
        work: impl FnOnce() -> Result<T, BlockingTaskCancelled> + Send + 'static,
    ) -> BlockingTask<Result<T, BlockingTaskCancelled>> {
        let _context = runtime.enter();
        BlockingTask::spawn(work)
    }
}
