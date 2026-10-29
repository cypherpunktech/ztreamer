//! The process signal, and the [`Supervisor`] that runs the tasks of the daemon in the caller's
//! task and drains them before it returns.

use std::{
    future::{Future, poll_fn},
    pin::{Pin, pin},
    task::{Context, Poll},
};

use eyre::{Result, WrapErr, eyre};
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing::info;

/// The tasks of a [`Supervisor`]: `()` for no task, or `(earlier, task)` for the earlier tasks
/// and the newest task. The tasks keep their types. Thus the future of [`Supervisor::run`] is
/// `Send` when each task is `Send`, and a task needs no `Send` bound.
///
/// Each method takes the tasks in the order that they were added: the earlier tasks first.
pub(crate) trait Tasks {
    /// Polls each task, and stops at the first task that completes.
    fn poll_first(&mut self, cx: &mut Context<'_>) -> Poll<()>;

    /// Polls each task that still runs. Ready when all tasks have stopped.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<()>;

    /// The unexpected stop of the task that has stopped, if it must run until shutdown.
    fn unexpected_stop(&self) -> Option<&'static str>;

    /// The first error of the tasks. Call it only after [`Self::poll_drain`] is ready.
    fn into_result(self) -> Result<()>;
}

impl Tasks for () {
    fn poll_first(&mut self, _: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }

    fn poll_drain(&mut self, _: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }

    fn unexpected_stop(&self) -> Option<&'static str> {
        None
    }

    fn into_result(self) -> Result<()> {
        Ok(())
    }
}

impl<T: Tasks, F: Future<Output = Result<()>>> Tasks for (T, Supervised<F>) {
    fn poll_first(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let (earlier, task) = self;
        if earlier.poll_first(cx).is_ready() {
            return Poll::Ready(());
        }
        task.poll(cx)
    }

    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let (earlier, task) = self;
        let earlier = earlier.poll_drain(cx);
        let task = if task.result.is_some() { Poll::Ready(()) } else { task.poll(cx) };
        if earlier.is_ready() && task.is_ready() { Poll::Ready(()) } else { Poll::Pending }
    }

    fn unexpected_stop(&self) -> Option<&'static str> {
        let (earlier, task) = self;
        earlier.unexpected_stop().or(task.result.as_ref().and(task.unexpected_stop))
    }

    fn into_result(self) -> Result<()> {
        let (earlier, task) = self;
        earlier.into_result()?;
        task.result.expect("the supervisor drains each task")
    }
}

/// Runs a set of tasks in the caller's task until a trigger completes or the first task stops.
/// Then it stops all tasks and drains each task that still runs, also when a task failed. Thus
/// writers and servers finish their cleanup.
///
/// The supervisor polls the trigger first, then the tasks in the order that they were added. It
/// never polls a completed task again.
///
/// The result is the first error in this order: the trigger, then each task in the order that
/// it was added. With no error, it is the error of a task that must run until shutdown and
/// stopped first.
pub(crate) struct Supervisor<T>(T);

impl Supervisor<()> {
    pub(crate) const fn new() -> Self {
        Self(())
    }
}

impl<T: Tasks> Supervisor<T> {
    /// Adds a task that can complete its work. When it stops first, the supervisor stops the
    /// other tasks, and the stop is no error.
    pub(crate) fn finite<F: Future<Output = Result<()>>>(
        self,
        task: F,
    ) -> Supervisor<(T, Supervised<F>)> {
        self.with(task, None)
    }

    /// Adds a task that must run until shutdown. When it stops first, the result is the error
    /// `unexpected_stop`, unless a task failed.
    pub(crate) fn until_shutdown<F: Future<Output = Result<()>>>(
        self,
        task: F,
        unexpected_stop: &'static str,
    ) -> Supervisor<(T, Supervised<F>)> {
        self.with(task, Some(unexpected_stop))
    }

    fn with<F: Future<Output = Result<()>>>(
        self,
        task: F,
        unexpected_stop: Option<&'static str>,
    ) -> Supervisor<(T, Supervised<F>)> {
        let Self(earlier) = self;
        Supervisor((earlier, Supervised { future: Box::pin(task), unexpected_stop, result: None }))
    }

    /// Runs the tasks until `signal` or the first stop. Then logs the shutdown, cancels
    /// `shutdown` and drains the tasks. A drop of the returned future also cancels `shutdown`.
    pub(crate) async fn run_until_signal(
        self,
        signal: impl Future<Output = Result<()>>,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let _cancel_on_drop = shutdown.clone().drop_guard();
        self.run(signal, || {
            info!("shutting down node and application");
            shutdown.cancel();
        })
        .await
    }

    /// Runs the tasks until `trigger` or the first stop. Then calls `stop`, which must make each
    /// task stop, and drains the tasks.
    pub(crate) async fn run(
        self,
        trigger: impl Future<Output = Result<()>>,
        stop: impl FnOnce(),
    ) -> Result<()> {
        let Self(mut tasks) = self;
        let mut trigger = pin!(trigger);
        // Poll the trigger first: a process signal registers before potentially lengthy
        // startup.
        let trigger_result = poll_fn(|cx| match trigger.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => tasks.poll_first(cx).map(Ok),
        })
        .await;
        let unexpected_stop = tasks.unexpected_stop();

        stop();
        poll_fn(|cx| tasks.poll_drain(cx)).await;

        trigger_result?;
        tasks.into_result()?;
        unexpected_stop.map_or(Ok(()), |message| Err(eyre!(message)))
    }
}

/// A task of a [`Supervisor`], and its result once it has stopped.
pub(crate) struct Supervised<F> {
    future: Pin<Box<F>>,
    /// The error when the task stops first. `None` for a task that can complete its work.
    unexpected_stop: Option<&'static str>,
    result: Option<Result<()>>,
}

impl<F: Future<Output = Result<()>>> Supervised<F> {
    /// Polls the task, and keeps its result when it completes. Call it only while the task
    /// runs.
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx).map(|result| self.result = Some(result))
    }
}

pub(crate) async fn shutdown_signal() -> Result<()> {
    let mut terminate = signal(SignalKind::terminate()).wrap_err("SIGTERM handler failed")?;

    tokio::select! {
        result = tokio::signal::ctrl_c() => result.wrap_err("Ctrl-C handler failed"),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(test)]
mod tests;
