//! The process signal, and the [`Supervisor`]: it runs the main work of the daemon in the
//! caller's task and the tasks of a [`TaskSet`] on the runtime. On a signal, at the end of the
//! main work, or at the first stop of a task, it stops all of them and drains them before it
//! returns.

use std::{
    collections::HashMap,
    pin::{Pin, pin},
};

use eyre::{Result, WrapErr, eyre};
use tokio::{
    runtime::Handle,
    signal::unix::{SignalKind, signal},
    sync::mpsc,
    task::{Id, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::info;

/// The tasks of the daemon that run until shutdown. A clone adds to the same set. Each task
/// must stop on [`Self::shutdown`].
///
/// A task that the [`Supervisor`] has not started when it stops never starts.
#[derive(Clone)]
pub(crate) struct TaskSet {
    added: mpsc::UnboundedSender<Task>,
    shutdown: CancellationToken,
}

impl TaskSet {
    /// The token that tells each task and the main work to stop.
    pub(crate) const fn shutdown(&self) -> &CancellationToken {
        &self.shutdown
    }

    /// Adds `task` on the runtime. A panic of the task is the error "`name` task panicked". A
    /// stop before shutdown is the error `unexpected_stop`, unless a task failed.
    pub(crate) fn spawn(
        &self,
        name: &'static str,
        unexpected_stop: &'static str,
        task: impl Future<Output = Result<()>> + Send + 'static,
    ) {
        self.add(name, unexpected_stop, Work::Async(Box::pin(task)));
    }

    /// Adds the future that `task` makes on a thread of the blocking pool, for a future that is
    /// not `Send`. The future holds the thread until it completes. The tasks that it spawns run
    /// on the runtime. The errors are the same as for [`Self::spawn`].
    pub(crate) fn spawn_on_blocking_thread<F: Future<Output = Result<()>>>(
        &self,
        name: &'static str,
        unexpected_stop: &'static str,
        task: impl FnOnce() -> F + Send + 'static,
    ) {
        let work = move || Handle::current().block_on(task());
        self.add(name, unexpected_stop, Work::Blocking(Box::new(work)));
    }

    fn add(&self, name: &'static str, unexpected_stop: &'static str, work: Work) {
        // The send fails only after the supervisor stopped: then the task must not start.
        let _ = self.added.send(Task { name, unexpected_stop, work });
    }
}

/// Runs the main work of the daemon and the tasks of a [`TaskSet`] until a signal, the end of
/// the main work, or the first stop of a task. Then it stops all of them and drains each one
/// that still runs, also when one failed. Thus writers and servers finish their cleanup.
pub(crate) struct Supervisor {
    tasks: TaskSet,
    added: mpsc::UnboundedReceiver<Task>,
}

impl Supervisor {
    /// A supervisor that cancels `shutdown` to stop the main work and the tasks.
    pub(crate) fn new(shutdown: CancellationToken) -> Self {
        let (added_tx, added) = mpsc::unbounded_channel();
        Self { tasks: TaskSet { added: added_tx, shutdown }, added }
    }

    /// The set that the tasks of this supervisor join.
    pub(crate) const fn tasks(&self) -> &TaskSet {
        &self.tasks
    }

    /// Runs `main` in the caller's task, and the tasks of [`Self::tasks`] on the runtime, until
    /// `signal`, the end of `main`, or the first stop of a task. Then logs the shutdown, cancels
    /// the shutdown token, and drains `main` and the tasks. A drop of the returned future also
    /// cancels the shutdown token.
    ///
    /// It polls `signal` first: a process signal registers before potentially lengthy startup.
    ///
    /// The result is the first error in this order: `signal`, each task in the order that it
    /// started, then `main`. With no error, it is the `unexpected_stop` of the task that stopped
    /// first, if a task stopped first.
    pub(crate) async fn run(
        self,
        signal: impl Future<Output = Result<()>>,
        main: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        let Self { tasks: TaskSet { shutdown, .. }, mut added } = self;
        let _cancel_on_drop = shutdown.clone().drop_guard();
        let mut running = Running::default();
        let mut signal = pin!(signal);
        let mut main = pin!(main);

        let (signal_result, main_result, unexpected_stop) = loop {
            let event = tokio::select! {
                biased;
                result = &mut signal => Event::Signal(result),
                Some(task) = added.recv() => Event::Added(task),
                Some(unexpected_stop) = running.join_next() => Event::Stopped(unexpected_stop),
                result = &mut main => Event::MainDone(result),
            };
            match event {
                Event::Added(task) => running.start(task),
                Event::Signal(result) => break (result, None, None),
                Event::Stopped(unexpected_stop) => break (Ok(()), None, Some(unexpected_stop)),
                Event::MainDone(result) => break (Ok(()), Some(result), None),
            }
        };

        info!("shutting down node and application");
        shutdown.cancel();
        // Refuse new tasks, and drop the tasks that have not started.
        drop(added);
        let main = async {
            match main_result {
                Some(result) => result,
                None => main.await,
            }
        };
        let (main_result, ()) = tokio::join!(main, running.drain());

        signal_result?;
        running.into_result()?;
        main_result?;
        unexpected_stop.map_or(Ok(()), |message| Err(eyre!(message)))
    }
}

/// A task of a [`TaskSet`] that the [`Supervisor`] has not started yet.
struct Task {
    name: &'static str,
    unexpected_stop: &'static str,
    work: Work,
}

/// The work of a [`Task`].
enum Work {
    /// A future for the runtime.
    Async(Pin<Box<dyn Future<Output = Result<()>> + Send>>),
    /// A function for a thread of the blocking pool.
    Blocking(Box<dyn FnOnce() -> Result<()> + Send>),
}

/// What the [`Supervisor`] waits for.
enum Event {
    Signal(Result<()>),
    Added(Task),
    /// A task stopped. It has this unexpected stop.
    Stopped(&'static str),
    MainDone(Result<()>),
}

/// The tasks that the [`Supervisor`] started, and the result of each one that stopped.
#[derive(Default)]
struct Running {
    set: JoinSet<Result<()>>,
    /// The tasks that still run, by id.
    started: HashMap<Id, Started>,
    /// The result of each task in the order that it started. `None` while the task runs.
    results: Vec<Option<Result<()>>>,
}

impl Running {
    fn start(&mut self, task: Task) {
        let Task { name, unexpected_stop, work } = task;
        let handle = match work {
            Work::Async(future) => self.set.spawn(future),
            Work::Blocking(function) => self.set.spawn_blocking(function),
        };
        let started = Started { place: self.results.len(), name, unexpected_stop };
        self.started.insert(handle.id(), started);
        self.results.push(None);
    }

    /// Waits until a task stops, and keeps its result. Gives the unexpected stop of the task, or
    /// `None` when no task runs.
    async fn join_next(&mut self) -> Option<&'static str> {
        let (id, result) = match self.set.join_next_with_id().await? {
            Ok((id, result)) => (id, Ok(result)),
            Err(error) => (error.id(), Err(error)),
        };
        let Started { place, name, unexpected_stop } =
            self.started.remove(&id).expect("the supervisor starts each task of its set");
        self.results[place] =
            Some(result.wrap_err_with(|| format!("{name} task panicked")).flatten());
        Some(unexpected_stop)
    }

    /// Waits until each task has stopped.
    async fn drain(&mut self) {
        while self.join_next().await.is_some() {}
    }

    /// The first error of the tasks in the order that they started. Call it only after
    /// [`Self::drain`].
    fn into_result(self) -> Result<()> {
        self.results
            .into_iter()
            .try_for_each(|result| result.expect("the supervisor drains each task"))
    }
}

/// A task that runs, and its place in the start order.
struct Started {
    place: usize,
    name: &'static str,
    unexpected_stop: &'static str,
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
