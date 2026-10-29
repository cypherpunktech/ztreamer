//! Polls both lifecycles in the caller's task and drains them before returning.

use std::future::Future;

use eyre::{Result, WrapErr, eyre};
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

pub(crate) async fn shutdown_signal() -> Result<()> {
    let mut terminate = signal(SignalKind::terminate()).wrap_err("SIGTERM handler failed")?;

    tokio::select! {
        result = tokio::signal::ctrl_c() => result.wrap_err("Ctrl-C handler failed"),
        _ = terminate.recv() => Ok(()),
    }
}

pub(crate) async fn supervise(
    node: impl Future<Output = Result<()>>,
    application: impl Future<Output = Result<()>>,
    shutdown: CancellationToken,
    signal: impl Future<Output = Result<()>>,
) -> Result<()> {
    let _cancel_on_drop = shutdown.clone().drop_guard();
    tokio::pin!(node, application);
    let mut node_result = None;
    let mut application_result = None;
    let mut signal_result = Ok(());

    tokio::select! {
        biased;
        // Register the process signal before polling potentially lengthy startup.
        result = signal => signal_result = result,
        result = &mut node => node_result = Some(result),
        result = &mut application => application_result = Some(result),
    }

    let node_exited_first = node_result.is_some();
    tracing::info!("shutting down node and application");
    shutdown.cancel();

    let (node_result, application_result) =
        drain_both((node, node_result), (application, application_result)).await;

    signal_result?;
    node_result?;
    application_result?;
    if node_exited_first {
        return Err(eyre!("Zakura stopped unexpectedly"));
    }
    Ok(())
}

/// Finishes two futures concurrently, reusing any result a `select!` already took.
///
/// Pair each future with the result it produced, if any. A completed future is
/// not polled again, because polling a completed future can panic. Both sides
/// are drained even if one failed, so writers and servers finish cleanup.
pub(crate) async fn drain_both<A: Future, B: Future>(
    (a, a_result): (A, Option<A::Output>),
    (b, b_result): (B, Option<B::Output>),
) -> (A::Output, B::Output) {
    tokio::join!(
        async {
            match a_result {
                Some(result) => result,
                None => a.await,
            }
        },
        async {
            match b_result {
                Some(result) => result,
                None => b.await,
            }
        },
    )
}

#[cfg(test)]
mod tests;
