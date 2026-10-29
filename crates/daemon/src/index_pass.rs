//! A pass of the block pipeline over finalized blocks. The embedded historical stage and the
//! catch-up stage of both modes run one.

use std::{
    ops::ControlFlow,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use eyre::{Result, WrapErr, eyre};
use tracing::info;
use ztreamer_index::index::IndexState;
use ztreamer_sync::pipeline::{FinalizedBlockSource, HistoricalPipeline};

use crate::{SyncConfig, node::StageContext};

/// A pass of the block pipeline on a blocking thread, until the source is stable.
pub(crate) struct IndexPass<S> {
    /// The stage of the pass. Only the logs and the errors use it.
    pub(crate) kind: PassKind,
    /// The finalized blocks.
    pub(crate) source: S,
}

impl<S: FinalizedBlockSource + Send + 'static> IndexPass<S> {
    /// Runs the pass on the index of `stage`. Breaks if shutdown stopped the pass, which is an
    /// error with `--index-only` because the index was the whole job.
    pub(crate) async fn run(self, stage: &StageContext<'_>) -> Result<ControlFlow<(), IndexState>> {
        let Self { kind, source } = self;
        let SyncConfig { pipeline, index_only } = stage.sync;
        let sync_index = Arc::clone(stage.index);
        kind.log_start();
        let started = Instant::now();
        let stop = Arc::new(AtomicBool::new(false));
        let mut pass = tokio::task::spawn_blocking({
            let stop = Arc::clone(&stop);
            move || {
                HistoricalPipeline::new(&sync_index, &source, pipeline)?
                    .with_stop(&stop)
                    .sync_until_stable()
            }
        });
        let state = tokio::select! {
            result = &mut pass => result.wrap_err("historical index task panicked")??,
            _ = stage.shutdown.cancelled() => {
                kind.log_stopping();
                stop.store(true, Ordering::Relaxed);
                let state = pass.await.wrap_err("historical index task panicked")??;
                kind.log_stopped(&state, started);
                if index_only {
                    return Err(eyre!(kind.stopped_by_shutdown()));
                }
                return Ok(ControlFlow::Break(()));
            }
        };
        kind.log_complete(&state, started);
        Ok(ControlFlow::Continue(state))
    }
}

/// The stage of an [`IndexPass`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum PassKind {
    /// The pass of the embedded historical stage.
    Historical,
    /// The pass of the catch-up stage.
    CatchUp,
}

impl PassKind {
    fn log_start(self) {
        match self {
            Self::Historical => info!("syncing historical compact index"),
            Self::CatchUp => info!("catching up the compact index to the finalized tip"),
        }
    }

    fn log_stopping(self) {
        match self {
            Self::Historical => info!("stopping the historical index pass before shutdown"),
            Self::CatchUp => info!("stopping the compact index catch-up before shutdown"),
        }
    }

    fn log_stopped(self, state: &IndexState, started: Instant) {
        let height = state.durable_tip().map(|tip| tip.height);
        let elapsed_seconds = started.elapsed().as_secs_f64();
        match self {
            Self::Historical => {
                info!(height, elapsed_seconds, "historical compact index stopped before shutdown")
            }
            Self::CatchUp => {
                info!(height, elapsed_seconds, "compact index catch-up stopped before shutdown")
            }
        }
    }

    fn log_complete(self, state: &IndexState, started: Instant) {
        let height = state.durable_tip().map(|tip| tip.height);
        let elapsed_seconds = started.elapsed().as_secs_f64();
        match self {
            Self::Historical => {
                info!(height, elapsed_seconds, "historical compact index complete")
            }
            Self::CatchUp => {
                info!(height, elapsed_seconds, "compact index caught up to the finalized tip")
            }
        }
    }

    /// The error when shutdown stops the pass with `--index-only`.
    const fn stopped_by_shutdown(self) -> &'static str {
        match self {
            Self::Historical => "historical compact index stopped by shutdown before it completed",
            Self::CatchUp => "compact index catch-up stopped by shutdown before it completed",
        }
    }
}
