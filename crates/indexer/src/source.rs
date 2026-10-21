//! Repeats finalized historical passes until two consecutive Zakura tips agree.

use std::sync::atomic::{AtomicBool, Ordering};

use zakura_state::ZakuraDb;

use crate::{
    index::{Index, IndexState},
    pipeline::{HistoricalPipeline, PipelineConfig, PipelineError},
};

#[derive(Debug, thiserror::Error)]
pub enum ZakuraSyncError {
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
}

/// In-process access to Zakura's finalized database.
pub struct ZakuraSource {
    db: ZakuraDb,
}

impl std::fmt::Debug for ZakuraSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZakuraSource").finish_non_exhaustive()
    }
}

impl ZakuraSource {
    pub const fn new(db: ZakuraDb) -> Self {
        Self { db }
    }

    /// Starts the historical sync process, populating the index. When `stop` is set, the
    /// current pass completes early and the sync returns its state.
    pub fn sync(
        &self,
        index: &Index,
        pipeline_config: PipelineConfig,
        stop: &AtomicBool,
    ) -> Result<IndexState, ZakuraSyncError> {
        let pipeline = HistoricalPipeline::new(index, &self.db, pipeline_config)?.with_stop(stop);
        let mut previous_tip = None;
        loop {
            let state = pipeline.sync()?;
            if stop.load(Ordering::Relaxed) || previous_tip == Some(state.durable_tip()) {
                return Ok(state);
            }
            previous_tip = Some(state.durable_tip());
        }
    }
}
