//! The serving snapshot: the view of the index that ztreamer serves, and its readiness.
//!
//! [`HeadFollower`](super::HeadFollower) publishes one [`ServingSnapshot`] after each change of
//! the index or of the head source. The readers of the index serve at its generation.

use std::{sync::Arc, time::Instant};

use crate::{
    codec::CompactBlockRecord,
    index::{BlockId, IndexState},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingSnapshot {
    pub generation: u64,
    pub durable_tip: Option<BlockId>,
    pub visible_tip: Option<BlockId>,
    pub volatile_head: Arc<[CompactBlockRecord]>,
    pub ready: bool,
    pub tip_fresh: bool,
    pub last_source_success: Option<Instant>,
    pub source_error: Option<Arc<str>>,
}

impl From<IndexState> for ServingSnapshot {
    fn from(state: IndexState) -> Self {
        Self {
            generation: state.generation(),
            durable_tip: state.durable_tip(),
            visible_tip: state.durable_tip(),
            volatile_head: Arc::default(),
            ready: true,
            tip_fresh: false,
            last_source_success: None,
            source_error: None,
        }
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("volatile head does not connect to the durable index")]
pub struct SnapshotError;

/// A [`ServingSnapshot`] refuses reads while deep reorg recovery runs.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("deep reorg recovery is active")]
pub struct RecoveryActive;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Readiness {
    pub historical: bool,
    pub tip: bool,
    pub recovering: bool,
    pub source_error: Option<Arc<str>>,
}

impl ServingSnapshot {
    /// Returns the generation of the snapshot. Refuses with [`RecoveryActive`] while deep reorg
    /// recovery runs.
    pub const fn ready_generation(&self) -> Result<u64, RecoveryActive> {
        if self.ready { Ok(self.generation) } else { Err(RecoveryActive) }
    }

    /// The readiness that the snapshot gives.
    pub fn readiness(&self) -> Readiness {
        Readiness {
            historical: self.durable_tip.is_some(),
            tip: self.ready && self.tip_fresh,
            recovering: !self.ready,
            source_error: self.source_error.clone(),
        }
    }

    pub(crate) fn with_head(
        state: IndexState,
        volatile_head: Vec<CompactBlockRecord>,
    ) -> Result<Self, SnapshotError> {
        let mut expected_height = state.durable_tip().map_or(0, |tip| tip.height.saturating_add(1));
        let mut previous_hash = state.durable_tip().map(|tip| tip.hash);
        for block in &volatile_head {
            if block.height != expected_height
                || previous_hash.is_some_and(|hash| block.previous_hash != hash)
            {
                return Err(SnapshotError);
            }
            expected_height = expected_height.checked_add(1).ok_or(SnapshotError)?;
            previous_hash = Some(block.hash);
        }
        let visible_tip = volatile_head
            .last()
            .map(|block| BlockId::new(block.height, block.hash))
            .or(state.durable_tip());
        Ok(Self {
            generation: state.generation(),
            durable_tip: state.durable_tip(),
            visible_tip,
            volatile_head: volatile_head.into(),
            ready: true,
            tip_fresh: true,
            last_source_success: Some(Instant::now()),
            source_error: None,
        })
    }

    /// The block of the volatile head at `height`, if the volatile head has one.
    pub fn volatile(&self, height: u32) -> Option<&CompactBlockRecord> {
        self.volatile_head
            .binary_search_by_key(&height, |block| block.height)
            .ok()
            .map(|index| &self.volatile_head[index])
    }
}
