//! Head follower metrics: the published snapshot, the head source tip, and the time of each
//! reconciliation.

use std::time::Instant;

use metrics::{Gauge, Histogram};
use zakura_chain::block;

use super::{CanonicalBlockSource, HeadError, ServingSnapshot};
use crate::index::BlockId;
use ztreamer_chain::types::RawIndexBlock;

pub(crate) struct HeadMetrics {
    visible: Gauge,
    ready: Gauge,
    fresh: Gauge,
    source_tip: Gauge,
    publication: Histogram,
    pub head_success: Histogram,
    pub head_error: Histogram,
}

impl HeadMetrics {
    pub(crate) fn new() -> Self {
        Self {
            visible: metrics::gauge!("ztreamer.head.visible.height"),
            ready: metrics::gauge!("ztreamer.head.ready"),
            fresh: metrics::gauge!("ztreamer.head.fresh"),
            source_tip: metrics::gauge!("ztreamer.head.source.height"),
            publication: metrics::histogram!("ztreamer.head.publication.seconds"),
            head_success: metrics::histogram!("ztreamer.head.reconcile.seconds", "outcome" => "success"),
            head_error: metrics::histogram!("ztreamer.head.reconcile.seconds", "outcome" => "error"),
        }
    }

    pub(crate) fn snapshot(&self, snapshot: &ServingSnapshot) {
        self.visible.set(snapshot.visible_tip.map_or(f64::NAN, |tip| f64::from(tip.height)));
        self.ready.set(f64::from(snapshot.ready));
        self.fresh.set(f64::from(snapshot.tip_fresh));
    }

    pub(crate) fn published(&self, observed: Option<(BlockId, Instant)>, visible: Option<BlockId>) {
        if let Some((tip, at)) = observed
            && visible == Some(tip)
        {
            self.publication.record(at.elapsed().as_secs_f64());
        }
    }
}

/// Observe existing tip calls without adding source reads to reconciliation.
pub(crate) struct ObservedSource<'a, S> {
    pub source: &'a mut S,
    pub previous: Option<BlockId>,
    pub observed: Option<(BlockId, Instant)>,
    pub metrics: &'a HeadMetrics,
}

#[tonic::async_trait]
impl<S: CanonicalBlockSource> CanonicalBlockSource for ObservedSource<'_, S> {
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        let tip = self.source.tip().await?;
        self.metrics.source_tip.set(tip.map_or(f64::NAN, |tip| f64::from(tip.height)));
        if tip != self.previous {
            self.observed = tip.map(|tip| (tip, Instant::now()));
        }
        Ok(tip)
    }

    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        self.source.block(height).await
    }

    async fn block_hash(&mut self, height: u32) -> Result<Option<block::Hash>, HeadError> {
        self.source.block_hash(height).await
    }
}
