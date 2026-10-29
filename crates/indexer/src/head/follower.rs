//! The head follower: it keeps the index at the canonical head of a node, and publishes a
//! [`ServingSnapshot`] after each change.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::watch;

use super::{
    CanonicalBlockSource, HeadSyncError, recover_deep_reorg,
    serving::{Readiness, ServingSnapshot, SnapshotError},
    sync_head_once,
    telemetry::{HeadMetrics, ObservedSource},
};
use crate::{
    codec::CompactBlockRecord,
    index::{Index, IndexState},
    pipeline::PipelineConfig,
};

#[derive(Clone, Copy, Debug)]
pub struct HeadFollowerConfig {
    pub poll_interval: Duration,
    pub attempt_timeout: Duration,
    pub freshness_timeout: Duration,
}

impl Default for HeadFollowerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            attempt_timeout: Duration::from_secs(30),
            freshness_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum HeadFollowerError {
    #[error("head follower durations are invalid")]
    Config,
}

/// Keeps the index at the canonical head of a node, and publishes a [`ServingSnapshot`] after
/// each change. The wallet service and the Replicator read the snapshots through
/// [`HeadFollower::subscribe`].
///
/// Clones share the index, the snapshot channel and the metrics.
#[derive(Clone)]
pub struct HeadFollower {
    index: Arc<Index>,
    snapshot: watch::Sender<ServingSnapshot>,
    metrics: Arc<HeadMetrics>,
}

impl std::fmt::Debug for HeadFollower {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeadFollower")
            .field("snapshot", &*self.snapshot.borrow())
            .finish_non_exhaustive()
    }
}

impl HeadFollower {
    /// A follower over `index` that publishes the snapshot of `state` first.
    pub fn new(index: Arc<Index>, state: IndexState) -> Self {
        let (snapshot, _) = watch::channel(state.into());
        let metrics = Arc::new(HeadMetrics::new());
        metrics.snapshot(&snapshot.borrow());
        Self { index, snapshot, metrics }
    }

    /// Returns a receiver for each serving snapshot that the follower publishes after this call.
    pub fn subscribe(&self) -> watch::Receiver<ServingSnapshot> {
        self.snapshot.subscribe()
    }

    /// Publishes the snapshot of `state` with `volatile_head` above its durable tip.
    pub fn publish_head(
        &self,
        state: IndexState,
        volatile_head: Vec<CompactBlockRecord>,
    ) -> Result<(), SnapshotError> {
        let snapshot = ServingSnapshot::with_head(state, volatile_head)?;
        self.metrics.snapshot(&snapshot);
        self.snapshot.send_replace(snapshot);
        Ok(())
    }

    /// Stops new chain-data requests while a deep replacement is staged.
    pub fn begin_recovery(&self) {
        let mut snapshot = self.snapshot();
        snapshot.ready = false;
        snapshot.tip_fresh = false;
        self.metrics.snapshot(&snapshot);
        self.snapshot.send_replace(snapshot);
    }

    /// The readiness of the current snapshot.
    pub fn readiness(&self) -> Readiness {
        self.snapshot().readiness()
    }

    /// Records a failed attempt of the head source. The tip stays fresh while the last success
    /// is younger than `freshness_timeout`.
    pub fn mark_source_failure(&self, error: String, freshness_timeout: Duration) {
        let mut snapshot = self.snapshot();
        snapshot.tip_fresh =
            snapshot.last_source_success.is_some_and(|last| last.elapsed() < freshness_timeout);
        snapshot.source_error = Some(error.into());
        self.metrics.snapshot(&snapshot);
        self.snapshot.send_replace(snapshot);
    }

    /// The current snapshot.
    pub fn snapshot(&self) -> ServingSnapshot {
        self.snapshot.borrow().clone()
    }

    /// Reconciles one head view and fails closed when it detects a deep reorg.
    pub async fn sync_head(
        &self,
        source: &mut impl CanonicalBlockSource,
        config: PipelineConfig,
    ) -> Result<IndexState, HeadSyncError> {
        let started = Instant::now();
        let snapshot = self.snapshot();
        let mut observed = ObservedSource {
            source,
            previous: snapshot.visible_tip,
            observed: None,
            metrics: &self.metrics,
        };
        let result =
            sync_head_once(&self.index, &mut observed, &snapshot.volatile_head, config).await;
        if matches!(result, Err(HeadSyncError::DeepReorg { .. })) {
            self.begin_recovery();
        }
        let (state, head) = result.inspect_err(|_| {
            self.metrics.head_error.record(started.elapsed().as_secs_f64());
        })?;
        self.publish_head(state, head)
            .expect("head reconciliation must produce a connected snapshot");
        self.metrics.published(observed.observed, self.snapshot().visible_tip);
        self.metrics.head_success.record(started.elapsed().as_secs_f64());
        Ok(state)
    }

    /// Stages and publishes a deep replacement while new requests remain disabled.
    pub async fn recover_deep_reorg(
        &self,
        source: &mut impl CanonicalBlockSource,
        config: PipelineConfig,
    ) -> Result<IndexState, HeadSyncError> {
        self.begin_recovery();
        let snapshot = self.snapshot();
        let (state, head) =
            recover_deep_reorg(&self.index, source, &snapshot.volatile_head, config).await?;
        self.publish_head(state, head).expect("deep recovery must produce a connected snapshot");
        Ok(state)
    }

    /// Polls Zakura until `shutdown` becomes true. Source errors are retried.
    pub async fn follow_head(
        &self,
        mut source: impl CanonicalBlockSource,
        pipeline: PipelineConfig,
        config: HeadFollowerConfig,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), HeadFollowerError> {
        if config.poll_interval.is_zero()
            || config.attempt_timeout.is_zero()
            || config.freshness_timeout.is_zero()
            || config.poll_interval > config.freshness_timeout
            || config.attempt_timeout > config.freshness_timeout
        {
            return Err(HeadFollowerError::Config);
        }

        let mut interval = tokio::time::interval(config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            }

            let attempt = async {
                if self.snapshot().ready {
                    match self.sync_head(&mut source, pipeline).await {
                        Err(HeadSyncError::DeepReorg { .. }) => {
                            self.recover_deep_reorg(&mut source, pipeline).await
                        }
                        result => result,
                    }
                } else {
                    self.recover_deep_reorg(&mut source, pipeline).await
                }
            };
            let result = tokio::select! {
                result = tokio::time::timeout(config.attempt_timeout, attempt) => {
                    result
                        .map_err(|_| "Zakura head attempt timed out".to_owned())
                        .and_then(|result| result.map_err(|error| error.to_string()))
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            };
            if let Err(error) = result {
                self.mark_source_failure(error, config.freshness_timeout);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        head::HeadError,
        test_utils::{open_index, raw_block},
        types::RawIndexBlock,
    };

    struct HeadSource(Vec<RawIndexBlock>);

    #[tonic::async_trait]
    impl CanonicalBlockSource for HeadSource {
        async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
            Ok(self.0.get(height as usize).cloned())
        }
    }

    #[tokio::test]
    async fn follower_polls_until_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(open_index(dir.path()));
        let head = HeadFollower::new(Arc::clone(&index), index.state().unwrap());
        let (shutdown, receiver) = watch::channel(false);
        let follower = {
            let head = head.clone();
            tokio::spawn(async move {
                head.follow_head(
                    HeadSource((0..=12).map(raw_block).collect()),
                    PipelineConfig::default(),
                    HeadFollowerConfig {
                        poll_interval: Duration::from_millis(1),
                        attempt_timeout: Duration::from_millis(100),
                        freshness_timeout: Duration::from_millis(100),
                    },
                    receiver,
                )
                .await
            })
        };

        tokio::time::timeout(Duration::from_secs(1), async {
            while !head.readiness().tip {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(head.snapshot().visible_tip.unwrap().height, 12);
        shutdown.send(true).unwrap();
        follower.await.unwrap().unwrap();
    }
}
