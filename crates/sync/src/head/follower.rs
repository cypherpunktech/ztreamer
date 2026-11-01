//! The head follower: it keeps the index at the canonical head of a node, and publishes a
//! [`ServingSnapshot`] after each change.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::watch;
use ztreamer_chain::codec::CompactBlockRecord;
use ztreamer_index::index::{Index, IndexState};

use super::{
    CanonicalBlockSource, HeadSyncError, recover_deep_reorg,
    serving::{Readiness, ServingSnapshot, SnapshotError},
    sync_head_once,
};
use crate::{
    metrics::{HeadMetrics, ObservedSource},
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

#[derive(Debug, thiserror::Error)]
pub enum HeadFollowerError {
    #[error("head follower durations are invalid")]
    Config,
    /// The head source cannot recover ([`HeadSyncError::stops_follower`]).
    #[error("head source failed")]
    Source(#[source] HeadSyncError),
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
            self.metrics.reconcile(started.elapsed(), false);
        })?;
        self.publish_head(state, head)
            .expect("head reconciliation must produce a connected snapshot");
        self.metrics.published(observed.observed, self.snapshot().visible_tip);
        self.metrics.reconcile(started.elapsed(), true);
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

    /// Polls Zakura until `shutdown` becomes true.
    ///
    /// A failed attempt marks the tip stale ([`Self::mark_source_failure`]), and the next poll
    /// tries again. Thus a source that answers but is behind stays stale, and readiness is false
    /// after `freshness_timeout`. It stops with an error on a source that cannot recover
    /// ([`HeadSyncError::stops_follower`]).
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
                result = tokio::time::timeout(config.attempt_timeout, attempt) => result,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            };
            let error = match result {
                Ok(Ok(_)) => continue,
                Ok(Err(error)) if error.stops_follower() => {
                    return Err(HeadFollowerError::Source(error));
                }
                Ok(Err(error)) => error.to_string(),
                Err(_) => "Zakura head attempt timed out".to_owned(),
            };
            self.mark_source_failure(error, config.freshness_timeout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::head::HeadError;
    use ztreamer_chain::{test_utils::raw_block, types::RawIndexBlock};
    use ztreamer_index::test_utils::open_index;

    struct HeadSource(Vec<RawIndexBlock>);

    #[tonic::async_trait]
    impl CanonicalBlockSource for HeadSource {
        async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
            Ok(self.0.get(height as usize).cloned())
        }
    }

    /// A source whose each read fails with the error of `fail`.
    struct FailingSource {
        fail: fn() -> HeadError,
    }

    #[tonic::async_trait]
    impl CanonicalBlockSource for FailingSource {
        async fn block(&mut self, _height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
            Err((self.fail)())
        }
    }

    const FAST: HeadFollowerConfig = HeadFollowerConfig {
        poll_interval: Duration::from_millis(1),
        attempt_timeout: Duration::from_millis(50),
        freshness_timeout: Duration::from_millis(100),
    };

    /// Follows `source` from an index at height 0 for at most one second.
    async fn follow(source: FailingSource) -> Option<Result<(), HeadFollowerError>> {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(open_index(dir.path()));
        let head = HeadFollower::new(Arc::clone(&index), index.state().unwrap());
        let (_shutdown, receiver) = watch::channel(false);
        let follow = head.follow_head(source, PipelineConfig::default(), FAST, receiver);
        tokio::time::timeout(Duration::from_secs(1), follow).await.ok()
    }

    #[tokio::test]
    async fn follower_stops_on_a_gone_node() {
        let gone = || HeadError::RemoteFatal("remote zakura is gone".into());
        let result = follow(FailingSource { fail: gone }).await;

        let error = result.expect("the follower stops by itself").unwrap_err();
        assert!(matches!(error, HeadFollowerError::Source(ref source) if source.stops_follower()));
        let causes: Vec<_> =
            std::iter::successors(Some(&error as &dyn std::error::Error), |e| e.source())
                .map(ToString::to_string)
                .collect();
        assert!(causes.iter().any(|cause| cause.contains("remote zakura is gone")), "{causes:?}");
    }

    #[tokio::test]
    async fn follower_tries_again_on_a_syncing_node() {
        let syncing = || HeadError::Remote("remote zakura is syncing".into());
        let result = follow(FailingSource { fail: syncing }).await;
        assert!(result.is_none(), "the follower runs until the shutdown");
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
