//! Canonical-head following: reconciling with Zakura's head and recovering deep reorgs.

use std::time::{Duration, Instant};

use tokio::sync::watch;
use ztreamer_indexer::{
    head::{CanonicalBlockSource, HeadSyncError, recover_deep_reorg},
    index::IndexState,
    pipeline::PipelineConfig,
};

use super::CompactService;
use crate::telemetry::ObservedSource;

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

impl CompactService {
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
        let result = ztreamer_indexer::head::sync_head_once(
            &self.index,
            &mut observed,
            &snapshot.volatile_head,
            config,
        )
        .await;
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
    use std::sync::Arc;
    use zakura_chain::{block, parameters::Network};
    use zakura_state::Config;
    use ztreamer_indexer::{head::HeadError, index::Index, parser::RawIndexBlock};

    use crate::service::test_utils::raw_block;

    struct HeadSource(Vec<RawIndexBlock>);

    #[tonic::async_trait]
    impl CanonicalBlockSource for HeadSource {
        async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
            Ok(self.0.get(height as usize).cloned())
        }
    }

    #[test]
    fn follower_polls_until_shutdown() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service =
                    CompactService::new(index, IndexState::default(), "main", read_service);
                let (shutdown, receiver) = watch::channel(false);
                let follower = {
                    let service = service.clone();
                    tokio::spawn(async move {
                        service
                            .follow_head(
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
                    while !service.readiness().tip {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(service.snapshot().visible_tip.unwrap().height, 12);
                shutdown.send(true).unwrap();
                follower.await.unwrap().unwrap();
            },
        );
    }
}
