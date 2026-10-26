//! The readiness of a replica: how far the import of the snapshot and its verification are, and
//! the state of the node.
//!
//! The daemon of a replica makes one [`ReplicaStatus`] and changes it at each phase of the
//! startup. [`CompactService::with_replica_status`](crate::CompactService::with_replica_status)
//! adds the heights of each serving snapshot. [`ReplicaReadiness`] is the result:
//! [`CompactService::readiness`](crate::CompactService::readiness) returns it, and `GetLightdInfo`
//! gives its text. Each field is also a `ztreamer.replicate.*` Prometheus gauge.
//!
//! A wallet operator routes traffic to a replica when `verified_through == sealed_through` and
//! `import_state == following`, unless the replica runs with `--serve-unverified`.

use std::{fmt, sync::Arc};

use metrics::Gauge;
use tokio::sync::watch;

use crate::service::ServingSnapshot;

/// The phase of the startup of a replica.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ImportState {
    /// The replica fetches and installs ranges from its sources.
    #[default]
    Fetching,
    /// The replica serves ranges that passed the chain check, and attests them in the background.
    Verifying,
    /// The replica indexes the blocks above the snapshot from the node.
    Tail,
    /// The replica follows the head of the node and serves.
    Following,
}

impl ImportState {
    /// All states, in the order of the startup.
    pub const ALL: [Self; 4] = [Self::Fetching, Self::Verifying, Self::Tail, Self::Following];

    /// The name in `GetLightdInfo` and in the `state` label of the gauge.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fetching => "fetching",
            Self::Verifying => "verifying",
            Self::Tail => "tail",
            Self::Following => "following",
        }
    }
}

/// The state of the node that the replica trusts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NodeState {
    /// The node answers, and the serving tip is fresh.
    #[default]
    Ok,
    /// The serving tip is older than the freshness timeout, and the last head read did not fail.
    Stale,
    /// The last head read failed, and the serving tip is older than the freshness timeout.
    Unreachable,
}

impl NodeState {
    /// All states.
    pub const ALL: [Self; 3] = [Self::Ok, Self::Stale, Self::Unreachable];

    /// The name in `GetLightdInfo` and in the `state` label of the gauge.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Stale => "stale",
            Self::Unreachable => "unreachable",
        }
    }
}

impl From<&ServingSnapshot> for NodeState {
    fn from(snapshot: &ServingSnapshot) -> Self {
        match (snapshot.tip_fresh, &snapshot.source_error) {
            (true, _) => Self::Ok,
            (false, Some(_)) => Self::Unreachable,
            (false, None) => Self::Stale,
        }
    }
}

/// A source of ranges that the import uses, and the blobs with a wrong digest that it sent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportSource {
    /// The snapshot URL or the `host:port` of the peer.
    pub name: String,
    /// The blobs with a wrong digest from this source.
    pub strikes: u8,
}

/// What the daemon of a replica reports. [`ReplicaReadiness`] adds the heights of the index.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaProgress {
    /// The phase of the startup.
    pub import_state: ImportState,
    /// The state of the node.
    pub node_state: NodeState,
    /// The last height of the ranges that the replica installed with a chain check only, when
    /// it serves unverified ranges.
    pub chain_ok_through: Option<u32>,
    /// The sources of the import.
    pub import_sources: Vec<ImportSource>,
}

/// The readiness of a replica, as `GetLightdInfo` and Prometheus give it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaReadiness {
    /// The last sealed height of the index.
    pub sealed_through: Option<u32>,
    /// The last height of the verified prefix of the index.
    pub verified_through: Option<u32>,
    /// The progress that the daemon reports.
    pub progress: ReplicaProgress,
}

impl fmt::Display for ReplicaReadiness {
    /// Writes `key=value` pairs with one space between them. A height that is not set is
    /// `none`. Each source is `name:strikes`, with a comma between two sources.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let height =
            |height: Option<u32>| height.map_or_else(|| "none".to_owned(), |h| h.to_string());
        let progress = &self.progress;
        write!(
            f,
            "sealed_through={} verified_through={} chain_ok_through={} import_state={} \
             node_state={} import_sources=",
            height(self.sealed_through),
            height(self.verified_through),
            height(progress.chain_ok_through),
            progress.import_state.as_str(),
            progress.node_state.as_str(),
        )?;
        for (at, source) in progress.import_sources.iter().enumerate() {
            let separator = if at == 0 { "" } else { "," };
            write!(f, "{separator}{}:{}", source.name, source.strikes)?;
        }
        Ok(())
    }
}

/// The readiness of a replica that the daemon changes. Clones share it.
///
/// ```
/// use ztreamer_service::replica::{ImportSource, ImportState, ReplicaStatus};
///
/// let source = ImportSource { name: "https://snapshots.example/mainnet".to_owned(), strikes: 0 };
/// let status = ReplicaStatus::new(vec![source]);
/// status.update(|progress| progress.import_state = ImportState::Tail);
/// assert_eq!(status.progress().import_state, ImportState::Tail);
/// ```
#[derive(Clone, Debug)]
pub struct ReplicaStatus {
    progress: Arc<watch::Sender<ReplicaProgress>>,
    metrics: Arc<ReplicaMetrics>,
}

impl ReplicaStatus {
    /// A status in [`ImportState::Fetching`] for an import from `sources`.
    pub fn new(sources: Vec<ImportSource>) -> Self {
        let progress = ReplicaProgress { import_sources: sources, ..ReplicaProgress::default() };
        let metrics = ReplicaMetrics::default();
        metrics.progress(&progress);
        Self { progress: Arc::new(watch::channel(progress).0), metrics: Arc::new(metrics) }
    }

    /// Changes the progress with `change`, and sets the gauges.
    pub fn update(&self, change: impl FnOnce(&mut ReplicaProgress)) {
        self.progress.send_modify(|progress| {
            change(progress);
            self.metrics.progress(progress);
        });
    }

    /// The progress now.
    pub fn progress(&self) -> ReplicaProgress {
        self.progress.borrow().clone()
    }

    /// The readiness with the heights of `snapshot`.
    pub(crate) fn readiness(&self, snapshot: &ServingSnapshot) -> ReplicaReadiness {
        ReplicaReadiness {
            sealed_through: snapshot.sealed_through,
            verified_through: snapshot.verified_through,
            progress: self.progress(),
        }
    }

    /// Takes the heights and the node state of a new serving snapshot.
    pub(crate) fn observe(&self, snapshot: &ServingSnapshot) {
        self.metrics.heights(snapshot);
        let node_state = NodeState::from(snapshot);
        self.update(|progress| progress.node_state = node_state);
    }
}

/// The `ztreamer.replicate.*` gauges. A height that is not set is `NaN`. A state gauge is 1 for
/// the current state and 0 for the others.
struct ReplicaMetrics {
    sealed_through: Gauge,
    verified_through: Gauge,
    chain_ok_through: Gauge,
    import_state: [Gauge; ImportState::ALL.len()],
    node_state: [Gauge; NodeState::ALL.len()],
}

impl Default for ReplicaMetrics {
    fn default() -> Self {
        Self {
            sealed_through: metrics::gauge!("ztreamer.replicate.sealed_through"),
            verified_through: metrics::gauge!("ztreamer.replicate.verified_through"),
            chain_ok_through: metrics::gauge!("ztreamer.replicate.chain_ok_through"),
            import_state: ImportState::ALL.map(|state| {
                metrics::gauge!("ztreamer.replicate.import_state", "state" => state.as_str())
            }),
            node_state: NodeState::ALL.map(|state| {
                metrics::gauge!("ztreamer.replicate.node_state", "state" => state.as_str())
            }),
        }
    }
}

impl fmt::Debug for ReplicaMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicaMetrics").finish_non_exhaustive()
    }
}

impl ReplicaMetrics {
    fn heights(&self, snapshot: &ServingSnapshot) {
        self.sealed_through.set(Self::height(snapshot.sealed_through));
        self.verified_through.set(Self::height(snapshot.verified_through));
    }

    fn progress(&self, progress: &ReplicaProgress) {
        self.chain_ok_through.set(Self::height(progress.chain_ok_through));
        for (gauge, state) in self.import_state.iter().zip(ImportState::ALL) {
            gauge.set(f64::from(state == progress.import_state));
        }
        for (gauge, state) in self.node_state.iter().zip(NodeState::ALL) {
            gauge.set(f64::from(state == progress.node_state));
        }
        for source in &progress.import_sources {
            let source_name = source.name.clone();
            metrics::gauge!("ztreamer.replicate.source_strikes", "source" => source_name)
                .set(f64::from(source.strikes));
        }
    }

    fn height(height: Option<u32>) -> f64 {
        height.map_or(f64::NAN, f64::from)
    }
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use ztreamer_indexer::index::IndexState;

    use super::*;

    fn source(name: &str, strikes: u8) -> ImportSource {
        ImportSource { name: name.to_owned(), strikes }
    }

    #[test]
    fn the_text_gives_each_field_and_none_for_a_height_that_is_not_set() {
        let readiness = ReplicaReadiness {
            sealed_through: Some(24_999),
            verified_through: Some(9_999),
            progress: ReplicaProgress {
                import_state: ImportState::Verifying,
                node_state: NodeState::Unreachable,
                chain_ok_through: None,
                import_sources: vec![source("https://a.example", 0), source("peer:9068", 2)],
            },
        };

        assert_eq!(
            readiness.to_string(),
            "sealed_through=24999 verified_through=9999 chain_ok_through=none \
             import_state=verifying node_state=unreachable \
             import_sources=https://a.example:0,peer:9068:2"
        );
    }

    #[test]
    fn the_node_state_follows_the_freshness_and_the_last_error() {
        let mut snapshot = ServingSnapshot::from(IndexState::default());
        assert_eq!(NodeState::from(&snapshot), NodeState::Stale);
        snapshot.source_error = Some("connection refused".into());
        assert_eq!(NodeState::from(&snapshot), NodeState::Unreachable);
        snapshot.tip_fresh = true;
        assert_eq!(NodeState::from(&snapshot), NodeState::Ok);
    }

    #[test]
    fn the_gauges_follow_the_status() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let status = metrics::with_local_recorder(&recorder, || {
            let status = ReplicaStatus::new(vec![source("https://a.example", 0)]);
            status.update(|progress| {
                progress.import_state = ImportState::Following;
                progress.chain_ok_through = Some(19_999);
                progress.import_sources[0].strikes = 1;
            });
            status
        });

        let gauges: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| {
                let labels: Vec<_> =
                    key.key().labels().map(|label| label.value().to_owned()).collect();
                let DebugValue::Gauge(value) = value else { panic!("only gauges") };
                (key.key().name().to_owned(), labels, value.into_inner())
            })
            .collect();
        let gauge = |name: &str, label: &[&str]| {
            gauges
                .iter()
                .find(|(key, labels, _)| key == name && labels == label)
                .map(|(_, _, value)| *value)
        };
        assert_eq!(gauge("ztreamer.replicate.chain_ok_through", &[]), Some(19_999.0));
        assert_eq!(gauge("ztreamer.replicate.import_state", &["following"]), Some(1.0));
        assert_eq!(gauge("ztreamer.replicate.import_state", &["fetching"]), Some(0.0));
        assert_eq!(gauge("ztreamer.replicate.node_state", &["ok"]), Some(1.0));
        assert_eq!(gauge("ztreamer.replicate.source_strikes", &["https://a.example"]), Some(1.0));
        assert_eq!(status.progress().import_sources[0].strikes, 1);
    }
}
