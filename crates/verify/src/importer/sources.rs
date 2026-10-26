//! The sources of one import: the manifest that all sources agree on, and the fetch of a blob
//! from one of the sources.

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures_util::future;
use metrics::Counter;
use tracing::warn;
use ztreamer_indexer::index::range_meta::ManifestEntry;

use super::ImportError;
use crate::{
    fetch::{Fetcher, RangeSource},
    manifest::Manifest,
};

/// The sources of one import, in the order of the caller.
#[derive(Debug)]
pub(crate) struct Sources {
    /// The fetcher of each source, and its `ztreamer.replicate.bytes_fetched_total`.
    fetchers: Vec<(Fetcher, Counter)>,
    /// The count of blob fetches. It selects the first source that a fetch asks.
    blob_fetch_count: AtomicUsize,
}

impl Sources {
    pub(crate) fn new(sources: Vec<Box<dyn RangeSource>>) -> Self {
        let fetchers = sources
            .into_iter()
            .enumerate()
            .map(|(position, source)| {
                let label = position.to_string();
                let bytes_fetched =
                    metrics::counter!("ztreamer.replicate.bytes_fetched_total", "source" => label);
                (Fetcher::new(source), bytes_fetched)
            })
            .collect();
        Self { fetchers, blob_fetch_count: AtomicUsize::new(0) }
    }

    /// The shortest manifest of the sources, after a check that each other manifest gives the
    /// same blob digest for each entry of it. The sources answer at the same time.
    ///
    /// A source with no manifest stops the import with its error. A source with another blob
    /// digest stops the import with [`ImportError::SourcesDisagree`]. The other manifests are
    /// not kept.
    pub(crate) async fn fetch_agreed_manifest(&self) -> Result<Manifest, ImportError> {
        let manifests = self.fetchers.iter().map(|(fetcher, _)| fetcher.manifest());
        let mut manifests = future::try_join_all(manifests).await?;
        let shortest_source = (0..manifests.len())
            .min_by_key(|&source| manifests[source].entries().len())
            .ok_or(ImportError::NoSources)?;
        let reference = manifests[shortest_source].entries();
        for (other_source, manifest) in manifests.iter().enumerate() {
            let other_digest = reference
                .iter()
                .zip(manifest.entries())
                .find(|(reference, entry)| reference.blob_digest != entry.blob_digest);
            if let Some((entry, _)) = other_digest {
                let start_height = entry.start_height;
                let error =
                    ImportError::SourcesDisagree { shortest_source, other_source, start_height };
                return Err(error);
            }
        }
        Ok(manifests.swap_remove(shortest_source))
    }

    /// The blob of `entry`. The fetch asks each source once, from the source after the first
    /// source of the fetch before. A wrong digest or another error moves the fetch to the next
    /// source. When no source sends the blob, the error is the error of the last source.
    pub(crate) async fn fetch_blob(&self, entry: &ManifestEntry) -> Result<Bytes, ImportError> {
        let count = self.fetchers.len();
        let first_source = self.blob_fetch_count.fetch_add(1, Ordering::Relaxed) % count.max(1);
        let mut failure = None;
        for source in (0..count).map(|step| (first_source + step) % count) {
            let (fetcher, bytes_fetched) = &self.fetchers[source];
            match fetcher.blob(entry).await {
                Ok(blob) => {
                    bytes_fetched.increment(entry.blob_len);
                    return Ok(blob);
                }
                Err(error) => {
                    let start_height = entry.start_height;
                    warn!(source, start_height, %error, "blob fetch failed; asking next source");
                    failure = Some(error);
                }
            }
        }
        Err(failure.map_or(ImportError::NoSources, ImportError::from))
    }
}
