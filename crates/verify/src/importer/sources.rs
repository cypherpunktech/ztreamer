//! The sources of one import: their manifests, the versions of each epoch that the manifests
//! give, and the fetch of a blob from a source that has it.

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures_util::future;
use metrics::Counter;
use tracing::warn;
use ztreamer_indexer::index::{RANGE_SIZE, range_meta::ManifestEntry};

use super::ImportError;
use crate::{
    epoch::Epochs,
    fetch::{FetchError, Fetcher, RangeSource},
    manifest::Manifest,
    range::RangeLink,
};

/// The sources of one import, in the order of the caller. Blob fetches rotate across them.
#[derive(Debug)]
pub(crate) struct Sources<S> {
    /// The fetcher of each source, and its `ztreamer.replicate.bytes_fetched_total`.
    fetchers: Vec<(Fetcher<S>, Counter)>,
    /// The count of blob fetches. It selects the first source of the next fetch.
    rotation: AtomicUsize,
}

impl<S: RangeSource> Sources<S> {
    pub(crate) fn new(sources: Vec<S>) -> Self {
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
        Self { fetchers, rotation: AtomicUsize::new(0) }
    }

    /// The manifest of each source that gives one, with the position of the source. The
    /// sources answer at the same time. A source with no manifest has no part in the import.
    /// When no source gives a manifest, the error is the error of the last source.
    pub(crate) async fn manifests(&self) -> Result<Vec<(usize, Manifest)>, ImportError> {
        let answers =
            future::join_all(self.fetchers.iter().map(|(fetcher, _)| fetcher.manifest())).await;
        let (mut manifests, mut failure) = (Vec::new(), None);
        for (source, answer) in answers.into_iter().enumerate() {
            match answer {
                Ok(manifest) => manifests.push((source, manifest)),
                Err(error) => {
                    warn!(source, %error, "source gave no manifest");
                    failure = Some(error);
                }
            }
        }
        if manifests.is_empty() {
            return Err(failure.map_or(ImportError::NoSources, ImportError::from));
        }
        Ok(manifests)
    }

    /// The blob of `entry` from one of `holders`. The fetch starts at the next holder in the
    /// rotation, and asks each holder once. After a wrong digest or another error, it asks the
    /// next holder. When no holder sends the blob, the error is the last error that was not a
    /// wrong digest, else [`ImportError::NoSources`].
    pub(crate) async fn blob(
        &self,
        entry: &ManifestEntry,
        holders: &[usize],
    ) -> Result<Bytes, ImportError> {
        let first = self.rotation.fetch_add(1, Ordering::Relaxed) % holders.len().max(1);
        let mut failure = None;
        for &source in holders.iter().cycle().skip(first).take(holders.len()) {
            let (fetcher, bytes_fetched) = &self.fetchers[source];
            match fetcher.blob(entry).await {
                Ok(blob) => {
                    bytes_fetched.increment(entry.blob_len);
                    return Ok(blob);
                }
                Err(FetchError::Digest { .. } | FetchError::SourceDropped { .. }) => {}
                Err(error) => {
                    let start_height = entry.start_height;
                    warn!(source, start_height, %error, "fetch failed; trying the next source");
                    failure = Some(error);
                }
            }
        }
        Err(failure.map_or(ImportError::NoSources, ImportError::from))
    }

    /// Adds a strike against each of `holders`, because their version of an epoch failed
    /// verification.
    pub(crate) fn strike(&self, holders: &[usize]) {
        for &source in holders {
            let strikes = self.fetchers[source].0.strike();
            warn!(source, strikes, "source sent an epoch that failed verification");
        }
    }
}

/// One version of an epoch: an epoch of one manifest, and the sources whose manifests agree
/// with it, as [`Version::compare_manifests`] gives.
#[derive(Clone, Debug)]
pub(crate) struct Version {
    /// The position of the manifest in the manifests of the import.
    pub(crate) manifest: usize,
    /// The position of the first entry of the epoch in that manifest.
    pub(crate) offset: usize,
    /// The sources whose manifests agree with this version.
    pub(crate) holders: Vec<usize>,
}

impl Version {
    /// The versions of each epoch of the first manifest, the reference.
    ///
    /// Two manifests agree on an epoch when they give the same blob digests for its heights, and
    /// the same end hash, end tree sizes and frontiers for the entry before it. `Epoch::verify`
    /// starts from that entry. Thus after a dispute that the reference loses, the next epoch is
    /// disputed too, if the entry before it is not the same.
    ///
    /// The first version of an epoch is the version of the reference. Each manifest that does
    /// not agree adds a version, if its entries from the first height of the epoch group to the
    /// same end height. A manifest that does not reach the end of the epoch has no version of
    /// it.
    pub(crate) fn compare_manifests(manifests: &[(usize, Manifest)]) -> Vec<Vec<Self>> {
        let (_, reference) = &manifests[0];
        reference
            .epochs()
            .map(|epoch| {
                // The entries of a manifest follow each other from genesis.
                let offset = (epoch.entries()[0].start_height / RANGE_SIZE) as usize;
                let positions = offset..offset + epoch.entries().len();
                let previous = offset.checked_sub(1);
                let agree = |one: &[ManifestEntry], other: &[ManifestEntry]| {
                    let same_link = previous.is_none_or(|at| {
                        let (one, other) = (&one[at], &other[at]);
                        RangeLink::from(one) == RangeLink::from(other)
                            && one.frontiers == other.frontiers
                    });
                    let (one, other) = (&one[positions.clone()], &other[positions.clone()]);
                    same_link
                        && one
                            .iter()
                            .zip(other)
                            .all(|(one, other)| one.blob_digest == other.blob_digest)
                };
                let mut versions: Vec<Self> = Vec::new();
                for (manifest, (source, other)) in manifests.iter().enumerate() {
                    if other.entries().len() < positions.end {
                        continue;
                    }
                    let agreed = versions.iter_mut().find(|version| {
                        agree(manifests[version.manifest].1.entries(), other.entries())
                    });
                    if let Some(version) = agreed {
                        version.holders.push(*source);
                        continue;
                    }
                    let same_heights = Epochs::starting_at(other.entries(), offset)
                        .next()
                        .is_some_and(|other| other.end_height() == epoch.end_height());
                    if same_heights {
                        versions.push(Self { manifest, offset, holders: vec![*source] });
                    }
                }
                versions
            })
            .collect()
    }
}
