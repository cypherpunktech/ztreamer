//! Write-stage timing for [`Index::write`](super::Index::write) and the index state gauges.

use std::time::{Duration, Instant};

use super::IndexState;

#[derive(Clone, Copy)]
pub(crate) enum WriteStage {
    Encode,
    HashIndex,
    SealRead,
    SealEncode,
    SealPut,
    MutablePut,
    StatePut,
    Commit,
}

const WRITE_STAGES: usize = 8;

impl WriteStage {
    const ALL: [Self; WRITE_STAGES] = [
        Self::Encode,
        Self::HashIndex,
        Self::SealRead,
        Self::SealEncode,
        Self::SealPut,
        Self::MutablePut,
        Self::StatePut,
        Self::Commit,
    ];

    pub(crate) fn record(self, elapsed: Duration) {
        metrics::histogram!("ztreamer.index.write.seconds", "stage" => self.label())
            .record(elapsed.as_secs_f64());
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Encode => "encode",
            Self::HashIndex => "hash_index",
            Self::SealRead => "seal_read",
            Self::SealEncode => "seal_encode",
            Self::SealPut => "seal_put",
            Self::MutablePut => "mutable_put",
            Self::StatePut => "state_put",
            Self::Commit => "commit",
        }
    }
}

/// Measures the stages of one [`Index::write`]. [`WriteTimer::finish`] records the times and the
/// bytes after the commit. A write that fails records nothing here, because its transaction
/// writes nothing: the pipeline commit metric counts it with the outcome `error`. A stage that
/// did not run, for example a seal, has no sample.
pub(crate) struct WriteTimer {
    last: Instant,
    elapsed: [Option<Duration>; WRITE_STAGES],
    bytes: [usize; WRITE_STAGES],
    seal_read_blocks: usize,
}

impl WriteTimer {
    pub(crate) fn start() -> Self {
        Self {
            last: Instant::now(),
            elapsed: [None; WRITE_STAGES],
            bytes: [0; WRITE_STAGES],
            seal_read_blocks: 0,
        }
    }

    pub(crate) fn lap(&mut self, stage: WriteStage) {
        let now = Instant::now();
        *self.elapsed[stage as usize].get_or_insert(Duration::ZERO) += now - self.last;
        self.last = now;
    }

    /// Records the stage only if it did work. Else the time goes to no stage.
    pub(crate) fn lap_if(&mut self, ran: bool, stage: WriteStage) {
        if ran {
            self.lap(stage);
        } else {
            self.last = Instant::now();
        }
    }

    pub(crate) fn seal_read(&mut self, blocks: usize) {
        self.lap_if(blocks > 0, WriteStage::SealRead);
        self.seal_read_blocks += blocks;
    }

    pub(crate) const fn bytes(&mut self, stage: WriteStage, bytes: usize) {
        self.bytes[stage as usize] += bytes;
    }
}

impl WriteTimer {
    /// Records the stages of a write that committed.
    pub(crate) fn finish(self) {
        for stage in WriteStage::ALL {
            let index = stage as usize;
            if let Some(elapsed) = self.elapsed[index] {
                stage.record(elapsed);
            }
            if self.bytes[index] > 0 {
                metrics::counter!("ztreamer.index.write.bytes", "stage" => stage.label())
                    .increment(self.bytes[index] as u64);
            }
        }
        metrics::counter!("ztreamer.index.write.seal_read.blocks")
            .increment(self.seal_read_blocks as u64);
    }
}

pub(crate) fn record_state_metrics(state: IndexState) {
    metrics::gauge!("ztreamer.index.durable.block.height")
        .set(state.durable_tip.map_or(f64::NAN, |tip| tip.height.into()));
    metrics::gauge!("ztreamer.index.sealed.block.height")
        .set(state.sealed_through.map_or(f64::NAN, Into::into));
    metrics::gauge!("ztreamer.index.generation").set(state.generation as f64);
}
