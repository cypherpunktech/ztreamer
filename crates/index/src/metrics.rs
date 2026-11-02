//! The metrics of the index: the time of each write stage, and the height of the index.
//! `scripts/benchmark-stages.py` reads each of them.
//!
//! Each metric name and its help text is a constant in this file, and [`DESCRIBE`] gives the help
//! text to the recorder once. Other modules record only through the types and functions of this
//! file.

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use metrics::{
    Counter, Histogram, counter, describe_counter, describe_gauge, describe_histogram, gauge,
    histogram,
};

use crate::index::IndexState;

const WRITE_SECONDS: &str = "ztreamer.index.write.seconds";
const WRITE_SECONDS_HELP: &str = "Time of each stage of one index write that committed.";

const WRITE_SEAL_READ_BLOCKS: &str = "ztreamer.index.write.seal_read.blocks";
const WRITE_SEAL_READ_BLOCKS_HELP: &str =
    "Mutable blocks that index writes read again to seal a range.";

const DURABLE_HEIGHT: &str = "ztreamer.index.durable.block.height";
const DURABLE_HEIGHT_HELP: &str = "The highest block in the index. NaN on an empty index.";

/// Gives the help text of each metric to the recorder. Each record forces it.
static DESCRIBE: LazyLock<()> = LazyLock::new(|| {
    describe_histogram!(WRITE_SECONDS, WRITE_SECONDS_HELP);
    describe_counter!(WRITE_SEAL_READ_BLOCKS, WRITE_SEAL_READ_BLOCKS_HELP);
    describe_gauge!(DURABLE_HEIGHT, DURABLE_HEIGHT_HELP);
});

#[derive(Clone, Copy)]
pub(crate) enum WriteStage {
    HashIndex,
    SealRead,
    SealEncode,
    SealPut,
    MutablePut,
    StatePut,
    Commit,
}

const WRITE_STAGES: usize = 7;

impl WriteStage {
    const ALL: [Self; WRITE_STAGES] = [
        Self::HashIndex,
        Self::SealRead,
        Self::SealEncode,
        Self::SealPut,
        Self::MutablePut,
        Self::StatePut,
        Self::Commit,
    ];

    const fn label(self) -> &'static str {
        match self {
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

/// Measures the stages of one [`Index::write`](crate::index::Index::write). It takes its handles
/// when the write starts. [`WriteTimer::finish`] records the stage times and the seal reads after
/// the commit.
///
/// A write that fails records nothing here, because its transaction writes nothing. The pipeline
/// commit metric counts it with the outcome `error`. A stage that did not run, for example a
/// seal, has no sample.
pub(crate) struct WriteTimer {
    stages: [Histogram; WRITE_STAGES],
    seal_read_counter: Counter,
    last: Instant,
    elapsed: [Option<Duration>; WRITE_STAGES],
    seal_read_blocks: usize,
}

impl WriteTimer {
    pub(crate) fn start() -> Self {
        LazyLock::force(&DESCRIBE);
        Self {
            stages: WriteStage::ALL
                .map(|stage| histogram!(WRITE_SECONDS, "stage" => stage.label())),
            seal_read_counter: counter!(WRITE_SEAL_READ_BLOCKS),
            last: Instant::now(),
            elapsed: [None; WRITE_STAGES],
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

    /// Records the stages of a write that committed.
    pub(crate) fn finish(self) {
        self.stages
            .iter()
            .zip(self.elapsed)
            .filter_map(|(handle, elapsed)| Some((handle, elapsed?)))
            .for_each(|(handle, elapsed)| handle.record(elapsed.as_secs_f64()));
        self.seal_read_counter.increment(self.seal_read_blocks as u64);
    }
}

/// Sets the durable height gauge to the tip of `state`.
pub(crate) fn record_durable_height(state: IndexState) {
    LazyLock::force(&DESCRIBE);
    gauge!(DURABLE_HEIGHT).set(state.durable_tip().map_or(f64::NAN, |tip| tip.height.into()));
}
