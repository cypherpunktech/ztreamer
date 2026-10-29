//! The metrics of the index: the time of each write stage, and the heights of the index.
//!
//! Each metric name and its help text is a constant in this file, and [`DESCRIBE`] gives the help
//! text to the recorder once. Other modules record only through the types and functions of this
//! file.

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

use crate::index::IndexState;

const WRITE_SECONDS: &str = "ztreamer.index.write.seconds";
const WRITE_SECONDS_HELP: &str = "Time of each stage of one index write that committed.";

const WRITE_SEAL_READ_BLOCKS: &str = "ztreamer.index.write.seal_read.blocks";
const WRITE_SEAL_READ_BLOCKS_HELP: &str =
    "Mutable blocks that index writes read again to seal a range.";

const DURABLE_HEIGHT: &str = "ztreamer.index.durable.block.height";
const DURABLE_HEIGHT_HELP: &str = "The highest block in the index. NaN on an empty index.";

const SEALED_HEIGHT: &str = "ztreamer.index.sealed.block.height";
const SEALED_HEIGHT_HELP: &str =
    "The last block of the highest sealed range. NaN when no range is sealed.";

/// Gives the help text of each metric to the recorder. Each record forces it.
static DESCRIBE: LazyLock<()> = LazyLock::new(|| {
    describe_histogram!(WRITE_SECONDS, WRITE_SECONDS_HELP);
    describe_counter!(WRITE_SEAL_READ_BLOCKS, WRITE_SEAL_READ_BLOCKS_HELP);
    describe_gauge!(DURABLE_HEIGHT, DURABLE_HEIGHT_HELP);
    describe_gauge!(SEALED_HEIGHT, SEALED_HEIGHT_HELP);
});

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
        LazyLock::force(&DESCRIBE);
        histogram!(WRITE_SECONDS, "stage" => self.label()).record(elapsed.as_secs_f64());
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
    seal_read_blocks: usize,
}

impl WriteTimer {
    pub(crate) fn start() -> Self {
        Self { last: Instant::now(), elapsed: [None; WRITE_STAGES], seal_read_blocks: 0 }
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
}

impl WriteTimer {
    /// Records the stages of a write that committed.
    pub(crate) fn finish(self) {
        for stage in WriteStage::ALL {
            let index = stage as usize;
            if let Some(elapsed) = self.elapsed[index] {
                stage.record(elapsed);
            }
        }
        LazyLock::force(&DESCRIBE);
        counter!(WRITE_SEAL_READ_BLOCKS).increment(self.seal_read_blocks as u64);
    }
}

pub(crate) fn record_state_metrics(state: IndexState) {
    LazyLock::force(&DESCRIBE);
    gauge!(DURABLE_HEIGHT).set(state.durable_tip().map_or(f64::NAN, |tip| tip.height.into()));
    gauge!(SEALED_HEIGHT).set(state.sealed_through().map_or(f64::NAN, Into::into));
}
