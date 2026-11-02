//! [`capture_spans`]: the spans and events of a test, as JSON.

use std::{
    io,
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError},
};

use serde_json::Value;
use tracing_subscriber::fmt::{MakeWriter, format::FmtSpan};

/// The output of the global test subscriber, and the lock that lets one capture run at a time.
struct Capture {
    output: Arc<Mutex<Vec<u8>>>,
    serial: Mutex<()>,
}

/// Installs the global subscriber on first use. A thread-local subscriber does not see the
/// spans on blocking threads and scoped threads, thus the test subscriber is global.
static CAPTURE: LazyLock<Capture> = LazyLock::new(|| {
    let output = Arc::new(Mutex::new(Vec::new()));
    tracing_subscriber::fmt()
        .json()
        .with_span_list(true)
        .with_current_span(true)
        .with_span_events(FmtSpan::CLOSE)
        .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE)
        .with_writer(SharedOutput(Arc::clone(&output)))
        .try_init()
        .expect("capture_spans installs the only global subscriber of the test binary");
    Capture { output, serial: Mutex::new(()) }
});

/// Runs `run`, and returns its result and each JSON line that the global test subscriber wrote
/// meanwhile. A line is an event, or the close of a span. An event has its spans in `"spans"`,
/// from the root. A close has the span in `"span"`, with its last fields, and the parents of the
/// span in `"spans"`. [`closed_spans`] finds the closes.
///
/// Other tests of the same binary can write lines at the same time. Find the lines of a test by
/// the names of its spans or by the messages of its events.
///
/// ```
/// use ztreamer_chain::test_utils::{capture_spans, closed_spans};
///
/// let ((), lines) = capture_spans(|| {
///     let span = tracing::info_span!("test.work", blocks = 3, done = tracing::field::Empty);
///     span.in_scope(|| tracing::info!("work done"));
///     span.record("done", true);
/// });
/// let event = lines.iter().find(|line| line["fields"]["message"] == "work done").unwrap();
/// assert_eq!(event["spans"][0]["name"], "test.work");
/// let work: Vec<_> = closed_spans(&lines, "test.work").collect();
/// assert_eq!(work[0]["blocks"], 3);
/// assert_eq!(work[0]["done"], true);
/// ```
pub fn capture_spans<T>(run: impl FnOnce() -> T) -> (T, Vec<Value>) {
    let capture = &*CAPTURE;
    let _serial = capture.serial.lock().unwrap_or_else(PoisonError::into_inner);
    lock(&capture.output).clear();
    let result = run();
    let lines = lock(&capture.output)
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("the test subscriber writes JSON lines"))
        .collect();
    (result, lines)
}

/// The spans named `name` that closed in `lines`, with their last fields, in the order that they
/// closed.
pub fn closed_spans<'a>(lines: &'a [Value], name: &str) -> impl Iterator<Item = &'a Value> {
    lines
        .iter()
        .filter(|line| line["fields"]["message"] == "close")
        .map(|line| &line["span"])
        .filter(move |span| span["name"] == name)
}

fn lock(output: &Mutex<Vec<u8>>) -> MutexGuard<'_, Vec<u8>> {
    output.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A writer that appends to the shared output. The subscriber writes each line in one call.
#[derive(Clone)]
struct SharedOutput(Arc<Mutex<Vec<u8>>>);

impl io::Write for SharedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        lock(&self.0).extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedOutput {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
