//! [`capture_spans`]: the spans and events of a test, as JSON.

use std::{
    fmt,
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError},
};

use serde_json::{Map, Value, json};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt as _},
    registry::LookupSpan,
    util::SubscriberInitExt as _,
};

/// The lines of the global test subscriber, and the lock that lets one capture run at a time.
struct Capture {
    lines: Arc<Mutex<Vec<Value>>>,
    serial: Mutex<()>,
}

/// Installs the global subscriber on first use. A thread-local subscriber does not see the
/// spans on blocking threads and scoped threads, thus the test subscriber is global.
static CAPTURE: LazyLock<Capture> = LazyLock::new(|| {
    let lines = Arc::new(Mutex::new(Vec::new()));
    tracing_subscriber::registry()
        .with(CaptureLayer(Arc::clone(&lines)))
        .try_init()
        .expect("capture_spans installs the only global subscriber of the test binary");
    Capture { lines, serial: Mutex::new(()) }
});

/// Runs `run`, and returns its result and each line that the global test subscriber made
/// meanwhile, at all levels. A line is an event, or the close of a span:
///
/// - An event has its fields in `"fields"`, and its spans in `"spans"`, from the root.
/// - A close has `"fields": {"message": "close"}`, the span with its last fields in `"span"`, and
///   the parents of the span in `"spans"`, from the root. The parents are the parents of the
///   span, also when the span closed on another thread. [`closed_spans`] finds the closes.
///
/// Other tests of the same binary can make lines at the same time. Run the work of a test in a
/// span of its own, and find its lines with [`closed_spans_under`].
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
    lock(&capture.lines).clear();
    let result = run();
    let lines = std::mem::take(&mut *lock(&capture.lines));
    (result, lines)
}

/// The spans named `name` that closed in `lines`, with their last fields, in the order that they
/// closed.
pub fn closed_spans<'a>(lines: &'a [Value], name: &str) -> impl Iterator<Item = &'a Value> {
    lines
        .iter()
        .filter(move |line| line["fields"]["message"] == "close" && line["span"]["name"] == name)
        .map(|line| &line["span"])
}

/// The spans named `name` that closed in `lines` below a span named `ancestor`.
pub fn closed_spans_under<'a>(
    lines: &'a [Value],
    name: &str,
    ancestor: &str,
) -> impl Iterator<Item = &'a Value> {
    lines
        .iter()
        .filter(move |line| line["fields"]["message"] == "close" && line["span"]["name"] == name)
        .filter(move |line| {
            line["spans"]
                .as_array()
                .is_some_and(|spans| spans.iter().any(|span| span["name"] == ancestor))
        })
        .map(|line| &line["span"])
}

fn lock(lines: &Mutex<Vec<Value>>) -> MutexGuard<'_, Vec<Value>> {
    lines.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The layer of the test subscriber. It keeps the fields of each span in the span extensions.
struct CaptureLayer(Arc<Mutex<Vec<Value>>>);

/// The fields of a span, with its name.
#[derive(Clone)]
struct SpanFields(Map<String, Value>);

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for CaptureLayer {
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let mut fields = Map::new();
        fields.insert("name".to_owned(), attributes.metadata().name().into());
        attributes.record(&mut JsonVisitor(&mut fields));
        let span = context.span(id).expect("a new span is in the registry");
        span.extensions_mut().insert(SpanFields(fields));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
        let span = context.span(id).expect("a recorded span is in the registry");
        if let Some(SpanFields(fields)) = span.extensions_mut().get_mut::<SpanFields>() {
            values.record(&mut JsonVisitor(fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let mut fields = Map::new();
        event.record(&mut JsonVisitor(&mut fields));
        let spans: Vec<_> = context
            .event_scope(event)
            .into_iter()
            .flat_map(|scope| scope.from_root())
            .map(|span| span_fields(&span))
            .collect();
        lock(&self.0).push(json!({ "fields": fields, "spans": spans }));
    }

    fn on_close(&self, id: Id, context: Context<'_, S>) {
        let span = context.span(&id).expect("a closed span is in the registry");
        let parents: Vec<_> =
            span.scope().skip(1).collect::<Vec<_>>().iter().rev().map(span_fields).collect();
        let line = json!({ "fields": { "message": "close" }, "span": span_fields(&span), "spans": parents });
        lock(&self.0).push(line);
    }
}

fn span_fields<S: for<'a> LookupSpan<'a>>(
    span: &tracing_subscriber::registry::SpanRef<'_, S>,
) -> Value {
    span.extensions()
        .get::<SpanFields>()
        .map_or(Value::Null, |SpanFields(fields)| fields.clone().into())
}

/// Records each field as a JSON value. A `Display` or `Debug` value becomes a string.
struct JsonVisitor<'a>(&'a mut Map<String, Value>);

impl Visit for JsonVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}").into());
    }
}
