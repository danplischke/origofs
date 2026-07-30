//! Metrics — an **emit-only** facade (roadmap M9, "metrics/tracing").
//!
//! This is the numeric counterpart to the `tracing` story described in
//! `CLAUDE.md`: the library *records* measurements and **installs no exporter and
//! starts no server**. A Rust embedder who does nothing pays nothing; a binary
//! that wants Prometheus installs a recorder (the `origofs` CLI does — see
//! `init_metrics` in `crates/origofs-cli/src/main.rs`) and everything recorded
//! here starts landing in it.
//!
//! Two independent halves:
//!
//! 1. **Recording** — the `record_*` helpers and [`OpTimer`] below. They are
//!    compiled against the [`metrics`](https://docs.rs/metrics) facade crate only
//!    when the **`metrics` feature** is on; with the feature off every helper has
//!    an empty body, so call sites can be written unconditionally and cost
//!    literally nothing (they optimize away). Nothing here allocates a recorder:
//!    with a feature-on build and no recorder installed, the `metrics` crate's
//!    global no-op recorder swallows everything.
//! 2. **Exposition** — [`set_renderer`]/[`render`]. The process that installs a
//!    recorder also installs a closure that renders the current values in the
//!    Prometheus text format; `GET /metrics` on the HTTP API surface
//!    (`origofs_sdk::api`, `metrics` feature) serves whatever that closure
//!    returns, and answers "metrics not enabled" when none was installed. This
//!    keeps the exporter dependency in the *binary* while the *route* lives with
//!    the rest of the HTTP surface.
//!
//! ## Naming
//!
//! Names follow the Prometheus conventions: a `origofs_` namespace prefix, base
//! units (bytes, seconds), `_total` on monotonic counters, and low-cardinality
//! labels only (an error `code`/`class`, a fixed operation name, a *matched route
//! template* — never a user path, actor id, or hash).

use std::sync::OnceLock;

// ── metric names ────────────────────────────────────────────────────────────
// Declared once, here, so every surface records under the same key and a
// dashboard/alert has a single place to be checked against.

/// Counter: attributed and unattributed file writes that completed.
pub const WRITES_TOTAL: &str = "origofs_writes_total";
/// Counter: bytes accepted by file writes.
pub const WRITE_BYTES_TOTAL: &str = "origofs_write_bytes_total";
/// Counter: file reads that completed.
pub const READS_TOTAL: &str = "origofs_reads_total";
/// Counter: bytes served by file reads.
pub const READ_BYTES_TOTAL: &str = "origofs_read_bytes_total";

/// Counter: chunks handed to the content store by the chunker.
pub const CHUNKS_PUT_TOTAL: &str = "origofs_chunks_put_total";
/// Counter: chunks the content store already had — the dedup hit rate is
/// `origofs_chunks_deduped_total / origofs_chunks_put_total`.
pub const CHUNKS_DEDUPED_TOTAL: &str = "origofs_chunks_deduped_total";

/// Counter: commits crystallized from the working tree.
pub const COMMITS_TOTAL: &str = "origofs_commits_total";
/// Counter: unreachable objects swept by mark-and-sweep GC.
pub const GC_OBJECTS_DELETED_TOTAL: &str = "origofs_gc_objects_deleted_total";
/// Counter: bytes reclaimed by mark-and-sweep GC.
pub const GC_BYTES_FREED_TOTAL: &str = "origofs_gc_bytes_freed_total";
/// Change-feed events delivered to subscribers.
pub const FEED_EVENTS_DELIVERED_TOTAL: &str = "origofs_feed_events_delivered_total";
/// Times a subscriber's `LISTEN` connection was (re)established.
pub const FEED_RECONNECTS_TOTAL: &str = "origofs_feed_reconnects_total";
/// How far behind the newest event a drain started, in events.
pub const FEED_LAG_EVENTS: &str = "origofs_feed_lag_events";

/// Counter, labeled `code` (the stable [`crate::OrigoFSError::code`]) and `class`
/// (`retryable` | `unavailable` | `fatal` | `none` for the non-backend variants):
/// errors surfaced to a caller. Both labels are closed sets, so cardinality is
/// bounded.
pub const ERRORS_TOTAL: &str = "origofs_errors_total";

/// Histogram, labeled `op` (a fixed operation name such as `write`, `read`,
/// `commit`, `gc`): wall-clock duration of an engine operation, **in seconds**.
pub const OP_DURATION_SECONDS: &str = "origofs_op_duration_seconds";

/// Counter, labeled `method`, `path` (the *matched route template*, e.g.
/// `/v1/files/{*path}` — never the requested path) and `status`: HTTP API
/// requests served.
pub const HTTP_REQUESTS_TOTAL: &str = "origofs_http_requests_total";
/// Histogram, labeled `method` and `path` (matched route template): HTTP API
/// request duration **in seconds**.
pub const HTTP_REQUEST_DURATION_SECONDS: &str = "origofs_http_request_duration_seconds";

/// The label value used when a request matched no route, so a 404 storm against
/// random URLs cannot blow up label cardinality.
pub const UNMATCHED_ROUTE: &str = "<unmatched>";

/// `Content-Type` for the Prometheus text exposition format — what `GET /metrics`
/// must answer with for a scraper to accept the body.
pub const EXPOSITION_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

// ── exposition seam ─────────────────────────────────────────────────────────

type RenderFn = Box<dyn Fn() -> String + Send + Sync + 'static>;

static RENDERER: OnceLock<RenderFn> = OnceLock::new();

/// Install the closure that renders the process's current metrics in the
/// Prometheus text exposition format. **A binary calls this, never the library** —
/// it is the metrics equivalent of installing a `tracing` subscriber, and it is
/// what makes `GET /metrics` on the HTTP surface return a body instead of
/// "metrics not enabled".
///
/// Typically the closure just calls `PrometheusHandle::render` from
/// `metrics-exporter-prometheus`, so the exporter dependency stays in the binary.
///
/// Returns `false` if a renderer was already installed (the first one wins; this
/// is a process-global, install-once seam, exactly like the global recorder the
/// `metrics` crate itself uses).
pub fn set_renderer<F>(render: F) -> bool
where
    F: Fn() -> String + Send + Sync + 'static,
{
    RENDERER.set(Box::new(render)).is_ok()
}

/// Whether a binary has installed an exposition renderer via [`set_renderer`].
pub fn renderer_installed() -> bool {
    RENDERER.get().is_some()
}

/// Render the current metrics in the Prometheus text format, or `None` when no
/// renderer was installed (i.e. this process opted out of metrics).
pub fn render() -> Option<String> {
    RENDERER.get().map(|f| f())
}

// ── descriptions ────────────────────────────────────────────────────────────

/// Register `# HELP`/`# TYPE`/unit metadata for every metric declared above.
///
/// Call this once from a binary right after installing a recorder; it is a no-op
/// without the `metrics` feature, and harmless (idempotent) if called twice.
/// Descriptions are metadata only — they never create a time series, so a metric
/// that was never recorded still does not appear in the exposition.
pub fn describe() {
    #[cfg(feature = "metrics")]
    {
        use ::metrics::{Unit, describe_counter, describe_histogram};
        describe_counter!(WRITES_TOTAL, Unit::Count, "File writes that completed.");
        describe_counter!(
            WRITE_BYTES_TOTAL,
            Unit::Bytes,
            "Bytes accepted by file writes."
        );
        describe_counter!(READS_TOTAL, Unit::Count, "File reads that completed.");
        describe_counter!(READ_BYTES_TOTAL, Unit::Bytes, "Bytes served by file reads.");
        describe_counter!(
            CHUNKS_PUT_TOTAL,
            Unit::Count,
            "Chunks handed to the content store."
        );
        describe_counter!(
            CHUNKS_DEDUPED_TOTAL,
            Unit::Count,
            "Chunks the content store already had (dedup hits)."
        );
        describe_counter!(
            COMMITS_TOTAL,
            Unit::Count,
            "Commits crystallized from the working tree."
        );
        describe_counter!(
            GC_OBJECTS_DELETED_TOTAL,
            Unit::Count,
            "Unreachable objects swept by garbage collection."
        );
        describe_counter!(
            FEED_EVENTS_DELIVERED_TOTAL,
            Unit::Count,
            "Change-feed events delivered to subscribers."
        );
        describe_counter!(
            FEED_RECONNECTS_TOTAL,
            Unit::Count,
            "Times a subscriber's feed connection was established."
        );
        ::metrics::describe_gauge!(
            FEED_LAG_EVENTS,
            Unit::Count,
            "Events a feed drain was behind the newest event when it started."
        );
        describe_counter!(
            GC_BYTES_FREED_TOTAL,
            Unit::Bytes,
            "Bytes reclaimed by garbage collection."
        );
        describe_counter!(
            ERRORS_TOTAL,
            Unit::Count,
            "Errors surfaced to a caller, by stable code and retry class."
        );
        describe_histogram!(
            OP_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of an engine operation."
        );
        describe_counter!(
            HTTP_REQUESTS_TOTAL,
            Unit::Count,
            "HTTP API requests served, by method, matched route and status."
        );
        describe_histogram!(
            HTTP_REQUEST_DURATION_SECONDS,
            Unit::Seconds,
            "HTTP API request duration."
        );
    }
}

// ── recording helpers ───────────────────────────────────────────────────────
//
// Each is a thin, allocation-free wrapper whose body is `#[cfg]`-gated, so call
// sites stay unconditional (no `#[cfg]` sprinkled through the engine) and a
// feature-off build compiles them to nothing.

/// Record one completed file write of `bytes` bytes.
#[inline]
pub fn record_write(bytes: u64) {
    #[cfg(feature = "metrics")]
    {
        ::metrics::counter!(WRITES_TOTAL).increment(1);
        ::metrics::counter!(WRITE_BYTES_TOTAL).increment(bytes);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = bytes;
}

/// Record one completed file read of `bytes` bytes.
#[inline]
pub fn record_read(bytes: u64) {
    #[cfg(feature = "metrics")]
    {
        ::metrics::counter!(READS_TOTAL).increment(1);
        ::metrics::counter!(READ_BYTES_TOTAL).increment(bytes);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = bytes;
}

/// Record `put` chunks offered to the content store, of which `deduped` were
/// already present. Recording both as counters (rather than a ratio) keeps them
/// aggregatable across processes.
#[inline]
pub fn record_chunks(put: u64, deduped: u64) {
    #[cfg(feature = "metrics")]
    {
        ::metrics::counter!(CHUNKS_PUT_TOTAL).increment(put);
        ::metrics::counter!(CHUNKS_DEDUPED_TOTAL).increment(deduped);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (put, deduped);
}

/// Record one commit.
#[inline]
pub fn record_commit() {
    #[cfg(feature = "metrics")]
    ::metrics::counter!(COMMITS_TOTAL).increment(1);
}

/// Record a change-feed drain: how many events it delivered, and how far behind
/// the newest event the subscriber was when it started.
///
/// The push feed's own health was invisible. `subscribe` reconnects and recovers
/// gaps correctly, but nothing reported that it *had* — so a subscriber silently
/// falling behind, or flapping its connection, looked exactly like a quiet
/// workspace. Both labels-free counters, so no cardinality risk.
#[inline]
pub fn record_feed_drain(delivered: u64, lag_events: u64) {
    #[cfg(feature = "metrics")]
    {
        ::metrics::counter!(FEED_EVENTS_DELIVERED_TOTAL).increment(delivered);
        ::metrics::gauge!(FEED_LAG_EVENTS).set(lag_events as f64);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (delivered, lag_events);
}

/// Record that a subscriber established (or re-established) its feed connection.
#[inline]
pub fn record_feed_connect() {
    #[cfg(feature = "metrics")]
    ::metrics::counter!(FEED_RECONNECTS_TOTAL).increment(1);
}

/// Record the outcome of a garbage-collection sweep.
#[inline]
pub fn record_gc(objects_deleted: u64, bytes_freed: u64) {
    #[cfg(feature = "metrics")]
    {
        ::metrics::counter!(GC_OBJECTS_DELETED_TOTAL).increment(objects_deleted);
        ::metrics::counter!(GC_BYTES_FREED_TOTAL).increment(bytes_freed);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (objects_deleted, bytes_freed);
}

/// Count an error under its stable [`crate::OrigoFSError::code`] and retry
/// [`crate::ErrorClass`]. Both labels are closed sets, so this cannot grow
/// unbounded cardinality no matter what the failing input was.
#[inline]
pub fn record_error(err: &crate::OrigoFSError) {
    #[cfg(feature = "metrics")]
    {
        use crate::ErrorClass;
        let class = match err.class() {
            Some(ErrorClass::Retryable) => "retryable",
            Some(ErrorClass::Unavailable) => "unavailable",
            Some(ErrorClass::Fatal) => "fatal",
            None => "none",
        };
        ::metrics::counter!(ERRORS_TOTAL, "code" => err.code(), "class" => class).increment(1);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = err;
}

/// Record the duration of an engine operation. `op` must be a fixed, small set of
/// names (`write`, `read`, `commit`, `gc`, …) — never anything derived from user
/// input.
#[inline]
pub fn record_op_duration(op: &'static str, seconds: f64) {
    #[cfg(feature = "metrics")]
    ::metrics::histogram!(OP_DURATION_SECONDS, "op" => op).record(seconds);
    #[cfg(not(feature = "metrics"))]
    let _ = (op, seconds);
}

/// Record one served HTTP request. `route` is the **matched route template**
/// (`/v1/files/{*path}`), or [`UNMATCHED_ROUTE`] — never the requested path, which
/// would be unbounded cardinality and would leak workspace paths into the
/// exposition.
#[inline]
pub fn record_http_request(method: &'static str, route: String, status: u16, seconds: f64) {
    #[cfg(feature = "metrics")]
    {
        let status = status.to_string();
        ::metrics::histogram!(
            HTTP_REQUEST_DURATION_SECONDS,
            "method" => method,
            "path" => route.clone(),
        )
        .record(seconds);
        ::metrics::counter!(
            HTTP_REQUESTS_TOTAL,
            "method" => method,
            "path" => route,
            "status" => status,
        )
        .increment(1);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (method, route, status, seconds);
}

/// A scope timer that records into [`OP_DURATION_SECONDS`] when it is dropped:
///
/// ```no_run
/// # fn body() {}
/// let _t = origofs_core::metrics::OpTimer::start("commit");
/// body();
/// // duration recorded here, on drop — including on an early `?` return.
/// ```
///
/// With the `metrics` feature off it holds nothing and does nothing.
#[must_use = "an OpTimer records on drop; binding it to `_` drops it immediately"]
#[derive(Debug)]
pub struct OpTimer {
    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    op: &'static str,
    #[cfg(feature = "metrics")]
    start: std::time::Instant,
}

impl OpTimer {
    /// Start timing `op` (a fixed operation name, see [`record_op_duration`]).
    #[inline]
    pub fn start(op: &'static str) -> Self {
        Self {
            op,
            #[cfg(feature = "metrics")]
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for OpTimer {
    #[inline]
    fn drop(&mut self) {
        #[cfg(feature = "metrics")]
        record_op_duration(self.op, self.start.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_without_a_recorder_is_a_no_op() {
        // The whole point of an emit-only library: every helper is safe to call
        // in a process that installed nothing.
        record_write(7);
        record_read(7);
        record_chunks(3, 1);
        record_commit();
        record_gc(2, 4096);
        record_error(&crate::OrigoFSError::NotFound("/x".into()));
        record_op_duration("write", 0.001);
        record_http_request("GET", "/v1/files/{*path}".into(), 200, 0.001);
        drop(OpTimer::start("write"));
    }

    #[test]
    fn names_follow_prometheus_conventions() {
        for name in [
            WRITES_TOTAL,
            WRITE_BYTES_TOTAL,
            READS_TOTAL,
            READ_BYTES_TOTAL,
            CHUNKS_PUT_TOTAL,
            CHUNKS_DEDUPED_TOTAL,
            COMMITS_TOTAL,
            GC_OBJECTS_DELETED_TOTAL,
            GC_BYTES_FREED_TOTAL,
            ERRORS_TOTAL,
            HTTP_REQUESTS_TOTAL,
        ] {
            assert!(name.starts_with("origofs_"), "{name} needs the namespace");
            assert!(name.ends_with("_total"), "{name} is a counter");
        }
        for name in [OP_DURATION_SECONDS, HTTP_REQUEST_DURATION_SECONDS] {
            assert!(name.starts_with("origofs_"), "{name} needs the namespace");
            assert!(name.ends_with("_seconds"), "{name} must use base units");
        }
    }
}
