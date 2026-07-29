//! The metrics facade is **emit-only** (roadmap M9): the library records, it never
//! installs an exporter. These tests pin that contract — recording is safe with
//! nothing installed, and the exposition seam is an install-once, binary-owned
//! hook that starts out empty.
//!
//! Runs with the `metrics` feature on *or* off: with it off every `record_*` call
//! is an empty body, which is exactly what must stay callable.

use origofs_core::metrics;

#[test]
fn library_installs_no_renderer_and_recording_is_always_safe() {
    // Nothing in origofs-core/-sdk may install an exporter — only a binary does.
    assert!(
        !metrics::renderer_installed(),
        "the library must never install an exposition renderer"
    );
    assert!(metrics::render().is_none());

    // Every helper is callable in a process with no recorder at all.
    metrics::describe();
    metrics::record_write(1024);
    metrics::record_read(1024);
    metrics::record_chunks(8, 3);
    metrics::record_commit();
    metrics::record_gc(4, 65536);
    metrics::record_op_duration("write", 0.002);
    metrics::record_http_request("GET", "/v1/files/{*path}".into(), 200, 0.002);
    metrics::record_error(&origofs_core::OrigoFSError::NotFound("/missing".into()));
    metrics::record_error(&origofs_core::OrigoFSError::Backend {
        origin: origofs_core::BackendOrigin::Metadata,
        class: origofs_core::ErrorClass::Retryable,
        source: Box::new(std::io::Error::other("busy")),
    });
    {
        let _t = origofs_core::OpTimer::start("commit");
    }

    // A binary installs the renderer; the first one wins.
    assert!(metrics::set_renderer(
        || "origofs_writes_total 1\n".to_string()
    ));
    assert!(metrics::renderer_installed());
    assert_eq!(
        metrics::render().as_deref(),
        Some("origofs_writes_total 1\n")
    );
    assert!(
        !metrics::set_renderer(|| "second".to_string()),
        "set_renderer is install-once, like a tracing subscriber"
    );
}

#[test]
fn exposition_content_type_is_the_prometheus_one() {
    assert_eq!(
        metrics::EXPOSITION_CONTENT_TYPE,
        "text/plain; version=0.0.4"
    );
}
