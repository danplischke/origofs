//! Metrics surface (M9): the emit-only facade plus `GET /metrics`.
//!
//! Drives the axum router in-process (`tower::oneshot`, exactly like `api.rs`),
//! makes a few real API calls, then scrapes `/metrics` and asserts the Prometheus
//! exposition carries the expected series with plausible values.
//!
//! The whole flow lives in **one** test function on purpose: the exposition
//! renderer is a process-global, install-once seam (like a tracing subscriber), so
//! "no renderer installed → 503" can only be observed deterministically before the
//! test installs one.
#![cfg(all(feature = "api", feature = "metrics"))]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use metrics_exporter_prometheus::PrometheusBuilder;
use origofs_sdk::Workspace;
use origofs_sdk::api::{BearerAuth, router};
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "t-agent";

/// Histogram buckets a *binary* would choose — sub-millisecond to ~8s.
const BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[tokio::test]
async fn metrics_endpoint_serves_prometheus_exposition() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let agent = ws.create_agent("claude", "opus", None).await.unwrap();
    let session = ws.create_session(agent, Some("metrics")).await.unwrap();
    let auth = BearerAuth::new().with_token(TOKEN, agent, Some(session));
    let app = router(Arc::new(ws), Arc::new(auth));

    // 1. Before a binary installs an exporter the route exists but has nothing to
    //    serve — an honest failed scrape, not a 404 that looks like a typo.
    let (status, body) = send(&app, get("/metrics")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        String::from_utf8_lossy(&body).contains("metrics not enabled"),
        "expected a clear opt-in hint, got {:?}",
        String::from_utf8_lossy(&body)
    );

    // 2. Do what `origofs serve --metrics` does: install a recorder, describe the
    //    metrics, and hand the renderer to the API surface. The library never does
    //    this itself.
    let handle = PrometheusBuilder::new()
        .set_buckets(BUCKETS)
        .unwrap()
        .install_recorder()
        .expect("install the Prometheus recorder");
    origofs_core::metrics::describe();
    assert!(origofs_sdk::api::set_metrics_renderer(
        move || handle.render()
    ));

    // 3. Exercise the surface: two writes, a read, a miss (error counter), a commit.
    let (status, _) = send(&app, put_as("/files/notes/a.txt", b"hello")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&app, put_as("/files/notes/b.txt", b"world!")).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(&app, get("/files/notes/a.txt")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body, b"hello");

    let (status, _) = send(&app, get("/files/nope.txt")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        &app,
        post_json_as("/commit", serde_json::json!({ "message": "first" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // 4. Scrape.
    let resp = app.clone().oneshot(get("/metrics")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/plain; version=0.0.4"),
        "a Prometheus scraper requires the text exposition content type"
    );
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8(body.to_vec()).expect("exposition is UTF-8 text");

    // Every name the facade declares that this flow should have produced.
    for name in [
        "origofs_writes_total",
        "origofs_write_bytes_total",
        "origofs_reads_total",
        "origofs_read_bytes_total",
        "origofs_commits_total",
        "origofs_errors_total",
        "origofs_http_requests_total",
        "origofs_http_request_duration_seconds",
    ] {
        assert!(body.contains(name), "missing {name} in exposition:\n{body}");
    }

    // Plausible values, not just presence.
    assert_eq!(
        counter(&body, "origofs_writes_total"),
        Some(2.0),
        "two PUTs landed:\n{body}"
    );
    assert_eq!(
        counter(&body, "origofs_write_bytes_total"),
        Some(11.0),
        "\"hello\" + \"world!\" = 11 bytes:\n{body}"
    );
    // Reads are counted per streamed body; `hello` is 5 bytes.
    assert_eq!(counter(&body, "origofs_reads_total"), Some(1.0));
    assert_eq!(counter(&body, "origofs_read_bytes_total"), Some(5.0));
    assert_eq!(counter(&body, "origofs_commits_total"), Some(1.0));

    // The error counter is keyed off the stable `OrigoFSError::code()`/`class()`.
    assert!(
        body.contains(r#"code="not_found""#) && body.contains(r#"class="none""#),
        "the missing-file read must be counted by code and class:\n{body}"
    );

    // Request labels use the *matched route template*, never the requested path —
    // otherwise a scrape would both leak workspace paths and explode cardinality.
    assert!(
        body.contains(r#"method="PUT""#) && body.contains(r#"status="200""#),
        "per-request labels missing:\n{body}"
    );
    assert!(
        body.contains(r#"path="/v1/files/{*path}""#),
        "the `path` label must be the matched route template:\n{body}"
    );
    assert!(
        !body.contains("notes/a.txt"),
        "a requested path must never reach the exposition:\n{body}"
    );

    // `describe()` metadata reaches the exposition as HELP/TYPE lines.
    assert!(
        body.contains("# HELP origofs_writes_total")
            && body.contains("# TYPE origofs_writes_total"),
        "describe() should emit HELP/TYPE:\n{body}"
    );
}

/// The value of an unlabeled counter line (`name <value>`), if present.
fn counter(body: &str, name: &str) -> Option<f64> {
    body.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.trim().parse().ok())
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, body)
}

/// Data routes live under `/v1`; `/metrics` (like `/health` and `/readyz`) is at
/// the root so an orchestrator/scraper reaches it independent of the API version.
fn v1(uri: &str) -> String {
    format!("/v1{uri}")
}

fn get(uri: &str) -> Request<Body> {
    let uri = if uri == "/metrics" {
        uri.to_string()
    } else {
        v1(uri)
    };
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

fn put_as(uri: &str, body: &[u8]) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(v1(uri))
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::from(body.to_vec()))
        .unwrap()
}

fn post_json_as(uri: &str, v: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(v1(uri))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&v).unwrap()))
        .unwrap()
}
