//! Serving media over the HTTP API: `Range`, `Content-Type`, and the headers a
//! player needs.
//!
//! `GET /v1/files/*` used to stream the whole file, always as
//! `application/octet-stream`, with no `Range` handling, no `Accept-Ranges`, and no
//! `Content-Length`. For media that means a `<video>` element cannot seek, a
//! download cannot resume, and the browser downloads the file rather than playing
//! it. The Python router (`origofs.fastapi`) had honoured `Range` from the start;
//! the Rust surface it mirrors did not.
#![cfg(feature = "api")]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use origofs_sdk::Workspace;
use origofs_sdk::api::{BearerAuth, router};
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "t";

/// A body big enough to span several chunks, so a range genuinely crosses chunk
/// boundaries and exercises the trimming rather than reading one chunk whole.
fn media_body() -> Vec<u8> {
    let mut x = 0x9E3779B97F4A7C15u64;
    let mut out = vec![0u8; 1 << 20];
    for b in out.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }
    out
}

async fn fixture(path: &str, body: &[u8]) -> Router {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let actor = ws.create_human("dan", None).await.unwrap();
    ws.write(path, body).await.unwrap();
    let auth = BearerAuth::new().with_token(TOKEN, actor, None);
    router(Arc::new(ws), Arc::new(auth))
}

async fn get(
    app: &Router,
    path: &str,
    range: Option<&str>,
) -> (StatusCode, Vec<(String, String)>, Vec<u8>) {
    let mut req = Request::builder()
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"));
    if let Some(r) = range {
        req = req.header(header::RANGE, r);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let headers = resp
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, headers, bytes)
}

fn header_of<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// A whole-file GET advertises that seeking is possible. Without `Accept-Ranges` a
/// browser will not offer to scrub a video, however correct the range handling is.
#[tokio::test]
async fn a_media_file_is_served_seekable_and_typed() {
    let body = media_body();
    let app = fixture("/clip.mp4", &body).await;

    let (status, headers, got) = get(&app, "/v1/files/clip.mp4", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(header_of(&headers, "content-type"), Some("video/mp4"));
    assert_eq!(header_of(&headers, "accept-ranges"), Some("bytes"));
    assert_eq!(
        header_of(&headers, "content-length"),
        Some(body.len().to_string().as_str())
    );
    assert_eq!(got, body);
}

/// The seek itself: a mid-file range crossing chunk boundaries.
#[tokio::test]
async fn a_range_request_returns_exactly_that_range() {
    let body = media_body();
    let app = fixture("/clip.mp4", &body).await;

    // Deliberately not chunk-aligned, so the first and last chunks are trimmed.
    let (first, last) = (300_001u64, 700_000u64);
    let (status, headers, got) = get(
        &app,
        "/v1/files/clip.mp4",
        Some(&format!("bytes={first}-{last}")),
    )
    .await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        header_of(&headers, "content-range"),
        Some(format!("bytes {first}-{last}/{}", body.len()).as_str())
    );
    assert_eq!(
        header_of(&headers, "content-length"),
        Some((last - first + 1).to_string().as_str())
    );
    assert_eq!(got, &body[first as usize..=last as usize]);
}

/// The three range spellings a player actually sends.
#[tokio::test]
async fn open_ended_and_suffix_ranges_work() {
    let body = media_body();
    let size = body.len() as u64;
    let app = fixture("/clip.mp4", &body).await;

    // `bytes=N-`: a player resuming from an offset.
    let (status, _, got) = get(&app, "/v1/files/clip.mp4", Some("bytes=1048000-")).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(got, &body[1_048_000..]);

    // `bytes=-N`: the trailing N bytes — how a player finds an MP4 moov atom that
    // was not placed at the front.
    let (status, headers, got) = get(&app, "/v1/files/clip.mp4", Some("bytes=-512")).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(got, &body[body.len() - 512..]);
    assert_eq!(
        header_of(&headers, "content-range"),
        Some(format!("bytes {}-{}/{size}", size - 512, size - 1).as_str())
    );

    // `bytes=0-` — the probe a browser opens a video with. Must stream, not buffer.
    let (status, _, got) = get(&app, "/v1/files/clip.mp4", Some("bytes=0-")).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(got, body);

    // An end past EOF is legal and clamps, rather than erroring.
    let (status, headers, got) =
        get(&app, "/v1/files/clip.mp4", Some("bytes=1048570-999999999")).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(got, &body[1_048_570..]);
    assert_eq!(
        header_of(&headers, "content-range"),
        Some(format!("bytes 1048570-{}/{size}", size - 1).as_str())
    );
}

/// A range wholly past the end is a 416 carrying the real size — that is how a
/// client discovers what it should have asked for.
#[tokio::test]
async fn an_unsatisfiable_range_is_416_with_the_size() {
    let body = media_body();
    let app = fixture("/clip.mp4", &body).await;

    let (status, headers, _) = get(&app, "/v1/files/clip.mp4", Some("bytes=99999999-")).await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        header_of(&headers, "content-range"),
        Some(format!("bytes */{}", body.len()).as_str())
    );
}

/// A `Range` this server does not honour falls back to the whole representation,
/// which RFC 9110 explicitly permits — rather than implementing
/// `multipart/byteranges` for a case no media player sends.
#[tokio::test]
async fn a_multi_range_request_serves_the_whole_file() {
    let body = media_body();
    let app = fixture("/clip.mp4", &body).await;

    let (status, _, got) = get(&app, "/v1/files/clip.mp4", Some("bytes=0-99,200-299")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got, body);

    // A malformed header is ignored the same way, not treated as an error.
    let (status, _, got) = get(&app, "/v1/files/clip.mp4", Some("furlongs=0-99")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got, body);
}

/// Types are guessed from the extension so a browser plays media instead of
/// downloading it; anything unrecognised stays `application/octet-stream`, which is
/// the safe answer.
#[tokio::test]
async fn content_types_cover_media_and_default_safely() {
    for (name, expected) in [
        ("clip.mp4", "video/mp4"),
        ("clip.webm", "video/webm"),
        ("song.mp3", "audio/mpeg"),
        ("photo.jpg", "image/jpeg"),
        ("photo.PNG", "image/png"), // case-insensitive
        ("doc.pdf", "application/pdf"),
        ("data.bin", "application/octet-stream"),
        ("noextension", "application/octet-stream"),
    ] {
        let app = fixture(&format!("/{name}"), b"x").await;
        let (status, headers, _) = get(&app, &format!("/v1/files/{name}"), None).await;
        assert_eq!(status, StatusCode::OK, "{name}");
        assert_eq!(
            header_of(&headers, "content-type"),
            Some(expected),
            "{name}"
        );
    }
}

/// An empty file still answers cleanly — no manifest object exists for it, which is
/// the case most likely to be missed when adding a size-aware code path.
#[tokio::test]
async fn an_empty_file_is_served_with_a_zero_length() {
    let app = fixture("/empty.mp4", b"").await;

    let (status, headers, got) = get(&app, "/v1/files/empty.mp4", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(header_of(&headers, "content-length"), Some("0"));
    assert_eq!(header_of(&headers, "content-type"), Some("video/mp4"));
    assert!(got.is_empty());

    // Any range against an empty file is unsatisfiable.
    let (status, _, _) = get(&app, "/v1/files/empty.mp4", Some("bytes=0-10")).await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
}
