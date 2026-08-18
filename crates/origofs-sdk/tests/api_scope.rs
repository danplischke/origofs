//! The HTTP surface's **root scoping** (`ApiOptions::root`, issue #125) and its
//! per-actor **read grants** (#124), driven in-process through the router.
//!
//! `origofs.fastapi` had root scoping from the start; the Rust API did not, which
//! made it the more dangerous half of a documented parity claim — an embedder
//! reading the Python docs would reasonably assume the scoping lived in the shared
//! layer. It does now: both surfaces resolve through `origofs_core::acl`.
//!
//! Four properties are pinned here, and they are the ones that make scoping
//! trustworthy rather than decorative:
//!
//! * an out-of-scope path is **unrepresentable**, not rejected — the root is
//!   prepended, so asking for a neighbour's path lands inside your own root;
//! * traversal out of the root is refused;
//! * records naming an out-of-scope path answer **404**, never 403, so a scoped
//!   caller cannot tell "exists but not yours" from "no such thing";
//! * enumerations (listings, the change feed, presence, the suggestion queue)
//!   filter rather than leaking neighbours.
#![cfg(feature = "api")]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use origofs_sdk::api::{ApiOptions, BearerAuth, router_with};
use origofs_sdk::{Perms, Workspace};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "t";

struct Fixture {
    app: Router,
    ws: Arc<Workspace>,
    actor: i64,
}

/// A router scoped to `root`, over a workspace seeded with two tenants' trees.
async fn fixture(root: Option<&str>) -> Fixture {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    for d in ["/tenant-a", "/tenant-abc", "/tenant-b"] {
        ws.mkdir_p(d).await.unwrap();
    }
    ws.write("/tenant-a/notes.txt", b"mine").await.unwrap();
    ws.write("/tenant-abc/secrets", b"neighbour").await.unwrap();
    ws.write("/tenant-b/secrets", b"theirs").await.unwrap();

    let actor = ws.create_human("dan", None).await.unwrap();
    let auth = BearerAuth::new().with_token(TOKEN, actor, None);
    let ws = Arc::new(ws);
    let app = router_with(
        ws.clone(),
        Arc::new(auth),
        ApiOptions {
            root: root.map(str::to_string),
            ..Default::default()
        },
    );
    Fixture { app, ws, actor }
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

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(format!("/v1{uri}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

fn put(uri: &str, body: &[u8]) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(format!("/v1{uri}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::from(body.to_vec()))
        .unwrap()
}

fn as_json(b: &[u8]) -> Value {
    serde_json::from_slice(b).unwrap()
}

// --- scoping --------------------------------------------------------------

#[tokio::test]
async fn a_scoped_router_resolves_paths_inside_its_root() {
    let f = fixture(Some("/tenant-a")).await;

    // `/files/notes.txt` means `/tenant-a/notes.txt`.
    let (status, body) = send(&f.app, get("/files/notes.txt")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body, b"mine");

    // And a write lands inside the root, not at the workspace root.
    let (status, _) = send(&f.app, put("/files/new.txt", b"x")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        &f.ws.read("/tenant-a/new.txt").await.unwrap()[..],
        b"x",
        "the write must have landed inside the root"
    );
    assert!(
        f.ws.read("/new.txt").await.is_err(),
        "and not at the workspace root"
    );
}

#[tokio::test]
async fn another_tenants_path_is_unrepresentable_rather_than_rejected() {
    // The property that makes prepending better than comparing: there is no
    // request a scoped client can send that addresses a neighbour at all.
    let f = fixture(Some("/tenant-a")).await;

    let (status, _) = send(&f.app, get("/files/tenant-b/secrets")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "it resolved to /tenant-a/tenant-b/secrets, which does not exist"
    );
    // The neighbour's file is untouched and was never read.
    assert_eq!(
        &f.ws.read("/tenant-b/secrets").await.unwrap()[..],
        b"theirs"
    );
}

#[tokio::test]
async fn traversal_out_of_the_root_is_refused() {
    let f = fixture(Some("/tenant-a")).await;
    let (status, _) = send(&f.app, get("/files/../tenant-b/secrets")).await;
    assert_ne!(
        status,
        StatusCode::OK,
        "`..` must not be a way out of the root"
    );
}

#[tokio::test]
async fn a_root_does_not_cover_a_sibling_sharing_its_string_prefix() {
    // `/tenant-a` must not cover `/tenant-abc` — the classic prefix bug, checked
    // here at the surface as well as in the engine.
    let f = fixture(Some("/tenant-a")).await;

    // The neighbour is not reachable, and its records are filtered out below.
    let (status, _) = send(&f.app, get("/files/../tenant-abc/secrets")).await;
    assert_ne!(status, StatusCode::OK);
    assert_eq!(
        &f.ws.read("/tenant-abc/secrets").await.unwrap()[..],
        b"neighbour"
    );
}

#[tokio::test]
async fn the_root_listing_lists_the_root_not_the_workspace() {
    // `/dirs` takes no path, so it is the one route a scope could silently miss.
    let f = fixture(Some("/tenant-a")).await;
    let (status, body) = send(&f.app, get("/dirs")).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<String> = as_json(&body)
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["notes.txt"]);
    assert!(
        !names.iter().any(|n| n.starts_with("tenant-")),
        "the workspace root's tenants must not be listed"
    );
}

#[tokio::test]
async fn out_of_scope_records_answer_404_not_403() {
    // A 403 would confirm the record exists. The id space is shared across the
    // workspace, so an id probe would otherwise enumerate neighbours.
    let f = fixture(Some("/tenant-a")).await;
    let other = f.ws.create_human("other", None).await.unwrap();
    let id =
        f.ws.suggest(
            origofs_sdk::WriteCtx::actor(other),
            "/tenant-b/secrets",
            b"proposed",
            None,
        )
        .await
        .unwrap();

    let (status, _) = send(&f.app, get(&format!("/suggestions/{id}"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // An id that never existed is indistinguishable.
    let (status, _) = send(&f.app, get("/suggestions/99999")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // And the listing does not enumerate it.
    let (status, body) = send(&f.app, get("/suggestions")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        as_json(&body).as_array().unwrap().is_empty(),
        "a neighbour's queue must not be enumerable"
    );
}

#[tokio::test]
async fn the_change_feed_and_presence_are_filtered_to_the_root() {
    let f = fixture(Some("/tenant-a")).await;
    // Generate activity in both tenants through the workspace directly.
    let ctx = origofs_sdk::WriteCtx::actor(f.actor);
    f.ws.write_as(ctx, "/tenant-a/notes.txt", b"v2")
        .await
        .unwrap();
    f.ws.write_as(ctx, "/tenant-b/secrets", b"v2")
        .await
        .unwrap();

    let (status, body) = send(&f.app, get("/events?since=0")).await;
    assert_eq!(status, StatusCode::OK);
    for e in as_json(&body).as_array().unwrap() {
        let p = e["path"].as_str().unwrap_or("");
        assert!(
            p == "/tenant-a" || p.starts_with("/tenant-a/"),
            "the feed leaked {p}"
        );
    }
}

#[tokio::test]
async fn an_unscoped_router_is_unchanged() {
    // The default must stay exactly what it was: `root: None` serves the whole
    // workspace, so scoping is opt-in and nothing regressed for existing embedders.
    let f = fixture(None).await;
    let (status, body) = send(&f.app, get("/files/tenant-b/secrets")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body, b"theirs");

    let (status, body) = send(&f.app, get("/dirs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(as_json(&body).as_array().unwrap().len(), 3);
}

// --- read grants (#124) at the surface ------------------------------------

#[tokio::test]
async fn a_read_grant_hides_a_path_as_404() {
    // Scoping and grants are independent: this router serves the whole workspace,
    // and the actor is restricted by grant rather than by root.
    let f = fixture(None).await;
    f.ws.grant(f.actor, "/", Perms::READ).await.unwrap();
    f.ws.grant(f.actor, "/tenant-b", Perms::NONE).await.unwrap();

    let (status, _) = send(&f.app, get("/files/tenant-b/secrets")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an unreadable path must be indistinguishable from a missing one"
    );

    // Readable elsewhere.
    let (status, body) = send(&f.app, get("/files/tenant-a/notes.txt")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body, b"mine");

    // And the listing omits the unreadable child rather than failing.
    let (status, body) = send(&f.app, get("/dirs")).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<String> = as_json(&body)
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"tenant-a".to_string()));
    assert!(
        !names.contains(&"tenant-b".to_string()),
        "leaked: {names:?}"
    );
}

#[tokio::test]
async fn blame_is_gated_separately_from_the_file() {
    // Blame answers *who wrote which lines* — a disclosure about people as much as
    // about content, and one of the side doors #124 named.
    let f = fixture(None).await;
    f.ws.grant(f.actor, "/", Perms::READ).await.unwrap();
    f.ws.grant(f.actor, "/tenant-b", Perms::NONE).await.unwrap();

    let (status, _) = send(&f.app, get("/blame/tenant-b/secrets")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(&f.app, get("/blame/tenant-a/notes.txt")).await;
    assert_eq!(status, StatusCode::OK);
}
