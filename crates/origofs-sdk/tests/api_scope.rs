//! The HTTP surface can be scoped to one subtree (issue #125).
//!
//! `origofs.fastapi`'s root-scoping was for a long time the **only** working
//! per-path access control in the repository, and it was Python-only. That is the
//! more dangerous direction of a parity gap than the usual one: a Rust embedder
//! reading the Python docs would reasonably assume the scoping lived in the shared
//! layer, and build on an assumption the Rust router did not honour.
//!
//! The tests below are organized by the four properties a scope has to get right.
//! Three of them are things a naive implementation gets wrong, and each has its own
//! case here rather than being folded into a general "scoping works" test — a
//! `starts_with` implementation passes every obvious test and still leaks to the
//! neighbour.
#![cfg(feature = "api")]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use origofs_sdk::Workspace;
use origofs_sdk::api::{ApiOptions, BearerAuth, router_with};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "t";

struct Fixture {
    /// Scoped to `/tenant-a`.
    scoped: Router,
    /// The whole workspace, for setting up a neighbour's data and for checking
    /// that the unscoped behaviour is unchanged.
    open: Router,
    ws: Arc<Workspace>,
    actor: i64,
}

async fn fixture() -> Fixture {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Arc::new(
        Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap(),
    );
    let actor = ws.create_human("dan", None).await.unwrap();
    let session = ws.create_session(actor, Some("api")).await.unwrap();
    let auth = || {
        Arc::new(BearerAuth::new().with_token(TOKEN, actor, Some(session)))
            as Arc<dyn origofs_sdk::api::Authenticator>
    };

    // Two neighbours plus a lookalike whose name shares a textual prefix with the
    // scope root — the case a `starts_with` implementation gets wrong.
    ws.mkdir_p("/tenant-a").await.unwrap();
    ws.mkdir_p("/tenant-abc").await.unwrap();
    ws.mkdir_p("/other").await.unwrap();

    let scoped = router_with(
        ws.clone(),
        auth(),
        ApiOptions {
            root: Some("/tenant-a".into()),
            ..Default::default()
        },
    );
    let open = router_with(ws.clone(), auth(), ApiOptions::default());
    Fixture {
        scoped,
        open,
        ws,
        actor,
    }
}

// --- request helpers ---------------------------------------------------------

fn v1(p: &str) -> String {
    format!("/v1{p}")
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, body)
}

fn as_json(b: &[u8]) -> Value {
    serde_json::from_slice(b).unwrap_or(Value::Null)
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(v1(uri))
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

fn put(uri: &str, body: &[u8]) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(v1(uri))
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::from(body.to_vec()))
        .unwrap()
}

fn post(uri: &str, v: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(v1(uri))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(v.to_string()))
        .unwrap()
}

// --- property 2: prepend, do not compare -------------------------------------

/// A write through the scoped router lands **inside** the root, and a read of the
/// same path sees it. The caller never says `/tenant-a`.
#[tokio::test]
async fn paths_resolve_inside_the_root() {
    let f = fixture().await;

    let (status, _) = send(&f.scoped, put("/files/notes.md", b"hello")).await;
    assert_eq!(status, StatusCode::OK);

    // It landed under the root, not at the workspace top level.
    assert_eq!(
        &f.ws.read("/tenant-a/notes.md").await.unwrap()[..],
        b"hello"
    );
    assert!(
        f.ws.stat("/notes.md").await.is_err(),
        "the write escaped the scope"
    );

    let (status, body) = send(&f.scoped, get("/files/notes.md")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"hello");
}

/// **The** property: a caller asking for another tenant by name gets its own
/// subtree, not the neighbour. Out-of-scope paths are not representable rather
/// than representable-and-refused, which is what makes the guarantee structural.
#[tokio::test]
async fn a_path_naming_another_tenant_cannot_reach_it() {
    let f = fixture().await;
    f.ws.write("/other/secrets.txt", b"not yours")
        .await
        .unwrap();

    let (status, body) = send(&f.scoped, get("/files/other/secrets.txt")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a scoped request must not reach /other/secrets.txt; got {}",
        String::from_utf8_lossy(&body)
    );

    // And a *write* to that path lands inside the caller's own tree.
    send(&f.scoped, put("/files/other/mine.txt", b"x")).await;
    assert_eq!(
        &f.ws.read("/tenant-a/other/mine.txt").await.unwrap()[..],
        b"x",
        "the write should have been resolved inside the caller's root"
    );
    assert!(
        f.ws.stat("/other/mine.txt").await.is_err(),
        "the write escaped into the neighbour's tree"
    );
}

/// `..` is refused outright, as a 400. `validate_component` already refuses to
/// *store* one, but that is a different guarantee — it stops a poisoned name being
/// persisted, not a path resolving out of its scope.
#[tokio::test]
async fn traversal_is_refused() {
    let f = fixture().await;
    let (status, _) = send(&f.scoped, get("/files/../other/secrets.txt")).await;
    assert!(
        status == StatusCode::BAD_REQUEST || status == StatusCode::NOT_FOUND,
        "a `..` must not resolve; got {status}"
    );
    assert_ne!(status, StatusCode::OK);
}

// --- property 1: directory-boundary matching ---------------------------------

/// `/tenant-a` must not cover `/tenant-abc` — the exact neighbour a scope exists
/// to exclude, and the one a `starts_with` implementation silently admits.
#[tokio::test]
async fn a_lookalike_sibling_is_not_in_scope() {
    let f = fixture().await;
    f.ws.write("/tenant-abc/secret.txt", b"neighbour")
        .await
        .unwrap();
    f.ws.write("/tenant-a/mine.txt", b"mine").await.unwrap();

    // The scoped caller sees only its own.
    let (status, body) = send(&f.scoped, get("/files/mine.txt")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"mine");

    // A listing of the root shows the caller's entry and not the lookalike's.
    let (status, body) = send(&f.scoped, get("/dirs")).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<String> = as_json(&body)
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"mine.txt".to_string()));
    assert!(
        !names.iter().any(|n| n.contains("secret")),
        "the lookalike sibling's contents leaked into a scoped listing: {names:?}"
    );
}

// --- property 4: not-found, never forbidden ----------------------------------

/// A suggestion belonging to a neighbour is a **404**, identical to one that never
/// existed. Suggestion ids are workspace-global, so without this, knowing an id was
/// enough — and `accept` *lands a write* into the neighbour's tree.
#[tokio::test]
async fn a_neighbours_suggestion_is_not_found_not_forbidden() {
    let f = fixture().await;
    let ctx = origofs_sdk::WriteCtx::actor(f.actor);
    f.ws.write("/other/doc.txt", b"original").await.unwrap();
    let id =
        f.ws.suggest(ctx, "/other/doc.txt", b"tampered", Some("nope"), None)
            .await
            .unwrap();

    // An id that never existed, for comparison.
    let (missing_status, _) = send(&f.scoped, get("/suggestions/999999")).await;

    for uri in [
        format!("/suggestions/{id}"),
        format!("/suggestions/{id}/diff"),
    ] {
        let (status, _) = send(&f.scoped, get(&uri)).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{uri} must be 404, not 403 — a 403 confirms the id exists"
        );
        assert_eq!(
            status, missing_status,
            "{uri} must be indistinguishable from an id that never existed"
        );
    }

    // Accepting is the dangerous one: it would write into the neighbour's tree.
    let (status, _) = send(
        &f.scoped,
        post(&format!("/suggestions/{id}/accept"), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        &f.ws.read("/other/doc.txt").await.unwrap()[..],
        b"original",
        "a scoped caller accepted a suggestion into a tree it cannot address"
    );
}

/// The scoped router still serves its own suggestions — the refusal above is a
/// scope boundary, not the feature being broken.
#[tokio::test]
async fn a_suggestion_in_scope_is_still_served() {
    let f = fixture().await;
    let ctx = origofs_sdk::WriteCtx::actor(f.actor);
    f.ws.write("/tenant-a/doc.txt", b"original").await.unwrap();
    let id =
        f.ws.suggest(ctx, "/tenant-a/doc.txt", b"better", Some("ok"), None)
            .await
            .unwrap();

    let (status, body) = send(&f.scoped, get(&format!("/suggestions/{id}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(as_json(&body)["path"], json!("/tenant-a/doc.txt"));
}

// --- property 3 + the side doors ---------------------------------------------

/// The workspace-wide listings are filtered, not just the path routes.
///
/// This is the half that is easy to miss: a scope that gates `GET /files/…` but
/// leaves the change feed and the suggestion list alone has not scoped anything,
/// because both report a neighbour's paths directly.
#[tokio::test]
async fn workspace_wide_listings_are_filtered() {
    let f = fixture().await;
    let ctx = origofs_sdk::WriteCtx::actor(f.actor);
    f.ws.write_as(ctx, "/tenant-a/mine.txt", b"mine")
        .await
        .unwrap();
    f.ws.write_as(ctx, "/other/theirs.txt", b"theirs")
        .await
        .unwrap();
    f.ws.write_as(ctx, "/tenant-abc/lookalike.txt", b"nope")
        .await
        .unwrap();
    f.ws.suggest(ctx, "/other/theirs.txt", b"x", None, None)
        .await
        .unwrap();
    f.ws.suggest(ctx, "/tenant-a/mine.txt", b"y", None, None)
        .await
        .unwrap();

    // The change feed: paths, sizes, and timing all leak without filtering.
    let (status, body) = send(&f.scoped, get("/events?since=0")).await;
    assert_eq!(status, StatusCode::OK);
    let paths: Vec<String> = as_json(&body)
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["path"].as_str().map(str::to_string))
        .collect();
    assert!(
        paths
            .iter()
            .all(|p| p.starts_with("/tenant-a/") || p == "/tenant-a"),
        "the change feed leaked a neighbour's paths: {paths:?}"
    );
    assert!(
        paths.iter().any(|p| p == "/tenant-a/mine.txt"),
        "the caller's own events were filtered out too: {paths:?}"
    );

    // The suggestion queue: pending content and paths.
    let (status, body) = send(&f.scoped, get("/suggestions")).await;
    assert_eq!(status, StatusCode::OK);
    let paths: Vec<String> = as_json(&body)
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["path"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        paths,
        vec!["/tenant-a/mine.txt".to_string()],
        "the suggestion queue leaked a neighbour's proposals"
    );

    // The unscoped router still sees everything — this is a scope, not a filter
    // that was always on.
    let (_, body) = send(&f.open, get("/suggestions")).await;
    assert_eq!(as_json(&body).as_array().unwrap().len(), 2);
}

/// A presence row naming **no path** is filtered out too.
///
/// The subtlest of the four. An idle session has no path, so a naive "filter by
/// path" keeps it — and its mere presence tells a scoped reader that somebody else
/// is connected to a tenant it cannot see.
#[tokio::test]
async fn a_pathless_presence_row_is_filtered_out() {
    let f = fixture().await;
    let other = f.ws.create_human("intruder", None).await.unwrap();
    let other_session = f.ws.create_session(other, Some("idle")).await.unwrap();

    // An idle session: present, working on nothing.
    f.ws.touch(other, other_session, None).await.unwrap();

    let (status, body) = send(&f.scoped, get("/presence")).await;
    assert_eq!(status, StatusCode::OK);
    let rows = as_json(&body);
    let rows = rows.as_array().unwrap();
    assert!(
        rows.is_empty(),
        "an idle (pathless) presence row survived scoping, which tells a scoped \
         reader that a neighbour is connected: {rows:?}"
    );

    // The unscoped router still reports it.
    let (_, body) = send(&f.open, get("/presence")).await;
    assert_eq!(as_json(&body).as_array().unwrap().len(), 1);
}

// --- operations with no per-tenant meaning -----------------------------------

/// Commit, log, branches, and checkout act on the **whole working tree**, so a
/// scoped router refuses them rather than serving a partial answer that looks
/// complete. A checkout in particular rematerializes *every* tenant's files.
#[tokio::test]
async fn whole_tree_operations_are_refused_under_a_scope() {
    let f = fixture().await;

    for (name, req) in [
        ("commit", post("/commit", json!({"message": "m"}))),
        ("log", get("/log")),
        ("branches", get("/branches")),
        (
            "create branch",
            post("/branches", json!({"name": "feature"})),
        ),
        ("checkout", post("/checkout", json!({"name": "main"}))),
        ("diff", get("/diff?from=a&to=b")),
    ] {
        let (status, _) = send(&f.scoped, req).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{name} has no per-tenant meaning and must be refused under a scope"
        );
    }
}

/// The same operations still work on an unscoped router — the refusal is about
/// scoping, not about the routes being removed.
#[tokio::test]
async fn whole_tree_operations_still_work_unscoped() {
    let f = fixture().await;
    let (status, _) = send(&f.open, get("/branches")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&f.open, post("/commit", json!({"message": "m"}))).await;
    assert_eq!(status, StatusCode::OK);
}

// --- the unscoped default is unchanged ---------------------------------------

/// No root means no scoping, and nothing about the existing behaviour moves. This
/// is what makes the feature additive for every current embedder.
#[tokio::test]
async fn an_unscoped_router_reaches_everything() {
    let f = fixture().await;
    f.ws.write("/other/secrets.txt", b"visible").await.unwrap();

    let (status, body) = send(&f.open, get("/files/other/secrets.txt")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"visible");

    // And a plain path still means what it always did.
    send(&f.open, put("/files/top.txt", b"x")).await;
    assert_eq!(&f.ws.read("/top.txt").await.unwrap()[..], b"x");
}

/// A rename is scoped at **both** endpoints. Scoping only the source would let a
/// caller move a file it can address into a tree it cannot — the same rule
/// path-scoped ACLs will need (#123).
#[tokio::test]
async fn rename_is_scoped_at_both_endpoints() {
    let f = fixture().await;
    f.ws.write("/tenant-a/a.txt", b"data").await.unwrap();

    // The destination's parent must exist for the rename itself to succeed, so
    // that the only thing this test can fail on is the scoping.
    f.ws.mkdir_p("/tenant-a/other").await.unwrap();
    f.ws.mkdir_p("/other").await.unwrap();

    let (status, body) = send(
        &f.scoped,
        post(
            "/rename",
            json!({"from": "/a.txt", "to": "/other/escaped.txt"}),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the rename itself is legitimate: {}",
        String::from_utf8_lossy(&body)
    );

    // ...but it landed inside the caller's own tree, not the neighbour's.
    assert!(
        f.ws.stat("/other/escaped.txt").await.is_err(),
        "a scoped rename escaped into the neighbour's tree"
    );
    assert_eq!(
        &f.ws.read("/tenant-a/other/escaped.txt").await.unwrap()[..],
        b"data"
    );
}

// --- revising a proposal over HTTP (#164) -------------------------------------

/// `POST /suggestions?replaces=` retires the draft it revises, and the standalone
/// `POST /suggestions/{id}/supersede` retires one with nothing taking its place.
///
/// Both go through `suggestion_in_scope`, so a scoped caller cannot reach a
/// neighbour's queue by id — the same *not found* answer every other id-addressed
/// suggestion route gives, rather than a `403` that would confirm the id exists.
#[tokio::test]
async fn a_proposal_can_be_revised_and_withdrawn_over_http() {
    let f = fixture().await;
    let ctx = origofs_sdk::WriteCtx::actor(f.actor);
    f.ws.write("/tenant-a/doc.txt", b"original").await.unwrap();

    let (status, body) = send(
        &f.scoped,
        Request::builder()
            .method("POST")
            .uri("/v1/suggestions?path=/doc.txt")
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(Body::from("v1"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v1 = as_json(&body)["id"].as_i64().unwrap();

    let (status, body) = send(
        &f.scoped,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/suggestions?path=/doc.txt&replaces={v1}"))
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(Body::from("v2"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v2 = as_json(&body)["id"].as_i64().unwrap();

    let ws = f.ws.clone();
    let s = move |id: i64| {
        let ws = ws.clone();
        async move { ws.get_suggestion(id).await.unwrap().unwrap().status }
    };
    assert_eq!(
        s(v1).await,
        origofs_sdk::SuggestionStatus::Superseded,
        "the revised draft must not still be waiting to be accepted"
    );

    let (status, _) = send(
        &f.scoped,
        post(&format!("/suggestions/{v2}/supersede"), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(s(v2).await, origofs_sdk::SuggestionStatus::Superseded);
    assert_eq!(
        &f.ws.read("/tenant-a/doc.txt").await.unwrap()[..],
        b"original"
    );

    // A neighbour's proposal is out of scope by id, exactly as accept/reject are.
    f.ws.write("/other/doc.txt", b"original").await.unwrap();
    let theirs =
        f.ws.suggest(ctx, "/other/doc.txt", b"nope", None, None)
            .await
            .unwrap();
    let (status, _) = send(
        &f.scoped,
        post(&format!("/suggestions/{theirs}/supersede"), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        s(theirs).await,
        origofs_sdk::SuggestionStatus::Pending,
        "a scoped caller retired a proposal in a tree it cannot address"
    );
}
