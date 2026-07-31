//! The §6 write policy on the HTTP surface, and the structural guard that keeps
//! it that way.
//!
//! `tests/mcp.rs::every_mutating_mcp_tool_is_policy_classified` exists because a
//! new MCP tool is invisible to behavioural tests and can ship ungated. The HTTP
//! route table had no equivalent, and that is exactly how `POST /v1/checkout` and
//! `POST /v1/branches` shipped taking `_auth: Auth` — authenticated, then
//! discarding the identity and calling the *unattributed* engine method. Checkout
//! truncates and rematerializes the whole working tree, so a propose-only token
//! could destroy every uncommitted edit in the workspace.
#![cfg(feature = "api")]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use origofs_sdk::api::{BearerAuth, router};
use origofs_sdk::{Workspace, WritePolicy};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

const T_TRUSTED: &str = "t-trusted";
const T_PROPOSE: &str = "t-propose";

struct Fixture {
    app: Router,
    ws: Arc<Workspace>,
}

/// A workspace with one `Direct` actor and one `Propose`-only actor, plus a
/// commit and a branch so checkout has somewhere to go.
async fn fixture() -> Fixture {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();

    let trusted = ws.create_human("trusted", None).await.unwrap();
    let restricted = ws.create_agent("restricted", "opus", None).await.unwrap();
    ws.set_write_policy(restricted, WritePolicy::Propose)
        .await
        .unwrap();

    ws.write("/committed.txt", b"v1").await.unwrap();
    ws.commit("trusted", "base").await.unwrap();
    ws.create_branch("side").await.unwrap();

    let auth = BearerAuth::new()
        .with_token(T_TRUSTED, trusted, None)
        .with_token(T_PROPOSE, restricted, None);

    let ws = Arc::new(ws);
    Fixture {
        app: router(ws.clone(), Arc::new(auth)),
        ws,
    }
}

fn post(path: &str, token: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// A propose-only actor cannot discard the workspace by switching branches.
#[tokio::test]
async fn checkout_is_refused_for_a_propose_only_actor() {
    let f = fixture().await;

    // An uncommitted edit that a checkout would destroy.
    f.ws.write("/scratch.txt", b"unsaved work").await.unwrap();

    let (status, body) = send(
        &f.app,
        post("/v1/checkout", T_PROPOSE, json!({"name": "side"})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "propose-only actor was allowed to check out: {body}"
    );
    assert_eq!(body["error"]["code"], "denied");

    // And the edit it would have destroyed is still there.
    assert_eq!(
        &f.ws.read("/scratch.txt").await.unwrap()[..],
        b"unsaved work"
    );
}

/// A propose-only actor cannot create refs either.
#[tokio::test]
async fn create_branch_is_refused_for_a_propose_only_actor() {
    let f = fixture().await;
    let (status, body) = send(
        &f.app,
        post("/v1/branches", T_PROPOSE, json!({"name": "sneaky"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(
        !f.ws
            .list_branches()
            .await
            .unwrap()
            .iter()
            .any(|(n, _)| n == "sneaky"),
        "the branch was created despite the 403"
    );
}

/// The same routes still work for a trusted actor — the gate is a policy check,
/// not a blanket refusal.
#[tokio::test]
async fn a_direct_actor_can_still_branch_and_checkout() {
    let f = fixture().await;

    let (status, _) = send(
        &f.app,
        post("/v1/branches", T_TRUSTED, json!({"name": "feature"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &f.app,
        post("/v1/checkout", T_TRUSTED, json!({"name": "side"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        f.ws.current_branch().await.unwrap().as_deref(),
        Some("side")
    );
}

/// `POST /v1/sessions` opens a session for the *authenticated* actor, never for
/// one named in the body.
#[tokio::test]
async fn a_session_belongs_to_the_authenticated_actor() {
    let f = fixture().await;
    let trusted = f.ws.actor_by_subject("nobody").await.unwrap();
    assert!(trusted.is_none()); // sanity: subjects aren't set in this fixture

    // Name a different actor in the body; it must be ignored.
    let victim =
        f.ws.list_actors()
            .await
            .unwrap()
            .into_iter()
            .find(|a| a.display_name == "restricted")
            .unwrap();

    let (status, body) = send(
        &f.app,
        post(
            "/v1/sessions",
            T_TRUSTED,
            json!({"actor": victim.id, "client": "probe"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_ne!(
        body["actor"], victim.id,
        "the session was opened for the actor named in the request body"
    );
}

/// Every mutating HTTP route is accounted for, and none of them throws its
/// identity away.
///
/// The structural counterpart to the MCP test. It reads the router source, pairs
/// each `POST`/`PUT`/`DELETE` route with its handler, and requires that handler to
/// bind the principal (`Auth(principal)`) rather than discard it (`_auth: Auth`).
/// Discarding it is the precise shape of the bug: authentication passes, the
/// actor is dropped, and the handler calls an unattributed engine method that
/// skips `ensure_may_write` and records no `edit_op`.
///
/// A route that genuinely needs no actor must be named in `NO_ACTOR_NEEDED` with
/// a reason, which is the moment to notice whether that is actually true.
#[test]
fn every_mutating_route_binds_its_principal() {
    // Mutating routes that legitimately take no actor. Empty today; an entry here
    // is a claim that the operation mutates nothing an actor could be blamed for.
    const NO_ACTOR_NEEDED: &[&str] = &[
        // `POST /v1/dirs` — the root always exists, so this creates nothing and
        // there is nothing to attribute. It exists so the collection URL is not a
        // 405 beside `POST /v1/dirs/{path}`.
        "make_root",
    ];

    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/api/mod.rs"),
    )
    .unwrap();

    // Collect handler names registered under a mutating method: `.post(name)`,
    // `.put(name)`, `.delete(name)`.
    let mut handlers: Vec<String> = Vec::new();
    for verb in ["post(", "put(", "delete("] {
        let mut from = 0usize;
        while let Some(at) = src[from..].find(verb) {
            let start = from + at + verb.len();
            let name: String = src[start..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            from = start;
            // `post(` also appears in the `use axum::routing::{get, post}` import
            // and in doc comments; a real registration names an fn defined below.
            if !name.is_empty() && src.contains(&format!("async fn {name}(")) {
                handlers.push(name);
            }
        }
    }
    handlers.sort();
    handlers.dedup();

    assert!(
        handlers.len() >= 10,
        "route scan found only {} mutating handlers — the scan is broken, not the \
         router: {handlers:?}",
        handlers.len()
    );

    let mut offenders = Vec::new();
    for name in &handlers {
        if NO_ACTOR_NEEDED.contains(&name.as_str()) {
            continue;
        }
        // Take the handler's signature: from `async fn <name>(` to the closing
        // `) ->` of its parameter list.
        let at = src.find(&format!("async fn {name}(")).unwrap();
        let sig_end = src[at..].find(") ->").map(|e| at + e).unwrap_or(at);
        let sig = &src[at..sig_end];

        if sig.contains("_auth: Auth") || !sig.contains("Auth(") {
            offenders.push(name.clone());
        }
    }

    assert!(
        offenders.is_empty(),
        "these mutating routes do not bind their authenticated principal, so they \
         cannot be attributed or policy-gated — they authenticate and then call an \
         unattributed engine method:\n  {}\n\nBind `Auth(principal)` and call the \
         `*_as` variant, or add the route to NO_ACTOR_NEEDED with a reason.",
        offenders.join("\n  ")
    );
}
