//! Read enforcement on the HTTP surface (issue #124, phase 2).
//!
//! The engine has checked reads since #133: `acl_enforce_reads` is a workspace
//! switch, and `read_as`/`stat_as`/`ls_as`/`blame_as` and friends consult
//! `Perms::READ`. **No surface called them.** Every read route took
//! `State(ws)` and called the unattributed twin, so turning the switch on
//! changed nothing for any network caller — the one place the switch is for.
//!
//! Two properties are pinned here, plus the structural guard that keeps them:
//!
//! * **Nothing changes while the switch is off.** Reads stay open and anonymous,
//!   which is what every existing deployment has.
//! * **With the switch on, an anonymous read is a `401`.** A read that cannot be
//!   checked must not be served; otherwise the enforcing workspace's protection
//!   is one missing `Authorization` header deep.
#![cfg(feature = "api")]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use origofs_sdk::api::{BearerAuth, router};
use origofs_sdk::{Perms, Workspace, WriteCtx};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

const T_OWNER: &str = "t-owner";
const T_BOB: &str = "t-bob";

struct Fixture {
    app: Router,
    ws: Arc<Workspace>,
}

/// `owner` reads everything; `bob` reads `/proj` and nothing else. Both files
/// exist, and there is a commit so `diff` has two ends.
async fn fixture() -> Fixture {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();

    let owner = ws.create_human("owner", None).await.unwrap();
    let octx = WriteCtx::actor(owner);
    ws.grant(owner, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    let bob = ws.create_agent("bob", "opus", None).await.unwrap();
    ws.grant(bob, "/proj", Perms::READ, None).await.unwrap();

    ws.mkdir_as(octx, "/proj").await.unwrap();
    ws.write_as(octx, "/proj/open.md", b"shared\n")
        .await
        .unwrap();
    ws.write_as(octx, "/secret.md", b"private\n").await.unwrap();
    ws.commit_as(octx, "owner", "base").await.unwrap();
    ws.write_as(octx, "/secret.md", b"private v2\n")
        .await
        .unwrap();
    ws.commit_as(octx, "owner", "secret v2").await.unwrap();

    let auth = BearerAuth::new()
        .with_token(T_OWNER, owner, None)
        .with_token(T_BOB, bob, None);
    let ws = Arc::new(ws);
    Fixture {
        app: router(ws.clone(), Arc::new(auth)),
        ws,
    }
}

fn get(uri: &str, token: Option<&str>) -> Request<Body> {
    let b = Request::builder().method("GET").uri(uri);
    let b = match token {
        Some(t) => b.header("authorization", format!("Bearer {t}")),
        None => b,
    };
    b.body(Body::empty()).unwrap()
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// Every route that reveals a path or its content, addressed at `/secret.md`.
fn secret_routes() -> Vec<&'static str> {
    vec![
        "/v1/files/secret.md",
        "/v1/stat/secret.md",
        "/v1/blame/secret.md",
        "/v1/diff/file?from=HEAD&to=HEAD&path=/secret.md",
    ]
}

// --- off by default ----------------------------------------------------------

#[tokio::test]
async fn reads_stay_open_and_anonymous_while_the_switch_is_off() {
    // The migration invariant, restated at the surface: adding the extractor
    // changes nothing for a workspace that has not opted in, credential or not.
    let f = fixture().await;
    for uri in secret_routes() {
        let (anon, _) = send(&f.app, get(uri, None)).await;
        let (named, _) = send(&f.app, get(uri, Some(T_BOB))).await;
        assert_eq!(anon, StatusCode::OK, "anonymous {uri}");
        assert_eq!(named, StatusCode::OK, "bob {uri}");
    }
}

// --- with the switch on ------------------------------------------------------

#[tokio::test]
async fn an_anonymous_read_is_refused_once_reads_are_enforced() {
    // The hole this extractor closes. Reads are open by default on this surface,
    // so a read handler cannot demand a credential unconditionally; it has to
    // demand one exactly when the workspace has something to check it against.
    // Otherwise every ACL on the workspace is one absent header deep.
    let f = fixture().await;
    f.ws.set_acl_enforce_reads(true).await.unwrap();

    for uri in secret_routes() {
        let (status, _) = send(&f.app, get(uri, None)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "anonymous {uri}");
    }
}

#[tokio::test]
async fn a_denied_actor_is_refused_every_per_path_read() {
    let f = fixture().await;
    f.ws.set_acl_default_deny(true).await.unwrap();
    f.ws.set_acl_enforce_reads(true).await.unwrap();

    for uri in secret_routes() {
        let (status, _) = send(&f.app, get(uri, Some(T_BOB))).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "bob {uri}");
    }
}

#[tokio::test]
async fn a_granted_actor_still_reads_what_it_holds() {
    let f = fixture().await;
    f.ws.set_acl_default_deny(true).await.unwrap();
    f.ws.set_acl_enforce_reads(true).await.unwrap();

    for uri in [
        "/v1/files/proj/open.md",
        "/v1/stat/proj/open.md",
        "/v1/blame/proj/open.md",
        "/v1/dirs/proj",
    ] {
        let (status, _) = send(&f.app, get(uri, Some(T_BOB))).await;
        assert_eq!(status, StatusCode::OK, "bob {uri}");
    }
}

#[tokio::test]
async fn a_listing_hides_what_the_actor_may_not_stat() {
    // The pair property, over HTTP: `GET /v1/dirs` and `GET /v1/stat/{path}`
    // must agree, or the difference between them is an existence oracle.
    let f = fixture().await;
    f.ws.set_acl_default_deny(true).await.unwrap();
    f.ws.set_acl_enforce_reads(true).await.unwrap();

    let (status, body) = send(&f.app, get("/v1/dirs", Some(T_OWNER))).await;
    assert_eq!(status, StatusCode::OK);
    let owner_names: Vec<String> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_string())
        .collect();
    assert!(owner_names.contains(&"secret.md".to_string()));

    // bob may list `/`? No — his grant is at /proj, so the directory check
    // refuses before any entry is considered. That is the correct answer, and
    // distinct from "the directory is empty".
    let (status, _) = send(&f.app, get("/v1/dirs", Some(T_BOB))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Give bob the root, and now the *entries* are what filters.
    f.ws.grant(
        f.ws.list_actors()
            .await
            .unwrap()
            .into_iter()
            .find(|a| a.display_name == "bob")
            .unwrap()
            .id,
        "/",
        Perms::READ,
        None,
    )
    .await
    .unwrap();
    f.ws.grant(
        f.ws.list_actors()
            .await
            .unwrap()
            .into_iter()
            .find(|a| a.display_name == "bob")
            .unwrap()
            .id,
        "/secret.md",
        Perms::NONE,
        None,
    )
    .await
    .unwrap();

    let (status, body) = send(&f.app, get("/v1/dirs", Some(T_BOB))).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<String> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_string())
        .collect();
    assert!(!names.contains(&"secret.md".to_string()), "{names:?}");
    let (status, _) = send(&f.app, get("/v1/stat/secret.md", Some(T_BOB))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_whole_tree_diff_drops_the_paths_the_actor_cannot_read() {
    let f = fixture().await;
    f.ws.set_acl_default_deny(true).await.unwrap();
    f.ws.set_acl_enforce_reads(true).await.unwrap();

    let log = f.ws.log().await.unwrap();
    let (head, base) = (log[0].hash.to_hex(), log[1].hash.to_hex());
    let uri = &format!("/v1/diff?from={base}&to={head}");
    let (status, body) = send(&f.app, get(uri, Some(T_OWNER))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.as_array()
            .unwrap()
            .iter()
            .any(|e| e["path"] == "/secret.md"),
        "the owner should see the change: {body}"
    );

    let (status, body) = send(&f.app, get(uri, Some(T_BOB))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.as_array().unwrap().is_empty(),
        "bob may not read /secret.md, so it must not appear in his diff: {body}"
    );
}

#[tokio::test]
async fn a_suggestion_against_an_unreadable_path_is_not_found_rather_than_denied() {
    // A suggestion id is a guessable, workspace-global handle, so a 403 would
    // confirm that a proposal exists at that id — the existence answer the check
    // is there to withhold. The engine makes this ruling, not the surface.
    let f = fixture().await;
    let owner =
        f.ws.list_actors()
            .await
            .unwrap()
            .into_iter()
            .find(|a| a.display_name == "owner")
            .unwrap()
            .id;
    let id =
        f.ws.suggest(
            WriteCtx::actor(owner),
            "/secret.md",
            b"proposed\n",
            None,
            None,
        )
        .await
        .unwrap();
    f.ws.set_acl_default_deny(true).await.unwrap();
    f.ws.set_acl_enforce_reads(true).await.unwrap();

    for uri in [
        format!("/v1/suggestions/{id}"),
        format!("/v1/suggestions/{id}/diff"),
    ] {
        let (status, _) = send(&f.app, get(&uri, Some(T_BOB))).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
    }
    // And it is still there for someone who may read the path.
    let (status, _) = send(&f.app, get(&format!("/v1/suggestions/{id}"), Some(T_OWNER))).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(&f.app, get("/v1/suggestions", Some(T_BOB))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.as_array().unwrap().is_empty(),
        "the queue must not list a proposal bob cannot read: {body}"
    );
}

// --- the structural guard ----------------------------------------------------

/// Every `GET` handler binds [`ReadAuth`], or is named here with a reason.
///
/// The sibling of `api_write_policy.rs::every_mutating_route_binds_its_principal`,
/// and for the same reason: the bug it catches is invisible to behavioural tests
/// on the routes that exist today. A new `GET` that takes `State(ws)` and calls
/// the unattributed engine method is a silent hole in read enforcement, and the
/// only moment anyone would notice is the moment someone writes this list entry.
#[test]
fn every_read_route_binds_its_read_auth() {
    // Reads that reveal no path and therefore have nothing to check. Each entry
    // is a claim that the route's response cannot tell a caller about a path it
    // may not read.
    const NO_READ_GATE_NEEDED: &[&str] = &[
        // Liveness and readiness. Outside `/v1`, no workspace data at all.
        "health",
        "readyz",
        // Prometheus text. Every metric label is a closed set — never a path,
        // actor or hash — which `CLAUDE.md` requires and this entry depends on.
        "metrics_endpoint",
        // Commit metadata: hash, author, message, timestamp. No paths. `diff`
        // is the route that turns a commit into paths, and it is gated.
        "log",
        // Branch names and the current head. Refs are workspace-wide, not
        // path-scoped, and a scoped router already refuses the route outright.
        "list_branches",
        // The change feed. Events *do* carry paths, so this one is a genuine
        // gap rather than a non-issue: filtering it needs a cursor the wire
        // format does not have. `watch(after_seq)` returns the events after a
        // sequence number and the client advances to the highest it saw, so
        // dropping rows would leave a client that can see none of them polling
        // the same cursor forever while the log grows behind it. Gating it
        // means giving the response a high-water mark first, which changes a
        // published response shape. Tracked, not forgotten.
        "events",
    ];

    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/api/mod.rs"),
    )
    .unwrap();

    let mut handlers: Vec<String> = Vec::new();
    let verb = "get(";
    let mut from = 0usize;
    while let Some(at) = src[from..].find(verb) {
        let start = from + at + verb.len();
        let name: String = src[start..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        from = start;
        // `get(` also appears in imports, doc comments and `HashMap::get(k)`; a
        // real registration names an fn defined below.
        if !name.is_empty() && src.contains(&format!("\nasync fn {name}(")) {
            handlers.push(name);
        }
    }
    handlers.sort();
    handlers.dedup();

    assert!(
        handlers.len() >= 12,
        "route scan found only {} GET handlers — the scan is broken, not the \
         router: {handlers:?}",
        handlers.len()
    );

    let mut offenders = Vec::new();
    for name in &handlers {
        if NO_READ_GATE_NEEDED.contains(&name.as_str()) {
            continue;
        }
        // Anchored at column 0. `ReadAuth` has helper methods named after the
        // handlers they dispatch (`stat`, `blame`, `diff`…) and they are
        // indented; matching unanchored found those instead and reported every
        // gated route as ungated.
        let at = src.find(&format!("\nasync fn {name}(")).unwrap();
        let sig_end = src[at..].find(") ->").map(|e| at + e).unwrap_or(at);
        let sig = &src[at..sig_end];
        if !sig.contains("ReadAuth") || sig.contains("_who: ReadAuth") {
            offenders.push(name.clone());
        }
    }

    assert!(
        offenders.is_empty(),
        "these read routes do not bind `ReadAuth`, so they answer without \
         consulting `Perms::READ` and `acl_enforce_reads` is decoration on \
         them:\n  {}\n\nBind `who: ReadAuth` and read through it, or add the \
         route to NO_READ_GATE_NEEDED with a reason.",
        offenders.join("\n  ")
    );
}
