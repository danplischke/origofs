//! HTTP surface: files, versioning, attribution, and the collaboration feed,
//! driven in-process through the router (no socket) via `tower::oneshot`.
//!
//! Mutations are authenticated: the fixture maps bearer tokens to actors the
//! server pre-provisioned, so attribution is derived from the verified identity —
//! never from the request. Reads are open.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use origofs_api::{router, router_with, ApiOptions, BearerAuth};
use origofs_sdk::Workspace;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const T_ADMIN: &str = "t-admin";
const T_AGENT: &str = "t-agent";
const T_HUMAN: &str = "t-human";

struct Fixture {
    app: Router,
    agent: i64,
    human: i64,
    session: i64,
}

async fn fixture() -> Fixture {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    // The server owns identity: actors exist in the DB and tokens map to them.
    let admin = ws.create_human("admin", None).await.unwrap();
    let agent = ws.create_agent("claude", "opus", None).await.unwrap();
    let human = ws.create_human("dan", None).await.unwrap();
    let session = ws.create_session(agent, Some("api")).await.unwrap();
    let auth = BearerAuth::new()
        .with_token(T_ADMIN, admin, None)
        .with_token(T_AGENT, agent, Some(session))
        .with_token(T_HUMAN, human, None);
    Fixture {
        app: router(Arc::new(ws), Arc::new(auth)),
        agent,
        human,
        session,
    }
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

fn as_json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap()
}

// --- read builders (open) ---------------------------------------------------

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

fn get_as(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

// --- unauthenticated mutation builders (for negative tests) -----------------

fn put_bytes(uri: &str, body: &[u8]) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .body(Body::from(body.to_vec()))
        .unwrap()
}

fn post_json(uri: &str, v: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&v).unwrap()))
        .unwrap()
}

// --- authenticated mutation builders ----------------------------------------

fn put_as(uri: &str, token: &str, body: &[u8]) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_vec()))
        .unwrap()
}

fn delete_as(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn post_json_as(uri: &str, token: &str, v: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&v).unwrap()))
        .unwrap()
}

fn post_bytes_as(uri: &str, token: &str, body: &[u8]) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_vec()))
        .unwrap()
}

fn post_empty_as(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn files_roundtrip_and_listing() {
    let fx = fixture().await;
    let app = &fx.app;

    // write (auto-creates the parent dir), authenticated
    let (st, body) = send(app, put_as("/files/notes/todo.txt", T_HUMAN, b"buy milk\n")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(as_json(&body)["written"], 9);

    // read it back verbatim (reads are open)
    let (st, body) = send(app, get("/files/notes/todo.txt")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, b"buy milk\n");

    // list the directory
    let (st, body) = send(app, get("/dirs/notes")).await;
    assert_eq!(st, StatusCode::OK);
    let entries = as_json(&body);
    assert_eq!(entries[0]["name"], "todo.txt");
    assert_eq!(entries[0]["kind"], "file");

    // stat
    let (st, body) = send(app, get("/stat/notes/todo.txt")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(as_json(&body)["size"], 9);
    assert_eq!(as_json(&body)["kind"], "file");

    // delete
    let (st, _) = send(app, delete_as("/files/notes/todo.txt", T_HUMAN)).await;
    assert_eq!(st, StatusCode::OK);
    let (st, _) = send(app, get("/files/notes/todo.txt")).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn missing_file_is_404_and_dir_read_is_400() {
    let fx = fixture().await;
    let app = &fx.app;
    let (st, body) = send(app, get("/files/nope.txt")).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert!(as_json(&body)["error"]
        .as_str()
        .unwrap()
        .contains("not found"));

    send(app, post_json_as("/dirs/adir", T_HUMAN, json!({}))).await;
    let (st, _) = send(app, get("/files/adir")).await; // reading a directory
    assert_eq!(st, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn large_file_streams_back_intact() {
    // A multi-chunk file (> MAX_CHUNK) round-trips over HTTP: the GET streams the
    // body chunk-by-chunk (never buffering it whole server-side) and reassembles
    // byte-for-byte. Deterministic pseudo-random bytes give real CDC boundaries.
    let fx = fixture().await;
    let app = &fx.app;

    let mut content = Vec::with_capacity(600 * 1024);
    let mut x: u64 = 0x1234_5678_9ABC_DEF0;
    while content.len() < 600 * 1024 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        content.extend_from_slice(&x.to_le_bytes());
    }

    let (st, _) = send(app, put_as("/files/big.bin", T_HUMAN, &content)).await;
    assert_eq!(st, StatusCode::OK);

    let (st, body) = send(app, get("/files/big.bin")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body, content, "streamed body reassembles byte-for-byte");
}

#[tokio::test]
async fn versioning_over_http() {
    let fx = fixture().await;
    let app = &fx.app;
    send(app, put_as("/files/a.txt", T_HUMAN, b"one")).await;
    // the commit author is the authenticated actor, not a client-supplied field
    let (st, body) = send(
        app,
        post_json_as("/commit", T_HUMAN, json!({"message": "first"})),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(as_json(&body)["hash"].as_str().unwrap().len() >= 12);

    let (st, body) = send(app, get("/log")).await;
    assert_eq!(st, StatusCode::OK);
    let log = as_json(&body);
    assert_eq!(log.as_array().unwrap().len(), 1);
    assert_eq!(log[0]["message"], "first");
    assert_eq!(log[0]["author"], "dan"); // derived from the T_HUMAN actor

    // a branch shows up as current
    let (_st, body) = send(app, get("/branches")).await;
    let branches = as_json(&body);
    assert!(branches
        .as_array()
        .unwrap()
        .iter()
        .any(|b| b["name"] == "main" && b["current"] == true));
}

#[tokio::test]
async fn suggestion_review_over_http() {
    let fx = fixture().await;
    let app = &fx.app;

    send(app, put_as("/files/notes.txt", T_HUMAN, b"one\ntwo\n")).await;

    // agent proposes an edit — attributed to the T_AGENT identity, not a query arg
    let (st, body) = send(
        app,
        post_bytes_as(
            "/suggestions?path=/notes.txt&summary=fix",
            T_AGENT,
            b"one\nTWO\n",
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let id = as_json(&body)["id"].as_i64().unwrap();

    // it's pending and the working tree is untouched
    let (_st, body) = send(app, get("/suggestions?status=pending")).await;
    let list = as_json(&body);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["actor_id"], fx.agent);
    assert_eq!(list[0]["summary"], "fix");
    let (_st, body) = send(app, get("/files/notes.txt")).await;
    assert_eq!(body, b"one\ntwo\n");

    // its diff renders base -> proposed
    let (_st, body) = send(app, get(&format!("/suggestions/{id}/diff"))).await;
    let patch = as_json(&body)["diff"].as_str().unwrap().to_string();
    assert!(patch.contains("-two") && patch.contains("+TWO"), "{patch}");

    // human accepts -> applied, credited to the T_HUMAN identity
    let (st, _) = send(
        app,
        post_empty_as(&format!("/suggestions/{id}/accept"), T_HUMAN),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (_st, body) = send(app, get("/files/notes.txt")).await;
    assert_eq!(body, b"one\nTWO\n");
    let (_st, body) = send(app, get(&format!("/suggestions/{id}"))).await;
    assert_eq!(as_json(&body)["status"], "accepted");
    assert_eq!(as_json(&body)["resolved_by"], fx.human);
}

#[tokio::test]
async fn diff_between_branches_over_http() {
    let fx = fixture().await;
    let app = &fx.app;
    send(app, put_as("/files/edit.txt", T_HUMAN, b"one\ntwo\n")).await;
    send(app, put_as("/files/keep.txt", T_HUMAN, b"same\n")).await;
    send(
        app,
        post_json_as("/commit", T_HUMAN, json!({"message": "base"})),
    )
    .await;
    send(
        app,
        post_json_as("/branches", T_HUMAN, json!({"name": "feature"})),
    )
    .await;
    send(
        app,
        post_json_as("/checkout", T_HUMAN, json!({"name": "feature"})),
    )
    .await;
    send(app, put_as("/files/edit.txt", T_HUMAN, b"one\nTWO\n")).await;
    send(app, put_as("/files/new.txt", T_HUMAN, b"added\n")).await;
    send(
        app,
        post_json_as("/commit", T_HUMAN, json!({"message": "work"})),
    )
    .await;

    // changed-path list
    let (st, body) = send(app, get("/diff?from=main&to=feature")).await;
    assert_eq!(st, StatusCode::OK);
    let changes = as_json(&body);
    let arr = changes.as_array().unwrap();
    assert_eq!(
        arr.len(),
        2,
        "edit.txt modified + new.txt added; keep.txt unchanged"
    );
    assert!(arr
        .iter()
        .any(|d| d["path"] == "/edit.txt" && d["status"] == "modified"));
    assert!(arr
        .iter()
        .any(|d| d["path"] == "/new.txt" && d["status"] == "added"));

    // per-file unified diff
    let (st, body) = send(app, get("/diff/file?from=main&to=feature&path=/edit.txt")).await;
    assert_eq!(st, StatusCode::OK);
    let patch = as_json(&body)["diff"].as_str().unwrap().to_string();
    assert!(patch.contains("-two") && patch.contains("+TWO"), "{patch}");

    // unchanged file -> empty diff
    let (_st, body) = send(app, get("/diff/file?from=main&to=feature&path=/keep.txt")).await;
    assert!(as_json(&body)["diff"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn attributed_write_shows_up_in_blame_and_feed() {
    let fx = fixture().await;
    let app = &fx.app;

    // write as the agent — identity comes from the token, not the request
    let (st, _) = send(
        app,
        put_as("/files/notes.txt", T_AGENT, b"line one\nline two\n"),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // blame attributes it to the agent
    let (st, body) = send(app, get("/blame/notes.txt")).await;
    assert_eq!(st, StatusCode::OK);
    let blame = as_json(&body);
    assert_eq!(blame[0]["kind"], "agent");
    assert_eq!(blame[0]["actor"], "claude");

    // the write is on the change feed, attributed to the token's actor + session
    let (st, body) = send(app, get("/events")).await;
    assert_eq!(st, StatusCode::OK);
    let events = as_json(&body);
    let write = events
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "write")
        .unwrap();
    assert_eq!(write["actor_id"].as_i64(), Some(fx.agent));
    assert_eq!(write["session_id"].as_i64(), Some(fx.session));
}

// SEC (security audit #1/#3): mutations require an authenticated principal, and
// attribution is derived from it — a client cannot write/commit/mint-actors
// anonymously, nor forge a different actor via the request.
#[tokio::test]
async fn unauthenticated_and_forged_mutations_are_rejected() {
    let fx = fixture().await;
    let app = &fx.app;

    // no credential -> 401
    let (st, _) = send(app, put_bytes("/files/x.txt", b"hi")).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // a bogus token -> 401
    let (st, _) = send(app, put_as("/files/x.txt", "not-a-real-token", b"hi")).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // no anonymous identity fabrication: creating an actor requires auth
    let (st, _) = send(app, post_json("/actors", json!({"name": "Dan (CTO)"}))).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // nothing was written by any of the above
    let (st, _) = send(app, get("/files/x.txt")).await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // a leftover `?actor=` query is ignored: attribution follows the token, so a
    // human-token write can't be booked to the agent by naming its id.
    let uri = format!("/files/forge.txt?actor={}&session=999", fx.agent);
    let (st, _) = send(app, put_as(&uri, T_HUMAN, b"not the agent\n")).await;
    assert_eq!(st, StatusCode::OK);
    let (_st, body) = send(app, get("/blame/forge.txt")).await;
    let blame = as_json(&body);
    assert_eq!(
        blame[0]["actor"], "dan",
        "attribution must follow the token, not ?actor="
    );
    assert_eq!(blame[0]["kind"], "human");
}

// SEC (security audit #8): reads are open by default, but `gate_reads` requires a
// credential for reads too — closing the full-disclosure surface for operators
// who want it. /health stays open regardless.
#[tokio::test]
async fn gate_reads_requires_a_credential_for_reads() {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let human = ws.create_human("dan", None).await.unwrap();
    let auth = BearerAuth::new().with_token(T_HUMAN, human, None);
    let app = router_with(
        Arc::new(ws),
        Arc::new(auth),
        ApiOptions { gate_reads: true },
    );

    // /health stays open even when reads are gated.
    let (st, _) = send(&app, get("/health")).await;
    assert_eq!(st, StatusCode::OK);

    // a read without a credential is now rejected...
    let (st, _) = send(&app, get("/log")).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    let (st, _) = send(&app, get("/files/anything.txt")).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // ...but works with a valid token.
    let (st, _) = send(&app, get_as("/log", T_HUMAN)).await;
    assert_eq!(st, StatusCode::OK);
}
