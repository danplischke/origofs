//! HTTP/JSON API surface (`api` feature) — over a workspace (`docs/DESIGN.md` §6, M7).
//!
//! A thin [`axum`] layer that exposes the same operations as the `origofs` CLI to any
//! HTTP client: read/write/list files, versioning (commit/log/branches/checkout),
//! attribution (blame), and the live-collaboration feed + presence. Everything
//! goes through [`crate::Workspace`], so writes are recorded on the change feed
//! and attributed exactly as they are everywhere else.
//!
//! **Authentication.** Every mutating route requires an authenticated
//! [`Principal`], resolved from the request by an [`Authenticator`] you supply to
//! [`router`]/[`serve`]. The actor a write is attributed to comes from that
//! verified identity, never from a request field, so a client cannot forge
//! attribution or mint identities anonymously. [`BearerAuth`] is a ready-made
//! `Authorization: Bearer` token→actor map; implement [`Authenticator`] for
//! anything dynamic (JWT, a session DB). Reads are open by default — pass
//! `gate_reads` to [`router_with`] (or gate at your proxy) to require a credential
//! for reads too. This mirrors the Python `origofs.fastapi.build_router` model.
//!
//! Files are transferred as raw bytes (`application/octet-stream`); everything
//! else is JSON. Paths are the URL tail after the resource segment, e.g.
//! `GET /files/notes/todo.txt` reads `/notes/todo.txt`.

use crate::{Workspace, WriteCtx, WriteOutcome};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, FromRef, FromRequestParts, Path, Query, Request, State},
    http::{HeaderMap, StatusCode, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::TraceLayer;

#[cfg(feature = "coedit")]
mod coedit;
#[cfg(feature = "coedit")]
pub use coedit::Coordinator;

type Shared = Arc<Workspace>;

// --- authentication ---------------------------------------------------------

/// The authenticated identity behind a request: the origofs actor a mutation is
/// attributed to, plus an optional session. origofs never trusts a client-named
/// actor — this is always resolved by an [`Authenticator`], never read from the
/// request body or query string.
#[derive(Clone, Copy, Debug)]
pub struct Principal {
    pub actor: i64,
    pub session: Option<i64>,
}

impl Principal {
    fn write_ctx(&self) -> WriteCtx {
        match self.session {
            Some(s) => WriteCtx::session(self.actor, s),
            None => WriteCtx::actor(self.actor),
        }
    }
}

/// Resolves a request's credentials to a [`Principal`]. The embedder owns
/// identity: decode your bearer token / session cookie / mTLS identity here and
/// map it to the origofs actor it should be attributed to. Return `None` to reject
/// the request with `401`. This is the Rust counterpart to the `authn`
/// dependency of the Python `origofs.fastapi.build_router`.
#[async_trait::async_trait]
pub trait Authenticator: Send + Sync + 'static {
    async fn authenticate(&self, headers: &HeaderMap) -> Option<Principal>;
}

/// A static `Authorization: Bearer <token>` → actor map. A reasonable default
/// when tokens are minted out of band; for anything dynamic (JWT, a session DB)
/// implement [`Authenticator`] yourself.
#[derive(Clone, Default)]
pub struct BearerAuth {
    tokens: HashMap<String, Principal>,
}

impl BearerAuth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Map a bearer token to an actor (and optional session).
    pub fn with_token(
        mut self,
        token: impl Into<String>,
        actor: i64,
        session: Option<i64>,
    ) -> Self {
        self.tokens
            .insert(token.into(), Principal { actor, session });
        self
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

#[async_trait::async_trait]
impl Authenticator for BearerAuth {
    async fn authenticate(&self, headers: &HeaderMap) -> Option<Principal> {
        let token = headers
            .get(axum::http::header::AUTHORIZATION)?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")?
            .trim();
        self.tokens.get(token).copied()
    }
}

/// Attributes **every** request to one fixed principal with no credential check.
/// For local single-user dev only — the CLI uses it when `origofs serve` runs on a
/// loopback address with no tokens configured. Never expose it publicly.
pub struct LocalDevAuth(pub Principal);

#[async_trait::async_trait]
impl Authenticator for LocalDevAuth {
    async fn authenticate(&self, _headers: &HeaderMap) -> Option<Principal> {
        Some(self.0)
    }
}

/// Router state: the workspace plus the authenticator. Handlers pull the
/// workspace via `State<Arc<Workspace>>` and the identity via the [`Auth`]
/// extractor, both through `FromRef`.
#[derive(Clone)]
struct AppState {
    ws: Arc<Workspace>,
    auth: Arc<dyn Authenticator>,
    /// The live co-editing room registry (roadmap M8). Shared across sockets, so
    /// it lives on the state rather than being opened per request like the rest.
    #[cfg(feature = "coedit")]
    coedit: Coordinator,
}

impl FromRef<AppState> for Shared {
    fn from_ref(s: &AppState) -> Shared {
        s.ws.clone()
    }
}

/// The authenticated principal, extracted per request. Rejects with `401` when
/// the [`Authenticator`] returns `None`, so every handler that takes it can only
/// run for a verified identity.
struct Auth(Principal);

impl FromRequestParts<AppState> for Auth {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        match state.auth.authenticate(&parts.headers).await {
            Some(p) => Ok(Auth(p)),
            None => Err(ApiError::status(
                StatusCode::UNAUTHORIZED,
                "unauthenticated: a valid credential is required",
            )),
        }
    }
}

/// Options for [`router_with`].
#[derive(Clone)]
pub struct ApiOptions {
    /// Require an authenticated principal for reads too, not only mutations. Off
    /// by default: reads are open (parity with the Python `build_router`). When
    /// on, every data route demands a valid credential (`/health`/`/readyz` stay
    /// open).
    pub gate_reads: bool,
    /// Origins allowed by CORS (e.g. `https://app.example.com`). Empty means no
    /// cross-origin access — same-origin only — which is the safe default; a
    /// browser client on another origin needs its origin listed here.
    pub cors_origins: Vec<String>,
    /// Maximum request-body size in bytes for uploads (`PUT /v1/files/…`).
    /// Defaults to 1 GiB.
    pub max_body_bytes: usize,
}

impl Default for ApiOptions {
    fn default() -> Self {
        Self {
            gate_reads: false,
            cors_origins: Vec::new(),
            max_body_bytes: 1 << 30,
        }
    }
}

/// Build the router for a workspace. Every mutating route requires an
/// authenticated [`Principal`]; reads are open by default — use [`router_with`]
/// with `gate_reads` to require a credential on reads too.
pub fn router(ws: Shared, auth: Arc<dyn Authenticator>) -> Router {
    router_with(ws, auth, ApiOptions::default())
}

/// Like [`router`], with [`ApiOptions`] (e.g. `gate_reads`).
pub fn router_with(ws: Shared, auth: Arc<dyn Authenticator>, options: ApiOptions) -> Router {
    let state = AppState {
        #[cfg(feature = "coedit")]
        coedit: Coordinator::new(ws.clone()),
        ws,
        auth,
    };
    let mut data = Router::new()
        .route(
            "/files/{*path}",
            get(read_file).put(write_file).delete(delete_file),
        )
        .route("/dirs", get(list_root).post(make_root))
        .route("/dirs/{*path}", get(list_dir).post(make_dir))
        .route("/stat/{*path}", get(stat))
        .route("/blame/{*path}", get(blame))
        .route("/rename", post(rename))
        .route("/commit", post(commit))
        .route("/log", get(log))
        .route("/diff", get(diff))
        .route("/diff/file", get(diff_file))
        .route("/branches", get(list_branches).post(create_branch))
        .route("/checkout", post(checkout))
        .route("/events", get(events))
        .route("/presence", get(presence))
        .route(
            "/suggestions",
            get(list_suggestions).post(create_suggestion),
        )
        .route("/suggestions/{id}", get(get_suggestion))
        .route("/suggestions/{id}/diff", get(suggestion_diff))
        .route("/suggestions/{id}/accept", post(accept_suggestion))
        .route("/suggestions/{id}/reject", post(reject_suggestion))
        .route("/actors", post(create_actor))
        .route("/sessions", post(create_session));
    if options.gate_reads {
        // Require a valid credential for every data route, reads included.
        // Mutations already enforce it in-handler; this closes reads too.
        data = data.route_layer(middleware::from_fn_with_state(state.clone(), require_auth));
    }
    // The co-editing WebSocket authenticates itself (it accepts a `?token=` query
    // param a browser can't send as a header), so it sits outside the read gate.
    #[cfg(feature = "coedit")]
    {
        data = data.route("/coedit/{*path}", get(coedit::coedit_ws));
    }
    // Versioned data surface. Liveness/readiness stay unversioned at the root so an
    // orchestrator probes them independent of the API version.
    let app = Router::new()
        .nest("/v1", data)
        .route("/health", get(health))
        .route("/readyz", get(readyz))
        .with_state(state);
    // Cross-cutting middleware (outermost first): an `x-request-id` set on the
    // request and echoed on the response, a tracing span per request, a
    // request-body size cap, and CORS for browser clients. CORS sits innermost so
    // it wraps the router's plain body (it requires a `Default` response body,
    // which the trace layer's wrapped body is not).
    app.layer(
        ServiceBuilder::new()
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
            .layer(PropagateRequestIdLayer::x_request_id())
            .layer(TraceLayer::new_for_http())
            .layer(DefaultBodyLimit::max(options.max_body_bytes))
            .layer(cors_layer(&options)),
    )
}

/// Build the CORS layer from the configured origins. Empty means no cross-origin
/// access is granted (same-origin only) — the safe default.
fn cors_layer(options: &ApiOptions) -> CorsLayer {
    use axum::http::{Method, header};
    let origins: Vec<axum::http::HeaderValue> = options
        .cors_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();
    CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
        .allow_origin(AllowOrigin::list(origins))
}

/// Middleware that rejects with `401` unless the request carries a credential the
/// [`Authenticator`] accepts. Applied to reads only when `gate_reads` is set
/// (mutations always gate in-handler via the [`Auth`] extractor).
async fn require_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    match state.auth.authenticate(req.headers()).await {
        Some(_) => next.run(req).await,
        None => ApiError::status(
            StatusCode::UNAUTHORIZED,
            "unauthenticated: a valid credential is required",
        )
        .into_response(),
    }
}

/// Serve the workspace over HTTP, blocking until the server stops.
pub async fn serve(
    ws: Shared,
    addr: SocketAddr,
    auth: Arc<dyn Authenticator>,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(ws, auth)).await
}

/// Normalize a URL-tail path to an absolute origofs path.
fn abspath(p: &str) -> String {
    if p.starts_with('/') {
        p.to_string()
    } else {
        format!("/{p}")
    }
}

// --- error mapping ----------------------------------------------------------

/// An HTTP error: either a mapped [`crate::OrigoFSError`] or an explicit status
/// (e.g. `401` from the [`Auth`] extractor).
enum ApiError {
    OrigoFS(crate::OrigoFSError),
    Status(StatusCode, String),
}

impl ApiError {
    fn status(code: StatusCode, msg: impl Into<String>) -> Self {
        ApiError::Status(code, msg.into())
    }
}

impl From<crate::OrigoFSError> for ApiError {
    fn from(e: crate::OrigoFSError) -> Self {
        ApiError::OrigoFS(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        use crate::OrigoFSError::*;
        // (status, machine-readable code, human message, retryable)
        let (status, code, message, retryable) = match self {
            ApiError::Status(status, msg) => {
                let code = match status {
                    StatusCode::UNAUTHORIZED => "unauthenticated",
                    StatusCode::FORBIDDEN => "forbidden",
                    _ => "error",
                };
                (status, code, msg, false)
            }
            ApiError::OrigoFS(e) => {
                let status = match &e {
                    NotFound(_) | ContentMissing(_) => StatusCode::NOT_FOUND,
                    AlreadyExists(_) | Conflict(_) => StatusCode::CONFLICT,
                    IsADirectory(_) | NotADirectory(_) | DirectoryNotEmpty(_) | InvalidPath(_)
                    | InvalidArgument(_) => StatusCode::BAD_REQUEST,
                    // A transient backend failure: tell the client it may retry.
                    e if e.retryable() => StatusCode::SERVICE_UNAVAILABLE,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (status, e.code(), e.to_string(), e.retryable())
            }
        };
        // Machine-readable envelope: a stable `code` a client can branch on, the
        // human `message`, and whether the operation is safe to retry.
        let mut resp = (
            status,
            Json(json!({
                "error": { "code": code, "message": message, "retryable": retryable }
            })),
        )
            .into_response();
        if retryable {
            resp.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
        }
        resp
    }
}

type ApiResult<T> = Result<T, ApiError>;

// --- files ------------------------------------------------------------------

/// Liveness: the process is up and serving HTTP. Does no I/O, so it stays `200`
/// even while a backend is down — that is what `/readyz` is for.
async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

/// Readiness: probe the backing stores. `200` when both answer, `503` (with the
/// per-store detail) when either is unreachable — so a load balancer or a k8s
/// readiness probe pulls this instance out of rotation until its database and
/// content store recover, instead of routing requests it cannot serve.
async fn readyz(State(ws): State<Shared>) -> Response {
    let report = ws.ready().await;
    let store = |probe: &Option<String>| match probe {
        Some(err) => json!({ "ok": false, "error": err }),
        None => json!({ "ok": true }),
    };
    let body = json!({
        "ready": report.is_ready(),
        "metadata": store(&report.metadata),
        "content": store(&report.content),
    });
    let status = if report.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body)).into_response()
}

async fn read_file(State(ws): State<Shared>, Path(path): Path<String>) -> ApiResult<Response> {
    // Stream the body so an arbitrarily large file is never buffered server-side.
    // `read_stream` resolves and validates first, so a missing file (or a
    // directory) is still a clean error here, before any bytes are streamed.
    let stream = ws.read_stream(&abspath(&path)).await?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(stream),
    )
        .into_response())
}

async fn write_file(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    Path(path): Path<String>,
    body: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let p = abspath(&path);
    if let Some((parent, _)) = p.rsplit_once('/')
        && !parent.is_empty()
    {
        ws.mkdir_p(parent).await?;
    }
    // Attribution comes only from the authenticated principal — never the request.
    // Governed by the principal's write policy: a propose-only actor's edit is
    // queued for review rather than landing directly.
    match ws
        .write_or_propose(principal.write_ctx(), &p, &body, None)
        .await?
    {
        WriteOutcome::Wrote => Ok(Json(json!({ "path": p, "written": body.len() }))),
        WriteOutcome::Proposed(id) => Ok(Json(json!({ "path": p, "proposed": id }))),
    }
}

async fn delete_file(
    State(ws): State<Shared>,
    _auth: Auth,
    Path(path): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let p = abspath(&path);
    ws.remove(&p).await?;
    Ok(Json(json!({ "removed": p })))
}

// --- directories ------------------------------------------------------------

#[derive(Serialize)]
struct EntryDto {
    name: String,
    kind: String,
}

async fn list_path(ws: &Workspace, path: &str) -> ApiResult<Json<Vec<EntryDto>>> {
    let entries = ws
        .ls(path)
        .await?
        .into_iter()
        .map(|e| EntryDto {
            name: e.name,
            kind: e.kind.as_str().to_string(),
        })
        .collect();
    Ok(Json(entries))
}

async fn list_root(State(ws): State<Shared>) -> ApiResult<Json<Vec<EntryDto>>> {
    list_path(&ws, "/").await
}

async fn list_dir(
    State(ws): State<Shared>,
    Path(path): Path<String>,
) -> ApiResult<Json<Vec<EntryDto>>> {
    list_path(&ws, &abspath(&path)).await
}

async fn make_root(State(_ws): State<Shared>, _auth: Auth) -> ApiResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "created": "/" })))
}

async fn make_dir(
    State(ws): State<Shared>,
    _auth: Auth,
    Path(path): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let p = abspath(&path);
    ws.mkdir_p(&p).await?;
    Ok(Json(json!({ "created": p })))
}

#[derive(Serialize)]
struct InodeDto {
    ino: i64,
    kind: String,
    mode: u32,
    nlink: i64,
    size: u64,
    mtime: i64,
    ctime: i64,
}

async fn stat(State(ws): State<Shared>, Path(path): Path<String>) -> ApiResult<Json<InodeDto>> {
    let i = ws.stat(&abspath(&path)).await?;
    Ok(Json(InodeDto {
        ino: i.ino,
        kind: i.kind.as_str().to_string(),
        mode: i.mode,
        nlink: i.nlink,
        size: i.size,
        mtime: i.mtime,
        ctime: i.ctime,
    }))
}

#[derive(Deserialize)]
struct RenameReq {
    from: String,
    to: String,
}

async fn rename(
    State(ws): State<Shared>,
    _auth: Auth,
    Json(req): Json<RenameReq>,
) -> ApiResult<Json<serde_json::Value>> {
    ws.rename(&req.from, &req.to).await?;
    Ok(Json(json!({ "from": req.from, "to": req.to })))
}

// --- versioning -------------------------------------------------------------

#[derive(Deserialize)]
struct CommitReq {
    message: String,
}

async fn commit(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    Json(req): Json<CommitReq>,
) -> ApiResult<Json<serde_json::Value>> {
    // The commit author is the authenticated actor's display name, not a
    // client-supplied string.
    let author = ws
        .get_actor(principal.actor)
        .await?
        .map(|a| a.display_name)
        .unwrap_or_else(|| format!("actor:{}", principal.actor));
    let hash = ws.commit(&author, &req.message).await?;
    Ok(Json(json!({ "hash": hash.to_hex() })))
}

#[derive(Serialize)]
struct CommitDto {
    hash: String,
    author: String,
    message: String,
    timestamp: i64,
    parents: Vec<String>,
}

#[derive(Deserialize)]
struct LogQuery {
    /// Maximum commits to return (default 100, capped at 1000).
    limit: Option<usize>,
    /// Continue after this commit hash — pass the last hash of the previous page
    /// to walk history in bounded pages.
    before: Option<String>,
}

async fn log(
    State(ws): State<Shared>,
    Query(q): Query<LogQuery>,
) -> ApiResult<Json<Vec<CommitDto>>> {
    let limit = q.limit.unwrap_or(100).min(1000);
    let all = ws.log().await?;
    // Skip past the cursor commit (exclusive), if one was given.
    let start = match &q.before {
        Some(h) => all
            .iter()
            .position(|ci| ci.hash.to_hex() == *h)
            .map(|i| i + 1)
            .unwrap_or(0),
        None => 0,
    };
    let out = all
        .into_iter()
        .skip(start)
        .take(limit)
        .map(|ci| CommitDto {
            hash: ci.hash.to_hex(),
            author: ci.commit.author,
            message: ci.commit.message,
            timestamp: ci.commit.timestamp,
            parents: ci.commit.parents.iter().map(|h| h.to_hex()).collect(),
        })
        .collect();
    Ok(Json(out))
}

#[derive(Deserialize)]
struct DiffQuery {
    from: String,
    to: String,
}

#[derive(Serialize)]
struct DiffEntryDto {
    path: String,
    status: &'static str,
}

/// `GET /diff?from=main&to=feature` — the changed-path list between two
/// refs/commits (compared by content address).
async fn diff(
    State(ws): State<Shared>,
    Query(q): Query<DiffQuery>,
) -> ApiResult<Json<Vec<DiffEntryDto>>> {
    let out = ws
        .diff(&q.from, &q.to)
        .await?
        .into_iter()
        .map(|d| DiffEntryDto {
            path: d.path,
            status: match d.status {
                crate::DiffStatus::Added => "added",
                crate::DiffStatus::Modified => "modified",
                crate::DiffStatus::Deleted => "deleted",
            },
        })
        .collect();
    Ok(Json(out))
}

#[derive(Deserialize)]
struct DiffFileQuery {
    from: String,
    to: String,
    path: String,
}

#[derive(Serialize)]
struct DiffFileDto {
    path: String,
    diff: String,
}

/// `GET /diff/file?from=main&to=feature&path=/x` — a unified line diff of one
/// path (empty `diff` when unchanged on both sides).
async fn diff_file(
    State(ws): State<Shared>,
    Query(q): Query<DiffFileQuery>,
) -> ApiResult<Json<DiffFileDto>> {
    let diff = ws.diff_file(&q.from, &q.to, &q.path).await?;
    Ok(Json(DiffFileDto { path: q.path, diff }))
}

// --- agent-suggestion review queue ------------------------------------------

#[derive(Serialize)]
struct SuggestionDto {
    id: i64,
    actor_id: i64,
    session_id: Option<i64>,
    branch: Option<String>,
    path: String,
    base_hash: Option<String>,
    proposed_hash: Option<String>,
    summary: Option<String>,
    status: String,
    created_ts: i64,
    resolved_ts: Option<i64>,
    resolved_by: Option<i64>,
}

impl From<crate::Suggestion> for SuggestionDto {
    fn from(s: crate::Suggestion) -> Self {
        Self {
            id: s.id,
            actor_id: s.actor_id,
            session_id: s.session_id,
            branch: s.branch,
            path: s.path,
            base_hash: s.base_hash,
            proposed_hash: s.proposed_hash,
            summary: s.summary,
            status: s.status.as_str().to_string(),
            created_ts: s.created_ts,
            resolved_ts: s.resolved_ts,
            resolved_by: s.resolved_by,
        }
    }
}

#[derive(Deserialize)]
struct CreateSuggestQuery {
    path: String,
    summary: Option<String>,
    #[serde(default)]
    delete: bool,
}

/// `POST /suggestions?path=&summary=` with the proposed bytes as the body (or
/// `&delete=true` and an empty body to propose a deletion). The proposing actor
/// is the authenticated principal, never a request field.
async fn create_suggestion(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    Query(q): Query<CreateSuggestQuery>,
    body: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let ctx = principal.write_ctx();
    let id = if q.delete {
        ws.suggest_delete(ctx, &q.path, q.summary.as_deref())
            .await?
    } else {
        ws.suggest(ctx, &q.path, &body, q.summary.as_deref())
            .await?
    };
    Ok(Json(json!({ "id": id })))
}

#[derive(Deserialize)]
struct ListSuggestQuery {
    status: Option<String>,
    path: Option<String>,
}

async fn list_suggestions(
    State(ws): State<Shared>,
    Query(q): Query<ListSuggestQuery>,
) -> ApiResult<Json<Vec<SuggestionDto>>> {
    let status = match q.status.as_deref() {
        Some(s) => Some(
            crate::SuggestionStatus::parse(s)
                .ok_or_else(|| crate::OrigoFSError::InvalidArgument(format!("bad status {s}")))?,
        ),
        None => None,
    };
    let out = ws
        .list_suggestions(status, q.path.as_deref())
        .await?
        .into_iter()
        .map(SuggestionDto::from)
        .collect();
    Ok(Json(out))
}

async fn get_suggestion(
    State(ws): State<Shared>,
    Path(id): Path<i64>,
) -> ApiResult<Json<SuggestionDto>> {
    let s = ws
        .get_suggestion(id)
        .await?
        .ok_or_else(|| crate::OrigoFSError::NotFound(format!("suggestion #{id}")))?;
    Ok(Json(s.into()))
}

async fn suggestion_diff(
    State(ws): State<Shared>,
    Path(id): Path<i64>,
) -> ApiResult<Json<serde_json::Value>> {
    let diff = ws.suggestion_diff(id).await?;
    Ok(Json(json!({ "id": id, "diff": diff })))
}

async fn accept_suggestion(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    Path(id): Path<i64>,
) -> ApiResult<Json<serde_json::Value>> {
    ws.accept_suggestion(id, principal.write_ctx()).await?;
    Ok(Json(json!({ "accepted": id })))
}

async fn reject_suggestion(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    Path(id): Path<i64>,
) -> ApiResult<Json<serde_json::Value>> {
    ws.reject_suggestion(id, principal.write_ctx()).await?;
    Ok(Json(json!({ "rejected": id })))
}

#[derive(Serialize)]
struct BranchDto {
    name: String,
    hash: String,
    current: bool,
}

async fn list_branches(State(ws): State<Shared>) -> ApiResult<Json<Vec<BranchDto>>> {
    let current = ws.current_branch().await?;
    let out = ws
        .list_branches()
        .await?
        .into_iter()
        .map(|(name, hash)| BranchDto {
            current: current.as_deref() == Some(&name),
            name,
            hash: hash.to_hex(),
        })
        .collect();
    Ok(Json(out))
}

#[derive(Deserialize)]
struct BranchReq {
    name: String,
}

async fn create_branch(
    State(ws): State<Shared>,
    _auth: Auth,
    Json(req): Json<BranchReq>,
) -> ApiResult<Json<serde_json::Value>> {
    ws.create_branch(&req.name).await?;
    Ok(Json(json!({ "created": req.name })))
}

async fn checkout(
    State(ws): State<Shared>,
    _auth: Auth,
    Json(req): Json<BranchReq>,
) -> ApiResult<Json<serde_json::Value>> {
    ws.checkout(&req.name).await?;
    Ok(Json(json!({ "branch": req.name })))
}

// --- attribution ------------------------------------------------------------

/// One blame span. `byte_start`/`byte_end` are the exact `[start, end)` byte
/// range — the design's ground truth — so a client can render sub-line,
/// character-level authorship (two authors on one line are two spans sharing a
/// line number). `line_start`/`line_end` are the inclusive 1-based lines the span
/// touches, kept for line-oriented UIs.
#[derive(Serialize)]
struct BlameDto {
    byte_start: u64,
    byte_end: u64,
    line_start: u32,
    line_end: u32,
    actor: String,
    session: Option<i64>,
    kind: String,
}

async fn blame(
    State(ws): State<Shared>,
    Path(path): Path<String>,
) -> ApiResult<Json<Vec<BlameDto>>> {
    let out = ws
        .blame(&abspath(&path))
        .await?
        .into_iter()
        .map(|r| BlameDto {
            byte_start: r.byte_start,
            byte_end: r.byte_end,
            line_start: r.line_start,
            line_end: r.line_end,
            actor: r.actor.display_name,
            session: r.session,
            kind: r.actor.kind.as_str().to_string(),
        })
        .collect();
    Ok(Json(out))
}

// --- collaboration ----------------------------------------------------------

#[derive(Serialize)]
struct EventDto {
    seq: i64,
    actor_id: Option<i64>,
    session_id: Option<i64>,
    kind: String,
    path: String,
    detail: Option<String>,
    ts: i64,
    branch: Option<String>,
}

#[derive(Deserialize)]
struct EventsQuery {
    since: Option<i64>,
    /// Restrict the feed to changes on this branch (the per-branch UI view).
    branch: Option<String>,
    /// Maximum events to return (default and cap 1000).
    limit: Option<usize>,
}

async fn events(
    State(ws): State<Shared>,
    Query(q): Query<EventsQuery>,
) -> ApiResult<Json<Vec<EventDto>>> {
    let limit = q.limit.unwrap_or(1000).min(1000);
    let out = ws
        .watch(q.since.unwrap_or(0))
        .await?
        .into_iter()
        .filter(|e| match &q.branch {
            Some(b) => e.branch.as_deref() == Some(b.as_str()),
            None => true,
        })
        .take(limit)
        .map(|e| EventDto {
            seq: e.seq,
            actor_id: e.actor_id,
            session_id: e.session_id,
            kind: e.kind,
            path: e.path,
            detail: e.detail,
            ts: e.ts,
            branch: e.branch,
        })
        .collect();
    Ok(Json(out))
}

#[derive(Serialize)]
struct PresenceDto {
    session_id: i64,
    actor_id: i64,
    display_name: String,
    kind: String,
    path: Option<String>,
    last_seen: i64,
}

#[derive(Deserialize)]
struct PresenceQuery {
    window: Option<i64>,
}

async fn presence(
    State(ws): State<Shared>,
    Query(q): Query<PresenceQuery>,
) -> ApiResult<Json<Vec<PresenceDto>>> {
    let out = ws
        .presence(q.window.unwrap_or(60))
        .await?
        .into_iter()
        .map(|p| PresenceDto {
            session_id: p.session_id,
            actor_id: p.actor_id,
            display_name: p.display_name,
            kind: p.kind.as_str().to_string(),
            path: p.path,
            last_seen: p.last_seen,
        })
        .collect();
    Ok(Json(out))
}

// --- actors + sessions ------------------------------------------------------

#[derive(Deserialize)]
struct ActorReq {
    name: String,
    #[serde(default)]
    agent: bool,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    controller: Option<i64>,
}

async fn create_actor(
    State(ws): State<Shared>,
    _auth: Auth,
    Json(req): Json<ActorReq>,
) -> ApiResult<Json<serde_json::Value>> {
    let id = if req.agent {
        ws.create_agent(
            &req.name,
            req.model.as_deref().unwrap_or("unknown"),
            req.controller,
        )
        .await?
    } else {
        ws.create_human(&req.name, None).await?
    };
    Ok(Json(json!({ "id": id })))
}

#[derive(Deserialize)]
struct SessionReq {
    actor: i64,
    #[serde(default)]
    client: Option<String>,
}

async fn create_session(
    State(ws): State<Shared>,
    _auth: Auth,
    Json(req): Json<SessionReq>,
) -> ApiResult<Json<serde_json::Value>> {
    let id = ws.create_session(req.actor, req.client.as_deref()).await?;
    Ok(Json(json!({ "id": id })))
}
