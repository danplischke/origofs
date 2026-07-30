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
//!
//! **Metrics** (`metrics` feature, M9). The surface records into the emit-only
//! [`origofs_core::metrics`] facade — request counts, latencies, error codes — and
//! serves the Prometheus text exposition at `GET /metrics`. Recording is
//! unconditional and free without the feature; the *route* and the per-request
//! middleware are feature-gated. The library still installs no exporter: a binary
//! calls [`set_metrics_renderer`] with a renderer (the CLI's `origofs serve
//! --metrics` does), and until it does `/metrics` answers `503 metrics not
//! enabled`.

use crate::{Workspace, WriteCtx, WriteOutcome, WritePolicy};
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

/// Install the closure that `GET /metrics` renders (Prometheus text format).
///
/// Re-exported from [`origofs_core::metrics::set_renderer`] so a binary that
/// serves the API has one place to opt into metrics: install a recorder (e.g.
/// `metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()`),
/// then hand its `render` here. The library never calls this — see the
/// "observability is emit-only" rule in `CLAUDE.md`.
#[cfg(feature = "metrics")]
pub use origofs_core::metrics::set_renderer as set_metrics_renderer;

/// Register `# HELP`/`# TYPE`/unit metadata for every origofs metric. Call once
/// from a binary after installing a recorder; re-exported from
/// [`origofs_core::metrics::describe`] so the CLI needs no direct dependency on
/// the core crate.
#[cfg(feature = "metrics")]
pub use origofs_core::metrics::describe as describe_metrics;

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
        .route("/presence", get(presence).post(heartbeat_presence))
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
    // Per-request metrics wrap the *data* surface only, outside the read gate so a
    // rejected (401) request is still counted. Liveness/readiness and the scrape
    // endpoint itself are deliberately excluded — a probe every second would
    // otherwise dominate the request rate. `route_layer` (not `layer`) means it
    // runs only for a *matched* route, which is also what makes the `path` label
    // safe: it is the route template, never the requested path.
    #[cfg(feature = "metrics")]
    {
        data = data.route_layer(middleware::from_fn(track_metrics));
    }
    // Versioned data surface. Liveness/readiness stay unversioned at the root so an
    // orchestrator probes them independent of the API version.
    #[cfg_attr(not(feature = "metrics"), allow(unused_mut))]
    let mut app = Router::new()
        .nest("/v1", data)
        .route("/health", get(health))
        .route("/readyz", get(readyz));
    #[cfg(feature = "metrics")]
    {
        app = app.route("/metrics", get(metrics_endpoint));
    }
    let app = app.with_state(state);
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
                // `origofs_errors_total{code,class}` — keyed off the stable machine
                // code, so an alert on e.g. `code="corrupt"` or
                // `class="unavailable"` survives message rewording.
                origofs_core::metrics::record_error(&e);
                let status = match &e {
                    NotFound(_) | ContentMissing(_) => StatusCode::NOT_FOUND,
                    AlreadyExists(_) | Conflict(_) => StatusCode::CONFLICT,
                    // Authenticated, but this actor's write policy forbids it —
                    // 403, not 401: re-authenticating would change nothing.
                    PermissionDenied(_) => StatusCode::FORBIDDEN,
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

// --- metrics (M9) -----------------------------------------------------------

/// Prometheus text exposition of everything the process has recorded.
///
/// **Authentication: the same treatment as `/readyz`.** It is registered at the
/// root, outside the `/v1` data surface, so — like `/health` and `/readyz` — it is
/// *not* covered by `gate_reads` and a scrape needs no credential. That is a
/// deliberate choice, not an oversight: the exposition carries only counters and
/// latencies labeled with closed sets (an error `code`/`class`, a fixed operation
/// name, a *matched route template*), so no path, actor, hash, or workspace
/// content can leak through it. If your `/readyz` is reachable from somewhere you
/// would not want scraping this, restrict both the same way — at your proxy, or
/// by binding the API to a private interface. `serve` already refuses to expose an
/// unauthenticated *write* surface on a non-loopback address (see `build_api_auth`
/// in the CLI); metrics change nothing about that posture.
///
/// Answers `503` with a plain-text `metrics not enabled` when the binary installed
/// no exporter — the library never installs one, so this is the response until
/// something calls [`set_metrics_renderer`]. A `503` (rather than a `404`) is what
/// a Prometheus scraper reports as a failed scrape of an endpoint that *exists*,
/// which is the honest signal.
#[cfg(feature = "metrics")]
async fn metrics_endpoint() -> Response {
    match origofs_core::metrics::render() {
        Some(body) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                origofs_core::metrics::EXPOSITION_CONTENT_TYPE,
            )],
            body,
        )
            .into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=utf-8",
            )],
            "metrics not enabled: this process installed no exporter (try `origofs serve --metrics`)\n",
        )
            .into_response(),
    }
}

/// Middleware recording one request: `origofs_http_requests_total{method,path,status}`
/// and `origofs_http_request_duration_seconds{method,path}`. `path` is the matched
/// route template (`/v1/files/{*path}`) so cardinality is bounded by the route
/// table and no requested path reaches the exposition.
#[cfg(feature = "metrics")]
async fn track_metrics(req: Request, next: Next) -> Response {
    let started = std::time::Instant::now();
    let method = method_label(req.method());
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_else(|| origofs_core::metrics::UNMATCHED_ROUTE.to_owned());
    let resp = next.run(req).await;
    origofs_core::metrics::record_http_request(
        method,
        route,
        resp.status().as_u16(),
        started.elapsed().as_secs_f64(),
    );
    resp
}

/// Map a request method onto one of a fixed set of label values — an extension
/// method must not become an unbounded label.
#[cfg(feature = "metrics")]
fn method_label(m: &axum::http::Method) -> &'static str {
    use axum::http::Method;
    match *m {
        _ if m == Method::GET => "GET",
        _ if m == Method::POST => "POST",
        _ if m == Method::PUT => "PUT",
        _ if m == Method::DELETE => "DELETE",
        _ if m == Method::PATCH => "PATCH",
        _ if m == Method::HEAD => "HEAD",
        _ if m == Method::OPTIONS => "OPTIONS",
        _ => "other",
    }
}

async fn read_file(State(ws): State<Shared>, Path(path): Path<String>) -> ApiResult<Response> {
    // Stream the body so an arbitrarily large file is never buffered server-side.
    // `read_stream` resolves and validates first, so a missing file (or a
    // directory) is still a clean error here, before any bytes are streamed.
    let stream = ws.read_stream(&abspath(&path)).await?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(CountedRead::new(stream)),
    )
        .into_response())
}

/// Counts a streamed read for metrics without buffering it: the only state is a
/// byte tally, recorded once the body ends (or is dropped early, e.g. a client
/// hang-up — which is why the count is "bytes served", not "file size").
///
/// This lives here rather than in the engine because the engine is where a
/// follow-up will instrument reads properly; recording at the surface keeps the
/// metric available today. The call is unconditional — `record_read` compiles to
/// nothing without `origofs-core/metrics`.
struct CountedRead {
    inner: crate::BoxStream<'static, origofs_core::Result<bytes::Bytes>>,
    bytes: u64,
}

impl CountedRead {
    fn new(inner: crate::BoxStream<'static, origofs_core::Result<bytes::Bytes>>) -> Self {
        Self { inner, bytes: 0 }
    }
}

impl futures::Stream for CountedRead {
    type Item = origofs_core::Result<bytes::Bytes>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let polled = this.inner.as_mut().poll_next(cx);
        if let std::task::Poll::Ready(Some(Ok(chunk))) = &polled {
            this.bytes += chunk.len() as u64;
        }
        polled
    }
}

impl Drop for CountedRead {
    fn drop(&mut self) {
        origofs_core::metrics::record_read(self.bytes);
    }
}

async fn write_file(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    Path(path): Path<String>,
    body: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let p = abspath(&path);
    let ctx = principal.write_ctx();
    // Attribution comes only from the authenticated principal — never the request.
    // Governed by the principal's write policy: a propose-only actor's edit is
    // queued for review rather than landing directly.
    //
    // Parent directories are created only when the edit actually lands. Creating
    // them up front meant an edit that was merely "queued for review" had already
    // mutated the tree — the one thing a propose-only actor must not be able to
    // do. `accept_suggestion` creates them on the way in instead.
    if matches!(ws.write_policy_for(ctx).await?, WritePolicy::Direct)
        && let Some((parent, _)) = p.rsplit_once('/')
        && !parent.is_empty()
    {
        ws.mkdir_p(parent).await?;
    }
    match ws.write_or_propose(ctx, &p, &body, None).await? {
        WriteOutcome::Wrote => {
            origofs_core::metrics::record_write(body.len() as u64);
            Ok(Json(json!({ "path": p, "written": body.len() })))
        }
        WriteOutcome::Proposed(id) => Ok(Json(json!({ "path": p, "proposed": id }))),
    }
}

async fn delete_file(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    Path(path): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let p = abspath(&path);
    // Governed by the write policy, like `write_file`. A propose-only actor used
    // to be able to delete any file outright — more destructive than the edits the
    // policy exists to gate.
    match ws
        .remove_or_propose(principal.write_ctx(), &p, None)
        .await?
    {
        WriteOutcome::Wrote => Ok(Json(json!({ "removed": p }))),
        WriteOutcome::Proposed(id) => Ok(Json(json!({ "path": p, "proposed": id }))),
    }
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
    Auth(principal): Auth,
    Path(path): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let p = abspath(&path);
    // Governed by the write policy. There is no way to queue a directory creation
    // for review, so a propose-only actor is refused rather than silently allowed.
    ws.mkdir_p_as(principal.write_ctx(), &p).await?;
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
    Auth(principal): Auth,
    Json(req): Json<RenameReq>,
) -> ApiResult<Json<serde_json::Value>> {
    // Governed by the write policy, like every other content mutation. A rename
    // has no suggestion form, so a propose-only actor is refused.
    ws.rename_as(principal.write_ctx(), &req.from, &req.to)
        .await?;
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
    origofs_core::metrics::record_commit();
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
    /// `bytes` (a whole file body) or `crdt` (a Yjs update to merge) — it decides
    /// what `base_hash`/`proposed_hash` mean and how `accept` applies it.
    kind: String,
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
            kind: s.kind.as_str().to_string(),
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

/// The body of `POST /v1/presence`. It carries **no** actor and **no** session
/// on purpose: like every other mutating route, identity is resolved server-side
/// from the credential, so a client can only ever heartbeat *itself* — naming
/// someone else is not expressible, let alone honoured.
#[derive(Deserialize, Default)]
struct PresenceBeatReq {
    /// The path this session is currently working on, if any. Normalized to an
    /// absolute workspace path; an empty string means "no current path".
    #[serde(default)]
    path: Option<String>,
}

/// `POST /v1/presence` — heartbeat the authenticated caller's presence, so a
/// browser client can appear in `GET /v1/presence` without an in-process SDK
/// bridge holding a [`Workspace`]. The body is optional (`{}` or nothing);
/// `{"path": "/notes.md"}` also records where the session is working.
///
/// Presence is keyed by **session**, so the credential must be bound to one — a
/// bare actor token gets a `400` telling it to create a session first. Minting a
/// session here instead would let a heartbeat loop create unbounded session rows,
/// and would make the presence list a directory of connections rather than of
/// working sessions.
async fn heartbeat_presence(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    body: Option<Json<PresenceBeatReq>>,
) -> ApiResult<Json<serde_json::Value>> {
    let req = body.map(|Json(r)| r).unwrap_or_default();
    let session = principal.session.ok_or_else(|| {
        crate::OrigoFSError::InvalidArgument(
            "this credential is not bound to a session; create one (POST /v1/sessions) and \
             present a session-bound credential to heartbeat presence"
                .into(),
        )
    })?;
    let path = req
        .path
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(abspath);
    ws.touch(principal.actor, session, path.as_deref()).await?;
    Ok(Json(json!({
        "session_id": session,
        "actor_id": principal.actor,
        "path": path,
    })))
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
