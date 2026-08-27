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

use crate::{Scope, ScopeError, Workspace, WriteCtx, WriteOutcome};
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
use std::time::Duration;
use tower::ServiceBuilder;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

#[cfg(feature = "coedit")]
mod coedit;
#[cfg(feature = "coedit")]
pub use coedit::{CheckpointPolicy, Coordinator};

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
    /// This router's view of the workspace (issue #125). `Scope::whole()` unless
    /// `ApiOptions::root` was set.
    scope: Scope,
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

impl FromRef<AppState> for Scope {
    fn from_ref(s: &AppState) -> Scope {
        s.scope.clone()
    }
}

/// A caller-supplied path, already resolved inside this router's [`Scope`].
///
/// Replaces the bare `Path(p): Path<String>` plus a normalize-to-absolute helper
/// that every path handler used to call by hand. That is the point: scoping applied *in the extractor* cannot be
/// forgotten by a handler, whereas a rule each handler has to remember is one a
/// new route will eventually skip — which is exactly how the workspace-wide
/// listings became side doors in the Python router before its own scoping landed.
struct ScopedPath(String);

impl FromRequestParts<AppState> for ScopedPath {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        let Path(raw) = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|e| ApiError::status(StatusCode::BAD_REQUEST, e.to_string()))?;
        Ok(ScopedPath(scope_path(&state.scope, &raw)?))
    }
}

/// Resolve `raw` inside `scope`, mapping a refusal to the right status.
fn scope_path(scope: &Scope, raw: &str) -> Result<String, ApiError> {
    scope.resolve(raw).map_err(scope_error)
}

/// Map a [`ScopeError`] to a response.
///
/// `OutOfScope` is a **404, never a 403**: a 403 confirms that something exists at
/// a path the caller may not see, which is the inference a scope exists to
/// prevent. A scoped caller must not be able to tell "exists but not yours" from
/// "no such thing".
fn scope_error(e: ScopeError) -> ApiError {
    match e {
        ScopeError::Traversal => {
            ApiError::status(StatusCode::BAD_REQUEST, "path may not contain '..'")
        }
        ScopeError::OutOfScope => ApiError::status(StatusCode::NOT_FOUND, "not found"),
    }
}

/// The refusal for an operation with no per-tenant meaning (issue #125).
///
/// Commit, log, branch, and checkout act on the **whole working tree**: a checkout
/// rematerializes every tenant's files, and the commit log is a shared history
/// whose messages and authors belong to everybody. There is no filter that makes
/// them tenant-scoped, so a scoped router refuses them outright rather than
/// serving a partial answer that looks complete.
fn unscopable(what: &str) -> ApiError {
    ApiError::status(
        StatusCode::FORBIDDEN,
        format!(
            "{what} acts on the whole workspace and has no meaning under a scoped \
             router; serve it from an unscoped router instead"
        ),
    )
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
    /// Restrict this router to one subtree of the workspace (issue #125).
    ///
    /// `None` (the default) serves the whole workspace. With a root set, every
    /// path a caller supplies is resolved *inside* it, every workspace-wide
    /// listing is filtered to it, and the operations that have no per-tenant
    /// meaning are refused — see [`crate::Scope`] for the four properties this
    /// relies on and why each is load-bearing.
    ///
    /// This is scoping, not authorization: it restricts what this surface can
    /// *address*, and says nothing about what an actor may *do*. A deployment
    /// wanting per-actor rules needs both (issue #123).
    ///
    /// Mount one router per tenant to serve several from one process.
    pub root: Option<String>,
    /// Maximum request-body size in bytes for uploads (`PUT /v1/files/…`).
    ///
    /// Defaults to 64 MiB. `PUT` buffers the whole body in memory before writing
    /// (reads stream, writes do not), so this is a direct bound on per-request
    /// allocation — with the old 1 GiB default, a handful of concurrent maximum
    /// uploads was enough to exhaust the process. Raise it deliberately if large
    /// single-request uploads are part of the workload.
    pub max_body_bytes: usize,
    /// How long a single request may run before it is abandoned with `408`.
    ///
    /// Defaults to 60 seconds. Without a timeout, one wedged metadata query or a
    /// slow-loris client pins a connection and its task indefinitely, and enough
    /// of them stall the server without anything looking like an error. `None`
    /// disables it, for a deployment that genuinely has unbounded operations and
    /// its own upstream deadline.
    pub request_timeout: Option<Duration>,
    /// Maximum number of requests processed concurrently; the rest queue.
    ///
    /// Defaults to 512. This is the backpressure valve: every accepted request
    /// costs memory (up to `max_body_bytes` for an upload) and a metadata
    /// connection, so an unbounded accept loop converts a traffic spike into an
    /// out-of-memory kill rather than into latency. `None` disables the limit.
    pub max_concurrent_requests: Option<usize>,
    /// When live co-editing rooms are written back to durable storage.
    ///
    /// A room's CRDT lives in memory; without periodic checkpointing it reaches
    /// storage only when its last socket leaves, and a browser tab left open on a
    /// document is an open room. The default checkpoints 5 seconds after a room
    /// goes quiet and at least every 60 seconds while it stays busy — see
    /// [`CheckpointPolicy`].
    ///
    /// Present only with the `coedit` feature, since without it there are no
    /// rooms to checkpoint and the type does not exist.
    #[cfg(feature = "coedit")]
    pub checkpoint: CheckpointPolicy,
}

impl Default for ApiOptions {
    fn default() -> Self {
        Self {
            gate_reads: false,
            cors_origins: Vec::new(),
            root: None,
            max_body_bytes: 64 << 20,
            request_timeout: Some(Duration::from_secs(60)),
            max_concurrent_requests: Some(512),
            #[cfg(feature = "coedit")]
            checkpoint: CheckpointPolicy::default(),
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
    // A malformed root is a deployment error, and a scope that silently fell back
    // to "the whole workspace" would fail open — serving every tenant's data from a
    // router meant for one. Panic here rather than degrade: this runs once at
    // startup, not per request.
    let scope = match options.root.as_deref() {
        Some(r) => Scope::at(r).expect("ApiOptions::root must be an absolute path without '..'"),
        None => Scope::whole(),
    };
    let state = AppState {
        #[cfg(feature = "coedit")]
        coedit: Coordinator::new(ws.clone()).with_checkpoint_policy(options.checkpoint),
        ws,
        auth,
        scope,
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
        .route("/revert-session", post(revert_session))
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
        data = data
            .route("/coedit/{*path}", get(coedit::coedit_ws))
            // The tree shape (#92): the same socket over a `Y.XmlFragment`, plus the
            // checkpoint the *host* drives — origofs does not own the document
            // schema, so only the host can serialize a tree to bytes. Both
            // authenticate themselves, for the same reason the flat socket does.
            .route("/coedit-tree/{*path}", get(coedit::coedit_tree_ws))
            .route(
                "/coedit-tree-checkpoint/{*path}",
                post(coedit::coedit_tree_checkpoint),
            );
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
    // concurrency cap, a per-request deadline, a request-body size cap, and CORS
    // for browser clients. CORS sits innermost so it wraps the router's plain body
    // (it requires a `Default` response body, which the trace layer's wrapped body
    // is not).
    //
    // The concurrency limit is outside the timeout on purpose: queued requests
    // should have their deadline start when they begin *executing*, not while they
    // are waiting for a slot, or a burst would time out everything behind it.
    app.layer(
        ServiceBuilder::new()
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
            .layer(PropagateRequestIdLayer::x_request_id())
            .layer(TraceLayer::new_for_http())
            .option_layer(
                options
                    .max_concurrent_requests
                    .map(tower::limit::ConcurrencyLimitLayer::new),
            )
            // `408 Request Timeout` rather than tower-http's legacy `500`: this is
            // a deadline the server imposed, not an internal failure, and the
            // distinction is what tells a client the request may be safe to retry.
            .option_layer(
                options
                    .request_timeout
                    .map(|d| TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, d)),
            )
            .layer(DefaultBodyLimit::max(options.max_body_bytes))
            // A bare 413 says nothing about what the limit is or how to change it,
            // which makes a first encounter with it needlessly opaque. Replacing
            // the body costs one comparison on the error path only.
            .layer(axum::middleware::from_fn(explain_body_limit(
                options.max_body_bytes,
            )))
            .layer(cors_layer(&options)),
    )
}

/// Replace an empty `413 Payload Too Large` with one that names the limit and the
/// knob that changes it.
///
/// `DefaultBodyLimit` rejects with a bare status and no body. A caller hitting it
/// otherwise has to go read the source to learn both that 64 MiB is the default
/// and that `ApiOptions::max_body_bytes` (via `serve_with`) is how to raise it.
fn explain_body_limit(
    limit: usize,
) -> impl Clone
+ Send
+ Sync
+ 'static
+ Fn(
    axum::extract::Request,
    axum::middleware::Next,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> {
    move |req: axum::extract::Request, next: axum::middleware::Next| {
        Box::pin(async move {
            let resp = next.run(req).await;
            if resp.status() != StatusCode::PAYLOAD_TOO_LARGE {
                return resp;
            }
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({
                    "error": format!(
                        "request body exceeds the {limit} byte limit. Raise \
                         ApiOptions::max_body_bytes and serve with \
                         api::serve_with, or stream the file in instead"
                    ),
                    "code": "body_too_large",
                    "limit_bytes": limit,
                })),
            )
                .into_response()
        })
    }
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
    serve_until(ws, addr, auth, crate::shutdown_signal()).await
}

/// [`serve`] with explicit [`ApiOptions`].
///
/// `serve`/`serve_until` build the router with defaults, which left `ApiOptions`
/// public but unreachable from the only entry point that also provides the
/// graceful drain: configuring anything meant calling [`router_with`] and running
/// your own `axum::serve`, throwing the drain away. Read gating, CORS origins,
/// body size, timeout, and the concurrency cap are all things a deployment needs
/// to set without giving up shutdown behaviour.
pub async fn serve_with(
    ws: Shared,
    addr: SocketAddr,
    auth: Arc<dyn Authenticator>,
    options: ApiOptions,
) -> std::io::Result<()> {
    serve_until_with(ws, addr, auth, options, crate::shutdown_signal()).await
}

/// [`serve`], stopping when `shutdown` resolves and then **draining**: the
/// listener closes to new connections while requests already in flight run to
/// completion.
///
/// Without this the server was a bare `axum::serve`, so a `SIGTERM` — an ordinary
/// Kubernetes rollout, a `docker stop` — severed every in-flight request at
/// whatever point it had reached. Content is written before the metadata that
/// references it, so a write cut in the middle leaves durable orphaned chunks and
/// a client that never learns whether its write landed. Draining turns a
/// deployment from a burst of failed requests into a quiet handover.
///
/// The drain is unbounded here on purpose: the caller owns the deadline, because
/// only it knows the orchestrator's grace period. Wrap it in `tokio::time::timeout`
/// to bound it.
pub async fn serve_until(
    ws: Shared,
    addr: SocketAddr,
    auth: Arc<dyn Authenticator>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    serve_until_with(ws, addr, auth, ApiOptions::default(), shutdown).await
}

/// [`serve_until`] with explicit [`ApiOptions`]. See [`serve_with`].
pub async fn serve_until_with(
    ws: Shared,
    addr: SocketAddr,
    auth: Arc<dyn Authenticator>,
    options: ApiOptions,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router_with(ws, auth, options))
        .with_graceful_shutdown(async move {
            shutdown.await;
            tracing::info!("shutdown signal received; draining in-flight requests");
        })
        .await
}

// --- error mapping ----------------------------------------------------------

/// The message an error is allowed to show a client.
///
/// Most `OrigoFSError` variants are *about the request* — a path that isn't
/// there, a name that isn't valid, a policy that refused — and their `Display` is
/// exactly what the caller needs. [`Backend`](crate::OrigoFSError::Backend) is
/// not: its `Display` interpolates the driver error verbatim, which for
/// tokio-postgres or rusqlite means SQL text, table and column names, constraint
/// names, and connection or file paths. Returning that over HTTP hands an
/// unauthenticated caller (reads are open by default) a description of the
/// schema and the deployment.
///
/// So a backend failure gets a fixed message plus the stable machine `code` and
/// `class` the envelope already carries — enough for a client to decide whether
/// to retry — while the real cause goes to the log, where the operator can
/// correlate it by request id.
fn client_message(e: &crate::OrigoFSError) -> String {
    match e {
        crate::OrigoFSError::Backend { origin, class, .. } => {
            tracing::error!(error = %e, %origin, %class, "backend error");
            format!("{origin} backend error ({class}); see server logs")
        }
        other => other.to_string(),
    }
}

/// An HTTP error: either a mapped [`crate::OrigoFSError`] or an explicit status
/// (e.g. `401` from the [`Auth`] extractor).
pub(super) enum ApiError {
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
                    // Well-formed, but this actor may not do it (§6 write policy).
                    Denied(_) => StatusCode::FORBIDDEN,
                    IsADirectory(_) | NotADirectory(_) | DirectoryNotEmpty(_) | InvalidPath(_)
                    | InvalidArgument(_) => StatusCode::BAD_REQUEST,
                    // A transient backend failure: tell the client it may retry.
                    e if e.retryable() => StatusCode::SERVICE_UNAVAILABLE,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (status, e.code(), client_message(&e), e.retryable())
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

/// Readiness: probe the backing stores. `200` when both answer, `503` when either
/// is unreachable — so a load balancer or a k8s readiness probe pulls this
/// instance out of rotation until its database and content store recover, instead
/// of routing requests it cannot serve.
///
/// **The response says only which store is unhealthy, never why.** This endpoint
/// sits at the root, outside `/v1`, so it is not covered by `gate_reads` and is
/// unauthenticated by design — a probe should not need a credential. It used to
/// echo the raw probe error, which for a metadata failure is a driver message
/// carrying the DSN host, database name, and connection details. A prober needs
/// `ready: false` and which half is down; the operator needs the cause, and gets
/// it from the log.
async fn readyz(State(ws): State<Shared>) -> Response {
    let report = ws.ready().await;
    if let Some(err) = &report.metadata {
        tracing::error!(error = %err, store = "metadata", "readiness probe failed");
    }
    if let Some(err) = &report.content {
        tracing::error!(error = %err, store = "content", "readiness probe failed");
    }
    let store = |probe: &Option<String>| match probe {
        Some(_) => json!({ "ok": false }),
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

/// Guess a `Content-Type` from a path's extension.
///
/// Deliberately a small closed table rather than a `mime_guess` dependency: this
/// exists so a browser can *play* media served from a workspace instead of
/// downloading it, and the set that matters for that is short. Anything unknown
/// stays `application/octet-stream`, which is the safe answer — a wrong type is
/// worse than a generic one, and for an unknown extension the browser's sniffing
/// is better informed than a guess here would be.
fn content_type_for(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        // Video — the reason `Range` support below matters.
        "mp4" | "m4v" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "mkv" => "video/x-matroska",
        "avi" => "video/x-msvideo",
        // Audio.
        "mp3" => "audio/mpeg",
        "m4a" => "audio/mp4",
        "aac" => "audio/aac",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        "ogg" | "oga" => "audio/ogg",
        "opus" => "audio/opus",
        // Images.
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "heic" => "image/heic",
        // Documents and text.
        "pdf" => "application/pdf",
        "json" => "application/json",
        "txt" | "md" | "log" => "text/plain; charset=utf-8",
        "csv" => "text/csv; charset=utf-8",
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// Parse a single-range `Range: bytes=…` header against a known size.
///
/// Returns `None` when there is no header or it is one this server chooses not to
/// honour — RFC 9110 explicitly permits ignoring a `Range` and returning the whole
/// representation, which is the right answer for a multi-range request rather than
/// implementing `multipart/byteranges` for a case no media player uses.
///
/// `Some(Err(()))` is unsatisfiable: a range wholly past the end, which must be a
/// `416` rather than an empty `206`.
#[allow(clippy::type_complexity)]
fn parse_range(headers: &HeaderMap, size: u64) -> Option<std::result::Result<(u64, u64), ()>> {
    let raw = headers.get(axum::http::header::RANGE)?.to_str().ok()?;
    let spec = raw.strip_prefix("bytes=")?.trim();
    if spec.contains(',') {
        return None; // multi-range: serve the whole thing instead
    }
    let (start, end) = spec.split_once('-')?;
    let (start, end) = (start.trim(), end.trim());

    let (first, last) = match (start.is_empty(), end.is_empty()) {
        // `bytes=-N`: the final N bytes. N == 0 is unsatisfiable by definition.
        (true, false) => {
            let n: u64 = end.parse().ok()?;
            if n == 0 {
                return Some(Err(()));
            }
            (size.saturating_sub(n), size.saturating_sub(1))
        }
        // `bytes=N-`: from N to the end.
        (false, true) => (start.parse().ok()?, size.saturating_sub(1)),
        // `bytes=N-M`, clamped to the end (a too-large M is legal, not an error).
        (false, false) => {
            let f: u64 = start.parse().ok()?;
            let l: u64 = end.parse().ok()?;
            (f, l.min(size.saturating_sub(1)))
        }
        (true, true) => return None,
    };

    if size == 0 || first >= size || first > last {
        return Some(Err(()));
    }
    Some(Ok((first, last)))
}

async fn read_file(
    State(ws): State<Shared>,
    ScopedPath(path): ScopedPath,
    headers: HeaderMap,
) -> ApiResult<Response> {
    // Stream the body so an arbitrarily large file is never buffered server-side.
    // `open_for_range` resolves and validates first, so a missing file (or a
    // directory) is still a clean error here, before any bytes are streamed — and
    // it yields the size, which `Content-Length`, `Content-Range` and `416` all
    // need before the first byte.
    let p = path;
    let (manifest, size) = ws.open_for_range(&p).await?;
    let ctype = content_type_for(&p);

    // Parsed before the empty-file branch below: a range against a zero-length
    // representation is unsatisfiable, so it is a 416 rather than an empty 200 —
    // and an early return for "no manifest" would otherwise skip range handling
    // entirely.
    let range = parse_range(&headers, size);

    let Some(manifest) = manifest else {
        // Empty file: no manifest object exists, so there is nothing to stream.
        if matches!(range, Some(Err(()))) {
            return Ok((
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(axum::http::header::CONTENT_RANGE, "bytes */0".to_string())],
            )
                .into_response());
        }
        return Ok((
            [
                (axum::http::header::CONTENT_TYPE, ctype.to_string()),
                (axum::http::header::ACCEPT_RANGES, "bytes".to_string()),
                (axum::http::header::CONTENT_LENGTH, "0".to_string()),
            ],
            Body::empty(),
        )
            .into_response());
    };

    match range {
        // Unsatisfiable: RFC 9110 requires the 416 to carry the true size, which is
        // how a client discovers it asked past the end.
        Some(Err(())) => Ok((
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(axum::http::header::CONTENT_RANGE, format!("bytes */{size}"))],
        )
            .into_response()),
        // A partial response. Streamed, not buffered: a player may legally ask for
        // `bytes=0-`, and materializing that would defeat streaming entirely.
        Some(Ok((first, last))) => {
            let len = last - first + 1;
            let stream = ws.read_range_stream(manifest, first, len);
            Ok((
                StatusCode::PARTIAL_CONTENT,
                [
                    (axum::http::header::CONTENT_TYPE, ctype.to_string()),
                    (axum::http::header::ACCEPT_RANGES, "bytes".to_string()),
                    (axum::http::header::CONTENT_LENGTH, len.to_string()),
                    (
                        axum::http::header::CONTENT_RANGE,
                        format!("bytes {first}-{last}/{size}"),
                    ),
                ],
                Body::from_stream(CountedRead::new(stream)),
            )
                .into_response())
        }
        // Whole file. `Accept-Ranges` advertises that seeking is available at all —
        // without it a browser will not offer to scrub a video, however well the
        // range handling above works.
        None => {
            let stream = ws.read_range_stream(manifest, 0, size);
            Ok((
                [
                    (axum::http::header::CONTENT_TYPE, ctype.to_string()),
                    (axum::http::header::ACCEPT_RANGES, "bytes".to_string()),
                    (axum::http::header::CONTENT_LENGTH, size.to_string()),
                ],
                Body::from_stream(CountedRead::new(stream)),
            )
                .into_response())
        }
    }
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
    ScopedPath(path): ScopedPath,
    body: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let p = path;
    let ctx = principal.write_ctx();
    // Attribution comes only from the authenticated principal — never the request.
    // Governed by the principal's write policy: a propose-only actor's edit is
    // queued for review rather than landing directly. Missing parents are created
    // by the engine *after* that decision, so a queued suggestion leaves the
    // working tree untouched.
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
    ScopedPath(path): ScopedPath,
) -> ApiResult<Json<serde_json::Value>> {
    let p = path;
    // Policy-governed like `PUT`: a propose-only actor's delete is queued for
    // review, not applied. Otherwise it could destroy a file it was refused
    // permission to overwrite (issue #78).
    let summary = format!("delete {p}");
    match ws
        .remove_or_propose(principal.write_ctx(), &p, Some(&summary))
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

/// `GET /v1/dirs` — the workspace root, or the **scope's** root when the router is
/// scoped (issue #125).
///
/// The one path route with no path parameter, which is exactly why it needed
/// saying explicitly: it does not pass through `ScopedPath`, so a hardcoded `"/"`
/// here listed every tenant's top-level directory from a scoped router while every
/// other route was correctly confined.
async fn list_root(
    State(ws): State<Shared>,
    State(scope): State<Scope>,
) -> ApiResult<Json<Vec<EntryDto>>> {
    list_path(&ws, &scope.resolve("/").map_err(scope_error)?).await
}

async fn list_dir(
    State(ws): State<Shared>,
    ScopedPath(path): ScopedPath,
) -> ApiResult<Json<Vec<EntryDto>>> {
    list_path(&ws, &path).await
}

/// `POST /v1/dirs` — the root directory.
///
/// The root always exists, so there is nothing to create and nothing to
/// attribute; this exists only so the collection URL is not a 405 next to
/// `POST /v1/dirs/{path}`. Says so rather than claiming it created something.
/// Listed in `NO_ACTOR_NEEDED` in `tests/api_write_policy.rs`.
async fn make_root(State(_ws): State<Shared>, _auth: Auth) -> ApiResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "path": "/", "created": false })))
}

async fn make_dir(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    ScopedPath(path): ScopedPath,
) -> ApiResult<Json<serde_json::Value>> {
    let p = path;
    ws.mkdir_as(principal.write_ctx(), &p).await?;
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

async fn stat(State(ws): State<Shared>, ScopedPath(path): ScopedPath) -> ApiResult<Json<InodeDto>> {
    let i = ws.stat(&path).await?;
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
    State(scope): State<Scope>,
    Auth(principal): Auth,
    Json(req): Json<RenameReq>,
) -> ApiResult<Json<serde_json::Value>> {
    // Both endpoints, not just the source: scoping only the source would let a
    // caller move a file it can address into a tree it cannot.
    let from = scope_path(&scope, &req.from)?;
    let to = scope_path(&scope, &req.to)?;
    ws.rename_as(principal.write_ctx(), &from, &to).await?;
    Ok(Json(json!({ "from": from, "to": to })))
}

// --- versioning -------------------------------------------------------------

#[derive(Deserialize)]
struct CommitReq {
    message: String,
}

async fn commit(
    State(ws): State<Shared>,
    State(scope): State<Scope>,
    Auth(principal): Auth,
    Json(req): Json<CommitReq>,
) -> ApiResult<Json<serde_json::Value>> {
    if !scope.is_whole() {
        return Err(unscopable("commit"));
    }
    // The commit author is the authenticated actor's display name, not a
    // client-supplied string.
    let author = ws
        .get_actor(principal.actor)
        .await?
        .map(|a| a.display_name)
        .unwrap_or_else(|| format!("actor:{}", principal.actor));
    let hash = ws
        .commit_as(principal.write_ctx(), &author, &req.message)
        .await?;
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
    State(scope): State<Scope>,
    Query(q): Query<LogQuery>,
) -> ApiResult<Json<Vec<CommitDto>>> {
    if !scope.is_whole() {
        return Err(unscopable("the commit log"));
    }
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
    State(scope): State<Scope>,
    Query(q): Query<DiffQuery>,
) -> ApiResult<Json<Vec<DiffEntryDto>>> {
    if !scope.is_whole() {
        return Err(unscopable("a whole-tree diff"));
    }
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
    State(scope): State<Scope>,
    Query(q): Query<DiffFileQuery>,
) -> ApiResult<Json<DiffFileDto>> {
    let path = scope_path(&scope, &q.path)?;
    let diff = ws.diff_file(&q.from, &q.to, &path).await?;
    Ok(Json(DiffFileDto { path, diff }))
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
    State(scope): State<Scope>,
    Auth(principal): Auth,
    Query(q): Query<CreateSuggestQuery>,
    body: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let ctx = principal.write_ctx();
    let path = scope_path(&scope, &q.path)?;
    let id = if q.delete {
        ws.suggest_delete(ctx, &path, q.summary.as_deref()).await?
    } else {
        ws.suggest(ctx, &path, &body, q.summary.as_deref()).await?
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
    State(scope): State<Scope>,
    Query(q): Query<ListSuggestQuery>,
) -> ApiResult<Json<Vec<SuggestionDto>>> {
    let status = match q.status.as_deref() {
        Some(s) => Some(
            crate::SuggestionStatus::parse(s)
                .ok_or_else(|| crate::OrigoFSError::InvalidArgument(format!("bad status {s}")))?,
        ),
        None => None,
    };
    // Both halves matter. Resolving the filter keeps a caller from *asking* about
    // a neighbour; filtering the results keeps an absent filter from *returning*
    // one, since `None` means "no path filter" and would otherwise mean "every
    // tenant's".
    let filter = scope.resolve_opt(q.path.as_deref()).map_err(scope_error)?;
    let rows = ws.list_suggestions(status, filter.as_deref()).await?;
    let out = scope
        .filter(rows, |r| Some(r.path.as_str()))
        .into_iter()
        .map(SuggestionDto::from)
        .collect();
    Ok(Json(out))
}

async fn get_suggestion(
    State(ws): State<Shared>,
    State(scope): State<Scope>,
    Path(id): Path<i64>,
) -> ApiResult<Json<SuggestionDto>> {
    let s = ws
        .get_suggestion(id)
        .await?
        .ok_or_else(|| crate::OrigoFSError::NotFound(format!("suggestion #{id}")))?;
    // Suggestion ids are workspace-global, so knowing an id was enough to read a
    // neighbour's proposed content. The refusal is a 404 for the same reason the
    // miss above is: a 403 would confirm the id exists.
    scope.require(Some(s.path.as_str())).map_err(scope_error)?;
    Ok(Json(s.into()))
}

/// Refuse an id-addressed suggestion route whose target lies outside the scope.
///
/// Suggestion ids are workspace-global, so without this a scoped caller could
/// read, accept, or reject a neighbour's proposal simply by guessing an id — and
/// `accept` *lands a write* into the neighbour's tree. The lookup happens before
/// the action, and a miss and an out-of-scope hit return the identical 404.
async fn suggestion_in_scope(ws: &Shared, scope: &Scope, id: i64) -> Result<(), ApiError> {
    let s = ws
        .get_suggestion(id)
        .await?
        .ok_or_else(|| crate::OrigoFSError::NotFound(format!("suggestion #{id}")))?;
    scope.require(Some(s.path.as_str())).map_err(scope_error)
}

async fn suggestion_diff(
    State(ws): State<Shared>,
    State(scope): State<Scope>,
    Path(id): Path<i64>,
) -> ApiResult<Json<serde_json::Value>> {
    suggestion_in_scope(&ws, &scope, id).await?;
    let diff = ws.suggestion_diff(id).await?;
    Ok(Json(json!({ "id": id, "diff": diff })))
}

async fn accept_suggestion(
    State(ws): State<Shared>,
    State(scope): State<Scope>,
    Auth(principal): Auth,
    Path(id): Path<i64>,
) -> ApiResult<Json<serde_json::Value>> {
    suggestion_in_scope(&ws, &scope, id).await?;
    ws.accept_suggestion(id, principal.write_ctx()).await?;
    Ok(Json(json!({ "accepted": id })))
}

async fn reject_suggestion(
    State(ws): State<Shared>,
    State(scope): State<Scope>,
    Auth(principal): Auth,
    Path(id): Path<i64>,
) -> ApiResult<Json<serde_json::Value>> {
    suggestion_in_scope(&ws, &scope, id).await?;
    ws.reject_suggestion(id, principal.write_ctx()).await?;
    Ok(Json(json!({ "rejected": id })))
}

#[derive(Serialize)]
struct BranchDto {
    name: String,
    hash: String,
    current: bool,
}

async fn list_branches(
    State(ws): State<Shared>,
    State(scope): State<Scope>,
) -> ApiResult<Json<Vec<BranchDto>>> {
    if !scope.is_whole() {
        return Err(unscopable("listing branches"));
    }
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
    State(scope): State<Scope>,
    Auth(principal): Auth,
    Json(req): Json<BranchReq>,
) -> ApiResult<Json<serde_json::Value>> {
    if !scope.is_whole() {
        return Err(unscopable("creating a branch"));
    }
    ws.create_branch_as(principal.write_ctx(), &req.name)
        .await?;
    Ok(Json(json!({ "created": req.name })))
}

/// Switching branches rematerializes the whole working tree, discarding every
/// uncommitted edit — so it goes through the attributed, policy-gated variant.
/// Taking only `_auth` here meant a propose-only token, held by an actor
/// deliberately barred from overwriting one file, could destroy the workspace.
async fn checkout(
    State(ws): State<Shared>,
    State(scope): State<Scope>,
    Auth(principal): Auth,
    Json(req): Json<BranchReq>,
) -> ApiResult<Json<serde_json::Value>> {
    if !scope.is_whole() {
        return Err(unscopable("checkout"));
    }
    ws.checkout_as(principal.write_ctx(), &req.name).await?;
    Ok(Json(json!({ "branch": req.name })))
}

/// The body of `POST /v1/revert-session`.
#[derive(Deserialize)]
struct RevertReq {
    actor: i64,
    session: i64,
    /// Optional subtree to bound the revert to, matched on directory boundaries
    /// (`/tenant-a` covers `/tenant-a/notes.txt`, never `/tenant-abc/notes.txt`).
    /// Omit to revert everywhere the session wrote.
    #[serde(default)]
    path_prefix: Option<String>,
}

/// Undo exactly the lines one actor authored in one session, across every file
/// that session touched, leaving other actors' edits intact.
///
/// This is the feature the README leads with — "can I undo just the agent's
/// work?" — and it existed only in the Rust SDK: no CLI subcommand, no HTTP route,
/// no MCP tool, no Python binding. Well-tested core logic, simply unexposed.
///
/// Gated, and the actor being reverted comes from the *body* on purpose: this is a
/// review action performed *on* someone else's work, so the target is not the
/// caller. The caller's own identity still has to clear the write policy — a
/// propose-only actor cannot revert anyone, including itself.
async fn revert_session(
    State(ws): State<Shared>,
    State(scope): State<Scope>,
    Auth(principal): Auth,
    Json(req): Json<RevertReq>,
) -> ApiResult<Json<serde_json::Value>> {
    ws.ensure_may_write(principal.write_ctx(), "revert a session")
        .await?;
    // An unscoped revert walks every file the session touched, across every
    // tenant. Under a scope the prefix is resolved inside the root, and an absent
    // prefix becomes the root itself rather than "everything" -- the one place
    // where `None` must not stay `None`.
    let prefix = match (scope.is_whole(), req.path_prefix.as_deref()) {
        // Unscoped: pass the prefix through untouched, so the engine's own rule
        // still applies — `revert_session` rejects a *relative* prefix rather than
        // reading it as absolute, because silently reinterpreting `tenant-a` as
        // `/tenant-a` could revert a subtree the caller did not mean.
        (true, p) => p.map(str::to_string),
        // Scoped with no prefix: the scope's own root. This is the one place a
        // `None` filter must *not* stay `None` — an unscoped revert walks every
        // file the session touched, across every tenant.
        (false, None) => Some(scope.root().to_string()),
        (false, Some(p)) => {
            // Absoluteness is checked here rather than left to `resolve`, which
            // would normalize it, so a scoped caller gets the same error an
            // unscoped one does instead of a quietly different behaviour.
            if !p.starts_with('/') {
                return Err(crate::OrigoFSError::InvalidArgument(format!(
                    "path prefix must be absolute, got {p:?}"
                ))
                .into());
            }
            Some(scope.resolve(p).map_err(scope_error)?)
        }
    };
    let paths = ws
        .revert_session(req.actor, req.session, prefix.as_deref())
        .await?;
    Ok(Json(json!({
        "actor": req.actor,
        "session": req.session,
        "files_changed": paths.len(),
        // Which paths, not just how many: a caller that caches per path can now
        // invalidate exactly what changed instead of dropping everything.
        "paths": paths,
        "reverted_by": principal.actor,
    })))
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
    ScopedPath(path): ScopedPath,
) -> ApiResult<Json<Vec<BlameDto>>> {
    let out = ws
        .blame(&path)
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
    State(scope): State<Scope>,
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
        .collect::<Vec<_>>();
    // The change feed is workspace-wide, so unfiltered it leaks a neighbour's
    // paths, sizes, and timing -- a side door around the path routes.
    let out = scope
        .filter(out, |e| Some(e.path.as_str()))
        .into_iter()
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
    State(scope): State<Scope>,
    Query(q): Query<PresenceQuery>,
) -> ApiResult<Json<Vec<PresenceDto>>> {
    let rows = ws.presence(q.window.unwrap_or(60)).await?;
    // Presence is workspace-wide. Note what this drops: a row with **no** path --
    // an idle session -- is filtered out too, because a record naming no path still
    // tells a scoped reader that a neighbour is connected. `Scope::contains(None)`
    // is false for exactly this case.
    let out = scope
        .filter(rows, |p| p.path.as_deref())
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
    State(scope): State<Scope>,
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
        .map(|p| scope_path(&scope, p))
        .transpose()?;
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

/// Register a new actor.
///
/// Gated, not merely authenticated. This mutates the identity registry rather
/// than the working tree, so there is no attributed variant to call — but leaving
/// it open to any valid credential let a propose-only actor, one the operator had
/// deliberately restricted, mint unbounded rows in the very table attribution is
/// resolved against.
async fn create_actor(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    Json(req): Json<ActorReq>,
) -> ApiResult<Json<serde_json::Value>> {
    ws.ensure_may_write(principal.write_ctx(), "register actors")
        .await?;
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

/// The body of `POST /v1/sessions`. It carries **no** actor: the session belongs
/// to whoever the credential resolves to, server-side.
#[derive(Deserialize)]
struct SessionReq {
    #[serde(default)]
    client: Option<String>,
}

/// Open a session for the *authenticated* actor.
///
/// This used to read `actor` out of the request body, which is the one place the
/// surface broke origofs's central rule that the server never trusts a
/// client-named actor. Writes were never forgeable through it — they attribute
/// from the token's principal, not from a client-supplied session — but any valid
/// credential could mint unbounded session rows belonging to *other* actors,
/// polluting the very audit trail sessions exist to provide.
async fn create_session(
    State(ws): State<Shared>,
    Auth(principal): Auth,
    Json(req): Json<SessionReq>,
) -> ApiResult<Json<serde_json::Value>> {
    let id = ws
        .create_session(principal.actor, req.client.as_deref())
        .await?;
    Ok(Json(json!({ "id": id, "actor": principal.actor })))
}
