//! A Postgres-backed [`MetadataStore`] — the multi-writer backend (`docs/DESIGN.md`
//! §4b).
//!
//! Runs the same schema as SQLite (via the shared [`crate::migrations`] list, in
//! the Postgres dialect) so the engine and the whole FS test suite work unchanged.
//! Postgres unlocks the shared-workspace goals: MVCC multi-writer, atomic
//! multi-step writes (a pinned-connection [`MetaTxn`] serializes hot-inode
//! critical sections on the unique dentry index), and `LISTEN/NOTIFY` change
//! feeds (consumed by the watch API in M8).

use crate::attribution::{
    Actor, ActorInit, ActorKind, EditOp, EditOpInit, ToolCallInit, WritePolicy,
};
use crate::collab::{EVENT_CHANNEL, Event, EventInit, LiveDoc, Presence};
use crate::error::{OrigoFSError, Result};
use crate::metadata::{MetaTxn, MetadataStore};
use crate::migrations::MIGRATIONS;
use crate::suggest::{Suggestion, SuggestionInit, SuggestionKind, SuggestionStatus};
use crate::types::{DirEntry, FileKind, Hash, Ino, Inode, InodeInit};
use crate::util::now_secs;
use async_trait::async_trait;
use deadpool_postgres::{Manager, Object, Pool};
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::error::SqlState;
use tokio_postgres::{AsyncMessage, Row};

const DIR_MODE: i64 = 0o040755;

/// The workspace every store is bound to until re-scoped with `with_workspace`;
/// its root is inode 1. Backfilled by migration V11 (`docs/MULTI_TENANCY.md`).
const DEFAULT_WORKSPACE: i64 = 1;

/// Advisory-lock key that serializes concurrent schema bootstraps (`init`).
const MIGRATION_LOCK_KEY: i64 = 0x0af5_0000_dbdb;

/// Advisory-lock key that serializes change-feed appends so a row's `seq`
/// commits in assignment order (H6). Held for the tiny insert+notify only.
/// Public so ops (and tests) can reason about the workspace's advisory locks.
pub const FEED_LOCK_KEY: i64 = 0x0af5_0000_feed;

/// Max events a single subscription `drain` pulls at once, so a lagging or
/// from-zero subscriber pages the backlog instead of loading it all into memory.
const DRAIN_BATCH: i64 = 1024;

/// A metadata store backed by a Postgres database (with a connection pool).
pub struct PostgresMetadataStore {
    pool: Pool,
    /// Kept so [`Self::subscribe`] can open a dedicated `LISTEN` connection
    /// (pooled connections can't surface async notifications).
    dsn: String,
    /// The workspace this handle is bound to (default = 1). Workspace-scoped
    /// statements stamp/filter by it; [`PostgresMetadataStore::with_workspace`]
    /// rebinds a handle sharing this pool (`docs/MULTI_TENANCY.md`).
    workspace_id: i64,
}

/// Environment variable naming a PEM bundle of extra CA certificates to trust,
/// for a Postgres presenting a certificate from a private CA.
pub const PG_CA_FILE_ENV: &str = "ORIGOFS_PG_CA_FILE";

/// Build the TLS connector every Postgres connection uses.
///
/// `tokio-postgres` ships only `NoTls`, and that is what origofs used — so it could
/// not connect to any managed Postgres at all (RDS, Cloud SQL, Neon, and Supabase
/// all require TLS), and where it could connect, every path, actor name, and hash
/// crossed the network in cleartext.
///
/// Whether TLS is actually *used* stays the DSN's decision: `tokio-postgres`
/// honours `sslmode`, so `disable` still connects in the clear (a unix socket or a
/// loopback test), while `prefer` — the default — and `require` negotiate it.
/// Handing over a connector only makes the option exist.
///
/// Certificates are verified against the platform root store, plus any bundle
/// named by [`PG_CA_FILE_ENV`]. Note this is deliberately stricter than libpq,
/// where `sslmode=require` encrypts but verifies *nothing*: a connection that
/// can't be verified is refused rather than silently downgraded to an encrypted
/// channel with an unauthenticated peer. Point [`PG_CA_FILE_ENV`] at your CA for a
/// self-signed or private-CA server.
fn tls_connector() -> Result<tokio_postgres_rustls::MakeRustlsConnect> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        // A root the platform store offers but rustls rejects is skipped rather
        // than fatal — one malformed system certificate must not make the database
        // unreachable.
        let _ = roots.add(cert);
    }
    if let Ok(path) = std::env::var(PG_CA_FILE_ENV) {
        let pem = std::fs::read(&path).map_err(|e| {
            OrigoFSError::Metadata(format!("{PG_CA_FILE_ENV}: cannot read {path}: {e}"))
        })?;
        let mut added = 0usize;
        for cert in rustls_pemfile::certs(&mut pem.as_slice()).flatten() {
            roots
                .add(cert)
                .map_err(|e| OrigoFSError::Metadata(format!("{PG_CA_FILE_ENV} ({path}): {e}")))?;
            added += 1;
        }
        if added == 0 {
            return Err(OrigoFSError::Metadata(format!(
                "{PG_CA_FILE_ENV} ({path}) contains no certificates"
            )));
        }
    }
    if roots.is_empty() {
        return Err(OrigoFSError::Metadata(format!(
            "no trusted CA certificates: the platform root store is empty and \
             {PG_CA_FILE_ENV} is unset, so no Postgres TLS certificate could be verified"
        )));
    }
    // Name the provider rather than relying on a process-wide default: installing
    // one is the binary's call, not a library's.
    let cfg = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| OrigoFSError::Metadata(format!("rustls: {e}")))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(cfg))
}

impl PostgresMetadataStore {
    /// Connect to Postgres. `dsn` is a libpq DSN or URL, e.g.
    /// `postgres://user:pass@host/db` or `host=/var/run/postgresql dbname=origofs`.
    ///
    /// TLS is negotiated per the DSN's `sslmode` (default `prefer`); see
    /// [`tls_connector`] for the verification policy.
    pub async fn connect(dsn: &str) -> Result<Self> {
        let cfg: tokio_postgres::Config = dsn.parse()?;
        let mgr = Manager::new(cfg, tls_connector()?);
        // Bound acquisition: without a wait timeout, exhausting the pool makes
        // `client()` hang forever instead of surfacing a retriable error. A
        // runtime must be set for the timeouts to be enforced.
        let pool = Pool::builder(mgr)
            .max_size(16)
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .wait_timeout(Some(std::time::Duration::from_secs(10)))
            .create_timeout(Some(std::time::Duration::from_secs(10)))
            .build()
            .map_err(|e| OrigoFSError::Metadata(e.to_string()))?;
        Ok(Self {
            pool,
            dsn: dsn.to_string(),
            workspace_id: DEFAULT_WORKSPACE,
        })
    }

    async fn client(&self) -> Result<deadpool_postgres::Object> {
        // A pool error (exhaustion, acquisition timeout, a dead connection) is a
        // classified `Backend` error (`Unavailable`) via `From<PoolError>`, so a
        // caller can tell "the store is unreachable" from a logic error.
        Ok(self.pool.get().await?)
    }

    /// A concrete handle to this store bound to `workspace_id`, sharing the same
    /// pool + DSN. The typed counterpart to [`MetadataStore::with_workspace`], for
    /// the SDK to re-scope the Postgres-only push feed (`subscribe`) to the
    /// workspace it is serving (`docs/MULTI_TENANCY.md`).
    pub fn for_workspace(&self, workspace_id: i64) -> Arc<PostgresMetadataStore> {
        Arc::new(PostgresMetadataStore {
            pool: self.pool.clone(),
            dsn: self.dsn.clone(),
            workspace_id,
        })
    }

    // A session-level `pg_advisory_lock` helper used to live here (H11). It was
    // structurally broken — it took the lock on a pooled connection and returned
    // that connection (lock still held) to the pool, so the unlock could land on
    // a different connection and the promised hot-inode serialization never
    // existed. The engine never called it. Its purpose (stop concurrent
    // same-path creates from orphaning an inode) is now served correctly by the
    // `begin`/`MetaTxn` transaction (C1): the create + dentry link commit
    // atomically, so a losing race errors on the unique dentry index and rolls
    // back the inode instead of leaking it.

    /// Send a `LISTEN/NOTIFY` message (change-feed plumbing).
    pub async fn notify(&self, channel: &str, payload: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute("SELECT pg_notify($1, $2)", &[&channel, &payload])
            .await?;
        Ok(())
    }

    /// Subscribe to the change feed with a real `LISTEN` — a **blocking push**
    /// stream, not a poll. Returns an [`EventSubscription`] whose `recv()` wakes
    /// on every committed change and yields the new [`Event`]s in order.
    ///
    /// `after_seq` is the cursor to start from (`0` for everything, or the last
    /// seq the caller has already seen). `branch`, if given, filters the stream
    /// to changes on that branch — the per-branch feed a multi-branch UI wants.
    ///
    /// Correctness: we `LISTEN` first, then the query is the source of truth on
    /// every wake, so notifications that coalesce or race the initial read never
    /// drop an event.
    pub async fn subscribe(
        &self,
        after_seq: i64,
        branch: Option<String>,
    ) -> Result<EventSubscription> {
        // A dedicated connection (pooled ones can't surface async notifications),
        // over the same TLS policy as the pool.
        let (client, mut connection) = tokio_postgres::connect(&self.dsn, tls_connector()?)
            .await
            .map_err(|e| OrigoFSError::Metadata(e.to_string()))?;

        // The connection future both drives the socket and surfaces async
        // NOTIFYs; forward each notification to the receiver as a bare wakeup.
        // A capacity-1 channel bounds memory and coalesces: a wakeup only means
        // "re-drain", so if one is already pending a burst of NOTIFYs collapses
        // into it instead of accreting an unbounded backlog (L5). The single
        // drained query is the source of truth, so no event is lost by coalescing.
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);
        let driver = tokio::spawn(async move {
            let mut stream =
                futures::stream::poll_fn(move |cx| Pin::new(&mut connection).poll_message(cx));
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(AsyncMessage::Notification(_)) => match tx.try_send(()) {
                        Ok(()) => {}
                        // A wakeup is already queued; the re-drain will see this
                        // change too, so dropping the extra wakeup is correct.
                        Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {}
                        // The subscriber was dropped.
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => break,
                    },
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        client
            .batch_execute(&format!("LISTEN {EVENT_CHANNEL}"))
            .await
            .map_err(|e| OrigoFSError::Metadata(e.to_string()))?;

        Ok(EventSubscription {
            client,
            wakeups: rx,
            cursor: after_seq,
            branch,
            workspace_id: self.workspace_id,
            driver,
        })
    }
}

/// A live `LISTEN`-backed subscription to the change feed. Dropping it tears
/// down the dedicated connection and stops the feed.
pub struct EventSubscription {
    client: tokio_postgres::Client,
    wakeups: tokio::sync::mpsc::Receiver<()>,
    cursor: i64,
    branch: Option<String>,
    /// The workspace this feed is scoped to — the change feed is per-workspace
    /// (`docs/MULTI_TENANCY.md`), so every drain filters by it.
    workspace_id: i64,
    /// The task draining the dedicated connection and forwarding NOTIFYs. Held
    /// so it is aborted when the subscription drops, rather than leaked (L5).
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        // Stop the forwarder task; the dedicated connection closes with `client`.
        self.driver.abort();
    }
}

impl EventSubscription {
    /// Block until at least one new event is available, then return the batch
    /// (ordered by `seq`) and advance the cursor. Returns `Ok(vec![])` only once
    /// the underlying connection has closed.
    pub async fn recv(&mut self) -> Result<Vec<Event>> {
        loop {
            let batch = self.drain().await?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            // Nothing new yet — park until a NOTIFY wakes us, then re-drain.
            if self.wakeups.recv().await.is_none() {
                return Ok(Vec::new());
            }
        }
    }

    /// Fetch every event past the cursor (optionally filtered to `branch`) and
    /// advance the cursor past them.
    async fn drain(&mut self) -> Result<Vec<Event>> {
        let rows = match &self.branch {
            Some(b) => {
                self.client
                    .query(
                        "SELECT seq, actor_id, session_id, kind, path, detail, ts, branch
                         FROM fs_event WHERE workspace_id = $1 AND seq > $2 AND branch = $3
                         ORDER BY seq LIMIT $4",
                        &[&self.workspace_id, &self.cursor, b, &DRAIN_BATCH],
                    )
                    .await
            }
            None => {
                self.client
                    .query(
                        "SELECT seq, actor_id, session_id, kind, path, detail, ts, branch
                         FROM fs_event WHERE workspace_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
                        &[&self.workspace_id, &self.cursor, &DRAIN_BATCH],
                    )
                    .await
            }
        }
        .map_err(|e| OrigoFSError::Metadata(e.to_string()))?;

        let events: Vec<Event> = rows.iter().map(row_to_event).collect();
        // Advance past the max seq we *saw*, so a branch filter still moves the
        // cursor forward and we don't re-scan skipped rows on the next wake.
        if let Some(last) = events.last() {
            self.cursor = last.seq;
        } else if self.branch.is_some() {
            // Filtered to nothing this round: bump the cursor to the table's max
            // so we don't rescan the same non-matching rows every wakeup.
            let max: Option<i64> = self
                .client
                .query_opt(
                    "SELECT max(seq) FROM fs_event WHERE workspace_id = $1",
                    &[&self.workspace_id],
                )
                .await
                .map_err(|e| OrigoFSError::Metadata(e.to_string()))?
                .and_then(|row| row.get(0));
            if let Some(m) = max {
                self.cursor = m.max(self.cursor);
            }
        }
        Ok(events)
    }
}

// --- co-editing cross-worker relay (M8) -------------------------------------
//
// Live co-editing rooms are per-process: every socket editing one path shares one
// CRDT within a worker. Across workers that isn't enough — two users on different
// workers would edit divergent copies. This relay is the cross-worker bus: a
// worker publishes each attributed update delta, and every other worker applies
// it to its own room and fans it out to its local sockets, so all replicas of a
// document converge (the CRDT merge is commutative + idempotent).
//
// It reuses the change-feed shape: a durable append-only `coedit_op` table is the
// source of truth, and `NOTIFY` is only a coalesced wakeup — so a dropped or
// merged notification never loses an op (the next drain reads it from the table).
// The table is ephemeral scratch space (GC'd on a TTL), not durable workspace
// data, so it lives outside the versioned migrations, created on demand.

/// The Postgres channel co-edit workers wake each other on.
#[cfg(feature = "coedit")]
pub const COEDIT_CHANNEL: &str = "origofs_coedit";

/// Advisory-lock key serializing concurrent creation of the relay table, so many
/// workers calling `coedit_relay_init` at once don't race the `CREATE TABLE`.
#[cfg(feature = "coedit")]
const COEDIT_RELAY_LOCK_KEY: i64 = 0x0af5_0000_c0ed;

/// How long a relayed op lingers in the table before GC. Generous enough to cover
/// the gap between a room's checkpoints, so a worker that starts hosting a document
/// can replay recent ops and catch up to the current state.
#[cfg(feature = "coedit")]
const COEDIT_OP_TTL_SECS: i64 = 300;

/// GC runs on roughly 1-in-N publishes (keyed off the op's `seq`), so the table
/// stays small without a DELETE on every write.
#[cfg(feature = "coedit")]
const COEDIT_GC_EVERY: i64 = 256;

/// Max relayed ops a single `recv` drains at once.
#[cfg(feature = "coedit")]
const COEDIT_DRAIN_BATCH: i64 = 1024;

/// One relayed update: the attributed delta `origin` produced for `path`.
#[cfg(feature = "coedit")]
#[derive(Clone, Debug)]
pub struct CoeditRelayNote {
    pub seq: i64,
    pub origin: String,
    pub path: String,
    pub delta: Vec<u8>,
}

#[cfg(feature = "coedit")]
impl PostgresMetadataStore {
    /// Create the ephemeral relay table if it's missing (idempotent). The relay is
    /// scratch space for cross-worker fan-out, not durable workspace data, so it
    /// sits outside the versioned migrations.
    ///
    /// `CREATE TABLE IF NOT EXISTS` alone races under concurrency in Postgres (two
    /// sessions passing the existence check at once error out), and every worker
    /// calls this on startup — so serialize on a transaction-scoped advisory lock,
    /// the same way schema bootstrap does. Whoever wins creates it; the rest wait
    /// and no-op.
    pub async fn coedit_relay_init(&self) -> Result<()> {
        let c = self.client().await?;
        c.batch_execute(&format!(
            "BEGIN;
             SELECT pg_advisory_xact_lock({COEDIT_RELAY_LOCK_KEY});
             CREATE TABLE IF NOT EXISTS coedit_op (
                 seq        BIGSERIAL PRIMARY KEY,
                 path       TEXT   NOT NULL,
                 origin     TEXT   NOT NULL,
                 delta      BYTEA  NOT NULL,
                 created_at BIGINT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS coedit_op_created ON coedit_op (created_at);
             COMMIT;"
        ))
        .await
        .map_err(|e| OrigoFSError::Metadata(e.to_string()))?;
        Ok(())
    }

    /// Publish an attributed `delta` for `path` from worker `origin`: persist it
    /// and wake every listening worker. Occasionally GCs expired ops.
    pub async fn coedit_publish(&self, path: &str, origin: &str, delta: &[u8]) -> Result<()> {
        let c = self.client().await?;
        let seq: i64 = c
            .query_one(
                "INSERT INTO coedit_op (path, origin, delta, created_at)
                 VALUES ($1, $2, $3, $4) RETURNING seq",
                &[&path, &origin, &delta, &now_secs()],
            )
            .await?
            .get(0);
        // A bare wakeup: the table is the source of truth, so the payload carries
        // nothing and a coalesced NOTIFY never loses an op.
        c.execute("SELECT pg_notify($1, '')", &[&COEDIT_CHANNEL])
            .await?;
        if seq % COEDIT_GC_EVERY == 0 {
            let cutoff = now_secs() - COEDIT_OP_TTL_SECS;
            let _ = c
                .execute("DELETE FROM coedit_op WHERE created_at < $1", &[&cutoff])
                .await;
        }
        Ok(())
    }

    /// Every relayed op currently held for `path` (within the TTL), oldest first —
    /// for a worker that has just started hosting `path` to replay and catch up to
    /// the state its peers already share. Applying is idempotent, so replaying ops
    /// already folded into the checkpoint is harmless.
    pub async fn coedit_replay(&self, path: &str) -> Result<Vec<CoeditRelayNote>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT seq, origin, path, delta FROM coedit_op
                 WHERE path = $1 ORDER BY seq",
                &[&path],
            )
            .await?;
        Ok(rows.iter().map(row_to_relay_note).collect())
    }

    /// Subscribe to the relay: a dedicated `LISTEN` connection (pooled connections
    /// can't surface notifications), mirroring [`Self::subscribe`]. The returned
    /// [`CoeditRelaySub`] drains every worker's ops in `seq` order; the caller skips
    /// its own (`origin`) and any path it isn't hosting.
    pub async fn coedit_subscribe(&self) -> Result<CoeditRelaySub> {
        self.coedit_relay_init().await?;
        // A dedicated connection (pooled ones can't surface async notifications),
        // over the same TLS policy as the pool.
        let (client, mut connection) = tokio_postgres::connect(&self.dsn, tls_connector()?)
            .await
            .map_err(|e| OrigoFSError::Metadata(e.to_string()))?;

        // Coalescing capacity-1 wakeups: a wake only means "re-drain", so a burst
        // collapses into one and the drained query stays the source of truth.
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);
        let driver = tokio::spawn(async move {
            let mut stream =
                futures::stream::poll_fn(move |cx| Pin::new(&mut connection).poll_message(cx));
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(AsyncMessage::Notification(_)) => match tx.try_send(()) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => break,
                    },
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        client
            .batch_execute(&format!("LISTEN {COEDIT_CHANNEL}"))
            .await
            .map_err(|e| OrigoFSError::Metadata(e.to_string()))?;

        // Start from 0 so the first drain reads ops already in the table — a
        // subscriber that came up just after a publish (or after its NOTIFY) still
        // sees it, closing the subscribe/publish race. Ops for paths this worker
        // isn't hosting are skipped by the caller, and re-applying one it has is a
        // no-op, so replaying the (TTL-bounded) backlog is cheap and safe.
        Ok(CoeditRelaySub {
            client,
            wakeups: rx,
            cursor: 0,
            driver,
        })
    }
}

/// A live `LISTEN`-backed subscription to the co-edit relay. Dropping it tears
/// down the dedicated connection and the forwarder task.
#[cfg(feature = "coedit")]
pub struct CoeditRelaySub {
    client: tokio_postgres::Client,
    wakeups: tokio::sync::mpsc::Receiver<()>,
    cursor: i64,
    driver: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "coedit")]
impl Drop for CoeditRelaySub {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

#[cfg(feature = "coedit")]
impl CoeditRelaySub {
    /// Block until at least one new op is published, then return the batch (in
    /// `seq` order) and advance the cursor. Returns `Ok(vec![])` only once the
    /// underlying connection has closed.
    pub async fn recv(&mut self) -> Result<Vec<CoeditRelayNote>> {
        loop {
            let batch = self.drain().await?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            if self.wakeups.recv().await.is_none() {
                return Ok(Vec::new());
            }
        }
    }

    async fn drain(&mut self) -> Result<Vec<CoeditRelayNote>> {
        let rows = self
            .client
            .query(
                "SELECT seq, origin, path, delta FROM coedit_op
                 WHERE seq > $1 ORDER BY seq LIMIT $2",
                &[&self.cursor, &COEDIT_DRAIN_BATCH],
            )
            .await
            .map_err(|e| OrigoFSError::Metadata(e.to_string()))?;
        let notes: Vec<CoeditRelayNote> = rows.iter().map(row_to_relay_note).collect();
        if let Some(last) = notes.last() {
            self.cursor = last.seq;
        }
        Ok(notes)
    }
}

/// Decode a `coedit_op` row (columns: seq, origin, path, delta).
#[cfg(feature = "coedit")]
fn row_to_relay_note(r: &Row) -> CoeditRelayNote {
    CoeditRelayNote {
        seq: r.get(0),
        origin: r.get(1),
        path: r.get(2),
        delta: r.get(3),
    }
}

/// Decode a `fs_event` row (columns: seq, actor_id, session_id, kind, path,
/// detail, ts, branch) into an [`Event`].
fn row_to_event(r: &Row) -> Event {
    Event {
        seq: r.get(0),
        actor_id: r.get(1),
        session_id: r.get(2),
        kind: r.get(3),
        path: r.get(4),
        detail: r.get(5),
        ts: r.get(6),
        branch: r.get(7),
    }
}

fn row_to_inode(r: &Row) -> Result<Inode> {
    let kind_s: String = r.get(1);
    let kind = FileKind::parse(&kind_s)
        .ok_or_else(|| OrigoFSError::Metadata(format!("unknown inode kind {kind_s:?}")))?;
    let content = match r.get::<_, Option<String>>(5) {
        Some(s) => Some(
            Hash::from_hex(&s)
                .ok_or_else(|| OrigoFSError::Metadata(format!("bad content hash {s:?}")))?,
        ),
        None => None,
    };
    Ok(Inode {
        ino: r.get(0),
        kind,
        mode: r.get::<_, i64>(2) as u32,
        nlink: r.get(3),
        size: r.get::<_, i64>(4) as u64,
        content,
        mtime: r.get(6),
        ctime: r.get(7),
    })
}

/// Clear only workspace `ws`'s working tree (checkout/merge/rebuild). Mirrors the
/// SQLite helper: `dentry`/`symlink` carry no `workspace_id`, so they are cleared
/// via inode ownership; the workspace's own root inode is kept.
async fn truncate_workspace_tree_pg(c: &tokio_postgres::Client, ws: i64) -> Result<()> {
    c.execute(
        "DELETE FROM dentry WHERE parent_ino IN (SELECT ino FROM inode WHERE workspace_id = $1)",
        &[&ws],
    )
    .await?;
    c.execute(
        "DELETE FROM symlink WHERE ino IN (SELECT ino FROM inode WHERE workspace_id = $1)",
        &[&ws],
    )
    .await?;
    c.execute(
        "DELETE FROM inode WHERE workspace_id = $1
           AND ino <> (SELECT root_ino FROM workspace WHERE id = $1)",
        &[&ws],
    )
    .await?;
    Ok(())
}

#[async_trait]
impl MetadataStore for PostgresMetadataStore {
    async fn init(&self) -> Result<()> {
        let mut c = self.client().await?;
        let now = now_secs();
        // The whole bootstrap is ONE transaction: a crash can never leave a
        // migration's DDL applied without its `schema_meta` row (which would
        // brick the next `init` on a non-idempotent step). A transaction-scoped
        // advisory lock serializes concurrent multi-writer bootstraps and is
        // auto-released at commit/rollback, so it can't leak on the error path.
        let tx = c.transaction().await?;
        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&MIGRATION_LOCK_KEY])
            .await?;
        tx.batch_execute(
            "CREATE TABLE IF NOT EXISTS schema_meta(version BIGINT PRIMARY KEY, applied_at BIGINT NOT NULL);",
        )
        .await?;
        for m in MIGRATIONS {
            let applied = tx
                .query_opt(
                    "SELECT 1 FROM schema_meta WHERE version = $1",
                    &[&m.version],
                )
                .await?
                .is_some();
            if !applied {
                tx.batch_execute(m.postgres).await?;
                tx.execute(
                    "INSERT INTO schema_meta(version, applied_at) VALUES ($1, $2)",
                    &[&m.version, &now],
                )
                .await?;
            }
        }
        // Root directory (ino=1). `ino` is `GENERATED BY DEFAULT AS IDENTITY`, so
        // inserting this explicit id does NOT advance the sequence.
        tx.execute(
            "INSERT INTO inode(ino, workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime)
             VALUES (1, 1, 'dir', $1, 1, 0, NULL, $2, $2) ON CONFLICT (ino) DO NOTHING",
            &[&DIR_MODE, &now],
        )
        .await?;
        // Advance the identity sequence past any explicitly-inserted inodes, but
        // ONLY when it is actually behind `MAX(ino)`. That happens on a fresh DB
        // (the hand-inserted root=1) and on a V10→V11 upgrade of a data-bearing
        // store (rows carried explicit ids the sequence never saw) — cases with no
        // concurrent writers. In normal operation the sequence is always ≥ MAX(ino)
        // (every inode came from `nextval`, which bumps it before the row commits),
        // so the guard is false and we don't touch it. Without the guard, running
        // this on every `open_pg` would race live writers: `setval` reads a snapshot
        // `MAX(ino)` and could reset the sequence *backward* past inodes another
        // connection had already allocated, handing out a duplicate id (`inode_pkey`).
        tx.execute(
            "SELECT setval(pg_get_serial_sequence('inode', 'ino'), (SELECT MAX(ino) FROM inode))
             WHERE (SELECT MAX(ino) FROM inode)
                 > COALESCE(pg_sequence_last_value(pg_get_serial_sequence('inode', 'ino')::regclass), 0)",
            &[],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn schema_version(&self) -> Result<i64> {
        let c = self.client().await?;
        match c
            .query_one("SELECT COALESCE(MAX(version), 0) FROM schema_meta", &[])
            .await
        {
            Ok(row) => Ok(row.get::<_, i64>(0)),
            // A store that was never initialized has no schema_meta table yet.
            Err(e) if e.to_string().contains("does not exist") => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    async fn begin(&self) -> Result<Box<dyn MetaTxn>> {
        // Pin one pooled connection for the whole `BEGIN … COMMIT`. All the
        // transaction's statements run on this same connection; it returns to
        // the pool only on commit or rollback.
        let obj = self.client().await?;
        obj.batch_execute("BEGIN").await?;
        Ok(Box::new(PostgresTxn {
            obj: Some(obj),
            workspace_id: self.workspace_id,
        }))
    }

    async fn get_inode(&self, ino: Ino) -> Result<Option<Inode>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT ino, kind, mode, nlink, size, content_hash, mtime, ctime
                 FROM inode WHERE ino = $1",
                &[&ino],
            )
            .await?;
        match row {
            Some(r) => Ok(Some(row_to_inode(&r)?)),
            None => Ok(None),
        }
    }

    async fn get_inodes(&self, inos: &[Ino]) -> Result<Vec<Inode>> {
        if inos.is_empty() {
            return Ok(Vec::new());
        }
        let c = self.client().await?;
        // `= ANY($1)` passes the whole list as one array parameter, so there is no
        // per-statement parameter ceiling to chunk around (unlike SQLite) and the
        // plan stays a single index probe per key.
        let rows = c
            .query(
                "SELECT ino, kind, mode, nlink, size, content_hash, mtime, ctime
                 FROM inode WHERE ino = ANY($1)",
                &[&inos],
            )
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(row_to_inode(&r)?);
        }
        Ok(out)
    }

    async fn create_inode(&self, init: InodeInit) -> Result<Ino> {
        let c = self.client().await?;
        let now = now_secs();
        let mode = init.mode as i64;
        let row = c
            .query_one(
                "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime)
                 VALUES ($1, $2, $3, 1, 0, NULL, $4, $4) RETURNING ino",
                &[&self.workspace_id, &init.kind.as_str(), &mode, &now],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn set_content(&self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()> {
        let c = self.client().await?;
        let hex = content.map(|h| h.to_hex());
        let size = size as i64;
        let now = now_secs();
        c.execute(
            "UPDATE inode SET content_hash = $1, size = $2, mtime = $3, ctime = $3 WHERE ino = $4",
            &[&hex, &size, &now, &ino],
        )
        .await?;
        Ok(())
    }

    async fn set_nlink(&self, ino: Ino, nlink: i64) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "UPDATE inode SET nlink = $1 WHERE ino = $2",
            &[&nlink, &ino],
        )
        .await?;
        Ok(())
    }

    async fn delete_inode(&self, ino: Ino) -> Result<()> {
        let c = self.client().await?;
        c.execute("DELETE FROM symlink WHERE ino = $1", &[&ino])
            .await?;
        c.execute("DELETE FROM inode WHERE ino = $1", &[&ino])
            .await?;
        Ok(())
    }

    async fn lookup(&self, parent: Ino, name: &str) -> Result<Option<Ino>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT ino FROM dentry WHERE parent_ino = $1 AND name = $2",
                &[&parent, &name],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn add_dentry(&self, parent: Ino, name: &str, ino: Ino) -> Result<()> {
        let c = self.client().await?;
        match c
            .execute(
                "INSERT INTO dentry(parent_ino, name, ino) VALUES ($1, $2, $3)",
                &[&parent, &name, &ino],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
                Err(OrigoFSError::AlreadyExists(name.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn remove_dentry(&self, parent: Ino, name: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "DELETE FROM dentry WHERE parent_ino = $1 AND name = $2",
            &[&parent, &name],
        )
        .await?;
        Ok(())
    }

    async fn list_dir(&self, parent: Ino) -> Result<Vec<DirEntry>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT d.name, d.ino, i.kind
                 FROM dentry d JOIN inode i ON i.ino = d.ino
                 WHERE d.parent_ino = $1
                 ORDER BY d.name",
                &[&parent],
            )
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let kind_s: String = r.get(2);
            let kind = FileKind::parse(&kind_s)
                .ok_or_else(|| OrigoFSError::Metadata(format!("unknown inode kind {kind_s:?}")))?;
            out.push(DirEntry {
                name: r.get(0),
                ino: r.get(1),
                kind,
            });
        }
        Ok(out)
    }

    async fn list_dir_page(
        &self,
        parent: Ino,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DirEntry>> {
        let c = self.client().await?;
        let limit = limit as i64;
        // Two statements rather than one with `($2::text IS NULL OR d.name > $2)`:
        // the OR would stop the planner using the `(parent_ino, name)` primary-key
        // index as a range scan and re-read the whole directory per page.
        let rows = match after_name {
            Some(after) => {
                c.query(
                    "SELECT d.name, d.ino, i.kind
                     FROM dentry d JOIN inode i ON i.ino = d.ino
                     WHERE d.parent_ino = $1 AND d.name > $2
                     ORDER BY d.name LIMIT $3",
                    &[&parent, &after, &limit],
                )
                .await?
            }
            None => {
                c.query(
                    "SELECT d.name, d.ino, i.kind
                     FROM dentry d JOIN inode i ON i.ino = d.ino
                     WHERE d.parent_ino = $1
                     ORDER BY d.name LIMIT $2",
                    &[&parent, &limit],
                )
                .await?
            }
        };
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let kind_s: String = r.get(2);
            let kind = FileKind::parse(&kind_s)
                .ok_or_else(|| OrigoFSError::Metadata(format!("unknown inode kind {kind_s:?}")))?;
            out.push(DirEntry {
                name: r.get(0),
                ino: r.get(1),
                kind,
            });
        }
        Ok(out)
    }

    async fn dentry_name(&self, parent: Ino, ino: Ino) -> Result<Option<String>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT name FROM dentry WHERE parent_ino = $1 AND ino = $2
                 ORDER BY name LIMIT 1",
                &[&parent, &ino],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn parent_of(&self, ino: Ino) -> Result<Option<Ino>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT parent_ino FROM dentry WHERE ino = $1 LIMIT 1",
                &[&ino],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn child_count(&self, parent: Ino) -> Result<usize> {
        let c = self.client().await?;
        let row = c
            .query_one(
                "SELECT COUNT(*) FROM dentry WHERE parent_ino = $1",
                &[&parent],
            )
            .await?;
        Ok(row.get::<_, i64>(0) as usize)
    }

    async fn set_symlink(&self, ino: Ino, target: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO symlink(ino, target) VALUES ($1, $2)
             ON CONFLICT (ino) DO UPDATE SET target = EXCLUDED.target",
            &[&ino, &target],
        )
        .await?;
        Ok(())
    }

    async fn get_symlink(&self, ino: Ino) -> Result<Option<String>> {
        let c = self.client().await?;
        let row = c
            .query_opt("SELECT target FROM symlink WHERE ino = $1", &[&ino])
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn get_ref(&self, name: &str) -> Result<Option<String>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT value FROM ref WHERE workspace_id = $1 AND name = $2",
                &[&self.workspace_id, &name],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn set_ref(&self, name: &str, value: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO ref(workspace_id, name, value) VALUES ($1, $2, $3)
             ON CONFLICT (workspace_id, name) DO UPDATE SET value = EXCLUDED.value",
            &[&self.workspace_id, &name, &value],
        )
        .await?;
        Ok(())
    }

    async fn cas_ref(&self, name: &str, expect: Option<&str>, new: &str) -> Result<bool> {
        let c = self.client().await?;
        let changed = match expect {
            None => {
                c.execute(
                    "INSERT INTO ref(workspace_id, name, value) VALUES ($1, $2, $3)
                     ON CONFLICT (workspace_id, name) DO NOTHING",
                    &[&self.workspace_id, &name, &new],
                )
                .await?
            }
            Some(v) => {
                c.execute(
                    "UPDATE ref SET value = $1 WHERE workspace_id = $2 AND name = $3 AND value = $4",
                    &[&new, &self.workspace_id, &name, &v],
                )
                .await?
            }
        };
        Ok(changed == 1)
    }

    async fn delete_ref(&self, name: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "DELETE FROM ref WHERE workspace_id = $1 AND name = $2",
            &[&self.workspace_id, &name],
        )
        .await?;
        Ok(())
    }

    async fn list_refs(&self) -> Result<Vec<(String, String)>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT name, value FROM ref WHERE workspace_id = $1 ORDER BY name",
                &[&self.workspace_id],
            )
            .await?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn get_config(&self, key: &str) -> Result<Option<String>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT value FROM config WHERE workspace_id = $1 AND key = $2",
                &[&self.workspace_id, &key],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn set_config(&self, key: &str, value: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO config(workspace_id, key, value) VALUES ($1, $2, $3)
             ON CONFLICT (workspace_id, key) DO UPDATE SET value = EXCLUDED.value",
            &[&self.workspace_id, &key, &value],
        )
        .await?;
        Ok(())
    }

    async fn bump_counter(&self, key: &str) -> Result<i64> {
        let c = self.client().await?;
        // One atomic upsert: create at 1, else increment the stored integer.
        let row = c
            .query_one(
                "INSERT INTO config(workspace_id, key, value) VALUES ($1, $2, '1')
                 ON CONFLICT (workspace_id, key) DO UPDATE SET value = (config.value::bigint + 1)::text
                 RETURNING value::bigint",
                &[&self.workspace_id, &key],
            )
            .await?;
        Ok(row.get(0))
    }

    fn with_workspace(&self, workspace_id: i64) -> Arc<dyn MetadataStore> {
        Arc::new(PostgresMetadataStore {
            pool: self.pool.clone(),
            dsn: self.dsn.clone(),
            workspace_id,
        })
    }

    async fn create_workspace(&self, name: &str) -> Result<(i64, Ino)> {
        let mut c = self.client().await?;
        let now = now_secs();
        let tx = c.transaction().await?;
        // Reserve the row (fails on a duplicate name), give it its own root
        // directory inode, then point the row at that inode — all atomic.
        let id: i64 = match tx
            .query_one(
                "INSERT INTO workspace(name, root_ino, created_at) VALUES ($1, 0, $2) RETURNING id",
                &[&name, &now],
            )
            .await
        {
            Ok(row) => row.get(0),
            Err(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
                return Err(OrigoFSError::AlreadyExists(format!("workspace {name}")));
            }
            Err(e) => return Err(e.into()),
        };
        let mode = DIR_MODE;
        let root_ino: i64 = tx
            .query_one(
                "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime)
                 VALUES ($1, 'dir', $2, 1, 0, NULL, $3, $3) RETURNING ino",
                &[&id, &mode, &now],
            )
            .await?
            .get(0);
        tx.execute(
            "UPDATE workspace SET root_ino = $1 WHERE id = $2",
            &[&root_ino, &id],
        )
        .await?;
        tx.commit().await?;
        Ok((id, root_ino))
    }

    async fn lookup_workspace(&self, name: &str) -> Result<Option<(i64, Ino)>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT id, root_ino FROM workspace WHERE name = $1",
                &[&name],
            )
            .await?;
        Ok(row.map(|r| (r.get(0), r.get(1))))
    }

    async fn list_workspaces(&self) -> Result<Vec<(i64, String, Ino)>> {
        let c = self.client().await?;
        let rows = c
            .query("SELECT id, name, root_ino FROM workspace ORDER BY id", &[])
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect())
    }

    async fn truncate_tree(&self) -> Result<()> {
        let c = self.client().await?;
        truncate_workspace_tree_pg(&c, self.workspace_id).await?;
        Ok(())
    }

    async fn set_conflict(&self, path: &str, kind: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO conflict(workspace_id, path, kind) VALUES ($1, $2, $3)
             ON CONFLICT (workspace_id, path) DO UPDATE SET kind = EXCLUDED.kind",
            &[&self.workspace_id, &path, &kind],
        )
        .await?;
        Ok(())
    }

    async fn list_conflicts(&self) -> Result<Vec<(String, String)>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT path, kind FROM conflict WHERE workspace_id = $1 ORDER BY path",
                &[&self.workspace_id],
            )
            .await?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn clear_conflicts(&self) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "DELETE FROM conflict WHERE workspace_id = $1",
            &[&self.workspace_id],
        )
        .await?;
        Ok(())
    }

    async fn acquire_lock(&self, path: &str, owner: &str, at: i64) -> Result<bool> {
        let c = self.client().await?;
        let changed = c
            .execute(
                "INSERT INTO file_lock(workspace_id, path, owner, acquired_at) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (workspace_id, path) DO NOTHING",
                &[&self.workspace_id, &path, &owner, &at],
            )
            .await?;
        Ok(changed == 1)
    }

    async fn release_lock(&self, path: &str, owner: &str) -> Result<bool> {
        let c = self.client().await?;
        let changed = c
            .execute(
                "DELETE FROM file_lock WHERE workspace_id = $1 AND path = $2 AND owner = $3",
                &[&self.workspace_id, &path, &owner],
            )
            .await?;
        Ok(changed == 1)
    }

    async fn list_locks(&self) -> Result<Vec<(String, String, i64)>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT path, owner, acquired_at FROM file_lock WHERE workspace_id = $1 ORDER BY path",
                &[&self.workspace_id],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect())
    }

    async fn create_actor(&self, init: ActorInit) -> Result<i64> {
        let c = self.client().await?;
        let kind = init.kind.unwrap_or(ActorKind::System).as_str();
        let now = now_secs();
        let row = c
            .query_one(
                "INSERT INTO actor(kind, display_name, auth_subject, agent_model, agent_vendor, controller_actor_id, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
                &[
                    &kind,
                    &init.display_name,
                    &init.auth_subject,
                    &init.agent_model,
                    &init.agent_vendor,
                    &init.controller_actor_id,
                    &now,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn get_actor(&self, id: i64) -> Result<Option<Actor>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT id, kind, display_name, auth_subject, agent_model, agent_vendor, controller_actor_id, created_at, write_policy
                 FROM actor WHERE id = $1",
                &[&id],
            )
            .await?;
        match row {
            Some(r) => {
                let kind_s: String = r.get(1);
                let kind = ActorKind::parse(&kind_s)
                    .ok_or_else(|| OrigoFSError::Metadata(format!("bad actor kind {kind_s:?}")))?;
                Ok(Some(Actor {
                    id: r.get(0),
                    kind,
                    display_name: r.get(2),
                    auth_subject: r.get(3),
                    agent_model: r.get(4),
                    agent_vendor: r.get(5),
                    controller_actor_id: r.get(6),
                    created_at: r.get(7),
                    write_policy: WritePolicy::from_i64(r.get(8)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn set_write_policy(&self, actor_id: i64, policy: WritePolicy) -> Result<()> {
        let c = self.client().await?;
        let n = c
            .execute(
                "UPDATE actor SET write_policy = $1 WHERE id = $2",
                &[&policy.as_i64(), &actor_id],
            )
            .await?;
        if n == 0 {
            return Err(OrigoFSError::NotFound(format!("actor #{actor_id}")));
        }
        Ok(())
    }

    async fn actor_by_subject(&self, subject: &str) -> Result<Option<Actor>> {
        // Resolve the id, then reuse get_actor for the row mapping.
        let id: Option<i64> = {
            let c = self.client().await?;
            c.query_opt("SELECT id FROM actor WHERE auth_subject = $1", &[&subject])
                .await?
                .map(|r| r.get(0))
        };
        match id {
            Some(id) => self.get_actor(id).await,
            None => Ok(None),
        }
    }

    async fn list_actors(&self) -> Result<Vec<Actor>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT id, kind, display_name, auth_subject, agent_model, agent_vendor, controller_actor_id, created_at, write_policy
                 FROM actor ORDER BY id",
                &[],
            )
            .await?;
        let mut actors = Vec::with_capacity(rows.len());
        for r in rows {
            let kind_s: String = r.get(1);
            let kind = ActorKind::parse(&kind_s)
                .ok_or_else(|| OrigoFSError::Metadata(format!("bad actor kind {kind_s:?}")))?;
            actors.push(Actor {
                id: r.get(0),
                kind,
                display_name: r.get(2),
                auth_subject: r.get(3),
                agent_model: r.get(4),
                agent_vendor: r.get(5),
                controller_actor_id: r.get(6),
                created_at: r.get(7),
                write_policy: WritePolicy::from_i64(r.get(8)),
            });
        }
        Ok(actors)
    }

    async fn create_session(
        &self,
        actor_id: i64,
        client: Option<&str>,
        started_at: i64,
    ) -> Result<i64> {
        let c = self.client().await?;
        let row = c
            .query_one(
                "INSERT INTO session(actor_id, client, started_at, ended_at) VALUES ($1, $2, $3, NULL) RETURNING id",
                &[&actor_id, &client, &started_at],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn record_tool_call(&self, tc: ToolCallInit) -> Result<i64> {
        let c = self.client().await?;
        let row = c
            .query_one(
                "INSERT INTO tool_calls(session_id, actor_id, name, parameters, result, error, started_at, completed_at, duration_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id",
                &[
                    &tc.session_id, &tc.actor_id, &tc.name, &tc.parameters, &tc.result, &tc.error,
                    &tc.started_at, &tc.completed_at, &tc.duration_ms,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn append_edit_op(&self, op: EditOpInit) -> Result<i64> {
        let c = self.client().await?;
        let row = c
            .query_one(
                "INSERT INTO edit_op(workspace_id, session_id, actor_id, tool_call_id, ino, path, op, byte_start, byte_len, pre_hash, post_hash, ts)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING id",
                &[
                    &self.workspace_id, &op.session_id, &op.actor_id, &op.tool_call_id, &op.ino, &op.path, &op.op,
                    &op.byte_start, &op.byte_len, &op.pre_hash, &op.post_hash, &op.ts,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn list_edit_ops(&self, actor_id: i64, session_id: Option<i64>) -> Result<Vec<EditOp>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT id, session_id, actor_id, tool_call_id, ino, path, op, byte_start, byte_len, pre_hash, post_hash, ts
                 FROM edit_op WHERE workspace_id = $1 AND actor_id = $2 AND ($3::bigint IS NULL OR session_id = $3::bigint) ORDER BY id",
                &[&self.workspace_id, &actor_id, &session_id],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| EditOp {
                id: r.get(0),
                session_id: r.get(1),
                actor_id: r.get(2),
                tool_call_id: r.get(3),
                ino: r.get(4),
                path: r.get(5),
                op: r.get(6),
                byte_start: r.get(7),
                byte_len: r.get(8),
                pre_hash: r.get(9),
                post_hash: r.get(10),
                ts: r.get(11),
            })
            .collect())
    }

    async fn set_blob_blame(&self, content: &Hash, runs: &str) -> Result<()> {
        let c = self.client().await?;
        let hex = content.to_hex();
        c.execute(
            "INSERT INTO blob_blame(workspace_id, content_hash, runs) VALUES ($1, $2, $3)
             ON CONFLICT (workspace_id, content_hash) DO UPDATE SET runs = EXCLUDED.runs",
            &[&self.workspace_id, &hex, &runs],
        )
        .await?;
        Ok(())
    }

    async fn get_blob_blame(&self, content: &Hash) -> Result<Option<String>> {
        let c = self.client().await?;
        let hex = content.to_hex();
        let row = c
            .query_opt(
                "SELECT runs FROM blob_blame WHERE workspace_id = $1 AND content_hash = $2",
                &[&self.workspace_id, &hex],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn append_event(&self, ev: EventInit, ts: i64) -> Result<i64> {
        let mut c = self.client().await?;
        let tx = c.transaction().await?;
        // Serialize appends so `seq` commits in assignment order (H6). `seq` is an
        // identity assigned at INSERT, but a row only becomes *visible* at COMMIT;
        // under concurrency a higher seq could commit first, and a tailer that
        // advanced its cursor past it would never deliver the lower seq once it
        // finally committed — a silently dropped change. Holding this lock from
        // before the INSERT until COMMIT means a lower seq is always committed
        // (and visible) before any higher seq is even assigned, so the feed's
        // `seq > cursor` scan can't skip one. It also makes the branch-filter
        // cursor's jump to `max(seq)` safe (L7). The critical section is just the
        // insert+notify, so contention is minimal. (A rollback still burns an
        // identity value, but that leaves a *permanent* gap the reader correctly
        // ignores — only *transient* gaps drop events.)
        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&FEED_LOCK_KEY])
            .await?;
        let row = tx
            .query_one(
                "INSERT INTO fs_event(workspace_id, actor_id, session_id, kind, path, detail, ts, branch)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING seq",
                &[
                    &self.workspace_id,
                    &ev.actor_id,
                    &ev.session_id,
                    &ev.kind,
                    &ev.path,
                    &ev.detail,
                    &ts,
                    &ev.branch,
                ],
            )
            .await?;
        let seq: i64 = row.get(0);
        // NOTIFY in the same transaction: Postgres queues it and delivers on
        // commit, discarding it on rollback. So the row and its wakeup are atomic
        // — closing the window where the row committed but a separate NOTIFY
        // failed and the caller retried, duplicating the event (L4).
        let payload = seq.to_string();
        tx.execute("SELECT pg_notify($1, $2)", &[&EVENT_CHANNEL, &payload])
            .await?;
        tx.commit().await?;
        Ok(seq)
    }

    async fn events_since(&self, after_seq: i64, limit: i64) -> Result<Vec<Event>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT seq, actor_id, session_id, kind, path, detail, ts, branch FROM fs_event
                 WHERE workspace_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
                &[&self.workspace_id, &after_seq, &limit],
            )
            .await?;
        Ok(rows.iter().map(row_to_event).collect())
    }

    async fn touch_presence(
        &self,
        session_id: i64,
        actor_id: i64,
        path: Option<&str>,
        at: i64,
    ) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO presence(session_id, workspace_id, actor_id, path, last_seen) VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (session_id) DO UPDATE SET
                 workspace_id = EXCLUDED.workspace_id, actor_id = EXCLUDED.actor_id,
                 path = EXCLUDED.path, last_seen = EXCLUDED.last_seen",
            &[&session_id, &self.workspace_id, &actor_id, &path, &at],
        )
        .await?;
        Ok(())
    }

    async fn active_presence(&self, since_ts: i64) -> Result<Vec<Presence>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT p.session_id, p.actor_id, a.display_name, a.kind, p.path, p.last_seen
                 FROM presence p JOIN actor a ON a.id = p.actor_id
                 WHERE p.workspace_id = $1 AND p.last_seen >= $2 ORDER BY p.last_seen DESC",
                &[&self.workspace_id, &since_ts],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let kind: String = r.get(3);
                Presence {
                    session_id: r.get(0),
                    actor_id: r.get(1),
                    display_name: r.get(2),
                    kind: ActorKind::parse(&kind).unwrap_or(ActorKind::System),
                    path: r.get(4),
                    last_seen: r.get(5),
                }
            })
            .collect())
    }

    async fn reap_presence(&self, older_than: i64) -> Result<u64> {
        let c = self.client().await?;
        // Scoped to this workspace (see the SQLite twin): a store-wide reap would
        // evict other workspaces' presence rows, including their live sessions.
        let n = c
            .execute(
                "DELETE FROM presence WHERE workspace_id = $1 AND last_seen < $2",
                &[&self.workspace_id, &older_than],
            )
            .await?;
        Ok(n)
    }

    async fn set_live_doc(
        &self,
        path: &str,
        session_id: Option<i64>,
        actor_id: i64,
        content_hash: Option<&str>,
        at: i64,
    ) -> Result<()> {
        let c = self.client().await?;
        // `since` is deliberately not in the DO UPDATE list: re-marking an
        // already-live path (a second joiner, a checkpoint) keeps when it first
        // went live.
        c.execute(
            "INSERT INTO live_doc(workspace_id, path, session_id, actor_id, content_hash, since)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (workspace_id, path) DO UPDATE SET
                 session_id = EXCLUDED.session_id,
                 actor_id = EXCLUDED.actor_id,
                 content_hash = EXCLUDED.content_hash",
            &[
                &self.workspace_id,
                &path,
                &session_id,
                &actor_id,
                &content_hash,
                &at,
            ],
        )
        .await?;
        Ok(())
    }

    async fn get_live_doc(&self, path: &str) -> Result<Option<LiveDoc>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT path, session_id, actor_id, content_hash, since
                 FROM live_doc WHERE workspace_id = $1 AND path = $2",
                &[&self.workspace_id, &path],
            )
            .await?;
        Ok(row.as_ref().map(row_to_live_doc))
    }

    async fn list_live_docs(&self) -> Result<Vec<LiveDoc>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT path, session_id, actor_id, content_hash, since
                 FROM live_doc WHERE workspace_id = $1 ORDER BY path",
                &[&self.workspace_id],
            )
            .await?;
        Ok(rows.iter().map(row_to_live_doc).collect())
    }

    async fn clear_live_doc(&self, path: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "DELETE FROM live_doc WHERE workspace_id = $1 AND path = $2",
            &[&self.workspace_id, &path],
        )
        .await?;
        Ok(())
    }

    async fn create_suggestion(&self, init: SuggestionInit, ts: i64) -> Result<i64> {
        let c = self.client().await?;
        let row = c
            .query_one(
                "INSERT INTO suggestion(workspace_id, actor_id, session_id, branch, path, base_hash,
                     proposed_hash, summary, status, created_ts, kind)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id",
                &[
                    &self.workspace_id,
                    &init.actor_id,
                    &init.session_id,
                    &init.branch,
                    &init.path,
                    &init.base_hash,
                    &init.proposed_hash,
                    &init.summary,
                    &SuggestionStatus::Pending.as_str(),
                    &ts,
                    &init.kind.as_str(),
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn get_suggestion(&self, id: i64) -> Result<Option<Suggestion>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT id, actor_id, session_id, branch, path, base_hash, proposed_hash,
                     summary, status, created_ts, resolved_ts, resolved_by, kind
                 FROM suggestion WHERE id = $1 AND workspace_id = $2",
                &[&id, &self.workspace_id],
            )
            .await?;
        Ok(row.as_ref().map(row_to_suggestion))
    }

    async fn list_suggestions(
        &self,
        status: Option<SuggestionStatus>,
        path: Option<&str>,
    ) -> Result<Vec<Suggestion>> {
        let c = self.client().await?;
        let st = status.map(|s| s.as_str());
        let rows = c
            .query(
                "SELECT id, actor_id, session_id, branch, path, base_hash, proposed_hash,
                     summary, status, created_ts, resolved_ts, resolved_by, kind
                 FROM suggestion
                 WHERE workspace_id = $1 AND ($2::text IS NULL OR status = $2) AND ($3::text IS NULL OR path = $3)
                 ORDER BY id DESC",
                &[&self.workspace_id, &st, &path],
            )
            .await?;
        Ok(rows.iter().map(row_to_suggestion).collect())
    }

    async fn resolve_suggestion(
        &self,
        id: i64,
        status: SuggestionStatus,
        resolved_by: Option<i64>,
        ts: i64,
    ) -> Result<bool> {
        let c = self.client().await?;
        let n = c
            .execute(
                "UPDATE suggestion SET status = $1, resolved_by = $2, resolved_ts = $3
                 WHERE id = $4 AND workspace_id = $5 AND status = 'pending'",
                &[&status.as_str(), &resolved_by, &ts, &id, &self.workspace_id],
            )
            .await?;
        Ok(n == 1)
    }
}

/// A Postgres metadata transaction ([`MetadataStore::begin`]). Pins one pooled
/// connection for `BEGIN … COMMIT`. Dropped without [`commit`](MetaTxn::commit)
/// — an error path or a panic — it rolls back before the connection returns to
/// the pool, so no half-applied write commits and no reused connection inherits
/// an open transaction.
struct PostgresTxn {
    /// `Some` while open; `commit`/`Drop` take it to close exactly once.
    obj: Option<Object>,
    /// The workspace this txn is scoped to (inherited from the store handle).
    workspace_id: i64,
}

impl PostgresTxn {
    fn conn(&self) -> &Object {
        self.obj.as_ref().expect("transaction already finished")
    }
}

#[async_trait]
impl MetaTxn for PostgresTxn {
    async fn create_inode(&mut self, init: InodeInit) -> Result<Ino> {
        let now = now_secs();
        let mode = init.mode as i64;
        let ws = self.workspace_id;
        let row = self
            .conn()
            .query_one(
                "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime)
                 VALUES ($1, $2, $3, 1, 0, NULL, $4, $4) RETURNING ino",
                &[&ws, &init.kind.as_str(), &mode, &now],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn set_content(&mut self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()> {
        let hex = content.map(|h| h.to_hex());
        let size = size as i64;
        let now = now_secs();
        self.conn()
            .execute(
                "UPDATE inode SET content_hash = $1, size = $2, mtime = $3, ctime = $3 WHERE ino = $4",
                &[&hex, &size, &now, &ino],
            )
            .await?;
        Ok(())
    }

    async fn set_content_if(
        &mut self,
        ino: Ino,
        expected: Option<&Hash>,
        content: Option<Hash>,
        size: u64,
    ) -> Result<bool> {
        let new_hex = content.map(|h| h.to_hex());
        let expected_hex = expected.map(|h| h.to_hex());
        let size = size as i64;
        let now = now_secs();
        // `IS NOT DISTINCT FROM` is Postgres's null-safe equality (matches a NULL
        // current content too).
        let n = self
            .conn()
            .execute(
                "UPDATE inode SET content_hash = $1, size = $2, mtime = $3, ctime = $3
                 WHERE ino = $4 AND content_hash IS NOT DISTINCT FROM $5",
                &[&new_hex, &size, &now, &ino, &expected_hex],
            )
            .await?;
        Ok(n == 1)
    }

    async fn set_nlink(&mut self, ino: Ino, nlink: i64) -> Result<()> {
        self.conn()
            .execute(
                "UPDATE inode SET nlink = $1 WHERE ino = $2",
                &[&nlink, &ino],
            )
            .await?;
        Ok(())
    }

    async fn delete_inode(&mut self, ino: Ino) -> Result<()> {
        let c = self.conn();
        c.execute("DELETE FROM symlink WHERE ino = $1", &[&ino])
            .await?;
        c.execute("DELETE FROM inode WHERE ino = $1", &[&ino])
            .await?;
        Ok(())
    }

    async fn add_dentry(&mut self, parent: Ino, name: &str, ino: Ino) -> Result<()> {
        match self
            .conn()
            .execute(
                "INSERT INTO dentry(parent_ino, name, ino) VALUES ($1, $2, $3)",
                &[&parent, &name, &ino],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
                Err(OrigoFSError::AlreadyExists(name.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn remove_dentry(&mut self, parent: Ino, name: &str) -> Result<()> {
        self.conn()
            .execute(
                "DELETE FROM dentry WHERE parent_ino = $1 AND name = $2",
                &[&parent, &name],
            )
            .await?;
        Ok(())
    }

    async fn set_symlink(&mut self, ino: Ino, target: &str) -> Result<()> {
        self.conn()
            .execute(
                "INSERT INTO symlink(ino, target) VALUES ($1, $2)
                 ON CONFLICT (ino) DO UPDATE SET target = EXCLUDED.target",
                &[&ino, &target],
            )
            .await?;
        Ok(())
    }

    async fn set_blob_blame(&mut self, content: &Hash, runs: &str) -> Result<()> {
        let ws = self.workspace_id;
        let hex = content.to_hex();
        self.conn()
            .execute(
                "INSERT INTO blob_blame(workspace_id, content_hash, runs) VALUES ($1, $2, $3)
                 ON CONFLICT (workspace_id, content_hash) DO UPDATE SET runs = EXCLUDED.runs",
                &[&ws, &hex, &runs],
            )
            .await?;
        Ok(())
    }

    async fn append_edit_op(&mut self, op: EditOpInit) -> Result<i64> {
        let ws = self.workspace_id;
        let row = self
            .conn()
            .query_one(
                "INSERT INTO edit_op(workspace_id, session_id, actor_id, tool_call_id, ino, path, op, byte_start, byte_len, pre_hash, post_hash, ts)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING id",
                &[
                    &ws, &op.session_id, &op.actor_id, &op.tool_call_id, &op.ino, &op.path, &op.op,
                    &op.byte_start, &op.byte_len, &op.pre_hash, &op.post_hash, &op.ts,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn set_ref(&mut self, name: &str, value: &str) -> Result<()> {
        let ws = self.workspace_id;
        self.conn()
            .execute(
                "INSERT INTO ref(workspace_id, name, value) VALUES ($1, $2, $3)
                 ON CONFLICT (workspace_id, name) DO UPDATE SET value = excluded.value",
                &[&ws, &name, &value],
            )
            .await?;
        Ok(())
    }

    async fn cas_ref(&mut self, name: &str, expect: Option<&str>, new: &str) -> Result<bool> {
        let ws = self.workspace_id;
        let changed = match expect {
            None => {
                self.conn()
                    .execute(
                        "INSERT INTO ref(workspace_id, name, value) VALUES ($1, $2, $3)
                         ON CONFLICT (workspace_id, name) DO NOTHING",
                        &[&ws, &name, &new],
                    )
                    .await?
            }
            Some(v) => {
                self.conn()
                    .execute(
                        "UPDATE ref SET value = $1
                         WHERE workspace_id = $2 AND name = $3 AND value = $4",
                        &[&new, &ws, &name, &v],
                    )
                    .await?
            }
        };
        Ok(changed == 1)
    }

    async fn delete_ref(&mut self, name: &str) -> Result<()> {
        let ws = self.workspace_id;
        self.conn()
            .execute(
                "DELETE FROM ref WHERE workspace_id = $1 AND name = $2",
                &[&ws, &name],
            )
            .await?;
        Ok(())
    }

    async fn set_conflict(&mut self, path: &str, kind: &str) -> Result<()> {
        let ws = self.workspace_id;
        self.conn()
            .execute(
                "INSERT INTO conflict(workspace_id, path, kind) VALUES ($1, $2, $3)
                 ON CONFLICT (workspace_id, path) DO UPDATE SET kind = excluded.kind",
                &[&ws, &path, &kind],
            )
            .await?;
        Ok(())
    }

    async fn clear_conflicts(&mut self) -> Result<()> {
        let ws = self.workspace_id;
        self.conn()
            .execute("DELETE FROM conflict WHERE workspace_id = $1", &[&ws])
            .await?;
        Ok(())
    }

    async fn set_config(&mut self, key: &str, value: &str) -> Result<()> {
        let ws = self.workspace_id;
        self.conn()
            .execute(
                "INSERT INTO config(workspace_id, key, value) VALUES ($1, $2, $3)
                 ON CONFLICT (workspace_id, key) DO UPDATE SET value = excluded.value",
                &[&ws, &key, &value],
            )
            .await?;
        Ok(())
    }

    async fn append_event(&mut self, ev: EventInit, ts: i64) -> Result<i64> {
        let ws = self.workspace_id;
        let row = self
            .conn()
            .query_one(
                "INSERT INTO fs_event(workspace_id, actor_id, session_id, kind, path, detail, ts, branch)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING seq",
                &[
                    &ws,
                    &ev.actor_id,
                    &ev.session_id,
                    &ev.kind,
                    &ev.path,
                    &ev.detail,
                    &ts,
                    &ev.branch,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn resolve_suggestion(
        &mut self,
        id: i64,
        status: SuggestionStatus,
        resolved_by: Option<i64>,
        ts: i64,
    ) -> Result<bool> {
        let ws = self.workspace_id;
        let n = self
            .conn()
            .execute(
                "UPDATE suggestion SET status = $1, resolved_by = $2, resolved_ts = $3
                 WHERE id = $4 AND workspace_id = $5 AND status = 'pending'",
                &[&status.as_str(), &resolved_by, &ts, &id, &ws],
            )
            .await?;
        Ok(n == 1)
    }

    async fn truncate_tree(&mut self) -> Result<()> {
        // Same as MetadataStore::truncate_tree, staged in this transaction.
        let ws = self.workspace_id;
        truncate_workspace_tree_pg(self.conn(), ws).await?;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        let obj = self.obj.take().expect("transaction already finished");
        obj.batch_execute("COMMIT").await?;
        // `obj` drops here, returning a clean (no open txn) connection to the pool.
        Ok(())
    }
}

impl Drop for PostgresTxn {
    fn drop(&mut self) {
        // If the transaction wasn't committed, roll it back before the pinned
        // connection returns to the pool — otherwise a reused connection would
        // inherit the open transaction. `Drop` can't `await`, so spawn the
        // ROLLBACK and move the connection into that task; it is recycled only
        // once the rollback completes. Outside a runtime (a drop in sync
        // context) we let the connection close instead.
        if let Some(obj) = self.obj.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            handle.spawn(async move {
                let _ = obj.batch_execute("ROLLBACK").await;
            });
        }
    }
}

fn row_to_live_doc(r: &Row) -> LiveDoc {
    LiveDoc {
        path: r.get(0),
        session_id: r.get(1),
        actor_id: r.get(2),
        content_hash: r.get(3),
        since: r.get(4),
    }
}

fn row_to_suggestion(r: &Row) -> Suggestion {
    let status: String = r.get(8);
    let kind: String = r.get(12);
    Suggestion {
        id: r.get(0),
        actor_id: r.get(1),
        session_id: r.get(2),
        branch: r.get(3),
        path: r.get(4),
        base_hash: r.get(5),
        proposed_hash: r.get(6),
        summary: r.get(7),
        kind: SuggestionKind::parse(&kind).unwrap_or_default(),
        status: SuggestionStatus::parse(&status).unwrap_or(SuggestionStatus::Pending),
        created_ts: r.get(9),
        resolved_ts: r.get(10),
        resolved_by: r.get(11),
    }
}

impl PostgresMetadataStore {
    /// Whether the server reports *this* pooled connection as TLS-encrypted.
    /// Test-only introspection; not part of the `MetadataStore` contract.
    #[doc(hidden)]
    pub async fn server_ssl_self(&self) -> Result<bool> {
        let c = self.client().await?;
        let row = c
            .query_one(
                "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
                &[],
            )
            .await?;
        Ok(row.get(0))
    }
}
