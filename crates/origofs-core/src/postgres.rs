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
use crate::posixlock::{LockRequest, PosixLock};
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

/// The working tree's not-yet-indexed content addresses (V22) — the indexer's
/// queue as a **set difference rather than a log**, so there is no event to miss
/// and a skipped blob reappears on the next sweep. Distinct on `content_hash`:
/// the unit of work is a blob, so one file at four paths, or the same content on
/// four branches, is indexed once. `$1` root ino, `$2` workspace, `$3` size cap.
const PENDING_SQL: &str = "
WITH RECURSIVE sub(ino) AS (
    SELECT $1::bigint
    UNION
    SELECT d.ino FROM dentry d JOIN sub ON d.parent_ino = sub.ino
)
SELECT DISTINCT i.content_hash, i.size
  FROM inode i JOIN sub ON i.ino = sub.ino
 WHERE i.kind = 'file'
   AND i.content_hash IS NOT NULL
   AND i.size <= $3
   AND NOT EXISTS (
        SELECT 1 FROM blob_index b
         WHERE b.workspace_id = $2 AND b.content_hash = i.content_hash)
";

/// [`PENDING_SQL`] with a bound, for one sweep batch. `$4` limit.
const UNINDEXED_SQL: &str = "
WITH RECURSIVE sub(ino) AS (
    SELECT $1::bigint
    UNION
    SELECT d.ino FROM dentry d JOIN sub ON d.parent_ino = sub.ino
)
SELECT DISTINCT i.content_hash, i.size
  FROM inode i JOIN sub ON i.ino = sub.ino
 WHERE i.kind = 'file'
   AND i.content_hash IS NOT NULL
   AND i.size <= $3
   AND NOT EXISTS (
        SELECT 1 FROM blob_index b
         WHERE b.workspace_id = $2 AND b.content_hash = i.content_hash)
 LIMIT $4
";

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

/// Read the PEM bundle at `path` into certificates.
///
/// A real CA bundle is rarely one bare certificate: it usually carries several,
/// with OpenSSL's human-readable preamble between them and whatever trailing text
/// the tool that concatenated it left behind. Every certificate in the file is
/// returned and the surrounding noise ignored — stopping at the first non-PEM line
/// would silently trust only part of the bundle, which surfaces much later as a
/// server that inexplicably fails to verify.
///
/// A bundle that yields *nothing* usable is an error rather than a silent
/// fall-back to the platform roots, since the operator named this file precisely
/// because those roots are not enough.
fn load_ca_bundle(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};

    let pem = std::fs::read(path).map_err(|e| {
        OrigoFSError::Metadata(format!("{PG_CA_FILE_ENV}: cannot read {path}: {e}"))
    })?;
    // Malformed entries are skipped rather than fatal, matching the tolerance
    // applied to the platform root store.
    let certs: Vec<_> = CertificateDer::pem_slice_iter(&pem).flatten().collect();
    if certs.is_empty() {
        return Err(OrigoFSError::Metadata(format!(
            "{PG_CA_FILE_ENV} ({path}) contains no certificates"
        )));
    }
    Ok(certs)
}

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
        for cert in load_ca_bundle(&path)? {
            roots
                .add(cert)
                .map_err(|e| OrigoFSError::Metadata(format!("{PG_CA_FILE_ENV} ({path}): {e}")))?;
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
        let mut cfg: tokio_postgres::Config = dsn.parse()?;
        // An optional per-statement ceiling, so one pathological query can't pin a
        // pooled connection for its whole lifetime. Off by default and deliberately
        // so: origofs's statements are small, but a few (`truncate_tree` on a large
        // working tree, a wide `list_dir`) legitimately run long, and a timeout
        // that aborts a checkout is worse than one that never fires. Operators who
        // want the ceiling set it; those who don't keep the old behaviour.
        if let Ok(ms) = std::env::var("ORIGOFS_PG_STATEMENT_TIMEOUT_MS")
            && ms.parse::<u64>().is_ok_and(|v| v > 0)
        {
            let existing = cfg.get_options().unwrap_or_default().to_string();
            cfg.options(format!("{existing} -c statement_timeout={ms}").trim());
        }
        let mgr = Manager::new(cfg, tls_connector()?);
        // Pool sizing is a deployment property, not a library constant: 16 is far
        // too many for a dozen sidecars sharing one small database and far too few
        // for a busy single writer. It was hardcoded with no way to change it.
        fn env_usize(var: &str, default: usize) -> usize {
            std::env::var(var)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }
        fn env_secs(var: &str, default: u64) -> std::time::Duration {
            std::time::Duration::from_secs(
                std::env::var(var)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(default),
            )
        }
        let pool = Pool::builder(mgr)
            .max_size(env_usize("ORIGOFS_PG_POOL_SIZE", 16))
            .runtime(deadpool_postgres::Runtime::Tokio1)
            // Bound acquisition: without a wait timeout, exhausting the pool makes
            // `client()` hang forever instead of surfacing a retriable error.
            .wait_timeout(Some(env_secs("ORIGOFS_PG_WAIT_TIMEOUT_SECS", 10)))
            .create_timeout(Some(env_secs("ORIGOFS_PG_CONNECT_TIMEOUT_SECS", 10)))
            .recycle_timeout(Some(env_secs("ORIGOFS_PG_RECYCLE_TIMEOUT_SECS", 10)))
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
        crate::metrics::record_feed_connect();

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
        // The feed's own health was invisible: a subscriber falling behind looked
        // exactly like a quiet workspace. A full batch means there is more waiting,
        // which is the signal that matters.
        crate::metrics::record_feed_drain(
            events.len() as u64,
            if events.len() as i64 >= DRAIN_BATCH {
                DRAIN_BATCH as u64
            } else {
                0
            },
        );
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
        crate::metrics::record_feed_connect();

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

/// An owned parameter for the portable loader, so a `Cell` can be bound without
/// borrowing from the row it came from.
#[derive(Debug)]
enum PgParam {
    Null,
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
}

impl From<&crate::portable::Cell> for PgParam {
    fn from(c: &crate::portable::Cell) -> Self {
        use crate::portable::Cell;
        match c {
            Cell::Null => PgParam::Null,
            Cell::Int(i) => PgParam::Int(*i),
            Cell::Text(s) => PgParam::Text(s.clone()),
            Cell::Bytes(b) => PgParam::Bytes(b.clone()),
        }
    }
}

impl tokio_postgres::types::ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        match self {
            PgParam::Null => Ok(tokio_postgres::types::IsNull::Yes),
            PgParam::Int(i) => i.to_sql(ty, out),
            PgParam::Text(s) => s.to_sql(ty, out),
            PgParam::Bytes(b) => b.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        // Accept whatever the column is; `to_sql` dispatches on the runtime type.
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

/// Read column `i` as a backend-neutral [`Cell`](crate::portable::Cell).
fn pg_cell(r: &Row, i: usize, ty: &tokio_postgres::types::Type) -> Result<crate::portable::Cell> {
    use crate::portable::Cell;
    use tokio_postgres::types::Type;
    Ok(match *ty {
        Type::INT2 => r
            .get::<_, Option<i16>>(i)
            .map(|v| Cell::Int(v as i64))
            .unwrap_or(Cell::Null),
        Type::INT4 => r
            .get::<_, Option<i32>>(i)
            .map(|v| Cell::Int(v as i64))
            .unwrap_or(Cell::Null),
        Type::INT8 => r
            .get::<_, Option<i64>>(i)
            .map(Cell::Int)
            .unwrap_or(Cell::Null),
        Type::BYTEA => r
            .get::<_, Option<Vec<u8>>>(i)
            .map(Cell::Bytes)
            .unwrap_or(Cell::Null),
        Type::BOOL => r
            .get::<_, Option<bool>>(i)
            .map(|v| Cell::Int(v as i64))
            .unwrap_or(Cell::Null),
        // TEXT/VARCHAR and anything else the schema uses.
        _ => r
            .get::<_, Option<String>>(i)
            .map(Cell::Text)
            .unwrap_or(Cell::Null),
    })
}

/// Build a [`TrashEntry`](crate::trash::TrashEntry) from a row (issue #115).
fn row_to_trash(r: &Row) -> Result<crate::trash::TrashEntry> {
    let kind_s: String = r.get(2);
    Ok(crate::trash::TrashEntry {
        id: r.get(0),
        path: r.get(1),
        kind: FileKind::parse(&kind_s)
            .ok_or_else(|| OrigoFSError::Metadata(format!("unknown trash kind {kind_s:?}")))?,
        mode: r.get::<_, i64>(3) as u32,
        size: r.get::<_, i64>(4) as u64,
        content: r
            .get::<_, Option<String>>(5)
            .as_deref()
            .and_then(Hash::from_hex),
        symlink_target: r.get(6),
        owner: crate::types::Owner::new(r.get::<_, i64>(7) as u32, r.get::<_, i64>(8) as u32),
        actor_id: r.get(9),
        session_id: r.get(10),
        deleted_at: r.get(11),
    })
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
        uid: r.get::<_, i64>(8) as u32,
        gid: r.get::<_, i64>(9) as u32,
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
    // xattrs are keyed by inode, so a truncated tree takes them with it (#119).
    c.execute(
        "DELETE FROM xattr WHERE ino IN (SELECT ino FROM inode WHERE workspace_id = $1)",
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

/// Decode one `posix_lock` row, shared by the listing and the transactional apply
/// so the two cannot drift in column order.
fn row_to_lock(r: &tokio_postgres::Row) -> PosixLock {
    PosixLock {
        owner: r.get(0),
        holder: r.get(1),
        pid: r.get(2),
        start: r.get(3),
        end: r.get(4),
        exclusive: r.get::<_, i64>(5) != 0,
    }
}

/// Advisory-lock key for one inode. Mixed rather than concatenated because the
/// key is a single `bigint` and both inputs use the full range.
fn posix_lock_key(workspace_id: i64, ino: Ino) -> i64 {
    (workspace_id as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(ino as u64) as i64
}

#[async_trait]
impl MetadataStore for PostgresMetadataStore {
    /// Close the connection pool (issue #154).
    ///
    /// The pool is shared by every clone of this store — `for_workspace` hands
    /// out handles over the same one — so this closes it for all of them. That is
    /// the point: an embedder calling it at shutdown wants the sockets gone, not
    /// gone for one handle while a forgotten clone keeps them.
    ///
    /// `deadpool` closes idle connections immediately and in-flight ones as they
    /// are returned, and every later `get()` fails with `Closed` — which reaches
    /// a caller as a classified `Backend`/`Unavailable` error through
    /// `From<PoolError>`, so a call after shutdown says the store is unavailable
    /// rather than hanging on an acquisition that will never be served.
    async fn close(&self) -> Result<()> {
        self.pool.close();
        Ok(())
    }

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
            // A store that was never initialized has no `schema_meta` table yet,
            // which is version 0 rather than an error.
            //
            // Matched on the SQLSTATE, not on the message text. `tokio_postgres::
            // Error`'s `Display` is a generic kind ("db error"); the `relation
            // "schema_meta" does not exist` string lives in its *source*, so the
            // obvious `e.to_string().contains("does not exist")` never matched and
            // a fresh database surfaced a hard error here instead of 0. Nothing
            // called `schema_version` before `init` until the metadata
            // forward-compatibility guard did, which is what exposed it.
            Err(e) if is_undefined_table(&e) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    async fn begin(&self) -> Result<Box<dyn MetaTxn>> {
        // Pin one pooled connection for the whole `BEGIN … COMMIT`. All the
        // transaction's statements run on this same connection; it returns to
        // the pool only on commit or rollback.
        let obj = self.client().await?;
        // The isolation level is stated rather than inherited. A bare `BEGIN`
        // takes whatever `default_transaction_isolation` happens to be, which is
        // a server/pooler setting an operator can change without ever seeing this
        // code — so the level every invariant below is argued against would be
        // silently swappable.
        //
        // READ COMMITTED is the right level *because* [`MetaTxn`] exposes no
        // plain reads. Every method on it is a blind write or a conditional one
        // (`cas_ref`, `set_content_if`, `resolve_suggestion`), so no flow depends
        // on two statements seeing the same snapshot — which is the one thing
        // READ COMMITTED does not give you. Cross-row invariants rest on those
        // conditional writes, on unique indexes, and on `bump_counter`, none of
        // which need snapshot isolation.
        //
        // Under READ COMMITTED a conditional `UPDATE … WHERE value = $expected`
        // that collides re-reads the row after the other writer commits and
        // re-evaluates the predicate, so the CAS decides on the latest committed
        // value — exactly the semantics `cas_ref` needs. REPEATABLE READ would
        // instead abort with `40001`, needing a retry to reach the same answer.
        //
        // **Keep `MetaTxn` write-only.** The moment a read lands on it, this
        // argument stops holding and the level has to be revisited.
        obj.batch_execute(BEGIN_TXN).await?;
        Ok(Box::new(PostgresTxn {
            obj: Some(obj),
            workspace_id: self.workspace_id,
        }))
    }

    async fn get_inode(&self, ino: Ino) -> Result<Option<Inode>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT ino, kind, mode, nlink, size, content_hash, mtime, ctime, uid, gid
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
                "SELECT ino, kind, mode, nlink, size, content_hash, mtime, ctime, uid, gid
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
                "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime, uid, gid)
                 VALUES ($1, $2, $3, 1, 0, NULL, $4, $4, $5, $6) RETURNING ino",
                &[
                    &self.workspace_id,
                    &init.kind.as_str(),
                    &mode,
                    &now,
                    &(init.owner.uid as i64),
                    &(init.owner.gid as i64),
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn set_content(&self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()> {
        let c = self.client().await?;
        let hex = content.map(|h| h.to_hex());
        let size = size as i64;
        let now = now_secs();
        let n = c
            .execute(
                "UPDATE inode SET content_hash = $1, size = $2, mtime = $3, ctime = $3 WHERE ino = $4",
                &[&hex, &size, &now, &ino],
            )
            .await?;
        // See the SQLite implementation: a zero-row update means the inode was
        // unlinked while the content was being written, and reporting that as
        // success loses the write silently.
        if n == 0 {
            return Err(OrigoFSError::NotFound(format!(
                "inode {ino} was removed before its content could be written"
            )));
        }
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

    async fn set_mode(&self, ino: Ino, mode: u32) -> Result<()> {
        let c = self.client().await?;
        // Mask in only the permission bits: the format bits are the inode's kind,
        // not a caller's to rewrite. `& 0o7777` keeps setuid/setgid/sticky.
        c.execute(
            "UPDATE inode SET mode = (mode & ~4095) | $1, ctime = $2 WHERE ino = $3",
            &[&((mode & 0o7777) as i64), &now_secs(), &ino],
        )
        .await?;
        Ok(())
    }

    async fn set_owner(&self, ino: Ino, uid: Option<u32>, gid: Option<u32>) -> Result<()> {
        let c = self.client().await?;
        // COALESCE so a `None` half leaves the stored value alone, which is what
        // chown(2)'s -1 sentinel means.
        c.execute(
            "UPDATE inode SET uid = COALESCE($1, uid), gid = COALESCE($2, gid), \
             ctime = $3 WHERE ino = $4",
            &[
                &uid.map(|v| v as i64),
                &gid.map(|v| v as i64),
                &now_secs(),
                &ino,
            ],
        )
        .await?;
        Ok(())
    }

    async fn delete_inode(&self, ino: Ino) -> Result<()> {
        let c = self.client().await?;
        c.execute("DELETE FROM symlink WHERE ino = $1", &[&ino])
            .await?;
        // xattrs are keyed by inode, so they die with it (issue #119).
        c.execute("DELETE FROM xattr WHERE ino = $1", &[&ino])
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

    // --- content search index (V22) -------------------------------------

    async fn index_blob(&self, hash: &Hash, bytes: u64, terms: &[String], now: i64) -> Result<()> {
        let hex = hash.to_hex();
        let mut c = self.client().await?;
        // One transaction, for the reason on the trait: a term set half-written
        // under an `indexed` marker is invisible to the sweep forever.
        let tx = c.transaction().await?;
        tx.execute(
            "DELETE FROM blob_term WHERE workspace_id = $1 AND content_hash = $2",
            &[&self.workspace_id, &hex],
        )
        .await?;
        if !terms.is_empty() {
            // One statement with an array parameter rather than a row per term:
            // a 20k-term blob is one round trip, not twenty thousand.
            tx.execute(
                "INSERT INTO blob_term(workspace_id, term, content_hash)
                 SELECT $1, t, $3 FROM UNNEST($2::text[]) AS t
                 ON CONFLICT DO NOTHING",
                &[&self.workspace_id, &terms, &hex],
            )
            .await?;
        }
        tx.execute(
            "INSERT INTO blob_index(workspace_id, content_hash, indexed_at, bytes, terms)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (workspace_id, content_hash) DO UPDATE SET
               indexed_at = EXCLUDED.indexed_at,
               bytes      = EXCLUDED.bytes,
               terms      = EXCLUDED.terms",
            &[
                &self.workspace_id,
                &hex,
                &now,
                &(bytes as i64),
                &(terms.len() as i64),
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn unindexed_blobs(&self, root: Ino, limit: i64) -> Result<Vec<(Hash, u64)>> {
        crate::metadata::reject_negative_limit(limit)?;
        let c = self.client().await?;
        let rows = c
            .query(
                UNINDEXED_SQL,
                &[
                    &root,
                    &self.workspace_id,
                    &(crate::search::MAX_INDEXED_BYTES as i64),
                    &limit,
                ],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                Hash::from_hex(r.get::<_, &str>(0)).map(|h| (h, r.get::<_, i64>(1) as u64))
            })
            .collect())
    }

    async fn index_status(&self, root: Ino) -> Result<(i64, i64)> {
        let c = self.client().await?;
        let indexed = c
            .query_one(
                "SELECT COUNT(*) FROM blob_index WHERE workspace_id = $1",
                &[&self.workspace_id],
            )
            .await?
            .get::<_, i64>(0);
        let pending = c
            .query_one(
                &format!("SELECT COUNT(*) FROM ({PENDING_SQL}) q"),
                &[
                    &root,
                    &self.workspace_id,
                    &(crate::search::MAX_INDEXED_BYTES as i64),
                ],
            )
            .await?
            .get::<_, i64>(0);
        Ok((indexed, pending))
    }

    async fn search_blobs(&self, terms: &[String], limit: i64) -> Result<Vec<(Ino, Hash)>> {
        crate::metadata::reject_negative_limit(limit)?;
        if terms.is_empty() {
            // "Nothing searchable was asked for", never "everything".
            return Ok(Vec::new());
        }
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT i.ino, bt.content_hash
                   FROM blob_term bt
                   JOIN inode i ON i.content_hash = bt.content_hash
                  WHERE bt.workspace_id = $1 AND bt.term = ANY($2::text[])
                  GROUP BY i.ino, bt.content_hash
                 HAVING COUNT(DISTINCT bt.term) = $3
                  ORDER BY i.ino
                  LIMIT $4",
                &[&self.workspace_id, &terms, &(terms.len() as i64), &limit],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| Hash::from_hex(r.get::<_, &str>(1)).map(|h| (r.get::<_, i64>(0), h)))
            .collect())
    }

    async fn forget_blob_index(&self, hash: &Hash) -> Result<bool> {
        let hex = hash.to_hex();
        let mut c = self.client().await?;
        let tx = c.transaction().await?;
        tx.execute(
            "DELETE FROM blob_term WHERE workspace_id = $1 AND content_hash = $2",
            &[&self.workspace_id, &hex],
        )
        .await?;
        let n = tx
            .execute(
                "DELETE FROM blob_index WHERE workspace_id = $1 AND content_hash = $2",
                &[&self.workspace_id, &hex],
            )
            .await?;
        tx.commit().await?;
        Ok(n > 0)
    }

    async fn workspace_usage(&self) -> Result<(u64, u64)> {
        let c = self.client().await?;
        let r = c
            .query_one(
                "SELECT COUNT(*)::BIGINT, COALESCE(SUM(size), 0)::BIGINT
                 FROM inode WHERE workspace_id = $1",
                &[&self.workspace_id],
            )
            .await?;
        Ok((
            r.get::<_, i64>(0).max(0) as u64,
            r.get::<_, i64>(1).max(0) as u64,
        ))
    }

    async fn subtree_usage(&self, ino: Ino) -> Result<(u64, u64)> {
        let c = self.client().await?;
        // `UNION` (not `UNION ALL`) dedups inode ids, so an inode reachable by
        // several names -- a hard link -- is counted once, as `du` does.
        let r = c
            .query_one(
                "WITH RECURSIVE sub(ino) AS (
                     SELECT $1::BIGINT
                     UNION
                     SELECT d.ino FROM dentry d JOIN sub ON d.parent_ino = sub.ino
                 )
                 SELECT COUNT(*)::BIGINT, COALESCE(SUM(i.size), 0)::BIGINT
                 FROM inode i JOIN sub ON i.ino = sub.ino",
                &[&ino],
            )
            .await?;
        Ok((
            r.get::<_, i64>(0).max(0) as u64,
            r.get::<_, i64>(1).max(0) as u64,
        ))
    }

    async fn export_table(&self, table: &str) -> Result<Vec<crate::portable::Row>> {
        let table = crate::sqlite::validated_dump_table(table)?;
        let c = self.client().await?;
        // Every column rendered as text by the server, then re-typed from the
        // catalog below. Postgres's binary protocol would need a `FromSql` impl per
        // column type, and the point of a *portable* dump is not to care.
        let rows = c.query(&format!("SELECT * FROM \"{table}\""), &[]).await?;
        let mut out = Vec::new();
        for r in &rows {
            let mut cells = Vec::with_capacity(r.columns().len());
            for (i, col) in r.columns().iter().enumerate() {
                cells.push((col.name().to_string(), pg_cell(r, i, col.type_())?));
            }
            out.push(crate::portable::Row(cells));
        }
        Ok(out)
    }

    async fn reset_for_load(&self) -> Result<()> {
        let mut c = self.client().await?;
        let tx = c.transaction().await?;
        for table in crate::portable::DUMP_TABLES.iter().rev() {
            tx.execute(&format!("DELETE FROM \"{table}\""), &[]).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn import_table(&self, table: &str, rows: &[crate::portable::Row]) -> Result<()> {
        let table = crate::sqlite::validated_dump_table(table)?;
        if rows.is_empty() {
            return Ok(());
        }
        let mut c = self.client().await?;
        let tx = c.transaction().await?;
        for row in rows {
            let quoted: Vec<String> = row.0.iter().map(|(cn, _)| format!("\"{cn}\"")).collect();
            let placeholders: Vec<String> = (1..=row.0.len()).map(|i| format!("${i}")).collect();
            // Cast every parameter from text and let Postgres coerce to the column
            // type. Sending a bare text parameter into a BIGINT column is a type
            // error; `$n` with an explicit cast is not.
            let sql = format!(
                "INSERT INTO \"{table}\"({}) VALUES ({})",
                quoted.join(", "),
                placeholders.join(", ")
            );
            let owned: Vec<PgParam> = row.0.iter().map(|(_, v)| PgParam::from(v)).collect();
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = owned
                .iter()
                .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            tx.execute(&sql, &params).await?;
        }
        // Identity sequences were bypassed by the explicit ids above, so advance
        // each past what was inserted -- otherwise the next natural insert collides
        // with a restored row. Same hazard the V11 bootstrap already handles for
        // `inode`.
        for (t, col) in [
            ("inode", "ino"),
            ("actor", "id"),
            ("session", "id"),
            ("suggestion", "id"),
            ("trash", "id"),
            ("workspace", "id"),
            ("edit_op", "id"),
            ("tool_calls", "id"),
        ] {
            if t == table {
                let _ = tx
                    .execute(
                        &format!(
                            "SELECT setval(pg_get_serial_sequence('{t}', '{col}'), \
                             GREATEST((SELECT COALESCE(MAX(\"{col}\"), 1) FROM \"{t}\"), 1))"
                        ),
                        &[],
                    )
                    .await;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn set_acl(
        &self,
        actor_id: i64,
        path_prefix: &str,
        perms: u32,
        granted_at: i64,
        granted_by: Option<i64>,
    ) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO acl(workspace_id, actor_id, path_prefix, perms, granted_at, granted_by)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (workspace_id, actor_id, path_prefix)
             DO UPDATE SET perms = EXCLUDED.perms,
                           granted_at = EXCLUDED.granted_at,
                           granted_by = EXCLUDED.granted_by",
            &[
                &self.workspace_id,
                &actor_id,
                &path_prefix,
                &(perms as i64),
                &granted_at,
                &granted_by,
            ],
        )
        .await?;
        Ok(())
    }

    async fn remove_acl(&self, actor_id: i64, path_prefix: &str) -> Result<bool> {
        let c = self.client().await?;
        let n = c
            .execute(
                "DELETE FROM acl WHERE workspace_id = $1 AND actor_id = $2 AND path_prefix = $3",
                &[&self.workspace_id, &actor_id, &path_prefix],
            )
            .await?;
        Ok(n > 0)
    }

    async fn list_acl(&self, actor_id: Option<i64>) -> Result<Vec<crate::acl::AclGrant>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT actor_id, path_prefix, perms, granted_at, granted_by FROM acl
                 WHERE workspace_id = $1 AND ($2::BIGINT IS NULL OR actor_id = $2)
                 ORDER BY LENGTH(path_prefix) DESC",
                &[&self.workspace_id, &actor_id],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| crate::acl::AclGrant {
                actor_id: r.get(0),
                path_prefix: r.get(1),
                perms: crate::acl::Perms::from_bits(r.get::<_, i64>(2) as u32),
                granted_at: r.get(3),
                granted_by: r.get(4),
            })
            .collect())
    }

    async fn push_trash(&self, init: crate::trash::TrashInit) -> Result<i64> {
        let c = self.client().await?;
        let row = c
            .query_one(
                "INSERT INTO trash(workspace_id, path, kind, mode, size, content_hash,
                                   symlink_target, uid, gid, actor_id, session_id, deleted_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING id",
                &[
                    &self.workspace_id,
                    &init.path,
                    &init.kind.as_str(),
                    &(init.mode as i64),
                    &(init.size as i64),
                    &init.content.map(|h| h.to_hex()),
                    &init.symlink_target,
                    &(init.owner.uid as i64),
                    &(init.owner.gid as i64),
                    &init.actor_id,
                    &init.session_id,
                    &init.deleted_at,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn get_trash(&self, id: i64) -> Result<Option<crate::trash::TrashEntry>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT id, path, kind, mode, size, content_hash, symlink_target,
                        uid, gid, actor_id, session_id, deleted_at
                 FROM trash WHERE id = $1 AND workspace_id = $2",
                &[&id, &self.workspace_id],
            )
            .await?;
        row.as_ref().map(row_to_trash).transpose()
    }

    async fn list_trash(&self) -> Result<Vec<crate::trash::TrashEntry>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT id, path, kind, mode, size, content_hash, symlink_target,
                        uid, gid, actor_id, session_id, deleted_at
                 FROM trash WHERE workspace_id = $1 ORDER BY deleted_at DESC, id DESC",
                &[&self.workspace_id],
            )
            .await?;
        rows.iter().map(row_to_trash).collect()
    }

    async fn delete_trash(&self, id: i64) -> Result<bool> {
        let c = self.client().await?;
        let n = c
            .execute(
                "DELETE FROM trash WHERE id = $1 AND workspace_id = $2",
                &[&id, &self.workspace_id],
            )
            .await?;
        Ok(n > 0)
    }

    async fn purge_trash_before(&self, cutoff: i64) -> Result<usize> {
        let c = self.client().await?;
        let n = c
            .execute(
                "DELETE FROM trash WHERE workspace_id = $1 AND deleted_at < $2",
                &[&self.workspace_id, &cutoff],
            )
            .await?;
        Ok(n as usize)
    }

    async fn trash_content_hashes(&self) -> Result<Vec<Hash>> {
        let c = self.client().await?;
        // Store-wide, not workspace-scoped: `gc` sweeps one shared content store,
        // so a workspace-scoped root would let it reclaim another workspace's
        // trashed content.
        let rows = c
            .query(
                "SELECT content_hash FROM trash WHERE content_hash IS NOT NULL",
                &[],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| Hash::from_hex(&r.get::<_, String>(0)))
            .collect())
    }

    async fn get_xattr(&self, ino: Ino, name: &str) -> Result<Option<Vec<u8>>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT value FROM xattr WHERE ino = $1 AND name = $2",
                &[&ino, &name],
            )
            .await?;
        Ok(row.map(|r| r.get::<_, Vec<u8>>(0)))
    }

    async fn set_xattr(&self, ino: Ino, name: &str, value: &[u8]) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO xattr(ino, name, value) VALUES ($1, $2, $3)
             ON CONFLICT (ino, name) DO UPDATE SET value = EXCLUDED.value",
            &[&ino, &name, &value],
        )
        .await?;
        Ok(())
    }

    async fn remove_xattr(&self, ino: Ino, name: &str) -> Result<bool> {
        let c = self.client().await?;
        let n = c
            .execute(
                "DELETE FROM xattr WHERE ino = $1 AND name = $2",
                &[&ino, &name],
            )
            .await?;
        Ok(n > 0)
    }

    async fn list_xattrs(&self, ino: Ino) -> Result<Vec<String>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT name FROM xattr WHERE ino = $1 ORDER BY name",
                &[&ino],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
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

    async fn posix_locks(&self, ino: Ino, now: i64) -> Result<Vec<PosixLock>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT owner, holder, pid, start_off, end_off, exclusive FROM posix_lock
                 WHERE workspace_id = $1 AND ino = $2 AND expires_at > $3 ORDER BY start_off",
                &[&self.workspace_id, &ino, &now],
            )
            .await?;
        Ok(rows.iter().map(row_to_lock).collect())
    }

    async fn apply_posix_lock(
        &self,
        ino: Ino,
        req: &LockRequest,
        expires_at: i64,
        now: i64,
    ) -> Result<Option<PosixLock>> {
        let mut c = self.client().await?;
        let tx = c.transaction().await?;
        // Serialize every request against this one inode for the life of the
        // transaction. `SELECT … FOR UPDATE` is not enough: Postgres takes no gap
        // locks, so two transactions both find *no* conflicting row and both
        // insert. The advisory lock covers the absent rows too. A hash collision
        // between inodes costs a little serialization and no correctness.
        tx.execute(
            "SELECT pg_advisory_xact_lock($1)",
            &[&posix_lock_key(self.workspace_id, ino)],
        )
        .await?;
        // An expired lease is not a blocker; clearing it here means progress needs
        // no background reaper.
        tx.execute(
            "DELETE FROM posix_lock WHERE workspace_id = $1 AND ino = $2 AND expires_at <= $3",
            &[&self.workspace_id, &ino, &now],
        )
        .await?;
        let rows = tx
            .query(
                "SELECT owner, holder, pid, start_off, end_off, exclusive FROM posix_lock
                 WHERE workspace_id = $1 AND ino = $2 ORDER BY start_off",
                &[&self.workspace_id, &ino],
            )
            .await?;
        let existing: Vec<PosixLock> = rows.iter().map(row_to_lock).collect();
        let res = crate::posixlock::resolve(&existing, req);
        for (owner, start) in &res.delete {
            tx.execute(
                "DELETE FROM posix_lock
                 WHERE workspace_id = $1 AND ino = $2 AND owner = $3 AND start_off = $4",
                &[&self.workspace_id, &ino, owner, start],
            )
            .await?;
        }
        for l in &res.insert {
            tx.execute(
                "INSERT INTO posix_lock(workspace_id, ino, owner, holder, pid, start_off,
                                        end_off, exclusive, acquired_at, expires_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                &[
                    &self.workspace_id,
                    &ino,
                    &l.owner,
                    &l.holder,
                    &l.pid,
                    &l.start,
                    &l.end,
                    &i64::from(l.exclusive),
                    &now,
                    &expires_at,
                ],
            )
            .await?;
        }
        // Committed even when refused: the request wrote nothing, but the expired
        // rows it cleared should stay cleared.
        tx.commit().await?;
        Ok(res.conflict)
    }

    async fn release_posix_locks_for_holder(&self, holder: &str) -> Result<u64> {
        let c = self.client().await?;
        let n = c
            .execute(
                "DELETE FROM posix_lock WHERE workspace_id = $1 AND holder = $2",
                &[&self.workspace_id, &holder],
            )
            .await?;
        Ok(n)
    }

    async fn renew_posix_lease(&self, holder: &str, expires_at: i64) -> Result<u64> {
        let c = self.client().await?;
        let n = c
            .execute(
                "UPDATE posix_lock SET expires_at = $3 WHERE workspace_id = $1 AND holder = $2",
                &[&self.workspace_id, &holder, &expires_at],
            )
            .await?;
        Ok(n)
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
        // Postgres would reject this itself, as a backend error; raising it here
        // makes it the same typed `InvalidArgument` SQLite returns. See the trait.
        crate::metadata::reject_negative_limit(limit)?;
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
        checkpointed_at: Option<i64>,
    ) -> Result<()> {
        let c = self.client().await?;
        // `since` is deliberately not in the DO UPDATE list: re-marking an
        // already-live path (a second joiner, a checkpoint) keeps when it first
        // went live.
        //
        // `checkpointed_at` uses COALESCE so a re-mark that is *not* a checkpoint
        // (EXCLUDED value NULL) keeps the previous stamp rather than erasing it —
        // the row must never claim a checkpoint that didn't happen, nor forget one
        // that did.
        c.execute(
            "INSERT INTO live_doc(workspace_id, path, session_id, actor_id, content_hash, since, checkpointed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (workspace_id, path) DO UPDATE SET
                 session_id = EXCLUDED.session_id,
                 actor_id = EXCLUDED.actor_id,
                 content_hash = EXCLUDED.content_hash,
                 checkpointed_at = COALESCE(EXCLUDED.checkpointed_at, live_doc.checkpointed_at)",
            &[
                &self.workspace_id,
                &path,
                &session_id,
                &actor_id,
                &content_hash,
                &at,
                &checkpointed_at,
            ],
        )
        .await?;
        Ok(())
    }

    async fn get_live_doc(&self, path: &str) -> Result<Option<LiveDoc>> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT path, session_id, actor_id, content_hash, since, checkpointed_at
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
                "SELECT path, session_id, actor_id, content_hash, since, checkpointed_at
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
                "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime, uid, gid)
                 VALUES ($1, $2, $3, 1, 0, NULL, $4, $4, $5, $6) RETURNING ino",
                &[
                    &ws,
                    &init.kind.as_str(),
                    &mode,
                    &now,
                    &(init.owner.uid as i64),
                    &(init.owner.gid as i64),
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn set_content(&mut self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()> {
        let hex = content.map(|h| h.to_hex());
        let size = size as i64;
        let now = now_secs();
        let n = self
            .conn()
            .execute(
                "UPDATE inode SET content_hash = $1, size = $2, mtime = $3, ctime = $3 WHERE ino = $4",
                &[&hex, &size, &now, &ino],
            )
            .await?;
        // See the SQLite implementation.
        if n == 0 {
            return Err(OrigoFSError::NotFound(format!(
                "inode {ino} was removed before its content could be written"
            )));
        }
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

    async fn adjust_nlink(&mut self, ino: Ino, delta: i64) -> Result<i64> {
        let row = self
            .conn()
            .query_one(
                "UPDATE inode SET nlink = nlink + $1 WHERE ino = $2 RETURNING nlink",
                &[&delta, &ino],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn delete_inode(&mut self, ino: Ino) -> Result<()> {
        let c = self.conn();
        c.execute("DELETE FROM symlink WHERE ino = $1", &[&ino])
            .await?;
        // xattrs are keyed by inode, so they die with it (issue #119).
        c.execute("DELETE FROM xattr WHERE ino = $1", &[&ino])
            .await?;
        c.execute("DELETE FROM inode WHERE ino = $1", &[&ino])
            .await?;
        Ok(())
    }

    async fn delete_inode_if_childless(&mut self, ino: Ino) -> Result<bool> {
        let c = self.conn();
        // Claim the row *before* asking whether it has children, with the same
        // statement `add_dentry` uses. This ordering is load-bearing and was
        // arrived at empirically (`postgres_rmdir_racing_mkdir_never_orphans_a_dentry`):
        // relying on the conditional delete alone is not enough, because when it
        // blocks on a concurrent locker Postgres re-evaluates its qualification
        // against the *updated row* but keeps the original snapshot for the
        // `dentry` subquery — so the child that just committed stays invisible
        // and the delete goes through anyway.
        //
        // Blocking on the claim instead moves the wait one statement earlier. In
        // READ COMMITTED each statement takes a fresh snapshot, so by the time
        // the delete below runs the inserter has committed and its child *is*
        // visible. The other order is safe too: a claim that matches no row means
        // the inode is already gone.
        if c.execute("UPDATE inode SET nlink = nlink WHERE ino = $1", &[&ino])
            .await?
            == 0
        {
            return Ok(false);
        }
        let n = c
            .execute(
                "DELETE FROM inode WHERE ino = $1
                   AND NOT EXISTS (SELECT 1 FROM dentry WHERE parent_ino = $1)",
                &[&ino],
            )
            .await?;
        if n == 1 {
            c.execute("DELETE FROM symlink WHERE ino = $1", &[&ino])
                .await?;
            // xattrs are keyed by inode, so they die with it (issue #119).
            c.execute("DELETE FROM xattr WHERE ino = $1", &[&ino])
                .await?;
        }
        Ok(n == 1)
    }

    async fn add_dentry(&mut self, parent: Ino, name: &str, ino: Ino) -> Result<()> {
        // Claim the parent first — a self-update rather than a read, so it both
        // proves the directory still exists inside this transaction and takes its
        // row. A concurrent `rmdir` then blocks here and, on re-evaluating its
        // conditional delete against the updated row, sees this child. See the
        // trait method's docs.
        if self
            .conn()
            .execute("UPDATE inode SET nlink = nlink WHERE ino = $1", &[&parent])
            .await?
            == 0
        {
            return Err(OrigoFSError::NotFound(format!(
                "parent inode {parent} no longer exists"
            )));
        }
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
        // The same two guarantees the store-level `append_event` documents, and for
        // the same reasons — this variant had neither.
        //
        // The advisory lock is held to *this* transaction's commit, so a lower seq
        // is always visible before a higher one is assigned; without it two
        // concurrent transactions could commit seq 11 before seq 10, and any
        // `seq > cursor` reader (`events_since`, `EventSubscription::drain`) would
        // advance past 10 and drop it permanently once it landed. SQLite's txn
        // variant needs no equivalent only because `begin` holds the global
        // connection mutex and `BEGIN IMMEDIATE`'s write lock, which serializes
        // assignment and commit together.
        self.conn()
            .execute("SELECT pg_advisory_xact_lock($1)", &[&FEED_LOCK_KEY])
            .await?;
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
        let seq: i64 = row.get(0);
        // NOTIFY inside the transaction: Postgres queues it and delivers on commit,
        // discarding it on rollback. Omitting it left a `subscribe()` push
        // subscriber asleep until some unrelated store-level append happened to
        // notify — so a transactionally appended event was in the table but never
        // pushed.
        let payload = seq.to_string();
        self.conn()
            .execute("SELECT pg_notify($1, $2)", &[&EVENT_CHANNEL, &payload])
            .await?;
        Ok(seq)
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

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        let obj = self.obj.take().expect("transaction already finished");
        // Awaited, unlike the `Drop` path: the connection is clean and back in the
        // pool before this returns, so a caller's next query cannot be handed a
        // connection with this transaction still open.
        obj.batch_execute("ROLLBACK").await?;
        Ok(())
    }
}

impl Drop for PostgresTxn {
    fn drop(&mut self) {
        // If the transaction wasn't committed, roll it back before the pinned
        // connection returns to the pool — otherwise a reused connection would
        // inherit the open transaction. `Drop` can't `await`, so spawn the
        // ROLLBACK and move the connection into that task; it is recycled only
        // once the rollback completes.
        //
        // This is the **backstop**, not the intended path: it cannot tell the
        // caller when the rollback finished, so a caller whose next step depends
        // on that ordering must call [`MetaTxn::rollback`] instead.
        //
        // Whatever happens, the connection must not go back to the pool with an
        // open transaction. Two ways that could happen, both handled by detaching
        // it (`Object::take` removes it from the pool permanently, so the manager
        // opens a fresh one instead of handing this one out dirty):
        //
        //   * the ROLLBACK itself fails — the connection's state is then unknown;
        //   * there is no runtime to spawn onto, so no rollback can be issued at
        //     all. Dropping the object here would recycle it as-is.
        //
        // The remaining hole is a runtime that shuts down before the spawned task
        // runs, which drops the task and with it the connection — dropping a
        // `deadpool` object during shutdown returns it to a pool nothing will
        // borrow from again, so it is harmless.
        if let Some(obj) = self.obj.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        if obj.batch_execute("ROLLBACK").await.is_err() {
                            let _ = deadpool_postgres::Object::take(obj);
                        }
                    });
                }
                Err(_) => {
                    let _ = deadpool_postgres::Object::take(obj);
                }
            }
        }
    }
}

/// Whether `e` is Postgres's `undefined_table` (SQLSTATE `42P01`).
///
/// Checked by code rather than by message: SQLSTATE is stable across Postgres
/// versions and locales, while the message is neither — a server running under a
/// non-English `lc_messages` does not say "does not exist" at all.
fn is_undefined_table(e: &tokio_postgres::Error) -> bool {
    e.as_db_error()
        .is_some_and(|db| *db.code() == tokio_postgres::error::SqlState::UNDEFINED_TABLE)
}

fn row_to_live_doc(r: &Row) -> LiveDoc {
    LiveDoc {
        path: r.get(0),
        session_id: r.get(1),
        actor_id: r.get(2),
        content_hash: r.get(3),
        since: r.get(4),
        checkpointed_at: r.get(5),
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

/// The statement every [`MetaTxn`] opens with. Named so the isolation level is a
/// single, testable fact rather than a string buried in one method — see the
/// comment at its use site in `begin` for why READ COMMITTED is the level the
/// design argues for, and `tests/postgres.rs` for the assertion that the server
/// agrees.
#[doc(hidden)]
pub const BEGIN_TXN: &str = "BEGIN ISOLATION LEVEL READ COMMITTED";

impl PostgresMetadataStore {
    /// The isolation level the server reports for a transaction opened the way
    /// [`MetadataStore::begin`] opens one. Test-only introspection.
    #[doc(hidden)]
    pub async fn begin_isolation_self(&self) -> Result<String> {
        let c = self.client().await?;
        c.batch_execute(BEGIN_TXN).await?;
        let row = c.query_one("SHOW transaction_isolation", &[]).await?;
        let level: String = row.get(0);
        c.batch_execute("ROLLBACK").await?;
        Ok(level)
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Two self-signed CAs, long-lived so these tests do not rot. Only their
    /// *parsing* is under test — nothing verifies a chain against them — so expiry
    /// is deliberately not part of what is asserted.
    const CA_ONE: &str = "-----BEGIN CERTIFICATE-----
MIIBkzCCATmgAwIBAgIUPMPvouh0uiI6Tdl1TgfxVD0/lQkwCgYIKoZIzj0EAwIw
HjEcMBoGA1UEAwwTb3JpZ29mcy10ZXN0LWNhLW9uZTAgFw0yNjA3MzAxMTM4MzJa
GA8yMTI2MDcwNjExMzgzMlowHjEcMBoGA1UEAwwTb3JpZ29mcy10ZXN0LWNhLW9u
ZTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABCb/5GuGDR/RqARGulE6Xkq472Qo
ZtON09yucyiE7FNo4UPj1QAd9Sox/LOxNCCjrEeRWOvwBlL5A/McvDiG8WujUzBR
MB0GA1UdDgQWBBQX6q4LfhrQjc9BZ10fapw3/+Nb6jAfBgNVHSMEGDAWgBQX6q4L
fhrQjc9BZ10fapw3/+Nb6jAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0gA
MEUCIQDHHuR5h4aRnkw9Jbis3tuIK50Sl1Ddrc1oajCWPV5DXgIgSKKMVQnKufxA
brgubkkchZOzzrml5MTLkzc216Exz+Y=
-----END CERTIFICATE-----
";

    const CA_TWO: &str = "-----BEGIN CERTIFICATE-----
MIIBlDCCATmgAwIBAgIUEy4wZSsI4c8c1kkdnsPm4U9QxLcwCgYIKoZIzj0EAwIw
HjEcMBoGA1UEAwwTb3JpZ29mcy10ZXN0LWNhLXR3bzAgFw0yNjA3MzAxMTM4MzJa
GA8yMTI2MDcwNjExMzgzMlowHjEcMBoGA1UEAwwTb3JpZ29mcy10ZXN0LWNhLXR3
bzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABOD6ti5O1W3/fvhoRWGz+ZtKCTn0
c71ORnGd2om3M4zcVlJeOgwunK5tb2d/DA/zZ5XIVHmMsWRTqtGR2ofOiJKjUzBR
MB0GA1UdDgQWBBRozCeGNdnnaZJ7YK0HfH3kmEhYqDAfBgNVHSMEGDAWgBRozCeG
NdnnaZJ7YK0HfH3kmEhYqDAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0kA
MEYCIQC7A4rQxYlr7zsrAXK80KMBjisVJh+pA0qevSbuvvKERQIhAMLLUq5p6xCY
yfosoM84rjsOMr7BDRLh0CR+NVw5yuPV
-----END CERTIFICATE-----
";

    /// Every certificate in a bundle must be read, not just the first — and the
    /// human-readable noise real bundles carry (OpenSSL's preamble, blank lines,
    /// trailing text) must not cut the parse short. A bundle silently truncated to
    /// its first certificate surfaces much later, as a server that inexplicably
    /// fails to verify.
    #[test]
    fn a_multi_certificate_bundle_is_read_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bundle.pem");
        std::fs::write(
            &path,
            format!(
                "# origofs test bundle\n\n{CA_ONE}\n\
                 # a comment between the two certificates\n\n{CA_TWO}\n\
                 trailing junk that is not a certificate\n"
            ),
        )
        .unwrap();

        let certs = load_ca_bundle(path.to_str().unwrap()).expect("bundle must parse");
        assert_eq!(certs.len(), 2, "both certificates must be read");

        // And both must be acceptable as roots, which is what they are read for.
        let mut roots = rustls::RootCertStore::empty();
        for cert in certs {
            roots.add(cert).expect("a self-signed CA is a valid root");
        }
        assert_eq!(roots.len(), 2);
    }

    /// A bundle with nothing usable in it is an error, not a silent fall-back to
    /// the platform roots — the operator named the file because those are not
    /// enough.
    #[test]
    fn a_bundle_with_no_certificates_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("junk.pem");
        std::fs::write(&path, b"not a certificate\n").unwrap();

        let err = load_ca_bundle(path.to_str().unwrap()).expect_err("junk must be refused");
        assert!(
            err.to_string().contains("no certificates"),
            "expected a clear parse error, got: {err}"
        );
    }

    #[test]
    fn an_unreadable_bundle_is_a_clean_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.pem");

        let err =
            load_ca_bundle(path.to_str().unwrap()).expect_err("a missing file must be refused");
        assert!(
            err.to_string().contains("cannot read"),
            "expected a clear read error, got: {err}"
        );
    }
}
