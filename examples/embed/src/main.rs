//! Embedding origofs in another Rust project.
//!
//! This is a *consumer* of `origofs-sdk`, not part of it — it depends on the SDK
//! and nothing else from this repository, exactly as a third-party crate would.
//! Being a workspace member means `cargo build --workspace` and
//! `cargo clippy --workspace --all-targets` compile it, so the embedding path
//! cannot silently break: every construct below failed to compile before the
//! SDK re-exported its own vocabulary.
//!
//! It demonstrates the four things an embedder actually needs:
//!
//! 1. **Naming the types** — holding a `Workspace` in your own struct and
//!    writing a signature over what `Workspace::fs()` returns.
//! 2. **Using the error type** — `origofs_sdk::Result` in your own helpers, and
//!    matching on `ErrorClass` to decide whether a failure is worth retrying.
//! 3. **Attributed writes** — the point of origofs: register actors, write as
//!    them, read back per-line blame.
//! 4. **Plugging in your own backend** — implementing `ContentStore` and handing
//!    it to `Workspace::open`, which is what makes the storage layer pluggable
//!    from outside this repo rather than only within it.
//!
//! Run with `cargo run -p origofs-embed-example`.

use origofs_sdk::{
    async_trait, ContentStore, ErrorClass, Fs, Hash, MetadataStore, OrigoFSError, Result,
    SqliteMetadataStore, VerifyingStore, Workspace, WriteCtx,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ── 1. Naming the types ─────────────────────────────────────────────────────

/// Your application struct, owning a workspace. `Workspace` is `Clone` and
/// `Send + Sync`, so this can live in an `Arc` behind a request handler.
struct DocumentService {
    ws: Workspace,
}

/// A signature over the engine handle `Workspace::fs()` hands back. Most
/// embedders never need this — the `Workspace` façade covers the common API —
/// but a public method returning a type you cannot name is a dead end, so this
/// pins that it stays nameable.
fn engine(svc: &DocumentService) -> &Fs<Arc<dyn MetadataStore>, Arc<dyn ContentStore>> {
    svc.ws.fs()
}

impl DocumentService {
    /// A fallible helper in *your* crate, using the SDK's `Result` alias.
    async fn put(&self, path: &str, body: &str, author: WriteCtx) -> Result<()> {
        self.ws.write_as(author, path, body.as_bytes()).await
    }
}

// ── 2. Reacting to errors by class ──────────────────────────────────────────

/// Whether a failed call is worth retrying. Backend errors carry a machine
/// `code()` and an [`ErrorClass`] precisely so a caller can branch on them
/// instead of string-matching a `Display` message.
fn should_retry(err: &OrigoFSError) -> bool {
    // A `Corrupt` read means the bytes failed their hash check on the way out;
    // re-reading returns the same bad bytes, so escalate rather than retry.
    if matches!(err, OrigoFSError::Corrupt(_)) {
        return false;
    }
    matches!(
        err.class(),
        Some(ErrorClass::Retryable | ErrorClass::Unavailable)
    )
}

// ── 4. Your own content backend ─────────────────────────────────────────────

/// A minimal in-process [`ContentStore`]. A real one would be your object store,
/// your cache tier, or a store that mirrors writes somewhere else; the shape is
/// the same. Addresses are content hashes, writes are idempotent, and `flush`
/// and `repack` keep their default no-op bodies because this store writes
/// through immediately.
#[derive(Default)]
struct MyContentStore {
    blobs: Mutex<HashMap<Hash, Vec<u8>>>,
}

#[async_trait]
impl ContentStore for MyContentStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let hash = Hash::of(bytes);
        self.put_keyed(&hash, bytes).await?;
        Ok(hash)
    }

    /// Note this takes the address explicitly: transforming layers such as
    /// `EncryptedStore` keep the *plaintext* hash as the address while storing
    /// ciphertext, so the caller owns the addressing invariant.
    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.blobs.lock().unwrap().insert(*key, bytes.to_vec());
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<origofs_sdk::Bytes> {
        self.blobs
            .lock()
            .unwrap()
            .get(hash)
            .map(|b| origofs_sdk::Bytes::from(b.clone()))
            .ok_or_else(|| OrigoFSError::NotFound(hash.to_hex()))
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<origofs_sdk::Bytes> {
        let all = self.get(hash).await?;
        let start = (off as usize).min(all.len());
        let end = start.saturating_add(len as usize).min(all.len());
        Ok(all.slice(start..end))
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        Ok(self.blobs.lock().unwrap().contains_key(hash))
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        Ok(self.blobs.lock().unwrap().keys().copied().collect())
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        // Idempotent: deleting an absent hash succeeds and frees 0.
        Ok(self
            .blobs
            .lock()
            .unwrap()
            .remove(hash)
            .map_or(0, |b| b.len() as u64))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;

    // Wire the workspace by hand from your own backend. `VerifyingStore` goes on
    // the OUTSIDE so integrity is re-checked at the chunk-addressed boundary
    // reads come through — keep that ordering when you stack decorators.
    let meta: Arc<dyn MetadataStore> =
        Arc::new(SqliteMetadataStore::open(dir.path().join("meta.db"))?);
    let content: Arc<dyn ContentStore> =
        Arc::new(VerifyingStore::new(Arc::new(MyContentStore::default())));
    let ws = Workspace::open(meta, content).await?;

    let svc = DocumentService { ws };
    println!("schema version: {}", svc.ws.schema_version().await?);

    // ── 3. Attributed writes ────────────────────────────────────────────────
    // Identity is resolved by *you*, server-side — never taken from a client
    // payload. Here a human and an agent each write, and blame keeps them apart.
    let human = svc
        .ws
        .create_human("dana", Some("dana@example.com"))
        .await?;
    let agent = svc
        .ws
        .create_agent("reviewer-bot", "claude-opus-5", Some(human))
        .await?;

    svc.put("/notes.md", "written by a human\n", WriteCtx::actor(human))
        .await?;
    svc.put(
        "/notes.md",
        "written by a human\nappended by an agent\n",
        WriteCtx::actor(agent),
    )
    .await?;

    for range in svc.ws.blame("/notes.md").await? {
        println!(
            "lines {}-{}: {} ({:?})",
            range.line_start, range.line_end, range.actor.display_name, range.actor.kind
        );
    }

    // The engine handle is reachable for anything the façade does not surface.
    println!(
        "engine sees root inode: {:?}",
        engine(&svc).stat("/").await?.ino
    );

    // Errors classify themselves, so callers can branch instead of string-match.
    let missing = svc.ws.read("/does-not-exist.md").await.unwrap_err();
    println!(
        "code={} retryable={} should_retry={}",
        missing.code(),
        missing.retryable(),
        should_retry(&missing)
    );

    // A batching backend (`PackStore`) buffers chunks in memory, so an embedder
    // that owns the lifecycle should flush before dropping the workspace.
    svc.ws.flush().await?;
    Ok(())
}
