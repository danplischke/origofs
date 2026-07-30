//! A tree swap must not hold the metadata connection while it waits on content.
//!
//! `checkout`, `merge` and `rebuild` all replace the working tree inside a single
//! `MetaTxn` (so the tree and the refs describing it commit together). The SQLite
//! backend's transaction holds an owned, *blocking* `parking_lot` guard on the one
//! connection from `begin` to `commit`, so if the materialization awaits the
//! content store per node while inside that transaction, every other metadata
//! caller blocks a runtime worker on the guard — for the length of an S3 round
//! trip, per node.
//!
//! On a `current_thread` runtime that is not slowness but a hard deadlock: the
//! thread that blocked on the guard is the only one that could have polled the
//! transaction to completion. It takes the timer driver down with it, which is why
//! the assertion below cannot live inside the runtime — a `tokio::time::timeout`
//! wrapped around the operation never fires either.
//!
//! The fix is `Fs::plan_materialize`: read and decode everything the swap needs
//! *before* `begin`, then replay it as pure metadata.

use bytes::Bytes;
use origofs_core::{ContentStore, Fs, Hash, MemStore, MetadataStore, SqliteMetadataStore};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::time::Duration;

/// Wraps a content store so a reader can be told the exact moment a `get` is in
/// flight, and holds that `get` open long enough for anything else on the runtime
/// to take a turn.
struct Signalling {
    inner: Arc<MemStore>,
    /// Announces "a content read is in flight right now". Taken by the first
    /// qualifying `get`.
    inside: std::sync::Mutex<Option<Sender<()>>>,
    /// The commit object's address. `checkout` reads it *before* opening the
    /// transaction under any implementation, so firing on it would prove nothing;
    /// every other read is one that used to happen inside the transaction.
    skip: std::sync::Mutex<Option<Hash>>,
    armed: AtomicBool,
}

#[async_trait::async_trait]
impl ContentStore for Signalling {
    async fn put(&self, b: &[u8]) -> origofs_core::Result<Hash> {
        self.inner.put(b).await
    }
    async fn put_keyed(&self, k: &Hash, b: &[u8]) -> origofs_core::Result<()> {
        self.inner.put_keyed(k, b).await
    }
    async fn get(&self, h: &Hash) -> origofs_core::Result<Bytes> {
        let interesting =
            self.armed.load(Ordering::SeqCst) && *self.skip.lock().unwrap() != Some(*h);
        let armed = if interesting {
            self.inside.lock().unwrap().take()
        } else {
            None
        };
        if let Some(tx) = armed {
            let _ = tx.send(());
            // Yield generously: on a single-threaded runtime this is the reader's
            // only chance to run, and it must run *while* this read is in flight.
            for _ in 0..200 {
                tokio::task::yield_now().await;
            }
        }
        self.inner.get(h).await
    }
    async fn get_range(&self, h: &Hash, o: u64, l: u64) -> origofs_core::Result<Bytes> {
        self.inner.get_range(h, o, l).await
    }
    async fn has(&self, h: &Hash) -> origofs_core::Result<bool> {
        self.inner.has(h).await
    }
    async fn list(&self) -> origofs_core::Result<Vec<Hash>> {
        self.inner.list().await
    }
    async fn list_with_age(&self) -> origofs_core::Result<Vec<(Hash, Option<u64>)>> {
        self.inner.list_with_age().await
    }
    async fn delete(&self, h: &Hash) -> origofs_core::Result<u64> {
        self.inner.delete(h).await
    }
}

/// Deliberately a plain `#[test]`: the failure this guards against wedges the
/// entire runtime thread, so the deadline has to be enforced from outside it.
#[test]
fn a_tree_swap_does_not_park_the_metadata_connection_on_content_io() {
    let (done_tx, done_rx) = channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("swap-runtime".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let _ = done_tx.send(rt.block_on(scenario()));
        })
        .unwrap();

    match done_rx.recv_timeout(Duration::from_secs(60)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("{e}"),
        Err(_) => panic!(
            "deadlock: a metadata read blocked behind a checkout that was parked on \
             content I/O while holding the connection. The tree swap must resolve its \
             content *before* it opens the transaction (Fs::plan_materialize)."
        ),
    }
}

async fn scenario() -> Result<(), String> {
    let (tx, rx) = channel();
    let meta = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let content = Arc::new(Signalling {
        inner: Arc::new(MemStore::new()),
        inside: std::sync::Mutex::new(None),
        skip: std::sync::Mutex::new(None),
        armed: AtomicBool::new(false),
    });
    let fs = Arc::new(Fs::new(meta.clone(), content.clone()));
    fs.init().await.unwrap();
    for i in 0..10 {
        fs.write(&format!("/f{i}.txt"), b"body").await.unwrap();
    }
    let base = fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    // Arm only now, so the setup writes don't trip it, and exempt the commit
    // object so the signal lands on a read that used to sit inside the txn.
    *content.skip.lock().unwrap() = Some(base);
    *content.inside.lock().unwrap() = Some(tx);
    content.armed.store(true, Ordering::SeqCst);

    let swap = {
        let fs = fs.clone();
        tokio::spawn(async move { fs.checkout("dev").await })
    };
    let reader = {
        let meta = meta.clone();
        tokio::spawn(async move {
            // Park off-runtime until a content read is genuinely in flight.
            let _ = tokio::task::spawn_blocking(move || rx.recv()).await;
            meta.get_inode(1).await
        })
    };

    reader
        .await
        .map_err(|e| format!("reader task panicked: {e}"))?
        .map_err(|e| format!("reader failed: {e}"))?;
    swap.await
        .map_err(|e| format!("checkout task panicked: {e}"))?
        .map_err(|e| format!("checkout failed: {e}"))?;
    Ok(())
}
