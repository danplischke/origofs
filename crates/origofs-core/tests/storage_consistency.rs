//! Regressions from the storage-logic review: places where the engine disagreed
//! with its own contracts, with itself across backends, or silently corrupted
//! something it had just reported as durable.
//!
//! Each test here fails on the code as it stood before the corresponding fix.
//! Grouped by the invariant at stake rather than by module, because most of these
//! bugs live in the seam between two modules.

use origofs_core::{
    Commit, ContentStore, Fs, Hash, MemStore, MergeOutcome, MetadataStore, OrigoFSError,
    SqliteMetadataStore, Tree, TreeEntry, TreeKind, WriteCtx,
};
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();
    fs
}

// --- merge state does not outlive the branch it belonged to ------------------

/// Switching branches abandons a conflicted merge, so `MERGE_HEAD` and the
/// conflict rows must go with it.
///
/// There is no merge-abort operation, which makes checkout the only way out of a
/// conflicted merge — and it used to leave the merge state behind. `commit` was
/// the sole place that cleared it, so the *next* commit on the branch you switched
/// to picked up the stale `MERGE_HEAD` as a second parent and recorded a merge
/// that never happened.
#[tokio::test]
async fn checkout_abandons_an_unresolved_merge() {
    let fs = fixture().await;
    fs.write("/f.txt", b"base\n").await.unwrap();
    let base = fs.commit("dan", "base").await.unwrap();

    fs.create_branch("other").await.unwrap();
    fs.write("/f.txt", b"ours\n").await.unwrap();
    fs.commit("dan", "ours").await.unwrap();

    fs.checkout("other").await.unwrap();
    fs.write("/f.txt", b"theirs\n").await.unwrap();
    let theirs = fs.commit("dan", "theirs").await.unwrap();

    fs.checkout("main").await.unwrap();
    let outcome = fs.merge(theirs, "dan", "merge").await.unwrap();
    assert!(
        matches!(outcome, MergeOutcome::Conflicts(_)),
        "the fixture must actually conflict, or this proves nothing"
    );
    assert!(
        fs.backends()
            .meta
            .get_ref("MERGE_HEAD")
            .await
            .unwrap()
            .is_some()
    );
    assert!(!fs.conflicts().await.unwrap().is_empty());

    // Walk away from the merge.
    fs.checkout("other").await.unwrap();

    assert!(
        fs.backends()
            .meta
            .get_ref("MERGE_HEAD")
            .await
            .unwrap()
            .is_none(),
        "MERGE_HEAD must not survive the checkout that abandoned its merge"
    );
    assert!(
        fs.conflicts().await.unwrap().is_empty(),
        "conflicts from the abandoned merge must not be reported against the new branch"
    );

    // The decisive consequence: the next commit here is an ordinary one.
    fs.write("/g.txt", b"unrelated\n").await.unwrap();
    let next = fs.commit("dan", "on other").await.unwrap();
    let commit = fs.commit_object(&next).await.unwrap();
    assert_eq!(
        commit.parents.len(),
        1,
        "a commit on the branch we switched to must not adopt the abandoned merge's second parent"
    );
    assert_ne!(commit.parents[0], base, "sanity: parented on `other`'s tip");
}

// --- attribution covers deletions too ----------------------------------------

/// Accepting a proposed *deletion* records an op-log entry attributed to the
/// actor who proposed it, exactly as accepting proposed bytes does.
///
/// The byte arm went through `write_as_expecting(author, …)`; the deletion arm
/// called the raw, unattributed `remove`. So the op-log — the ground truth blame
/// is rebuilt from — held a file removal with no author and no `pre_hash` naming
/// what was destroyed.
#[tokio::test]
async fn accepting_a_proposed_deletion_is_attributed() {
    let fs = fixture().await;
    let author = fs.create_agent("claude", "m", None).await.unwrap();
    let reviewer = fs.create_human("dan", None).await.unwrap();
    let session = fs.create_session(author, Some("test")).await.unwrap();
    let ctx = WriteCtx::session(author, session);

    fs.write("/doomed.txt", b"delete me\n").await.unwrap();
    let before = fs.stat("/doomed.txt").await.unwrap();

    let id = fs.suggest_delete(ctx, "/doomed.txt", None).await.unwrap();
    fs.accept_suggestion(id, WriteCtx::actor(reviewer))
        .await
        .unwrap();
    assert!(fs.read("/doomed.txt").await.is_err(), "the file is gone");

    let ops = fs
        .backends()
        .meta
        .list_edit_ops(author, Some(session))
        .await
        .unwrap();
    let removal = ops
        .iter()
        .find(|o| o.op == "remove" && o.path == "/doomed.txt")
        .expect("the accepted deletion must appear in the op-log, attributed to its author");
    assert_eq!(removal.actor_id, author, "attributed to the proposer");
    assert_eq!(
        removal.pre_hash,
        before.content.map(|h| h.to_hex()),
        "the op-log must name the content that was destroyed"
    );
}

// --- object encoding refuses to truncate -------------------------------------

/// A name too long for the format's `u16` length field is an error, not a
/// silently-wrapped length that writes an undecodable object.
///
/// `validate_component` caps nothing by length, and tree entries also arrive from
/// outside it entirely (a `git import` builds them from a foreign repository's
/// names), so the encoder is the boundary that has to hold.
#[test]
fn a_tree_entry_name_past_u16_is_refused_not_truncated() {
    let tree = Tree {
        entries: vec![TreeEntry {
            name: "x".repeat(u16::MAX as usize + 1),
            mode: 0o644,
            kind: TreeKind::File,
            hash: Hash::of(b"body"),
        }],
    };
    let err = tree
        .encode()
        .expect_err("an unencodable name must not encode");
    assert!(
        matches!(err, OrigoFSError::TooLarge(_)),
        "expected TooLarge, got {err}"
    );
}

/// Same for a commit author, which is a caller-supplied string with no length rule.
#[test]
fn a_commit_author_past_u16_is_refused_not_truncated() {
    let commit = Commit {
        tree: Hash::of(b"tree"),
        parents: vec![],
        author: "a".repeat(u16::MAX as usize + 1),
        message: "hello".into(),
        timestamp: 1,
    };
    let err = commit
        .encode()
        .expect_err("an unencodable author must not encode");
    assert!(
        matches!(err, OrigoFSError::TooLarge(_)),
        "expected TooLarge, got {err}"
    );
}

// --- the sweep acts on a fresh age, not the one it listed with ---------------

/// GC must not delete an object that was refreshed after the sweep listed it.
///
/// The age gate used to be check-then-act: ages were read at the start of the
/// pass, then every unmarked object was deleted unconditionally. A pass over a
/// large store runs for minutes, and a writer that dedups onto an object in that
/// window refreshes its recency and commits a reference to it — so the sweep
/// deleted content that had just been referenced, which is precisely the
/// `ContentMissing`-after-commit the grace period exists to prevent.
#[tokio::test]
async fn the_sweep_re_checks_age_before_deleting() {
    let store = Arc::new(MemStore::new());
    let orphan = store
        .put(b"unreferenced, and about to be dedup'd onto")
        .await
        .unwrap();

    // Old enough to sweep by the listing, then refreshed under the sweep's feet.
    assert_eq!(
        store.delete_if_older_than(&orphan, 600).await.unwrap(),
        None,
        "a young object must be declined, not deleted"
    );
    assert!(
        store.has(&orphan).await.unwrap(),
        "declining must leave the object in place"
    );

    // With the gate disabled the same call deletes, so the decline above is the
    // age check talking and not a broken code path.
    assert!(
        store
            .delete_if_older_than(&orphan, 0)
            .await
            .unwrap()
            .is_some(),
        "grace 0 opts out of the gate entirely"
    );
    assert!(!store.has(&orphan).await.unwrap());
}

// --- the two metadata backends answer the same way ---------------------------

/// `set_content` on an inode that was unlinked underneath is `NotFound`, not a
/// silently acknowledged write. Postgres already reported it; SQLite discarded
/// the affected-row count and returned `Ok(())`.
#[tokio::test]
async fn set_content_on_a_vanished_inode_is_not_silently_lost() {
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    meta.init().await.unwrap();
    let err = meta
        .set_content(999_999, Some(Hash::of(b"body")), 4)
        .await
        .expect_err("writing content to an inode that does not exist must fail");
    assert!(
        matches!(err, OrigoFSError::NotFound(_)),
        "expected NotFound, got {err}"
    );
}

/// `bump_counter` on a key that does not hold an integer is an error on both
/// backends. SQLite's `CAST` never fails — it yields 0 for text — so this used to
/// silently reset the counter to 1 while Postgres raised.
#[tokio::test]
async fn bump_counter_refuses_a_non_integer_value() {
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    meta.init().await.unwrap();

    meta.set_config("counter", "7").await.unwrap();
    assert_eq!(meta.bump_counter("counter").await.unwrap(), 8);

    meta.set_config("counter", "abc").await.unwrap();
    let err = meta
        .bump_counter("counter")
        .await
        .expect_err("a non-integer value cannot be counted");
    assert!(
        matches!(err, OrigoFSError::InvalidArgument(_)),
        "expected InvalidArgument, got {err}"
    );
    assert_eq!(
        meta.get_config("counter").await.unwrap().as_deref(),
        Some("abc"),
        "the refused bump must leave the value alone rather than resetting it"
    );
}

/// A negative `limit` is rejected on both backends. SQLite reads a negative
/// `LIMIT` as *unbounded*, so this used to dump the entire change feed on one
/// backend and raise a backend error on the other.
#[tokio::test]
async fn events_since_refuses_a_negative_limit() {
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    meta.init().await.unwrap();
    let err = meta
        .events_since(0, -1)
        .await
        .expect_err("a negative limit is a caller bug");
    assert!(
        matches!(err, OrigoFSError::InvalidArgument(_)),
        "expected InvalidArgument, got {err}"
    );
}

// --- content is durable before a ref names it --------------------------------

/// A content store that batches like [`PackStore`]: `put` only buffers, and
/// nothing survives a crash until `flush` seals it. Reads see buffered data (a
/// live process can read its own writes), so only [`Self::crash`] reveals what was
/// actually durable.
struct BufferingStore {
    durable: Arc<MemStore>,
    pending: parking_lot::Mutex<Vec<(Hash, Vec<u8>)>>,
    /// When set, a `put` of an object carrying this 4-byte type tag fails. Used to
    /// stop the engine at a chosen point mid-operation.
    fail_on_tag: parking_lot::Mutex<Option<[u8; 4]>>,
}

impl BufferingStore {
    fn new() -> Self {
        Self {
            durable: Arc::new(MemStore::new()),
            pending: parking_lot::Mutex::new(Vec::new()),
            fail_on_tag: parking_lot::Mutex::new(None),
        }
    }

    fn fail_on_tag(&self, tag: &[u8; 4]) {
        *self.fail_on_tag.lock() = Some(*tag);
    }

    /// Drop everything that was never flushed — what a process kill would cost.
    fn crash(&self) {
        self.pending.lock().clear();
    }
}

#[async_trait::async_trait]
impl ContentStore for BufferingStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash, OrigoFSError> {
        if let Some(tag) = *self.fail_on_tag.lock()
            && bytes.starts_with(&tag)
        {
            return Err(OrigoFSError::Content("injected failure".into()));
        }
        let hash = Hash::of(bytes);
        if !self.durable.has(&hash).await? {
            self.pending.lock().push((hash, bytes.to_vec()));
        }
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<(), OrigoFSError> {
        self.pending.lock().push((*key, bytes.to_vec()));
        Ok(())
    }

    async fn flush(&self) -> Result<(), OrigoFSError> {
        let staged = std::mem::take(&mut *self.pending.lock());
        for (hash, bytes) in staged {
            self.durable.put_keyed(&hash, &bytes).await?;
        }
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<bytes::Bytes, OrigoFSError> {
        if let Some((_, b)) = self.pending.lock().iter().find(|(h, _)| h == hash) {
            return Ok(bytes::Bytes::copy_from_slice(b));
        }
        self.durable.get(hash).await
    }

    async fn get_range(
        &self,
        hash: &Hash,
        off: u64,
        len: u64,
    ) -> Result<bytes::Bytes, OrigoFSError> {
        let full = self.get(hash).await?;
        let start = (off as usize).min(full.len());
        let end = start.saturating_add(len as usize).min(full.len());
        Ok(full.slice(start..end))
    }

    async fn has(&self, hash: &Hash) -> Result<bool, OrigoFSError> {
        Ok(self.pending.lock().iter().any(|(h, _)| h == hash) || self.durable.has(hash).await?)
    }

    async fn list(&self) -> Result<Vec<Hash>, OrigoFSError> {
        self.durable.list().await
    }

    async fn delete(&self, hash: &Hash) -> Result<u64, OrigoFSError> {
        self.durable.delete(hash).await
    }
}

/// A clean merge must flush its commit and trees **before** the branch ref names
/// them, or a crash in that window leaves the branch pointing at a commit that
/// does not exist.
///
/// `commit` has always paid this barrier; the clean-merge path did not — its only
/// flush was the incidental one inside `mirror_refs`, which runs *after* the
/// transaction that advances the ref. That is the window this test opens: the
/// ref-mirror snapshot (`ORGR`) is made to fail, so the engine stops exactly
/// between the committed ref and the flush that used to be the first one. On the
/// flagship packed object-store stack the whole merge fits in the in-memory
/// buffer, so the branch survived the crash and its commit did not: `log`,
/// `checkout`, and GC all fail with `ContentMissing`, and `fsck --rebuild` drops
/// the branch.
#[tokio::test]
async fn a_clean_merge_makes_its_commit_durable_before_advancing_the_branch() {
    let store = Arc::new(BufferingStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();

    fs.write("/base.txt", b"base\n").await.unwrap();
    fs.commit("dan", "base").await.unwrap();

    fs.create_branch("feature").await.unwrap();
    fs.checkout("feature").await.unwrap();
    fs.write("/theirs.txt", b"theirs\n").await.unwrap();
    let theirs = fs.commit("dan", "theirs").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/ours.txt", b"ours\n").await.unwrap();
    let ours = fs.commit("dan", "ours").await.unwrap();

    // Stop the engine right after the ref transaction commits: `mirror_refs` is
    // the next thing to run, and its snapshot object is the first thing it writes.
    store.fail_on_tag(b"ORGR");
    let _ = fs.merge(theirs, "dan", "merge").await;

    // Everything unflushed is lost; the metadata (and so the branch) survives.
    store.crash();

    let head = fs
        .branch_head("main")
        .await
        .unwrap()
        .expect("main still exists");
    assert_ne!(head, ours, "sanity: the merge did advance the branch");
    let commit = fs
        .commit_object(&head)
        .await
        .expect("the branch must never name a commit that was not made durable first");
    fs.tree_object(&commit.tree)
        .await
        .expect("the merged tree must be durable too");
}
