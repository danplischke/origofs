//! The Yjs sidecar-blob slot is pinned per commit and GC-reachable
//! (issue #75 §3.3).
//!
//! `checkpoint_coedit` persists the live CRDT to `/.origofs/ydoc/<hex(path)>`
//! through the ordinary `Fs::write`, so the sidecar is an ordinary working-tree
//! file. The claim that follows — that it therefore rides into every commit tree
//! and is marked reachable by `gc` from both the working-tree root and the commit
//! roots — is exactly the kind of thing that is true until some hidden-path
//! exclusion quietly makes it false. These tests **prove** it rather than assuming
//! it, and they are what would catch a regression:
//!
//! * a `gc()` pass right after a checkpoint must not reclaim the sidecar's chunks,
//!   and the document must still *resume* (not silently rebuild from flat text);
//! * the sidecar must be inside the commit tree, so a `checkout` that wipes and
//!   rematerializes the working tree brings it back;
//! * a sidecar for a path that has since been deleted, or that is only present in
//!   an older commit, must still not break a GC pass.

#![cfg(feature = "coedit")]

use origofs_core::{
    ContentStore, Fs, Hash, MemStore, MetadataStore, SqliteMetadataStore, WriteCtx,
    coedit_sidecar_path,
};
use std::sync::Arc;

async fn fixture() -> (Fs<SqliteMetadataStore, Arc<MemStore>>, Arc<MemStore>) {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();
    (fs, store)
}

/// Every content object the sidecar for `path` is made of: its manifest plus each
/// chunk the manifest references. These are what a GC pass must not reclaim.
async fn sidecar_objects<M: MetadataStore, C: ContentStore>(
    fs: &Fs<M, C>,
    path: &str,
) -> Vec<Hash> {
    let inode = fs.stat(&coedit_sidecar_path(path)).await.unwrap();
    let mhash = inode.content.expect("a checkpointed sidecar has content");
    let manifest = origofs_core::Manifest::decode(&fs.content.get(&mhash).await.unwrap())
        .expect("sidecar manifest decodes");
    let mut out = vec![mhash];
    out.extend(manifest.chunks.iter().map(|c| c.hash));
    out
}

async fn assert_all_present<C: ContentStore>(store: &C, hashes: &[Hash], when: &str) {
    for h in hashes {
        assert!(
            store.has(h).await.unwrap(),
            "{when}: content object {} was reclaimed",
            h.to_hex()
        );
    }
}

// A co-edit checkpoint followed by `gc()` must keep the sidecar's chunks, and the
// document must still *resume* from the CRDT afterwards rather than being rebuilt
// from the flat text. Resumption is checked structurally, not by comparing text:
// a rebuilt document is a brand-new `Doc` with a fresh client id and no history,
// so its encoded state differs from the checkpointed one even when the visible
// text is identical.
#[tokio::test]
async fn gc_does_not_reclaim_a_live_sidecar_and_the_doc_still_resumes() {
    let (fs, store) = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let a = WriteCtx::session(alice, s_a);

    let doc = fs.open_coedit(a, "/notes.md").await.unwrap();
    // Include a deletion so the CRDT carries history the flat text cannot: a
    // rebuild would lose the tombstone, which makes "resumed vs rebuilt" visible.
    doc.insert(a, 0, "hello cruel world\n");
    doc.remove(5, 6); // -> "hello world\n"
    fs.checkpoint_coedit(a, "/notes.md", &doc).await.unwrap();
    assert_eq!(&fs.read("/notes.md").await.unwrap()[..], b"hello world\n");

    let objects = sidecar_objects(&fs, "/notes.md").await;
    let checkpointed_state = doc.state_update();

    // Churn, so the sweep genuinely has something to delete and we know it ran.
    fs.write("/scratch.bin", &vec![7u8; 200 * 1024])
        .await
        .unwrap();
    fs.write("/scratch.bin", &vec![9u8; 200 * 1024])
        .await
        .unwrap();
    let stats = fs.gc().await.unwrap();
    assert!(stats.deleted > 0, "the sweep really ran");

    assert_all_present(&*store, &objects, "after gc").await;

    // The sidecar still reads back, and resuming yields the *same CRDT* — byte-for
    // byte the state we checkpointed — not a fresh document rebuilt from the text.
    let resumed = fs.open_coedit(a, "/notes.md").await.unwrap();
    assert_eq!(resumed.text(), "hello world\n");
    assert_eq!(
        resumed.state_update(),
        checkpointed_state,
        "open_coedit must resume the persisted CRDT after a GC pass, not rebuild it"
    );

    // And it is still a *working* document: further edits checkpoint normally.
    resumed.insert(a, 11, "!");
    fs.checkpoint_coedit(a, "/notes.md", &resumed)
        .await
        .unwrap();
    assert_eq!(&fs.read("/notes.md").await.unwrap()[..], b"hello world!\n");
}

// The sidecar is a normal file, so `commit` walks it into the commit tree. Proof:
// after committing, wipe and rematerialize the working tree via `checkout` — the
// sidecar comes back, and the document still resumes. (If commit excluded hidden
// paths, the sidecar would vanish here and `open_coedit` would silently fall back
// to rebuilding from flat text.)
#[tokio::test]
async fn the_sidecar_rides_into_the_commit_tree_and_survives_checkout() {
    let (fs, store) = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let a = WriteCtx::actor(alice);

    let doc = fs.open_coedit(a, "/notes.md").await.unwrap();
    doc.insert(a, 0, "committed text\n");
    fs.checkpoint_coedit(a, "/notes.md", &doc).await.unwrap();
    let checkpointed_state = doc.state_update();
    let objects = sidecar_objects(&fs, "/notes.md").await;

    fs.commit("alice", "with a live document").await.unwrap();

    // Round-trip the working tree through the commit: truncate + rematerialize.
    fs.checkout("main").await.unwrap();
    assert_eq!(
        &fs.read("/notes.md").await.unwrap()[..],
        b"committed text\n"
    );
    assert!(
        fs.read(&coedit_sidecar_path("/notes.md")).await.is_ok(),
        "the sidecar must be in the commit tree, or a checkout loses the CRDT"
    );

    // A GC pass now marks it through the commit root as well as the working tree.
    let stats = fs.gc().await.unwrap();
    assert_all_present(&*store, &objects, "after commit + checkout + gc").await;
    assert!(stats.reachable > 0);

    let resumed = fs.open_coedit(a, "/notes.md").await.unwrap();
    assert_eq!(
        resumed.state_update(),
        checkpointed_state,
        "the CRDT survives a commit/checkout round-trip"
    );
}

// A sidecar whose document has since been deleted from the working tree is
// ordinary garbage once no commit holds it — and reclaiming it must not break
// anything: reopening the (now absent) path yields a fresh empty document rather
// than an error or a dangling content read.
#[tokio::test]
async fn an_orphaned_sidecar_is_reclaimable_and_reopening_is_clean() {
    let (fs, store) = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let a = WriteCtx::actor(alice);

    let doc = fs.open_coedit(a, "/gone.md").await.unwrap();
    doc.insert(a, 0, "temporary\n");
    fs.checkpoint_coedit(a, "/gone.md", &doc).await.unwrap();
    let objects = sidecar_objects(&fs, "/gone.md").await;

    // Never committed, and now removed along with its sidecar.
    fs.remove("/gone.md").await.unwrap();
    fs.remove(&coedit_sidecar_path("/gone.md")).await.unwrap();
    fs.end_coedit("/gone.md").await.unwrap();

    fs.gc().await.unwrap();
    for h in &objects {
        assert!(
            !store.has(h).await.unwrap(),
            "an unreferenced sidecar is ordinary garbage: {}",
            h.to_hex()
        );
    }

    // Reopening the vanished path is clean: an empty document, no error.
    let reopened = fs.open_coedit(a, "/gone.md").await.unwrap();
    assert_eq!(reopened.text(), "");
}
