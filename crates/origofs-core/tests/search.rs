//! Content search over the working tree.
//!
//! The index is keyed by **content address**, never by path, and almost every
//! test here is really about one consequence of that: the operations that would
//! force a path-keyed index to be invalidated — rename, delete, checkout —
//! require no invalidation at all, because a hit is resolved against the live
//! tree at query time. If any of those start returning stale paths, the design
//! has been broken rather than merely slowed down.

use origofs_core::{Fs, MemStore, MetadataStore, SqliteMetadataStore, VersioningMode, WriteCtx};
use std::sync::Arc;

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

async fn fixture() -> TestFs {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

async fn paths(fs: &TestFs, q: &str) -> Vec<String> {
    let mut p: Vec<String> = fs
        .search(q, 50)
        .await
        .unwrap()
        .into_iter()
        .map(|h| h.path)
        .collect();
    p.sort();
    p
}

// --- the basics ------------------------------------------------------------

#[tokio::test]
async fn nothing_is_searchable_until_it_is_indexed() {
    let fs = fixture().await;
    fs.write("/notes.md", b"the quick brown fox").await.unwrap();

    // The honest failure mode: an unindexed workspace returns nothing, and the
    // status says why. A caller that renders this as "no matches" without
    // reading `pending` is lying to its user, which is what `pending` is for.
    assert!(paths(&fs, "quick").await.is_empty());
    let st = fs.search_status().await.unwrap();
    assert_eq!(st.pending, 1);
    assert!(!st.complete());

    fs.reindex().await.unwrap();
    assert_eq!(paths(&fs, "quick").await, ["/notes.md"]);
    assert!(fs.search_status().await.unwrap().complete());
}

#[tokio::test]
async fn terms_are_anded_and_case_folded() {
    let fs = fixture().await;
    fs.write("/a.txt", b"alpha beta").await.unwrap();
    fs.write("/b.txt", b"alpha gamma").await.unwrap();
    fs.reindex().await.unwrap();

    assert_eq!(paths(&fs, "ALPHA").await, ["/a.txt", "/b.txt"]);
    assert_eq!(paths(&fs, "alpha beta").await, ["/a.txt"]);
    assert!(paths(&fs, "alpha delta").await.is_empty());
}

#[tokio::test]
async fn a_query_with_no_searchable_terms_matches_nothing_rather_than_everything() {
    let fs = fixture().await;
    fs.write("/a.txt", b"alpha beta").await.unwrap();
    fs.reindex().await.unwrap();
    // "a" is below the minimum term length, so this query holds nothing. The
    // dangerous reading is "no filter, return all".
    assert!(paths(&fs, "a").await.is_empty());
    assert!(paths(&fs, "").await.is_empty());
}

#[tokio::test]
async fn binary_content_is_recorded_as_indexed_rather_than_retried_forever() {
    let fs = fixture().await;
    fs.write("/logo.png", b"\x89PNG\x00\x00\x00stuff readable")
        .await
        .unwrap();
    let report = fs.reindex().await.unwrap();
    assert_eq!(report.indexed, 1);
    assert_eq!(report.skipped_binary, 1);
    assert_eq!(report.terms, 0);

    // The point of recording it: nothing is pending, so the sweep will not read
    // this file again on every future pass.
    assert!(fs.search_status().await.unwrap().complete());
    assert!(paths(&fs, "readable").await.is_empty());
    assert_eq!(fs.index_pending(10).await.unwrap().indexed, 0);
}

// --- the properties that come free from content-addressing -----------------

#[tokio::test]
async fn a_rename_needs_no_reindex() {
    let fs = fixture().await;
    fs.write("/old.md", b"needle in here").await.unwrap();
    fs.reindex().await.unwrap();
    assert_eq!(paths(&fs, "needle").await, ["/old.md"]);

    fs.rename("/old.md", "/new.md").await.unwrap();

    // No reindex call in between. The index holds no path, so there is nothing
    // to update; the hit resolves through the live tree to the new name.
    assert_eq!(paths(&fs, "needle").await, ["/new.md"]);
    assert!(fs.search_status().await.unwrap().complete());
}

#[tokio::test]
async fn a_delete_stops_matching_with_no_tombstone() {
    let fs = fixture().await;
    fs.write("/doomed.md", b"needle in here").await.unwrap();
    fs.reindex().await.unwrap();
    assert_eq!(paths(&fs, "needle").await, ["/doomed.md"]);

    fs.remove("/doomed.md").await.unwrap();

    // The index row survives (the content may still be referenced elsewhere),
    // but the hit no longer resolves to a path, so it is simply not returned.
    // This is why the design needs no tombstones and no delete events.
    assert!(paths(&fs, "needle").await.is_empty());
}

#[tokio::test]
async fn identical_content_at_many_paths_is_indexed_once_and_found_at_all_of_them() {
    let fs = fixture().await;
    for p in ["/one.md", "/two.md", "/three.md"] {
        fs.write(p, b"shared needle text").await.unwrap();
    }
    let report = fs.reindex().await.unwrap();
    assert_eq!(
        report.indexed, 1,
        "the unit of work is a blob, so three identical files cost one extraction"
    );
    assert_eq!(
        paths(&fs, "needle").await,
        ["/one.md", "/three.md", "/two.md"]
    );
}

#[tokio::test]
async fn a_branch_checkout_costs_no_reindexing() {
    // The case that rules out the alternatives: a checkout re-materializes the
    // whole working tree, which a path-keyed index or a per-inode version column
    // would have to walk in full. Here the content addresses are unchanged, so
    // there is nothing to do.
    let fs = fixture().await;
    fs.set_versioning_mode(VersioningMode::Native)
        .await
        .unwrap();
    fs.write("/doc.md", b"content on main").await.unwrap();
    fs.commit("author", "main content").await.unwrap();
    fs.reindex().await.unwrap();

    fs.create_branch("feature").await.unwrap();
    fs.checkout("feature").await.unwrap();
    fs.write("/doc.md", b"content on feature").await.unwrap();
    fs.commit("author", "feature content").await.unwrap();
    fs.reindex().await.unwrap();
    assert_eq!(paths(&fs, "feature").await, ["/doc.md"]);

    // Back to main: every blob involved has been seen before.
    fs.checkout("main").await.unwrap();
    let report = fs.reindex().await.unwrap();
    assert_eq!(
        report.indexed, 0,
        "a checkout to previously-indexed content must do no extraction work"
    );
    // And the results follow the branch, because they resolve through the tree.
    assert_eq!(paths(&fs, "main").await, ["/doc.md"]);
    assert!(paths(&fs, "feature").await.is_empty());
}

#[tokio::test]
async fn origofs_own_state_is_never_a_search_hit() {
    // The co-edit CRDT sidecars are committed working-tree files, so code written
    // for user files reaches them — and they carry `(actor, session)` stamps and
    // node ids. Indexing them would make internal identifiers searchable, the
    // same class of leak as #143.
    let fs = fixture().await;
    fs.mkdir_p("/.origofs/ydoc").await.unwrap();
    fs.write("/.origofs/ydoc/state", b"secretterm inside internal state")
        .await
        .unwrap();
    // A real path that merely starts with the same characters must still work —
    // the rule is a directory boundary, not a prefix.
    fs.mkdir_p("/.origofs-bench").await.unwrap();
    fs.write("/.origofs-bench/run.md", b"secretterm inside a real path")
        .await
        .unwrap();
    fs.reindex().await.unwrap();

    assert_eq!(paths(&fs, "secretterm").await, ["/.origofs-bench/run.md"]);
}

#[tokio::test]
async fn a_file_too_large_to_index_is_not_pending_forever() {
    let fs = fixture().await;
    let big = vec![b'a'; (origofs_core::MAX_INDEXED_BYTES + 1) as usize];
    fs.write("/big.log", &big).await.unwrap();
    fs.write("/small.md", b"needle here").await.unwrap();
    fs.reindex().await.unwrap();

    // The cap is applied in the queue, so an over-size blob is never offered and
    // never counted as pending — a "pending" that can never drain would make
    // `complete()` useless as a signal.
    assert!(fs.search_status().await.unwrap().complete());
    assert_eq!(paths(&fs, "needle").await, ["/small.md"]);
}

#[tokio::test]
async fn indexing_is_resumable_and_idempotent() {
    let fs = fixture().await;
    for i in 0..5 {
        fs.write(&format!("/f{i}.md"), format!("doc number {i}").as_bytes())
            .await
            .unwrap();
    }
    // One batch at a time, the way a periodic task would drive it.
    let mut done = 0;
    loop {
        let r = fs.index_pending(2).await.unwrap();
        if r.indexed == 0 {
            break;
        }
        done += r.indexed;
    }
    assert_eq!(done, 5);
    assert!(fs.search_status().await.unwrap().complete());
    // Running again does nothing, rather than re-reading everything.
    assert_eq!(fs.reindex().await.unwrap().indexed, 0);
}

#[tokio::test]
async fn gc_drops_the_index_rows_for_content_it_reclaims() {
    let fs = fixture().await;
    fs.write("/tmp.md", b"reclaimable needle").await.unwrap();
    fs.reindex().await.unwrap();
    assert_eq!(fs.search_status().await.unwrap().indexed, 1);

    fs.remove("/tmp.md").await.unwrap();
    fs.gc_with_grace(0).await.unwrap();

    // Left behind, the row would keep the blob marked "already indexed", so
    // re-writing the same bytes later would never be re-indexed.
    assert_eq!(fs.search_status().await.unwrap().indexed, 0);
    fs.write("/again.md", b"reclaimable needle").await.unwrap();
    assert_eq!(fs.reindex().await.unwrap().indexed, 1);
    assert_eq!(paths(&fs, "reclaimable").await, ["/again.md"]);
}

// --- ACLs ------------------------------------------------------------------

#[tokio::test]
async fn search_as_is_open_while_read_enforcement_is_off() {
    let fs = fixture().await;
    let actor = fs
        .find_or_create_agent("agent", "agent", "test", None)
        .await
        .unwrap();
    fs.mkdir_p("/secret").await.unwrap();
    fs.write("/secret/plan.md", b"classified needle")
        .await
        .unwrap();
    fs.reindex().await.unwrap();

    // Same default as every other attributed read: inert until switched on, so
    // an upgrade does not stop every actor at once.
    let hits = fs
        .search_as(WriteCtx::actor(actor), "needle", 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
}

#[tokio::test]
async fn search_as_hides_hits_the_actor_may_not_read() {
    let fs = fixture().await;
    let actor = fs
        .find_or_create_agent("agent", "agent", "test", None)
        .await
        .unwrap();
    fs.mkdir_p("/open").await.unwrap();
    fs.mkdir_p("/secret").await.unwrap();
    fs.write("/open/notes.md", b"shared needle").await.unwrap();
    fs.write("/secret/plan.md", b"secret needle").await.unwrap();
    fs.reindex().await.unwrap();

    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    fs.grant(actor, "/open", origofs_core::Perms::READ, None)
        .await
        .unwrap();

    let hits = fs
        .search_as(WriteCtx::actor(actor), "needle", 10)
        .await
        .unwrap();
    assert_eq!(
        hits.iter().map(|h| h.path.as_str()).collect::<Vec<_>>(),
        ["/open/notes.md"],
        "a hit the actor cannot stat must not be discoverable by searching for it"
    );

    // And the unfiltered engine call still sees both — so the filter is doing
    // the work, not an accident of what got indexed.
    assert_eq!(fs.search("needle", 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn an_unreadable_run_of_hits_does_not_truncate_the_page() {
    // The "filter, then page" property. With page-then-filter, asking for 2
    // visible hits behind 10 unreadable ones returns an empty first page, which
    // a caller reads as end-of-results.
    let fs = fixture().await;
    let actor = fs
        .find_or_create_agent("agent", "agent", "test", None)
        .await
        .unwrap();
    fs.mkdir_p("/secret").await.unwrap();
    fs.mkdir_p("/open").await.unwrap();
    for i in 0..10 {
        fs.write(
            &format!("/secret/s{i}.md"),
            format!("needle {i}").as_bytes(),
        )
        .await
        .unwrap();
    }
    for i in 0..2 {
        fs.write(
            &format!("/open/o{i}.md"),
            format!("needle open {i}").as_bytes(),
        )
        .await
        .unwrap();
    }
    fs.reindex().await.unwrap();
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    fs.grant(actor, "/open", origofs_core::Perms::READ, None)
        .await
        .unwrap();

    let hits = fs
        .search_as(WriteCtx::actor(actor), "needle", 2)
        .await
        .unwrap();
    assert_eq!(
        hits.len(),
        2,
        "the page must fill with visible hits, not stop at the first invisible one"
    );
    assert!(hits.iter().all(|h| h.path.starts_with("/open/")));
}
