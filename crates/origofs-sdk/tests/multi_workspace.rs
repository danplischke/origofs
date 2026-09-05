//! Multi-workspace in one store (`docs/MULTI_TENANCY.md`): a single metadata +
//! content store holds many workspaces, separated by a `workspace_id`. Each has
//! its own root inode, working tree, refs, and versioning; content and identity
//! are shared. These tests pin the isolation, independent versioning, and
//! persistence guarantees.

use origofs_sdk::{MemStore, OrigoFSError, VersioningMode, Workspace, WriteCtx};

/// Files, directory listings, and reads are isolated per workspace, and each
/// workspace roots at its own inode (only the `default` workspace is `INO_ROOT`).
#[tokio::test]
async fn workspaces_isolate_their_trees() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();

    // Three workspaces sharing one store: the implicit `default` plus two named.
    ws.write("/d.txt", b"in-default").await.unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    alpha.write("/a.txt", b"in-alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();
    beta.write("/b.txt", b"in-beta").await.unwrap();

    // Each root lists only its own entries.
    assert_eq!(names(&ws).await, vec!["d.txt"]);
    assert_eq!(names(&alpha).await, vec!["a.txt"]);
    assert_eq!(names(&beta).await, vec!["b.txt"]);

    // Reads resolve within the workspace…
    assert_eq!(&alpha.read("/a.txt").await.unwrap()[..], b"in-alpha");
    assert_eq!(&beta.read("/b.txt").await.unwrap()[..], b"in-beta");

    // …and a file from another workspace is simply not there.
    assert!(matches!(
        alpha.read("/d.txt").await,
        Err(OrigoFSError::NotFound(_))
    ));
    assert!(matches!(
        ws.read("/a.txt").await,
        Err(OrigoFSError::NotFound(_))
    ));

    // The default workspace roots at INO_ROOT (1); a named one gets its own root.
    assert_eq!(ws.stat("/").await.unwrap().ino, 1);
    assert_ne!(alpha.stat("/").await.unwrap().ino, 1);
    assert_ne!(
        alpha.stat("/").await.unwrap().ino,
        beta.stat("/").await.unwrap().ino
    );

    // The registry enumerates every workspace, oldest first.
    assert_eq!(
        ws.workspaces().await.unwrap(),
        vec!["default", "alpha", "beta"]
    );
}

/// Refs, branches, and commit history are per workspace: a commit on one is
/// invisible to another sharing the same store.
#[tokio::test]
async fn workspaces_version_independently() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();

    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();

    alpha.write("/a.txt", b"hello").await.unwrap();
    alpha.commit("tester", "add a").await.unwrap();
    alpha.create_branch("feature").await.unwrap();

    // Alpha has one commit and two branches…
    assert_eq!(alpha.log().await.unwrap().len(), 1);
    let mut alpha_branches: Vec<String> = alpha
        .list_branches()
        .await
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    alpha_branches.sort();
    assert_eq!(alpha_branches, vec!["feature", "main"]);

    // …while beta, sharing the store, has neither the commit nor the branch.
    assert_eq!(beta.log().await.unwrap().len(), 0);
    let beta_branches: Vec<String> = beta
        .list_branches()
        .await
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert!(!beta_branches.contains(&"feature".to_string()));
}

/// Workspaces persist: reopening the store and asking for a named workspace
/// returns the same one, with its tree intact.
#[tokio::test]
async fn workspaces_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("meta.db");
    let cas = dir.path().join("cas");

    let root_ino = {
        let ws = Workspace::open_local(&db, &cas).await.unwrap();
        let alpha = ws.workspace("alpha").await.unwrap();
        alpha.write("/keep.txt", b"durable").await.unwrap();
        alpha.stat("/").await.unwrap().ino
    };

    // Reopen the store from scratch and re-resolve the workspace by name.
    let ws = Workspace::open_local(&db, &cas).await.unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    assert_eq!(alpha.stat("/").await.unwrap().ino, root_ino);
    assert_eq!(&alpha.read("/keep.txt").await.unwrap()[..], b"durable");
    // Opening the same name again is idempotent, not a duplicate.
    assert_eq!(ws.workspaces().await.unwrap(), vec!["default", "alpha"]);
}

/// The same isolation + independent-versioning guarantees hold on the Postgres
/// backend (many workspaces in one database via `workspace_id`). Self-skips unless
/// `ORIGOFS_PG_TEST_URL` points at a reachable database. Uses uniquely-named
/// workspaces so repeated runs against a shared DB never collide.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspaces_isolate_on_postgres() {
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TEST_URL") else {
        eprintln!("skipping workspaces_isolate_on_postgres: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let ws = Workspace::open_pg(&dsn, std::sync::Arc::new(MemStore::new()))
        .await
        .unwrap();

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let (na, nb) = (format!("a_{nonce}"), format!("b_{nonce}"));

    let a = ws.workspace(&na).await.unwrap();
    let b = ws.workspace(&nb).await.unwrap();
    a.write("/only-a.txt", b"A").await.unwrap();
    b.write("/only-b.txt", b"B").await.unwrap();

    // Isolated trees, distinct roots, no cross-workspace reads.
    assert_eq!(names(&a).await, vec!["only-a.txt"]);
    assert_eq!(names(&b).await, vec!["only-b.txt"]);
    assert!(matches!(
        a.read("/only-b.txt").await,
        Err(OrigoFSError::NotFound(_))
    ));
    assert_ne!(
        a.stat("/").await.unwrap().ino,
        b.stat("/").await.unwrap().ino
    );

    // Independent refs/history on Postgres too.
    a.commit("tester", "add only-a").await.unwrap();
    assert_eq!(a.log().await.unwrap().len(), 1);
    assert_eq!(b.log().await.unwrap().len(), 0);
}

/// The suggestion queue is workspace-isolated (migration V12): a proposal made in
/// one workspace is invisible in another and cannot be accepted into the wrong
/// tree — the cross-workspace-accept hole the fix closed.
#[tokio::test]
async fn suggestions_are_workspace_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();

    // Actors are store-wide; an author and a distinct reviewer (review needs a
    // different actor than the author).
    let author = ws.create_human("author", None).await.unwrap();
    let reviewer = ws.create_human("reviewer", None).await.unwrap();

    // A proposal made in alpha for a new file.
    let sid = alpha
        .suggest(
            WriteCtx::actor(author),
            "/x.txt",
            b"proposed",
            Some("add x"),
            None,
        )
        .await
        .unwrap();

    // Visible in alpha, invisible in beta.
    assert_eq!(alpha.list_suggestions(None, None).await.unwrap().len(), 1);
    assert!(beta.list_suggestions(None, None).await.unwrap().is_empty());
    assert!(beta.get_suggestion(sid).await.unwrap().is_none());

    // beta cannot accept alpha's suggestion (it can't even resolve it), and the
    // proposed file never leaks into beta's tree.
    assert!(
        beta.accept_suggestion(sid, WriteCtx::actor(reviewer))
            .await
            .is_err(),
        "beta must not accept another workspace's suggestion"
    );
    assert!(matches!(
        beta.read("/x.txt").await,
        Err(OrigoFSError::NotFound(_))
    ));

    // alpha accepts it → lands in alpha's tree only.
    alpha
        .accept_suggestion(sid, WriteCtx::actor(reviewer))
        .await
        .unwrap();
    assert_eq!(&alpha.read("/x.txt").await.unwrap()[..], b"proposed");
    assert!(matches!(
        beta.read("/x.txt").await,
        Err(OrigoFSError::NotFound(_))
    ));
}

/// The change feed and the op-log are workspace-isolated (migration V12): activity
/// in one workspace never surfaces on another's feed or attribution history.
#[tokio::test]
async fn change_feed_and_op_log_are_workspace_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();
    let actor = ws.create_human("writer", None).await.unwrap();

    // An attributed write in alpha.
    alpha
        .write_as(WriteCtx::actor(actor), "/only-alpha.txt", b"hi")
        .await
        .unwrap();

    // Change feed: alpha sees its write; beta's feed does not.
    let alpha_feed: Vec<String> = alpha
        .watch(0)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.path)
        .collect();
    let beta_feed: Vec<String> = beta
        .watch(0)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.path)
        .collect();
    assert!(alpha_feed.iter().any(|p| p == "/only-alpha.txt"));
    assert!(!beta_feed.iter().any(|p| p == "/only-alpha.txt"));

    // Op-log: the edit-op is on alpha's history, not beta's.
    assert!(!alpha.edit_ops(actor, None).await.unwrap().is_empty());
    assert!(beta.edit_ops(actor, None).await.unwrap().is_empty());
}

/// Disaster recovery restores **every** workspace of a multi-workspace store from
/// the content store alone: a fresh metadata DB over the surviving content
/// rebuilds the `default` workspace and each other one, with its committed tree.
#[tokio::test]
async fn rebuild_recovers_all_workspaces() {
    let dir = tempfile::tempdir().unwrap();
    let cas = dir.path().join("cas");

    // Author committed history across three workspaces sharing one content store.
    {
        let ws = Workspace::open_local(dir.path().join("db1.sqlite"), &cas)
            .await
            .unwrap();
        ws.write("/root.txt", b"in-default").await.unwrap();
        ws.commit("t", "default").await.unwrap();

        let alpha = ws.workspace("alpha").await.unwrap();
        alpha.write("/a.txt", b"in-alpha").await.unwrap();
        alpha.commit("t", "alpha").await.unwrap();

        let beta = ws.workspace("beta").await.unwrap();
        beta.mkdir_p("/d").await.unwrap();
        beta.write("/d/b.txt", b"in-beta").await.unwrap();
        beta.commit("t", "beta").await.unwrap();
    }

    // Catastrophe: the metadata DB is gone. A FRESH DB over the same content store
    // rebuilds every workspace from the object graph + tagged ref mirrors.
    let ws = Workspace::open_local(dir.path().join("db2.sqlite"), &cas)
        .await
        .unwrap();
    let report = ws.rebuild().await.unwrap();
    assert_eq!(
        report.extra_workspaces, 2,
        "alpha + beta should be recovered beyond the default"
    );

    // Every workspace and its committed tree came back.
    let mut ws_names = ws.workspaces().await.unwrap();
    ws_names.sort();
    assert_eq!(ws_names, vec!["alpha", "beta", "default"]);
    assert_eq!(&ws.read("/root.txt").await.unwrap()[..], b"in-default");
    let alpha = ws.workspace("alpha").await.unwrap();
    assert_eq!(&alpha.read("/a.txt").await.unwrap()[..], b"in-alpha");
    let beta = ws.workspace("beta").await.unwrap();
    assert_eq!(&beta.read("/d/b.txt").await.unwrap()[..], b"in-beta");
}

/// Blame is per workspace (migration V13): identical content in two workspaces —
/// which dedups to one object in the shared content store — still carries each
/// workspace's own authorship, not a single shared map. (Pre-V13 the second write
/// would overwrite the first's blame, since it was keyed by content hash alone.)
#[tokio::test]
async fn blame_is_workspace_isolated() {
    use std::collections::HashSet;

    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();

    // Two distinct authors (actors are store-wide).
    let author_a = ws.create_human("author-a", None).await.unwrap();
    let author_b = ws.create_human("author-b", None).await.unwrap();

    // The SAME bytes to the SAME path in both workspaces (content dedups in the
    // shared store), attributed to different authors.
    let content = b"line one\nline two\nline three\n";
    alpha
        .write_as(WriteCtx::actor(author_a), "/f.txt", content)
        .await
        .unwrap();
    beta.write_as(WriteCtx::actor(author_b), "/f.txt", content)
        .await
        .unwrap();

    // Each workspace's blame credits only its own author.
    let a_authors: HashSet<i64> = alpha
        .blame("/f.txt")
        .await
        .unwrap()
        .iter()
        .map(|r| r.actor.id)
        .collect();
    let b_authors: HashSet<i64> = beta
        .blame("/f.txt")
        .await
        .unwrap()
        .iter()
        .map(|r| r.actor.id)
        .collect();
    assert_eq!(
        a_authors,
        HashSet::from([author_a]),
        "alpha blame must credit only author-a"
    );
    assert_eq!(
        b_authors,
        HashSet::from([author_b]),
        "beta blame must credit only author-b"
    );
}

/// Exclusive file locks (LFS-style) are per workspace (V11): the same path can be
/// locked independently in two workspaces, and each sees only its own lock.
#[tokio::test]
async fn locks_are_workspace_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();

    // alpha locks /shared.bin; beta locks the SAME path — not blocked (separate
    // lock space).
    assert!(alpha.lock("/shared.bin", "alice").await.unwrap());
    assert!(
        beta.lock("/shared.bin", "bob").await.unwrap(),
        "beta's lock on the same path must not be blocked by alpha's"
    );

    // Each workspace sees only its own lock.
    let names_owners = |v: Vec<(String, String, i64)>| -> Vec<(String, String)> {
        v.into_iter().map(|(p, o, _)| (p, o)).collect()
    };
    assert_eq!(
        names_owners(alpha.locks().await.unwrap()),
        vec![("/shared.bin".to_string(), "alice".to_string())]
    );
    assert_eq!(
        names_owners(beta.locks().await.unwrap()),
        vec![("/shared.bin".to_string(), "bob".to_string())]
    );

    // Releasing alpha's lock leaves beta's intact.
    assert!(alpha.unlock("/shared.bin", "alice").await.unwrap());
    assert!(alpha.locks().await.unwrap().is_empty());
    assert_eq!(beta.locks().await.unwrap().len(), 1);
}

/// Versioning mode is a per-workspace config (V11): one workspace can run `off`
/// while another stays `native`.
#[tokio::test]
async fn versioning_mode_is_workspace_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();

    // Both default to native; flip alpha to off.
    alpha
        .set_versioning_mode(VersioningMode::Off)
        .await
        .unwrap();
    assert_eq!(alpha.versioning_mode().await.unwrap(), VersioningMode::Off);
    // beta and the default workspace are unaffected.
    assert_eq!(
        beta.versioning_mode().await.unwrap(),
        VersioningMode::Native
    );
    assert_eq!(ws.versioning_mode().await.unwrap(), VersioningMode::Native);
}

/// Presence is per workspace (V12): a session heartbeating in one workspace does
/// not appear in another's presence list.
#[tokio::test]
async fn presence_is_workspace_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();
    let actor = ws.create_human("worker", None).await.unwrap();
    let session = alpha.create_session(actor, Some("cli")).await.unwrap();

    alpha.touch(actor, session, Some("/wip.txt")).await.unwrap();

    // alpha sees the presence; beta does not.
    assert!(
        alpha
            .presence(3600)
            .await
            .unwrap()
            .iter()
            .any(|p| p.session_id == session)
    );
    assert!(
        beta.presence(3600)
            .await
            .unwrap()
            .iter()
            .all(|p| p.session_id != session)
    );
}

/// A merge conflict recorded in one workspace is invisible in another (the
/// `conflict` table is per workspace, V11): building a conflicting merge in `alpha`
/// leaves `beta`'s refs, tree, and (empty) conflict set intact.
#[tokio::test]
async fn merge_conflicts_are_workspace_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();

    // beta does independent committed work.
    beta.write("/b.txt", b"beta-content").await.unwrap();
    beta.commit("t", "beta base").await.unwrap();

    // alpha builds a conflicting merge: a base, then divergent edits to /f.txt on
    // two branches.
    alpha.write("/f.txt", b"base\n").await.unwrap();
    alpha.commit("t", "base").await.unwrap();
    alpha.create_branch("feature").await.unwrap();
    alpha.write("/f.txt", b"main edit\n").await.unwrap();
    alpha.commit("t", "main").await.unwrap();
    alpha.checkout("feature").await.unwrap();
    alpha.write("/f.txt", b"feature edit\n").await.unwrap();
    alpha.commit("t", "feature").await.unwrap();
    alpha.checkout("main").await.unwrap();
    let outcome = alpha.merge_branch("feature", "t", "merge").await.unwrap();

    // alpha recorded a conflict; beta sees none and is otherwise untouched.
    assert!(
        !alpha.conflicts().await.unwrap().is_empty(),
        "alpha's merge should have conflicted, got {outcome:?}"
    );
    assert!(
        beta.conflicts().await.unwrap().is_empty(),
        "beta must not see alpha's merge conflict"
    );
    assert_eq!(&beta.read("/b.txt").await.unwrap()[..], b"beta-content");
    let beta_branches: Vec<String> = beta
        .list_branches()
        .await
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert!(!beta_branches.contains(&"feature".to_string()));
}

/// The "one store, many workspaces" model under concurrent load on Postgres:
/// writers on *different* workspaces sharing one database/pool must not interfere
/// or deadlock — each workspace ends with exactly its own files. Self-skips unless
/// `ORIGOFS_PG_TEST_URL` is set; uniquely-named workspaces avoid shared-DB clashes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_across_workspaces_dont_interfere() {
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TEST_URL") else {
        eprintln!(
            "skipping concurrent_writers_across_workspaces_dont_interfere: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    let ws = Workspace::open_pg(&dsn, std::sync::Arc::new(MemStore::new()))
        .await
        .unwrap();

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n_ws = 6usize;
    let files_per = 12usize;

    // Open a handle per workspace up front (sequential create; concurrent writes).
    let mut handles = Vec::new();
    for i in 0..n_ws {
        handles.push(ws.workspace(&format!("cw_{nonce}_{i}")).await.unwrap());
    }

    // Each task writes its own workspace concurrently.
    let mut tasks = Vec::new();
    for (i, h) in handles.iter().cloned().enumerate() {
        tasks.push(tokio::spawn(async move {
            for f in 0..files_per {
                h.write(&format!("/f{f}.txt"), format!("ws{i}-file{f}").as_bytes())
                    .await
                    .unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // Each workspace has exactly its own files, uncorrupted by the others.
    for (i, h) in handles.iter().enumerate() {
        assert_eq!(
            h.ls("/").await.unwrap().len(),
            files_per,
            "ws{i}: wrong file count — cross-workspace interference"
        );
        for f in 0..files_per {
            assert_eq!(
                &h.read(&format!("/f{f}.txt")).await.unwrap()[..],
                format!("ws{i}-file{f}").as_bytes(),
                "ws{i}: /f{f}.txt content wrong"
            );
        }
    }
}

/// Actual contention (not just the disjoint-workspace case above): many writers on
/// the SAME workspace, plus a concurrent reader, all sharing one Postgres pool.
/// This stresses the genuinely shared state — the global inode-id sequence and the
/// dentry rows under one root — that the disjoint test never touches. Every distinct
/// file must land exactly once, uncorrupted, with no lost update or deadlock.
/// Self-skips unless `ORIGOFS_PG_TEST_URL` is set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_same_workspace_dont_corrupt() {
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TEST_URL") else {
        eprintln!(
            "skipping concurrent_writers_same_workspace_dont_corrupt: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    let ws = Workspace::open_pg(&dsn, std::sync::Arc::new(MemStore::new()))
        .await
        .unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let w = ws.workspace(&format!("cs_{nonce}")).await.unwrap();
    let n = 16usize;

    // N writers, each a distinct file (no same-name race), into the one workspace.
    let mut tasks = Vec::new();
    for i in 0..n {
        let h = w.clone();
        tasks.push(tokio::spawn(async move {
            h.write(&format!("/f{i}.txt"), format!("v{i}").as_bytes())
                .await
                .unwrap();
        }));
    }
    // A reader hammering the same root concurrently must never observe corruption
    // or panic (it may see a partial set mid-flight — that's fine).
    let hr = w.clone();
    let reader = tokio::spawn(async move {
        for _ in 0..40 {
            let entries = hr.ls("/").await.unwrap();
            assert!(
                entries.len() <= n,
                "reader saw more entries than were written"
            );
        }
    });
    for t in tasks {
        t.await.unwrap();
    }
    reader.await.unwrap();

    // Every write landed exactly once, with the right bytes.
    assert_eq!(
        w.ls("/").await.unwrap().len(),
        n,
        "same-workspace concurrent writers lost or duplicated a file"
    );
    for i in 0..n {
        assert_eq!(
            &w.read(&format!("/f{i}.txt")).await.unwrap()[..],
            format!("v{i}").as_bytes(),
            "/f{i}.txt content corrupted under contention"
        );
    }
}

/// Reaping presence in one workspace must not evict another workspace's rows.
/// Regression: `reap_presence` issued a store-wide `DELETE FROM presence` with no
/// `workspace_id` predicate, so one workspace's cleanup deleted every workspace's
/// presence — including live sessions — whenever it used a shorter cutoff.
#[tokio::test]
async fn reap_presence_is_workspace_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();
    let actor = ws.create_human("worker", None).await.unwrap();

    // A freshly-heartbeated session in each workspace (distinct sessions → distinct
    // presence rows, both with last_seen ≈ now).
    let sa = alpha.create_session(actor, Some("cli")).await.unwrap();
    let sb = beta.create_session(actor, Some("cli")).await.unwrap();
    alpha.touch(actor, sa, Some("/a.txt")).await.unwrap();
    beta.touch(actor, sb, Some("/b.txt")).await.unwrap();

    // alpha reaps aggressively (a negative grace pushes the cutoff into the future,
    // so every alpha row qualifies). It must reap exactly its own one row.
    let reaped = alpha.reap_presence(-100).await.unwrap();
    assert_eq!(
        reaped, 1,
        "alpha's reap must delete only alpha's presence row"
    );

    // alpha's session is gone; beta's live session is untouched.
    assert!(
        alpha
            .presence(3600)
            .await
            .unwrap()
            .iter()
            .all(|p| p.session_id != sa)
    );
    assert!(
        beta.presence(3600)
            .await
            .unwrap()
            .iter()
            .any(|p| p.session_id == sb),
        "beta's presence must survive alpha's reap"
    );
}

/// GC in one workspace must not reclaim another workspace's pending-suggestion
/// content. Regression: the suggestions GC root was marked only for the calling
/// workspace, so `beta.gc()` swept `alpha`'s proposed bytes out of the shared CAS
/// and a later `alpha.accept_suggestion` failed with `ContentMissing`.
#[tokio::test]
async fn gc_preserves_other_workspaces_pending_suggestions() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();
    let author = ws.create_human("author", None).await.unwrap();
    let reviewer = ws.create_human("reviewer", None).await.unwrap();

    // A pending proposal in alpha: its bytes live only in the CAS until accepted.
    let sid = alpha
        .suggest(
            WriteCtx::actor(author),
            "/x.txt",
            b"proposed body",
            Some("x"),
            None,
        )
        .await
        .unwrap();

    // GC driven from a *different* workspace must not touch alpha's proposal.
    beta.gc_with_grace(0).await.unwrap();

    // alpha can still accept it (the proposed content survived the sweep).
    alpha
        .accept_suggestion(sid, WriteCtx::actor(reviewer))
        .await
        .expect("accept must succeed: gc from beta must not reclaim alpha's proposed bytes");
    assert_eq!(&alpha.read("/x.txt").await.unwrap()[..], b"proposed body");
}

/// GC in one workspace must not destroy another workspace's disaster-recovery
/// mirror. Regression: the ref-mirror GC root was marked only for the calling
/// workspace, so `gc()` swept every *other* workspace's recovery snapshot — leaving
/// their committed data in the CAS but un-recoverable as named workspaces. This is
/// the gc-then-rebuild composition the separate gc/rebuild tests never exercised.
#[tokio::test]
async fn gc_preserves_other_workspaces_recovery_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let cas = dir.path().join("cas");

    // Committed history in three workspaces, then GC driven from the default one.
    {
        let ws = Workspace::open_local(dir.path().join("db1.sqlite"), &cas)
            .await
            .unwrap();
        ws.write("/root.txt", b"in-default").await.unwrap();
        ws.commit("t", "default").await.unwrap();
        let alpha = ws.workspace("alpha").await.unwrap();
        alpha.write("/a.txt", b"in-alpha").await.unwrap();
        alpha.commit("t", "alpha").await.unwrap();
        let beta = ws.workspace("beta").await.unwrap();
        beta.write("/b.txt", b"in-beta").await.unwrap();
        beta.commit("t", "beta").await.unwrap();

        // GC from the default workspace: must keep alpha's & beta's mirrors alive.
        ws.gc_with_grace(0).await.unwrap();
    }

    // Catastrophe after the GC: rebuild a fresh DB from the swept content store.
    let ws = Workspace::open_local(dir.path().join("db2.sqlite"), &cas)
        .await
        .unwrap();
    let report = ws.rebuild().await.unwrap();
    assert_eq!(
        report.extra_workspaces, 2,
        "alpha + beta mirrors must survive a gc() run in another workspace"
    );
    let mut ws_names = ws.workspaces().await.unwrap();
    ws_names.sort();
    assert_eq!(ws_names, vec!["alpha", "beta", "default"]);
    let alpha = ws.workspace("alpha").await.unwrap();
    assert_eq!(&alpha.read("/a.txt").await.unwrap()[..], b"in-alpha");
    let beta = ws.workspace("beta").await.unwrap();
    assert_eq!(&beta.read("/b.txt").await.unwrap()[..], b"in-beta");
}

/// The named data-loss path: content deduped across workspaces must survive a GC
/// that runs after *one* workspace stops referencing it. GC marks the union of
/// every workspace's reachability, so beta's copy of shared bytes is protected even
/// when alpha drops its own reference — while a genuinely-orphaned object is swept.
#[tokio::test]
async fn gc_keeps_content_shared_across_workspaces() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();

    // The SAME bytes in both workspaces dedup to ONE object in the shared CAS.
    let shared = b"shared bytes that both workspaces hold and dedup to one object";
    alpha.write("/a.txt", shared).await.unwrap();
    beta.write("/b.txt", shared).await.unwrap();

    // An object only alpha ever referenced, then orphaned — genuine sweep fodder so
    // we know GC actually ran a sweep (not a no-op that trivially "preserves" beta).
    alpha
        .write("/tmp.txt", b"garbage only alpha ever referenced")
        .await
        .unwrap();
    alpha.remove("/tmp.txt").await.unwrap();
    // alpha also stops referencing the shared bytes; only beta still holds them.
    alpha.remove("/a.txt").await.unwrap();

    let stats = alpha.gc_with_grace(0).await.unwrap();
    assert!(
        stats.deleted >= 1,
        "gc should have swept alpha's orphaned object, else the test proves nothing"
    );
    // beta's copy of the shared content survived despite alpha dropping it and
    // driving the collection.
    assert_eq!(&beta.read("/b.txt").await.unwrap()[..], shared);
}

/// Workspace names are validated at the entry point: a name becomes a registry
/// key, the recovery mirror tag, and a listing entry, so empty / path-like / NUL
/// names are rejected — while ordinary names (spaces, unicode, dashes) are allowed.
#[tokio::test]
async fn workspace_names_are_validated() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    for bad in ["", ".", "..", "a/b", "x\0y"] {
        assert!(
            matches!(
                ws.workspace(bad).await,
                Err(OrigoFSError::InvalidArgument(_))
            ),
            "workspace name {bad:?} must be rejected"
        );
    }
    // A name with spaces / unicode / punctuation is fine.
    assert!(ws.workspace("My Project — 2026").await.is_ok());
    // The rejected names never made it into the registry.
    assert_eq!(ws.workspaces().await.unwrap().len(), 2); // default + the valid one
}

/// Opening the same new workspace name concurrently must resolve to one workspace,
/// never a spurious `AlreadyExists` or a duplicate registry row: the loser of the
/// create race adopts the winner's row (UNIQUE(name) guarantees a single row).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopening_a_workspace_is_idempotent_even_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let (w1, w2) = tokio::join!(ws.workspace("shared"), ws.workspace("shared"));
    let (w1, w2) = (w1.unwrap(), w2.unwrap());
    assert_eq!(
        w1.stat("/").await.unwrap().ino,
        w2.stat("/").await.unwrap().ino,
        "concurrent opens of one name must resolve to the same workspace"
    );
    assert_eq!(
        ws.workspaces()
            .await
            .unwrap()
            .iter()
            .filter(|n| *n == "shared")
            .count(),
        1,
        "concurrent create must not mint a duplicate registry row"
    );
}

/// Blame is keyed by `(workspace_id, content_hash)` since V13, so the V8 property —
/// blame follows the checked-out content across a checkout — must still hold inside
/// a NAMED workspace (id != 1), not only the default one it was first proven on.
#[tokio::test]
async fn named_workspace_blame_survives_checkout() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    assert_ne!(
        alpha.stat("/").await.unwrap().ino,
        1,
        "alpha is not default"
    );
    let a = ws.create_human("a", None).await.unwrap();
    let b = ws.create_human("b", None).await.unwrap();

    // v1 on main: `a` writes two lines; branch `dev` off v1.
    alpha
        .write_as(WriteCtx::actor(a), "/f", b"one\ntwo\n")
        .await
        .unwrap();
    alpha.commit("a", "v1").await.unwrap();
    alpha.create_branch("dev").await.unwrap();
    // v2 on main: `b` appends a third line.
    alpha
        .write_as(WriteCtx::actor(b), "/f", b"one\ntwo\nthree\n")
        .await
        .unwrap();
    alpha.commit("b", "v2").await.unwrap();

    // main (v2): a on lines 1-2, b on line 3.
    let bl = alpha.blame("/f").await.unwrap();
    assert_eq!(bl.len(), 2);
    assert_eq!(
        (bl[0].actor.id, bl[0].line_start, bl[0].line_end),
        (a, 1, 2)
    );
    assert_eq!(
        (bl[1].actor.id, bl[1].line_start, bl[1].line_end),
        (b, 3, 3)
    );

    // Checkout dev (v1): blame follows the checked-out content — all `a`, no stale
    // `b` run carried over, inside this named workspace.
    alpha.checkout("dev").await.unwrap();
    assert_eq!(&alpha.read("/f").await.unwrap()[..], b"one\ntwo\n");
    let bl = alpha.blame("/f").await.unwrap();
    assert_eq!(bl.len(), 1);
    assert_eq!(
        (bl[0].actor.id, bl[0].line_start, bl[0].line_end),
        (a, 1, 2)
    );
}

/// Rebuild must be idempotent and recover a named workspace's *full* branch set.
/// Regression: recovery called `create_workspace` unconditionally, so a second
/// `rebuild()` failed with `AlreadyExists`; it now adopts the existing registry row.
#[tokio::test]
async fn rebuild_is_idempotent_and_recovers_named_workspace_branches() {
    let dir = tempfile::tempdir().unwrap();
    let cas = dir.path().join("cas");
    {
        let ws = Workspace::open_local(dir.path().join("db1.sqlite"), &cas)
            .await
            .unwrap();
        ws.write("/root.txt", b"d").await.unwrap();
        ws.commit("t", "default").await.unwrap();
        let alpha = ws.workspace("alpha").await.unwrap();
        alpha.write("/a.txt", b"v1").await.unwrap();
        alpha.commit("t", "a1").await.unwrap();
        alpha.create_branch("feature").await.unwrap();
        alpha.write("/a.txt", b"v2").await.unwrap();
        alpha.commit("t", "a2").await.unwrap(); // feature @ v2, main @ v1
    }

    let ws = Workspace::open_local(dir.path().join("db2.sqlite"), &cas)
        .await
        .unwrap();
    let r1 = ws.rebuild().await.unwrap();
    assert_eq!(r1.extra_workspaces, 1);
    // A second pass must not error now that recovery adopts an existing row.
    let r2 = ws.rebuild().await.unwrap();
    assert_eq!(r2.extra_workspaces, 1, "second rebuild must be idempotent");

    // alpha came back with BOTH branches, not just the checked-out one.
    let alpha = ws.workspace("alpha").await.unwrap();
    let mut branches: Vec<String> = alpha
        .list_branches()
        .await
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    branches.sort();
    assert_eq!(branches, vec!["feature", "main"]);
}

/// The sorted-by-insertion entry names directly under a workspace's root.
async fn names(ws: &Workspace) -> Vec<String> {
    ws.ls("/")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect()
}
