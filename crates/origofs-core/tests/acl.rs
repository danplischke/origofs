//! Path-scoped access grants (`docs/PERMISSIONS.md` §3b, issue #123).
//!
//! The `WritePolicy` gate (#78) is workspace-wide and binary: an actor writes
//! everywhere or proposes everywhere. Grants refine it per subtree, and these tests
//! pin the four things that make that safe:
//!
//! * **resolution** — longest covering prefix wins, and an actor with no covering
//!   grant falls back to its write policy, which is what makes migration V18
//!   behaviour-preserving with no backfill;
//! * **enforcement at the engine** — every path-taking attributed mutation checks,
//!   so a surface cannot forget one by not knowing about it (the shape of #78);
//! * **directory-boundary matching** — `/tenant-a` must never cover `/tenant-abc`;
//! * **the exemptions stay exempt** — the unattributed ops and the accept path,
//!   without which checkout, merge, and suggestion application break.
//!
//! Postgres runs the same battery, because `set_grant`/`list_grants` have a second
//! implementation and this repository has been bitten by a SQLite-only test before
//! (see `gc.rs`). Self-skips without `ORIGOFS_PG_TEST_URL`.

use origofs_core::{
    Fs, MemStore, MetadataStore, OrigoFSError, Perms, PostgresMetadataStore, SqliteMetadataStore,
    SuggestionStatus, WriteCtx, WriteOutcome, WritePolicy,
};
use std::sync::Arc;

async fn fs() -> Fs<Arc<dyn MetadataStore>, Arc<MemStore>> {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

async fn agent<M: MetadataStore>(fs: &Fs<M, Arc<MemStore>>) -> WriteCtx {
    WriteCtx::actor(fs.create_agent("claude", "opus", None).await.unwrap())
}

fn assert_denied(e: OrigoFSError, what: &str) {
    assert!(
        matches!(e, OrigoFSError::Denied(_)),
        "{what} should be Denied, got {e:?}"
    );
    assert_eq!(e.code(), "denied");
}

// --- resolution -----------------------------------------------------------

#[tokio::test]
async fn an_actor_with_no_grants_falls_back_to_its_write_policy() {
    // The property that makes V18 behaviour-preserving: no backfill, and an actor
    // created *after* the migration is governed exactly as before.
    let fs = fs().await;
    let a = agent(&fs).await;

    assert_eq!(
        fs.effective_perms(a.actor, "/anywhere").await.unwrap(),
        Perms::RW
    );

    fs.set_write_policy(a.actor, WritePolicy::Propose)
        .await
        .unwrap();
    assert_eq!(
        fs.effective_perms(a.actor, "/anywhere").await.unwrap(),
        Perms::RP
    );
}

#[tokio::test]
async fn the_longest_covering_grant_wins() {
    let fs = fs().await;
    let a = agent(&fs).await;

    fs.grant(a.actor, "/", Perms::READ).await.unwrap();
    fs.grant(a.actor, "/src", Perms::RW).await.unwrap();
    fs.grant(a.actor, "/src/vendor", Perms::READ).await.unwrap();

    let at = |p: &'static str| {
        let fs = &fs;
        async move { fs.effective_perms(a.actor, p).await.unwrap() }
    };
    assert_eq!(at("/docs/x.md").await, Perms::READ);
    assert_eq!(at("/src/main.rs").await, Perms::RW);
    assert_eq!(at("/src/vendor/z.c").await, Perms::READ);
    // The prefix itself, not just paths under it.
    assert_eq!(at("/src").await, Perms::RW);
}

#[tokio::test]
async fn a_grant_does_not_leak_across_a_sibling_with_a_shared_prefix() {
    // `/tenant-a` must not cover `/tenant-abc`. This is *the* bug in prefix-scoped
    // authorization, and getting it wrong hands one tenant's agent the next
    // tenant's subtree.
    let fs = fs().await;
    let a = agent(&fs).await;

    fs.grant(a.actor, "/", Perms::READ).await.unwrap();
    fs.grant(a.actor, "/tenant-a", Perms::RW).await.unwrap();

    assert_eq!(
        fs.effective_perms(a.actor, "/tenant-a/notes")
            .await
            .unwrap(),
        Perms::RW
    );
    assert_eq!(
        fs.effective_perms(a.actor, "/tenant-abc/secrets")
            .await
            .unwrap(),
        Perms::READ,
        "a shared string prefix must not be a shared grant"
    );

    fs.mkdir_p("/tenant-abc").await.unwrap();
    fs.write("/tenant-abc/secrets", b"theirs").await.unwrap();
    assert_denied(
        fs.write_as(a, "/tenant-abc/secrets", b"mine")
            .await
            .unwrap_err(),
        "write into a same-string-prefix sibling",
    );
}

#[tokio::test]
async fn regranting_the_same_prefix_replaces_it_and_revoke_reports_reality() {
    let fs = fs().await;
    let a = agent(&fs).await;

    fs.grant(a.actor, "/src", Perms::READ).await.unwrap();
    fs.grant(a.actor, "/src", Perms::RW).await.unwrap();
    assert_eq!(
        fs.grants(a.actor).await.unwrap().len(),
        1,
        "upsert, not add"
    );
    assert_eq!(
        fs.effective_perms(a.actor, "/src/x").await.unwrap(),
        Perms::RW
    );

    // A revoke against a prefix that has no grant must not look like it closed
    // access — an operator who typo'd would otherwise believe they had.
    assert!(!fs.revoke(a.actor, "/srcs").await.unwrap());
    assert!(fs.revoke(a.actor, "/src").await.unwrap());
    assert!(fs.grants(a.actor).await.unwrap().is_empty());

    // A trailing slash addresses the same grant.
    fs.grant(a.actor, "/src/", Perms::RW).await.unwrap();
    assert!(fs.revoke(a.actor, "/src").await.unwrap());
}

#[tokio::test]
async fn a_traversing_prefix_is_refused_rather_than_resolved() {
    // `/src/../etc` normalizing to `/etc` would silently widen a grant an operator
    // wrote narrowly.
    let fs = fs().await;
    let a = agent(&fs).await;
    assert!(fs.grant(a.actor, "/src/../etc", Perms::RW).await.is_err());
    assert!(fs.grant(a.actor, "relative", Perms::RW).await.is_err());
    // And a grant for an actor that does not exist is refused, so a typo'd id
    // cannot sit in the table looking like policy while governing nobody.
    assert!(fs.grant(9_999_999, "/src", Perms::RW).await.is_err());
}

// --- enforcement ----------------------------------------------------------

#[tokio::test]
async fn every_path_taking_mutation_is_refused_outside_the_grant() {
    // The #78 shape: the gate must live in the engine, so a surface cannot miss one.
    let fs = fs().await;
    let a = agent(&fs).await;

    fs.mkdir_p("/out/d").await.unwrap();
    fs.mkdir_p("/in").await.unwrap();
    fs.write("/out/f.txt", b"x").await.unwrap();
    fs.grant(a.actor, "/", Perms::READ).await.unwrap();
    fs.grant(a.actor, "/in", Perms::RW).await.unwrap();

    assert_denied(
        fs.write_as(a, "/out/f.txt", b"y").await.unwrap_err(),
        "write_as",
    );
    assert_denied(
        fs.remove_as(a, "/out/f.txt").await.unwrap_err(),
        "remove_as",
    );
    assert_denied(fs.mkdir_as(a, "/out/new").await.unwrap_err(), "mkdir_as");
    assert_denied(
        fs.symlink_as(a, "/out/f.txt", "/out/link")
            .await
            .unwrap_err(),
        "symlink_as",
    );
    assert_denied(
        fs.chmod_as(a, "/out/f.txt", 0o600).await.unwrap_err(),
        "chmod_as",
    );
    assert_denied(
        fs.chown_as(a, "/out/f.txt", Some(1), None)
            .await
            .unwrap_err(),
        "chown_as",
    );

    // Inside the grant, all of it works.
    fs.mkdir_as(a, "/in/d").await.unwrap();
    fs.write_as(a, "/in/f.txt", b"mine").await.unwrap();
    fs.chmod_as(a, "/in/f.txt", 0o600).await.unwrap();
    fs.remove_as(a, "/in/f.txt").await.unwrap();
}

#[tokio::test]
async fn rename_checks_both_sides() {
    // Checking only the source lets a scoped actor move a file it controls *into* a
    // subtree it does not — smuggling content into a neighbour's tree.
    let fs = fs().await;
    let a = agent(&fs).await;

    fs.grant(a.actor, "/", Perms::READ).await.unwrap();
    fs.grant(a.actor, "/in", Perms::RW).await.unwrap();
    fs.mkdir_p("/in").await.unwrap();
    fs.mkdir_p("/out").await.unwrap();
    fs.write("/in/mine.txt", b"x").await.unwrap();
    fs.write("/out/theirs.txt", b"y").await.unwrap();

    // Out of a granted subtree into an ungranted one.
    assert_denied(
        fs.rename_as(a, "/in/mine.txt", "/out/smuggled.txt")
            .await
            .unwrap_err(),
        "rename into an ungranted subtree",
    );
    // And the reverse: out of an ungranted subtree into a granted one.
    assert_denied(
        fs.rename_as(a, "/out/theirs.txt", "/in/taken.txt")
            .await
            .unwrap_err(),
        "rename out of an ungranted subtree",
    );
    // Both sides inside: fine.
    fs.rename_as(a, "/in/mine.txt", "/in/renamed.txt")
        .await
        .unwrap();
}

#[tokio::test]
async fn write_or_propose_queues_where_propose_is_granted_and_refuses_where_nothing_is() {
    let fs = fs().await;
    let a = agent(&fs).await;

    fs.grant(a.actor, "/", Perms::NONE).await.unwrap();
    fs.grant(a.actor, "/review", Perms::RP).await.unwrap();
    fs.grant(a.actor, "/mine", Perms::RW).await.unwrap();

    // Write access: lands.
    assert_eq!(
        fs.write_or_propose(a, "/mine/f.txt", b"x", None)
            .await
            .unwrap(),
        WriteOutcome::Wrote
    );

    // Propose access: queued, working tree untouched.
    let outcome = fs
        .write_or_propose(a, "/review/f.txt", b"x", None)
        .await
        .unwrap();
    assert!(matches!(outcome, WriteOutcome::Proposed(_)));
    assert!(fs.stat("/review/f.txt").await.is_err());

    // No access: refused outright, *not* queued. Queueing would file a suggestion
    // that accepting must refuse for the same reason, and would tell the caller its
    // edit was merely awaiting review.
    assert_denied(
        fs.write_or_propose(a, "/elsewhere/f.txt", b"x", None)
            .await
            .unwrap_err(),
        "write_or_propose with no access",
    );
    assert_eq!(
        fs.list_suggestions(Some(SuggestionStatus::Pending), None)
            .await
            .unwrap()
            .len(),
        1,
        "the refused write must not have left a suggestion behind"
    );
}

#[tokio::test]
async fn remove_or_propose_follows_the_same_three_way_split() {
    let fs = fs().await;
    let a = agent(&fs).await;

    fs.grant(a.actor, "/", Perms::NONE).await.unwrap();
    fs.grant(a.actor, "/review", Perms::RP).await.unwrap();
    fs.grant(a.actor, "/mine", Perms::RW).await.unwrap();
    for d in ["/mine", "/review", "/elsewhere"] {
        fs.mkdir_p(d).await.unwrap();
    }
    fs.write("/mine/f.txt", b"x").await.unwrap();
    fs.write("/review/f.txt", b"x").await.unwrap();
    fs.write("/elsewhere/f.txt", b"x").await.unwrap();

    assert_eq!(
        fs.remove_or_propose(a, "/mine/f.txt", None).await.unwrap(),
        WriteOutcome::Wrote
    );
    assert!(matches!(
        fs.remove_or_propose(a, "/review/f.txt", None)
            .await
            .unwrap(),
        WriteOutcome::Proposed(_)
    ));
    assert!(fs.stat("/review/f.txt").await.is_ok(), "still there");
    assert_denied(
        fs.remove_or_propose(a, "/elsewhere/f.txt", None)
            .await
            .unwrap_err(),
        "remove_or_propose with no access",
    );
}

#[tokio::test]
async fn a_grant_narrows_a_direct_actor_as_well_as_widening_a_proposer() {
    // Grants are not only a loosening of `Propose`. A `Direct` actor — the default —
    // is restricted by a read-only root grant, which is how deny-by-default is
    // expressed without a separate mode.
    let fs = fs().await;
    let a = agent(&fs).await;
    // A fresh actor is `Direct` by default (the column default), observable as
    // read+write everywhere before any grant narrows it.
    assert_eq!(
        fs.effective_perms(a.actor, "/f.txt").await.unwrap(),
        Perms::RW
    );

    fs.write("/f.txt", b"x").await.unwrap();
    fs.write_as(a, "/f.txt", b"y").await.unwrap(); // ungranted: policy applies

    fs.grant(a.actor, "/", Perms::READ).await.unwrap();
    assert_denied(
        fs.write_as(a, "/f.txt", b"z").await.unwrap_err(),
        "write under a read-only root grant",
    );
}

// --- exemptions -----------------------------------------------------------

#[tokio::test]
async fn unattributed_ops_and_the_accept_path_stay_exempt() {
    // Without this, checkout, merge materialization and suggestion application all
    // break — they are system actions with no requesting actor to police.
    let fs = fs().await;
    let a = agent(&fs).await;
    let human = WriteCtx::actor(fs.create_human("dan", None).await.unwrap());

    fs.grant(a.actor, "/", Perms::NONE).await.unwrap();

    // The raw engine ops carry no actor and are unaffected.
    fs.write("/f.txt", b"one").await.unwrap();
    fs.mkdir_p("/sub").await.unwrap();
    fs.chmod("/f.txt", 0o600).await.unwrap();
    fs.rename("/f.txt", "/sub/f.txt").await.unwrap();
    fs.remove("/sub/f.txt").await.unwrap();

    // And an accepted suggestion still lands, attributed to its original author,
    // even though that author now has no write access at the path. The reviewer is
    // the one acting; re-checking the author here would make the review queue
    // unusable for exactly the restricted actors it exists to serve.
    fs.write("/g.txt", b"before").await.unwrap();
    fs.grant(a.actor, "/g.txt", Perms::RP).await.unwrap();
    let id = fs.suggest(a, "/g.txt", b"after", None).await.unwrap();
    fs.grant(a.actor, "/g.txt", Perms::NONE).await.unwrap();
    fs.accept_suggestion(id, human).await.unwrap();
    assert_eq!(&fs.read("/g.txt").await.unwrap()[..], b"after");
}

#[tokio::test]
async fn grants_are_per_workspace() {
    // A grant in one workspace must not authorize anything in another; the acl
    // table is workspace-scoped like the rest of the working-tree tables.
    let base: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(base.clone(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    let a = agent(&fs).await;
    fs.grant(a.actor, "/", Perms::NONE).await.unwrap();
    fs.grant(a.actor, "/shared", Perms::RW).await.unwrap();

    let (id, root) = base.create_workspace("other").await.unwrap();
    let fs2 = fs.rebind(base.with_workspace(id), root);
    fs2.init().await.unwrap();

    // Actors are store-wide, so the same id resolves — but its grants do not
    // travel, and the fallback (its write policy) governs instead.
    assert_eq!(
        fs2.effective_perms(a.actor, "/shared").await.unwrap(),
        Perms::RW,
        "falls back to the write policy, not to the other workspace's grant"
    );
    assert_eq!(
        fs2.effective_perms(a.actor, "/elsewhere").await.unwrap(),
        Perms::RW,
        "the other workspace's deny-root grant must not follow the actor here"
    );
}

// --- Postgres -------------------------------------------------------------

fn dsn() -> Option<String> {
    std::env::var("ORIGOFS_PG_TEST_URL").ok()
}

/// A Postgres-backed `Fs` on its **own workspace** inside the shared test database.
///
/// Deliberately *not* `DROP SCHEMA public CASCADE`. `cargo test` runs test binaries
/// concurrently against one `ORIGOFS_PG_TEST_URL`, so a reset here tears the schema
/// out from under whatever else is mid-run — which is exactly how this file made
/// `fuse_mount_sees_remote_write_over_postgres` and the co-edit cluster test fail
/// while passing in isolation. A fresh workspace gives the same isolation for the
/// inode, grant and ref space without touching anyone else's rows.
async fn pg_fs(dsn: &str, workspace: &str) -> Fs<Arc<dyn MetadataStore>, Arc<MemStore>> {
    let base: Arc<dyn MetadataStore> = Arc::new(PostgresMetadataStore::connect(dsn).await.unwrap());
    base.init().await.unwrap(); // migrations are idempotent
    let root_fs = Fs::new(base.clone(), Arc::new(MemStore::new()));
    root_fs.init().await.unwrap();
    let (id, root) = match base.lookup_workspace(workspace).await.unwrap() {
        Some(w) => w,
        None => base.create_workspace(workspace).await.unwrap(),
    };
    let fs = root_fs.rebind(base.with_workspace(id), root);
    fs.init().await.unwrap();
    fs
}

#[tokio::test]
async fn postgres_grants_resolve_and_enforce_the_same_way() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let fs = pg_fs(&dsn, "acl-test").await;
    let a = agent(&fs).await;

    // Fallback, before any grant exists.
    assert_eq!(fs.effective_perms(a.actor, "/x").await.unwrap(), Perms::RW);

    fs.grant(a.actor, "/", Perms::READ).await.unwrap();
    fs.grant(a.actor, "/src", Perms::RW).await.unwrap();
    fs.grant(a.actor, "/src/vendor", Perms::READ).await.unwrap();

    assert_eq!(
        fs.effective_perms(a.actor, "/docs").await.unwrap(),
        Perms::READ
    );
    assert_eq!(
        fs.effective_perms(a.actor, "/src/main.rs").await.unwrap(),
        Perms::RW
    );
    assert_eq!(
        fs.effective_perms(a.actor, "/src/vendor/z").await.unwrap(),
        Perms::READ
    );
    // Directory-boundary matching, on the dialect where `LIKE`-shaped thinking
    // would have been tempting.
    assert_eq!(
        fs.effective_perms(a.actor, "/srcs/x").await.unwrap(),
        Perms::READ
    );

    fs.mkdir_p("/docs").await.unwrap();
    fs.mkdir_p("/src").await.unwrap();
    fs.write("/docs/f.txt", b"x").await.unwrap();
    assert_denied(
        fs.write_as(a, "/docs/f.txt", b"y").await.unwrap_err(),
        "write outside the grant (pg)",
    );
    fs.write_as(a, "/src/f.txt", b"y").await.unwrap();

    // Upsert and revoke round-trip.
    fs.grant(a.actor, "/src", Perms::READ).await.unwrap();
    assert_eq!(fs.grants(a.actor).await.unwrap().len(), 3);
    assert_eq!(
        fs.effective_perms(a.actor, "/src/f.txt").await.unwrap(),
        Perms::READ
    );
    assert!(fs.revoke(a.actor, "/src").await.unwrap());
    assert!(!fs.revoke(a.actor, "/src").await.unwrap());
    assert_eq!(fs.grants(a.actor).await.unwrap().len(), 2);
}

// --- reads (#124) ---------------------------------------------------------

#[tokio::test]
async fn a_read_denial_is_not_found_not_denied() {
    // A 403 on a path confirms the path exists, which is the leak a read grant
    // closes: an actor that may not see /secret must not be able to tell it from a
    // path that was never there.
    let fs = fs().await;
    let a = agent(&fs).await;
    fs.mkdir_p("/secret").await.unwrap();
    fs.write("/secret/x", b"classified").await.unwrap();
    fs.grant(a.actor, "/", Perms::NONE).await.unwrap();

    for e in [
        fs.read_as(a, "/secret/x").await.unwrap_err(),
        fs.stat_as(a, "/secret/x").await.unwrap_err(),
        fs.blame_as(a, "/secret/x").await.unwrap_err(),
        fs.read_range_as(a, "/secret/x", 0, 4).await.unwrap_err(),
    ] {
        assert!(
            matches!(e, OrigoFSError::NotFound(_)),
            "a read denial must be indistinguishable from absence, got {e:?}"
        );
    }

    // And a path that genuinely does not exist answers identically.
    let missing = fs.read_as(a, "/secret/never").await.unwrap_err();
    assert_eq!(missing.code(), "not_found");
}

#[tokio::test]
async fn the_write_denial_stays_denied() {
    // Deliberately the opposite of the read case: a writer that can *read* the path
    // already knows it exists, so "denied" is more useful than pretending it
    // vanished.
    let fs = fs().await;
    let a = agent(&fs).await;
    fs.write("/f", b"x").await.unwrap();
    fs.grant(a.actor, "/", Perms::READ).await.unwrap();

    assert_denied(fs.write_as(a, "/f", b"y").await.unwrap_err(), "write_as");
    // …and the same actor can still read it.
    assert_eq!(&fs.read_as(a, "/f").await.unwrap()[..], b"x");
}

#[tokio::test]
async fn a_listing_omits_unreadable_children_rather_than_failing() {
    // An erroring `ls` would itself signal that something unreadable is in there.
    let fs = fs().await;
    let a = agent(&fs).await;
    fs.mkdir_p("/proj/mine").await.unwrap();
    fs.mkdir_p("/proj/theirs").await.unwrap();
    fs.write("/proj/readme", b"x").await.unwrap();

    fs.grant(a.actor, "/", Perms::NONE).await.unwrap();
    fs.grant(a.actor, "/proj", Perms::READ).await.unwrap();
    fs.grant(a.actor, "/proj/theirs", Perms::NONE)
        .await
        .unwrap();

    let mut names: Vec<String> = fs
        .ls_as(a, "/proj")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["mine", "readme"], "theirs must be omitted");

    // The directory itself has to be readable, or its shape leaks.
    assert!(fs.ls_as(a, "/proj/theirs").await.is_err());
}

#[tokio::test]
async fn a_diff_is_filtered_to_the_paths_the_actor_may_read() {
    let fs = fs().await;
    let a = agent(&fs).await;
    fs.mkdir_p("/mine").await.unwrap();
    fs.mkdir_p("/theirs").await.unwrap();
    fs.write("/mine/f", b"one").await.unwrap();
    fs.write("/theirs/f", b"one").await.unwrap();
    fs.commit("alice", "base").await.unwrap();
    fs.write("/mine/f", b"two").await.unwrap();
    fs.write("/theirs/f", b"two").await.unwrap();

    fs.grant(a.actor, "/", Perms::NONE).await.unwrap();
    fs.grant(a.actor, "/mine", Perms::READ).await.unwrap();

    let paths: Vec<String> = fs
        .status_as(a)
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.path)
        .collect();
    assert_eq!(
        paths,
        vec!["/mine/f"],
        "a neighbour's change must not appear"
    );

    // The unattributed status still sees everything — internal machinery is exempt.
    assert_eq!(fs.status().await.unwrap().len(), 2);
}

#[tokio::test]
async fn the_unattributed_reads_stay_ungated() {
    // They are what checkout, merge, gc, recovery and the mounts are built on.
    let fs = fs().await;
    let a = agent(&fs).await;
    fs.write("/f", b"x").await.unwrap();
    fs.grant(a.actor, "/", Perms::NONE).await.unwrap();

    assert_eq!(&fs.read("/f").await.unwrap()[..], b"x");
    assert!(fs.stat("/f").await.is_ok());
    assert!(fs.ls("/").await.is_ok());
    assert!(fs.blame("/f").await.is_ok());
}
