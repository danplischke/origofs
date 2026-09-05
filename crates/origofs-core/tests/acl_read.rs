//! Read enforcement (issue #124, phase 1).
//!
//! `Perms::READ` has existed since #123 as a bit nothing consulted: grantable,
//! reported back in `effective permissions`, and decoration. Under
//! `acl_default_deny` an actor with no grant at all was correctly refused every
//! write and could still `read`, `blame` and `ls` anything it could name.
//!
//! These pin the check itself. Two properties matter more than "a denial denies":
//!
//! * **It is off by default.** Reads have never been checked, so an existing
//!   workspace has no read grants; enforcing on upgrade would stop every actor at
//!   once. Every test here that expects a refusal turns it on explicitly.
//! * **A denial says nothing about existence.** Probing for existence is the whole
//!   point of an unauthorized read, so a check that ran after the lookup would
//!   answer the question it exists to refuse.

use origofs_core::{
    Fs, MemStore, MetadataStore, OrigoFSError, Perms, SqliteMetadataStore, WriteCtx,
};
use std::sync::Arc;

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

/// An owner who may write, and `bob`, whose rights each test sets.
async fn fixture() -> (TestFs, WriteCtx, i64) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    let owner = fs.create_agent("owner", "opus", None).await.unwrap();
    let octx = WriteCtx::actor(owner);
    fs.grant(owner, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    let bob = fs.create_agent("bob", "opus", None).await.unwrap();
    fs.write_as(octx, "/doc.md", b"secret\n").await.unwrap();
    (fs, octx, bob)
}

fn denied(e: &OrigoFSError) -> bool {
    matches!(e, OrigoFSError::Denied(_))
}

// --- off by default ----------------------------------------------------------

#[tokio::test]
async fn reads_are_open_until_a_workspace_opts_in() {
    // The migration invariant. Nothing about adding the check changes an existing
    // workspace, which is why the switch exists at all.
    let (fs, _owner, bob) = fixture().await;
    fs.set_acl_default_deny(true).await.unwrap(); // bob has no grant anywhere
    let ctx = WriteCtx::actor(bob);

    assert!(!fs.acl_enforce_reads().await.unwrap());
    assert_eq!(
        fs.read_as(ctx, "/doc.md").await.unwrap().as_ref(),
        b"secret\n"
    );
    assert!(fs.stat_as(ctx, "/doc.md").await.is_ok());
    assert!(fs.ls_as(ctx, "/").await.is_ok());
    assert!(fs.blame_as(ctx, "/doc.md").await.is_ok());

    // ...and writing is still refused, so this is enforcement being off rather
    // than the fixture granting something by accident.
    assert!(
        fs.write_or_propose(ctx, "/doc.md", b"x", None, None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn the_switch_takes_effect_without_reopening() {
    let (fs, _owner, bob) = fixture().await;
    fs.set_acl_default_deny(true).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    assert!(fs.read_as(ctx, "/doc.md").await.is_ok());
    fs.set_acl_enforce_reads(true).await.unwrap();
    assert!(denied(&fs.read_as(ctx, "/doc.md").await.unwrap_err()));
    fs.set_acl_enforce_reads(false).await.unwrap();
    assert!(fs.read_as(ctx, "/doc.md").await.is_ok());
}

// --- the check itself --------------------------------------------------------

#[tokio::test]
async fn every_attributed_read_is_gated() {
    // A check on `read` alone leaves blame and ls as side doors — which is exactly
    // what #124 said made the bit decoration.
    let (fs, owner, bob) = fixture().await;
    fs.symlink_as(owner, "/doc.md", "/link").await.unwrap();
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    assert!(
        denied(&fs.read_as(ctx, "/doc.md").await.unwrap_err()),
        "read"
    );
    assert!(
        denied(&fs.read_range_as(ctx, "/doc.md", 0, 3).await.unwrap_err()),
        "read_range"
    );
    assert!(
        denied(&fs.stat_as(ctx, "/doc.md").await.unwrap_err()),
        "stat"
    );
    assert!(denied(&fs.ls_as(ctx, "/").await.unwrap_err()), "ls");
    assert!(
        denied(&fs.readlink_as(ctx, "/link").await.unwrap_err()),
        "readlink"
    );
    assert!(
        denied(&fs.blame_as(ctx, "/doc.md").await.unwrap_err()),
        "blame"
    );
    // Nothing is committed here, so the walk itself would find no history. The
    // refusal has to come first anyway, or it tells an actor that cannot read the
    // path whether it has ever been committed.
    assert!(
        denied(&fs.log_path_as(ctx, "/doc.md", None).await.unwrap_err()),
        "log_path"
    );
    assert!(
        denied(&fs.edit_ops_at_as(ctx, "/doc.md", None).await.unwrap_err()),
        "edit_ops_at"
    );
}

#[tokio::test]
async fn a_read_grant_admits_every_attributed_read() {
    let (fs, owner, bob) = fixture().await;
    fs.symlink_as(owner, "/doc.md", "/link").await.unwrap();
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    fs.grant(bob, "/", Perms::READ, None).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    assert_eq!(
        fs.read_as(ctx, "/doc.md").await.unwrap().as_ref(),
        b"secret\n"
    );
    assert_eq!(
        fs.read_range_as(ctx, "/doc.md", 0, 3)
            .await
            .unwrap()
            .as_ref(),
        b"sec"
    );
    assert!(fs.stat_as(ctx, "/doc.md").await.is_ok());
    assert!(!fs.ls_as(ctx, "/").await.unwrap().is_empty());
    assert_eq!(fs.readlink_as(ctx, "/link").await.unwrap(), "/doc.md");
    assert!(!fs.blame_as(ctx, "/doc.md").await.unwrap().is_empty());
    fs.commit_as(owner, "owner", "seed").await.unwrap();
    assert!(
        !fs.log_path_as(ctx, "/doc.md", None)
            .await
            .unwrap()
            .is_empty()
    );

    // READ alone is not WRITE.
    assert!(
        fs.write_or_propose(ctx, "/doc.md", b"x", None, None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn read_is_scoped_by_prefix_like_write_is() {
    let (fs, owner, bob) = fixture().await;
    fs.mkdir_as(owner, "/pub").await.unwrap();
    fs.mkdir_as(owner, "/priv").await.unwrap();
    fs.write_as(owner, "/pub/a.md", b"public\n").await.unwrap();
    fs.write_as(owner, "/priv/b.md", b"private\n")
        .await
        .unwrap();
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    fs.grant(bob, "/pub", Perms::READ, None).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    assert_eq!(
        fs.read_as(ctx, "/pub/a.md").await.unwrap().as_ref(),
        b"public\n"
    );
    assert!(denied(&fs.read_as(ctx, "/priv/b.md").await.unwrap_err()));
}

#[tokio::test]
async fn a_deeper_grant_overrides_a_broader_one_for_reads_too() {
    let (fs, owner, bob) = fixture().await;
    fs.mkdir_as(owner, "/all").await.unwrap();
    fs.mkdir_as(owner, "/all/locked").await.unwrap();
    fs.write_as(owner, "/all/open.md", b"open\n").await.unwrap();
    fs.write_as(owner, "/all/locked/shut.md", b"shut\n")
        .await
        .unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    fs.grant(bob, "/", Perms::READ, None).await.unwrap();
    fs.grant(bob, "/all/locked", Perms::NONE, None)
        .await
        .unwrap();
    let ctx = WriteCtx::actor(bob);

    assert!(fs.read_as(ctx, "/all/open.md").await.is_ok());
    assert!(denied(
        &fs.read_as(ctx, "/all/locked/shut.md").await.unwrap_err()
    ));
}

/// `WRITE` without `READ` is a grant an operator can write, and the bits are
/// independent, so it means what it says. Pinned because it is surprising, and a
/// surprise that is tested is a decision rather than an accident.
#[tokio::test]
async fn a_write_only_grant_does_not_imply_read() {
    let (fs, _owner, bob) = fixture().await;
    fs.set_acl_enforce_reads(true).await.unwrap();
    fs.grant(bob, "/", Perms::WRITE, None).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    assert!(
        fs.write_or_propose(ctx, "/doc.md", b"x", None, None)
            .await
            .is_ok()
    );
    assert!(denied(&fs.read_as(ctx, "/doc.md").await.unwrap_err()));
}

/// The fallback path: an actor with no grant, enforcement on, default-deny *off*.
/// `Perms::from_policy` includes READ for both policies, so the pre-ACL world
/// keeps reading — which is what makes enabling enforcement survivable for a
/// workspace that never wrote grants.
#[tokio::test]
async fn without_default_deny_an_ungranted_actor_still_reads() {
    let (fs, _owner, bob) = fixture().await;
    fs.set_acl_enforce_reads(true).await.unwrap();
    let ctx = WriteCtx::actor(bob);
    assert!(fs.read_as(ctx, "/doc.md").await.is_ok());
}

// --- a denial must not leak existence ----------------------------------------

#[tokio::test]
async fn a_denial_is_identical_for_a_path_that_is_not_there() {
    // The property the whole check is for. If refusing an existing file differed
    // in any way from refusing a missing one, the difference is an oracle and an
    // unauthorized reader can map the tree it cannot read.
    let (fs, _owner, bob) = fixture().await;
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    let real = fs.read_as(ctx, "/doc.md").await.unwrap_err();
    let ghost = fs.read_as(ctx, "/no-such-file.md").await.unwrap_err();
    assert!(denied(&real) && denied(&ghost), "{real:?} / {ghost:?}");
    assert_eq!(
        real.to_string().replace("/doc.md", "<path>"),
        ghost.to_string().replace("/no-such-file.md", "<path>"),
        "the refusal differs between an existing and a missing path"
    );

    // Same for stat, the cheapest existence probe there is.
    let real = fs.stat_as(ctx, "/doc.md").await.unwrap_err();
    let ghost = fs.stat_as(ctx, "/nope.md").await.unwrap_err();
    assert_eq!(
        real.to_string().replace("/doc.md", "<path>"),
        ghost.to_string().replace("/nope.md", "<path>")
    );
}

#[tokio::test]
async fn a_denied_read_of_a_missing_path_is_denied_not_not_found() {
    // Ordering: the check runs before the lookup. A `NotFound` here would confirm
    // absence to an actor with no right to ask.
    let (fs, _owner, bob) = fixture().await;
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    let e = fs
        .read_as(ctx, "/definitely/not/here.md")
        .await
        .unwrap_err();
    assert!(denied(&e), "leaked absence: {e:?}");
}

// --- the carve-out, stated as a test -----------------------------------------

/// The unattributed reads stay open, exactly as `remove`/`rename`/`mkdir_p` do on
/// the write side. They are what checkout, merge, gc and the CRDT coordinator are
/// built from; gating them would break the machinery rather than protect it.
#[tokio::test]
async fn the_unattributed_reads_are_not_gated() {
    let (fs, _owner, _bob) = fixture().await;
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();

    assert_eq!(fs.read("/doc.md").await.unwrap().as_ref(), b"secret\n");
    assert!(fs.stat("/doc.md").await.is_ok());
    assert!(fs.ls("/").await.is_ok());
    assert!(fs.blame("/doc.md").await.is_ok());
}

// --- interaction with phase 0 ------------------------------------------------

#[tokio::test]
async fn granting_read_takes_effect_immediately_under_the_cache() {
    let (fs, _owner, bob) = fixture().await;
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    assert!(denied(&fs.read_as(ctx, "/doc.md").await.unwrap_err()));
    fs.grant(bob, "/", Perms::READ, None).await.unwrap();
    assert!(
        fs.read_as(ctx, "/doc.md").await.is_ok(),
        "cached denial outlived the grant"
    );
    fs.revoke(bob, "/", None).await.unwrap();
    assert!(
        denied(&fs.read_as(ctx, "/doc.md").await.unwrap_err()),
        "cached grant outlived the revoke"
    );
}

#[tokio::test]
async fn the_enforcement_switch_is_seen_across_handles() {
    // Same store, two handles: turning enforcement on must not wait for the other
    // handle to notice.
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let content = Arc::new(MemStore::new());
    let a: TestFs = Fs::new(meta.clone(), content.clone());
    a.init().await.unwrap();
    let owner = a.create_agent("owner", "opus", None).await.unwrap();
    a.grant(owner, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    a.write_as(WriteCtx::actor(owner), "/doc.md", b"secret\n")
        .await
        .unwrap();
    let bob = a.create_agent("bob", "opus", None).await.unwrap();
    a.set_acl_default_deny(true).await.unwrap();

    let b: TestFs = Fs::new(meta, content);
    let ctx = WriteCtx::actor(bob);
    assert!(
        b.read_as(ctx, "/doc.md").await.is_ok(),
        "premise: enforcement off"
    );

    a.set_acl_enforce_reads(true).await.unwrap();
    assert!(
        denied(&b.read_as(ctx, "/doc.md").await.unwrap_err()),
        "handle B kept reading after handle A turned enforcement on"
    );
}

// --- ls and stat agree, entry by entry ---------------------------------------
//
// `ls_as` used to check only the directory, so a readable directory handed back
// every name under it and `stat_as` then refused the ones the actor could not
// read. That disagreement was in the safe direction — a listing that promises
// more than a stat delivers is useless, not dangerous — but the pair still has
// to be built as a pair, because the *other* direction (a listing that hides a
// name `stat_as` serves) is the existence oracle the module refuses to become.

/// `/pub` readable, `/dir/secret.md` not: the entry is absent, not refused.
async fn split_dir() -> (TestFs, i64, WriteCtx) {
    let (fs, octx, bob) = fixture().await;
    fs.mkdir_as(octx, "/dir").await.unwrap();
    fs.write_as(octx, "/dir/open.md", b"anyone\n")
        .await
        .unwrap();
    fs.write_as(octx, "/dir/secret.md", b"nobody\n")
        .await
        .unwrap();
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    fs.grant(bob, "/dir", Perms::READ, None).await.unwrap();
    (fs, bob, WriteCtx::actor(bob))
}

#[tokio::test]
async fn a_listing_hides_an_entry_the_actor_may_not_read() {
    let (fs, bob, ctx) = split_dir().await;
    // A deeper deny: bob reads /dir, but not this one name under it.
    fs.grant(bob, "/dir/secret.md", Perms::NONE, None)
        .await
        .unwrap();

    let names: Vec<String> = fs
        .ls_as(ctx, "/dir")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, vec!["open.md".to_string()]);
}

#[tokio::test]
async fn every_listed_entry_is_one_stat_would_serve() {
    // The agreement itself, asserted as the property rather than as a case: for
    // every name in the directory, presence in the listing and success of
    // `stat_as` are the same bit. Neither side can drift without failing here.
    let (fs, bob, ctx) = split_dir().await;
    fs.grant(bob, "/dir/secret.md", Perms::NONE, None)
        .await
        .unwrap();

    let all: Vec<String> = fs
        .ls("/dir")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    let listed: Vec<String> = fs
        .ls_as(ctx, "/dir")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(all.len(), 2, "the directory really does hold both");

    for name in all {
        let statted = fs.stat_as(ctx, &format!("/dir/{name}")).await.is_ok();
        assert_eq!(
            listed.contains(&name),
            statted,
            "{name}: listed={} but stat_as ok={statted}",
            listed.contains(&name)
        );
    }
}

#[tokio::test]
async fn filtering_leaves_a_fully_readable_directory_untouched() {
    let (fs, _bob, ctx) = split_dir().await;
    let mut names: Vec<String> = fs
        .ls_as(ctx, "/dir")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["open.md".to_string(), "secret.md".to_string()]);
}

#[tokio::test]
async fn an_unreadable_directory_is_still_refused_not_emptied() {
    // The directory check and the per-entry filter answer different questions,
    // and the first must stay a refusal: an empty listing would say "this
    // directory is here and holds nothing", which is more than a denied actor
    // may learn.
    let (fs, _bob, ctx) = split_dir().await;
    let e = fs.ls_as(ctx, "/").await.unwrap_err(); // bob's grant is at /dir only
    assert!(denied(&e), "expected Denied, got {e:?}");
}

#[tokio::test]
async fn filtering_does_not_run_while_enforcement_is_off() {
    // The cost argument: with the switch off nothing is filtered, so a workspace
    // that never opts in pays one cached config read and nothing per entry.
    let (fs, bob, _ctx) = split_dir().await;
    fs.grant(bob, "/dir/secret.md", Perms::NONE, None)
        .await
        .unwrap();
    fs.set_acl_enforce_reads(false).await.unwrap();

    assert_eq!(
        fs.ls_as(WriteCtx::actor(bob), "/dir").await.unwrap().len(),
        2
    );
}

#[tokio::test]
async fn the_root_directory_joins_its_children_without_doubling_a_slash() {
    // `join_child` concatenates, and `/` is the one path that already ends in a
    // separator. `//doc.md` is not a `Scope::contains` match for a grant at
    // `/doc.md`, so a doubled slash would resolve a root-level entry against the
    // grant at `/` instead of its own — silently ignoring every deny written
    // directly against a file in the root.
    let (fs, _octx, bob) = fixture().await;
    fs.set_acl_default_deny(true).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    fs.grant(bob, "/", Perms::READ, None).await.unwrap();
    fs.grant(bob, "/doc.md", Perms::NONE, None).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    let names: Vec<String> = fs
        .ls_as(ctx, "/")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert!(
        !names.contains(&"doc.md".to_string()),
        "the deny at /doc.md was not applied to the root listing: {names:?}"
    );
    // And the pair still agrees: what the listing hides, the stat refuses.
    assert!(denied(&fs.stat_as(ctx, "/doc.md").await.unwrap_err()));
}
