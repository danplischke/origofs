//! Path-scoped write ACLs (issue #123).
//!
//! Before this the only authorization in the engine was `WritePolicy`: per
//! **actor**, whole workspace, binary, writes only, and taking no path. That is a
//! trust gate, not access control, and `docs/MULTI_TENANCY.md` §7 named the gap.
//!
//! The tests are grouped by the invariants the issue lists, because most of them
//! are things an implementation can get subtly wrong while still passing an
//! obvious "does a grant work" test.
//!
//! # Which entry point is gated, and why it matters here
//!
//! `write_as` is **not** gated and must not become so. It is the attributed but
//! already-authorized primitive that `write_or_propose` calls *after* deciding, and
//! it is also what applies an accepted suggestion — whose author is typically
//! propose-only, so gating it there would refuse the very edit a reviewer just
//! approved. `CLAUDE.md` lists the gated set exactly: `write_or_propose`,
//! `remove_or_propose`, `rename_as`, `mkdir_as`, `symlink_as`, `commit_as`,
//! `accept_suggestion`, `reject_suggestion`.
//!
//! So these tests drive `write_or_propose` for the write path. An earlier draft
//! drove `write_as` and "failed" against correct behaviour, which is worth
//! recording: the distinction is easy to lose and the wrong reading of it would
//! have produced either a bypass or a broken review queue.

use origofs_core::{
    Fs, MemStore, MetadataStore, OrigoFSError, Perms, SqliteMetadataStore, WriteCtx, WriteOutcome,
    WritePolicy,
};
use std::sync::Arc;

async fn fixture() -> (Fs<Arc<dyn MetadataStore>, Arc<MemStore>>, i64) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    (fs, agent)
}

// --- behaviour preservation --------------------------------------------------

/// **The migration invariant.** An actor with no grant behaves exactly as it did
/// before ACLs existed: its `write_policy` decides, workspace-wide.
///
/// This is what makes V20 a pure table create with no backfill. If absence meant
/// deny, every existing workspace would stop working the moment it migrated.
#[tokio::test]
async fn an_actor_with_no_grant_falls_back_to_its_write_policy() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);

    // Direct by default: writes land.
    assert!(fs.list_grants(None).await.unwrap().is_empty());
    fs.write_as(ctx, "/anywhere.txt", b"x").await.unwrap();

    // Propose-only: refused, exactly as before.
    fs.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    // The gated front door queues it rather than writing, exactly as before.
    assert!(matches!(
        fs.write_or_propose(ctx, "/other.txt", b"x", None)
            .await
            .unwrap(),
        WriteOutcome::Proposed(_)
    ));
    assert!(
        fs.stat("/other.txt").await.is_err(),
        "a queued edit must not have touched the working tree"
    );
    // And the gated attributed ops refuse outright.
    assert!(matches!(
        fs.mkdir_as(ctx, "/nope").await,
        Err(OrigoFSError::Denied(_))
    ));
}

// --- the grant itself --------------------------------------------------------

/// A grant narrows an otherwise propose-only actor to write in one subtree —
/// "may write `/docs`, may only propose elsewhere", which the single
/// `write_policy` column could not express at all.
#[tokio::test]
async fn a_grant_makes_write_possible_only_where_it_covers() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);
    fs.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();

    fs.grant(agent, "/docs", Perms::WRITE, None).await.unwrap();

    assert!(matches!(
        fs.write_or_propose(ctx, "/docs/note.md", b"allowed", None)
            .await
            .unwrap(),
        WriteOutcome::Wrote
    ));
    assert_eq!(&fs.read("/docs/note.md").await.unwrap()[..], b"allowed");

    assert!(
        matches!(
            fs.write_or_propose(ctx, "/src/main.rs", b"nope", None)
                .await
                .unwrap(),
            WriteOutcome::Proposed(_)
        ),
        "the grant covers /docs only; a write to /src must be queued, not applied"
    );
    assert!(fs.stat("/src/main.rs").await.is_err());
}

/// **Longest prefix wins**, so a narrow grant overrides a broad one — including
/// overriding it *downwards*, which is how a carve-out is expressed.
#[tokio::test]
async fn the_longest_matching_prefix_decides() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);

    // Broad allow, narrow deny.
    fs.grant(agent, "/", Perms::WRITE, None).await.unwrap();
    fs.grant(agent, "/secrets", Perms::NONE, None)
        .await
        .unwrap();

    fs.mkdir_as(ctx, "/ordinary").await.unwrap();
    assert!(
        matches!(
            fs.write_or_propose(ctx, "/secrets/key.txt", b"x", None)
                .await,
            Err(OrigoFSError::Denied(_))
        ),
        "a narrower grant must override a broader one, including to deny"
    );

    // And deeper still can re-allow.
    fs.grant(agent, "/secrets/public", Perms::WRITE, None)
        .await
        .unwrap();
    assert!(matches!(
        fs.write_or_propose(ctx, "/secrets/public/ok.txt", b"x", None)
            .await
            .unwrap(),
        WriteOutcome::Wrote
    ));
}

/// Matching is on **directory boundaries**, so `/tenant-a` never covers
/// `/tenant-abc`. The same rule as the surface scope, and the third copy of it in
/// the tree folded into one implementation.
#[tokio::test]
async fn a_grant_does_not_cover_a_lookalike_sibling() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);
    fs.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    fs.grant(agent, "/tenant-a", Perms::WRITE, None)
        .await
        .unwrap();

    assert!(matches!(
        fs.write_or_propose(ctx, "/tenant-a/f.txt", b"mine", None)
            .await
            .unwrap(),
        WriteOutcome::Wrote
    ));
    assert!(
        matches!(
            fs.write_or_propose(ctx, "/tenant-abc/f.txt", b"theirs", None)
                .await
                .unwrap(),
            WriteOutcome::Proposed(_)
        ),
        "`/tenant-a` must not cover `/tenant-abc` — the exact neighbour a prefix \
         grant exists to exclude"
    );
    assert!(fs.stat("/tenant-abc/f.txt").await.is_err());
    // The grant covers the prefix path itself, not only what is under it.
    assert!(
        fs.effective_perms(agent, "/tenant-a")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
}

/// A relative prefix is refused rather than read as absolute. A grant that
/// silently applies to a subtree the operator did not mean fails **open**, which
/// is the direction that matters for an authorization rule.
#[tokio::test]
async fn a_relative_grant_prefix_is_refused() {
    let (fs, agent) = fixture().await;
    assert!(fs.grant(agent, "docs", Perms::WRITE, None).await.is_err());
    assert!(
        fs.grant(agent, "/a/../b", Perms::WRITE, None)
            .await
            .is_err()
    );
}

// --- the operations ----------------------------------------------------------

/// **A rename is two checks.** Checking only the source lets an actor move a file
/// it controls into a tree it does not — a write to that tree by any meaningful
/// definition, performed without permission on it.
#[tokio::test]
async fn a_rename_checks_both_endpoints() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);
    fs.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    fs.grant(agent, "/mine", Perms::WRITE, None).await.unwrap();
    fs.mkdir_p("/mine").await.unwrap();
    fs.mkdir_p("/theirs").await.unwrap();
    fs.write("/mine/f.txt", b"data").await.unwrap();

    // Out of the granted tree: refused on the destination.
    assert!(
        matches!(
            fs.rename_as(ctx, "/mine/f.txt", "/theirs/f.txt").await,
            Err(OrigoFSError::Denied(_))
        ),
        "a rename out of the granted subtree must be refused on the destination"
    );
    assert!(
        fs.stat("/mine/f.txt").await.is_ok(),
        "the refused rename must not have moved anything"
    );

    // Within it: allowed.
    fs.rename_as(ctx, "/mine/f.txt", "/mine/g.txt")
        .await
        .unwrap();
    assert!(fs.stat("/mine/g.txt").await.is_ok());
}

/// Every attributed mutation is covered, not just `write`. A gate that stops
/// writes but lets deletes through is not a gate — that is exactly how
/// `origofs_rm` shipped ungated (#78).
#[tokio::test]
async fn every_attributed_mutation_is_gated() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);
    // Seeded through the *unattributed* primitive, which is exempt by design.
    fs.write("/f.txt", b"x").await.unwrap();
    fs.write("/target.txt", b"t").await.unwrap();

    // Deny everything from here on.
    fs.grant(agent, "/", Perms::NONE, None).await.unwrap();

    assert!(matches!(
        fs.write_or_propose(ctx, "/f.txt", b"y", None).await,
        Err(OrigoFSError::Denied(_))
    ));
    assert!(matches!(
        fs.remove_as(ctx, "/f.txt").await,
        Err(OrigoFSError::Denied(_))
    ));
    assert!(matches!(
        fs.mkdir_as(ctx, "/d").await,
        Err(OrigoFSError::Denied(_))
    ));
    assert!(matches!(
        fs.symlink_as(ctx, "/target.txt", "/link").await,
        Err(OrigoFSError::Denied(_))
    ));
    assert!(matches!(
        fs.rename_as(ctx, "/f.txt", "/g.txt").await,
        Err(OrigoFSError::Denied(_))
    ));

    // Nothing landed.
    assert_eq!(&fs.read("/f.txt").await.unwrap()[..], b"x");
    assert!(fs.stat("/d").await.is_err());
    assert!(fs.stat("/link").await.is_err());
}

/// `remove_or_propose` uses the same path-scoped decision as `write_or_propose`.
/// A deletion is the same destruction one call further along, so the two must not
/// be able to disagree about what an actor may do at a path.
#[tokio::test]
async fn removal_and_writing_agree_about_a_path() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);
    fs.mkdir_p("/docs").await.unwrap();
    fs.mkdir_p("/src").await.unwrap();
    fs.write("/docs/f.txt", b"x").await.unwrap();
    fs.write("/src/f.txt", b"x").await.unwrap();

    fs.grant(agent, "/", Perms::PROPOSE, None).await.unwrap();
    fs.grant(agent, "/docs", Perms::WRITE, None).await.unwrap();

    // Where it may write, it may delete.
    assert!(matches!(
        fs.remove_or_propose(ctx, "/docs/f.txt", None)
            .await
            .unwrap(),
        WriteOutcome::Wrote
    ));
    // Where it may only propose, a delete is queued rather than performed.
    assert!(matches!(
        fs.remove_or_propose(ctx, "/src/f.txt", None).await.unwrap(),
        WriteOutcome::Proposed(_)
    ));
    assert!(
        fs.stat("/src/f.txt").await.is_ok(),
        "a proposed deletion must not have removed anything yet"
    );
}

/// A grant of neither permission is **refused**, not silently queued. Queueing
/// would tell the actor its edit is under review when nothing will ever review it.
#[tokio::test]
async fn a_grant_of_no_permissions_refuses_rather_than_queueing() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);
    fs.grant(agent, "/", Perms::NONE, None).await.unwrap();

    assert!(matches!(
        fs.write_or_propose(ctx, "/f.txt", b"x", None).await,
        Err(OrigoFSError::Denied(_))
    ));
    assert!(matches!(
        fs.remove_or_propose(ctx, "/f.txt", None).await,
        Err(OrigoFSError::Denied(_))
    ));
}

// --- the exemptions ----------------------------------------------------------

/// **The unattributed ops stay exempt.** They are checkout, merge
/// materialization, and applying an accepted suggestion — they have no actor to
/// judge, exactly as with the write policy. Pinned so the carve-out is a decision
/// rather than an oversight.
#[tokio::test]
async fn unattributed_ops_are_exempt() {
    let (fs, agent) = fixture().await;
    fs.grant(agent, "/", Perms::NONE, None).await.unwrap();

    // The raw primitives take no actor and are unaffected.
    fs.write("/f.txt", b"x").await.unwrap();
    fs.mkdir_p("/d").await.unwrap();
    fs.rename("/f.txt", "/g.txt").await.unwrap();
    fs.remove("/g.txt").await.unwrap();
}

// --- default deny ------------------------------------------------------------

/// Deny-by-default is available but **not** the default, because turning it on for
/// an existing workspace stops every actor that has no explicit grant — which is
/// all of them until an operator writes some.
#[tokio::test]
async fn deny_by_default_is_opt_in() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);
    assert!(!fs.acl_default_deny().await.unwrap());
    fs.mkdir_as(ctx, "/before").await.unwrap();

    fs.set_acl_default_deny(true).await.unwrap();
    assert!(
        matches!(
            fs.write_or_propose(ctx, "/g.txt", b"x", None).await,
            Err(OrigoFSError::Denied(_))
        ),
        "with deny-by-default on, an ungranted actor must be refused outright — \
         not even queued, since nothing grants it propose either"
    );

    // An explicit grant re-opens exactly what it covers.
    fs.grant(agent, "/allowed", Perms::WRITE, None)
        .await
        .unwrap();
    assert!(matches!(
        fs.write_or_propose(ctx, "/allowed/f.txt", b"x", None)
            .await
            .unwrap(),
        WriteOutcome::Wrote
    ));
    assert!(
        fs.write_or_propose(ctx, "/elsewhere.txt", b"x", None)
            .await
            .is_err()
    );
}

// --- grant management --------------------------------------------------------

/// Grants can be listed, replaced, and revoked, and every change is auditable.
#[tokio::test]
async fn grants_are_manageable_and_audited() {
    let (fs, agent) = fixture().await;
    let admin = fs.create_human("admin", None).await.unwrap();

    fs.grant(agent, "/docs", Perms::WRITE, Some(admin))
        .await
        .unwrap();
    let grants = fs.list_grants(Some(agent)).await.unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].path_prefix, "/docs");
    assert_eq!(grants[0].granted_by, Some(admin));

    // Re-granting the same prefix replaces rather than duplicating.
    fs.grant(agent, "/docs", Perms::PROPOSE, Some(admin))
        .await
        .unwrap();
    let grants = fs.list_grants(Some(agent)).await.unwrap();
    assert_eq!(grants.len(), 1, "a re-grant must replace, not duplicate");
    assert_eq!(grants[0].perms, Perms::PROPOSE);

    assert!(fs.revoke(agent, "/docs", Some(admin)).await.unwrap());
    assert!(
        !fs.revoke(agent, "/docs", Some(admin)).await.unwrap(),
        "revoking twice must report that there was nothing to revoke"
    );
    assert!(fs.list_grants(Some(agent)).await.unwrap().is_empty());

    // Every change is in the change feed — the auditability invariant.
    let events = fs.events_since(0, 100).await.unwrap();
    let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
    assert!(
        kinds.contains(&"acl_grant") && kinds.contains(&"acl_revoke"),
        "grant changes must be auditable; saw {kinds:?}"
    );
}

/// Grants are per-actor: one actor's grant does not give another anything.
#[tokio::test]
async fn grants_do_not_leak_between_actors() {
    let (fs, alice) = fixture().await;
    let bob = fs.create_agent("bob", "m", None).await.unwrap();
    fs.set_write_policy(bob, WritePolicy::Propose)
        .await
        .unwrap();

    fs.grant(alice, "/shared", Perms::WRITE, None)
        .await
        .unwrap();

    assert!(
        matches!(
            fs.write_or_propose(WriteCtx::actor(bob), "/shared/f.txt", b"x", None)
                .await
                .unwrap(),
            WriteOutcome::Proposed(_)
        ),
        "one actor's grant must not authorize another"
    );
    assert!(fs.stat("/shared/f.txt").await.is_err());
    assert!(fs.list_grants(Some(bob)).await.unwrap().is_empty());
}

/// `Perms` renders readably, because it appears in every denial message and an
/// operator reading "effective permissions: 5" learns nothing.
#[test]
fn perms_render_readably() {
    assert_eq!(Perms::NONE.to_string(), "none");
    assert_eq!(Perms::WRITE.to_string(), "write");
    assert_eq!((Perms::READ | Perms::PROPOSE).to_string(), "read+propose");
    assert_eq!(
        Perms::from_policy(WritePolicy::Direct).to_string(),
        "read+write+propose"
    );
    assert_eq!(
        Perms::from_policy(WritePolicy::Propose).to_string(),
        "read+propose"
    );
}
