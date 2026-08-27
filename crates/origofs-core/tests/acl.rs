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

// --- attribution completeness (issue #128) -----------------------------------

/// `require_attribution` is off by default, and turning it on makes an
/// unattributed *surface* mutation an error.
///
/// The engine half is small on purpose: the setting only ever refuses, and it is
/// the surfaces that decide when to consult it (a surface calls
/// `ensure_attributed` on the path where no actor was named). Keeping the
/// decision here rather than in each CLI arm is what lets the HTTP API or a future
/// surface honour the same workspace setting without re-deriving it.
#[tokio::test]
async fn require_attribution_is_opt_in_and_refuses_when_on() {
    let (fs, _agent) = fixture().await;

    // Off by default — turning it on for an existing workspace would break every
    // script that does not name an actor, the same reason `acl_default_deny` is
    // opt-in.
    assert!(!fs.require_attribution().await.unwrap());
    fs.ensure_attributed("rm")
        .await
        .expect("while off, an unattributed op is allowed");

    fs.set_require_attribution(true).await.unwrap();
    assert!(fs.require_attribution().await.unwrap());

    let err = fs.ensure_attributed("rm").await.unwrap_err();
    assert!(
        matches!(err, OrigoFSError::Denied(_)),
        "an unattributed op under this setting must be Denied (403 on the HTTP \
         surface), got {err:?}"
    );
    // The message has to name the operation and the fix, because the fix is always
    // the caller's and always the same: name an actor.
    let msg = err.to_string();
    assert!(msg.contains("rm"), "the error must name the op: {msg}");
    assert!(
        msg.contains("actor"),
        "the error must say an actor is what is missing: {msg}"
    );

    // Reversible: a workspace is not locked into the stricter mode.
    fs.set_require_attribution(false).await.unwrap();
    fs.ensure_attributed("rm").await.expect("off again");
}

/// The setting governs *unattributed* ops only — it must not add a second gate on
/// top of the write policy for an actor that named itself.
///
/// Conflating the two would make `require_attribution` a stealth access control,
/// which it explicitly is not: it asks "did anyone say who did this", not "may
/// this actor do it". The write policy answers the second question, and continues
/// to be the only thing that does.
#[tokio::test]
async fn require_attribution_does_not_second_guess_the_write_policy() {
    let (fs, agent) = fixture().await;
    fs.set_require_attribution(true).await.unwrap();

    // An attributed op by a `Direct` actor proceeds untouched.
    let ctx = WriteCtx::actor(agent);
    fs.mkdir_as(ctx, "/d").await.unwrap();
    fs.write_or_propose(ctx, "/d/f.txt", b"x", None)
        .await
        .unwrap();

    // And a propose-only actor is still governed by the policy, not by this
    // setting — it queues, exactly as it would with the setting off.
    fs.set_write_policy(agent, origofs_core::WritePolicy::Propose)
        .await
        .unwrap();
    assert!(
        matches!(
            fs.write_or_propose(ctx, "/d/f.txt", b"y", None).await,
            Ok(origofs_core::WriteOutcome::Proposed(_))
        ),
        "require_attribution must not turn a queued write into a refusal"
    );
}

// --- the operations that have no path ----------------------------------------
//
// `commit`, `checkout`, `create_branch` and an unbounded `revert_session` have no
// single path, so they were left on the path-less `ensure_may_write` — which
// consults the actor's `write_policy` and never looks at a grant. An ACL therefore
// could not contain them at all: under deny-by-default an actor with no grant
// anywhere still reached `checkout`, which truncates and rematerializes the whole
// working tree, and `revert_session`, which deletes another actor's lines
// everywhere they wrote.
//
// Having no path is not the same as touching none. They reach every path, so they
// are checked at the root.

/// Set up a workspace with commits enabled, deny-by-default on, and an actor whose
/// only grant is over one subtree.
async fn subtree_only() -> (Fs<Arc<dyn MetadataStore>, Arc<MemStore>>, i64, WriteCtx) {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);
    // Something to commit, written while the actor still has blanket permission.
    fs.mkdir_as(ctx, "/tenant-a").await.unwrap();
    fs.write_as(ctx, "/tenant-a/f.txt", b"hello\n")
        .await
        .unwrap();
    fs.commit_as(ctx, "seed", "t <t@example.com>")
        .await
        .unwrap();

    fs.set_acl_default_deny(true).await.unwrap();
    fs.grant(agent, "/tenant-a", Perms::WRITE | Perms::READ, None)
        .await
        .unwrap();
    (fs, agent, ctx)
}

fn denied(e: OrigoFSError, what: &str) -> String {
    match e {
        OrigoFSError::Denied(m) => m,
        other => panic!("{what} should have been denied, got {other:?}"),
    }
}

/// A subtree grant does not carry the workspace-wide operations with it.
#[tokio::test]
async fn workspace_wide_ops_need_permission_at_the_root() {
    let (fs, _agent, ctx) = subtree_only().await;

    // The write the grant *does* cover still works, so this is about scope and not
    // about the actor being broken.
    fs.write_or_propose(ctx, "/tenant-a/f.txt", b"edit\n", None)
        .await
        .unwrap();

    let m = denied(
        fs.commit_as(ctx, "m", "t <t@example.com>")
            .await
            .unwrap_err(),
        "commit",
    );
    assert!(m.contains("whole workspace"), "commit: {m}");

    fs.create_branch("side").await.unwrap();
    denied(fs.checkout_as(ctx, "side").await.unwrap_err(), "checkout");
    denied(
        fs.create_branch_as(ctx, "other").await.unwrap_err(),
        "create_branch",
    );
    denied(
        fs.revert_session_as(ctx, 1, 1, None).await.unwrap_err(),
        "unbounded revert_session",
    );
}

/// A revert *bounded to a subtree the actor holds* is allowed — the check follows
/// the prefix, so scoping the blast radius is also what earns the permission.
#[tokio::test]
async fn a_bounded_revert_is_checked_against_its_prefix() {
    let (fs, agent, ctx) = subtree_only().await;

    // In scope: permitted (it finds nothing to revert, which is fine — the point
    // is that it was not refused).
    fs.revert_session_as(ctx, agent, 1, Some("/tenant-a"))
        .await
        .expect("a revert bounded to a granted subtree");

    // Out of scope: refused, and the message names the subtree rather than
    // revealing anything about what is in it.
    let m = denied(
        fs.revert_session_as(ctx, agent, 1, Some("/tenant-b"))
            .await
            .unwrap_err(),
        "a revert bounded to an ungranted subtree",
    );
    assert!(m.contains("/tenant-b"), "{m}");
}

/// **The behaviour-preservation half.** With no grants written, the workspace-wide
/// operations behave exactly as they did before: the actor's `write_policy`
/// decides, because `effective_perms` falls back to it at the root just as it does
/// anywhere else.
#[tokio::test]
async fn workspace_wide_ops_fall_back_to_the_write_policy() {
    let (fs, agent) = fixture().await;
    let ctx = WriteCtx::actor(agent);
    fs.write_as(ctx, "/f.txt", b"x").await.unwrap();
    assert!(fs.list_grants(None).await.unwrap().is_empty());

    // Direct: permitted, as before.
    fs.commit_as(ctx, "one", "t <t@example.com>").await.unwrap();
    fs.create_branch_as(ctx, "side").await.unwrap();
    fs.checkout_as(ctx, "side").await.unwrap();

    // Propose-only: refused, as before — and still says so in those words, which
    // the CLI and the HTTP surface both surface to users.
    fs.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    let m = denied(
        fs.commit_as(ctx, "two", "t <t@example.com>")
            .await
            .unwrap_err(),
        "commit by a propose-only actor",
    );
    assert!(m.contains("propose-only"), "{m}");
}

/// Reviewing is checked at the **suggestion's own path**, not workspace-wide.
///
/// Accepting lands a write at that path, so the grant covering it is the one that
/// decides. The path-less check let an actor with write permission over one
/// subtree approve edits into any other.
#[tokio::test]
async fn reviewing_is_checked_at_the_suggestions_path() {
    let (fs, author) = fixture().await;
    let author_ctx = WriteCtx::actor(author);
    fs.mkdir_as(author_ctx, "/tenant-a").await.unwrap();
    fs.mkdir_as(author_ctx, "/tenant-b").await.unwrap();

    let reviewer = fs.create_human("rev", Some("rev")).await.unwrap();
    let rev_ctx = WriteCtx::actor(reviewer);
    fs.set_acl_default_deny(true).await.unwrap();
    fs.grant(reviewer, "/tenant-a", Perms::WRITE | Perms::READ, None)
        .await
        .unwrap();
    fs.grant(author, "", Perms::PROPOSE | Perms::READ, None)
        .await
        .unwrap();

    let in_scope = match fs
        .write_or_propose(author_ctx, "/tenant-a/x.txt", b"a", None)
        .await
        .unwrap()
    {
        WriteOutcome::Proposed(id) => id,
        other => panic!("expected a proposal, got {other:?}"),
    };
    let out_of_scope = match fs
        .write_or_propose(author_ctx, "/tenant-b/y.txt", b"b", None)
        .await
        .unwrap()
    {
        WriteOutcome::Proposed(id) => id,
        other => panic!("expected a proposal, got {other:?}"),
    };

    fs.accept_suggestion(in_scope, rev_ctx)
        .await
        .expect("accepting inside the granted subtree");
    let m = denied(
        fs.accept_suggestion(out_of_scope, rev_ctx)
            .await
            .unwrap_err(),
        "accepting outside the granted subtree",
    );
    assert!(m.contains("/tenant-b/y.txt"), "{m}");
    denied(
        fs.reject_suggestion(out_of_scope, rev_ctx)
            .await
            .unwrap_err(),
        "rejecting outside the granted subtree",
    );
}
