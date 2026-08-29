//! Who may change an ACL (issue #124, follow-on).
//!
//! `grant`/`revoke` take no authorization at all: `granted_by` is an audit field
//! the caller fills in, not a claim anything verifies. An actor reaching that
//! primitive can hand itself `WRITE` at `/`. Today no network surface exposes it —
//! there is no ACL route on the HTTP API, no MCP tool, no CLI subcommand — but
//! safety by absence of a route is not safety, and the first admin endpoint anyone
//! writes would inherit an unguarded primitive.
//!
//! `grant_as`/`revoke_as` are the checked entry points a surface must use. Two
//! conditions, and the tests below exist mostly to pin the second: **you may
//! delegate only where you can write**, and **you may not hand on a permission you
//! do not hold**. The first alone is not enough — it would let a write-only actor
//! mint itself `READ`.

use origofs_core::{
    Fs, MemStore, MetadataStore, OrigoFSError, Perms, SqliteMetadataStore, WriteCtx, WritePolicy,
};
use std::sync::Arc;

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

/// A workspace under deny-by-default, with alice administering `/proj`.
async fn fixture() -> (TestFs, i64, i64) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    let alice = fs.create_human("alice", None).await.unwrap();
    let agent = fs
        .create_agent("claude", "opus", Some(alice))
        .await
        .unwrap();
    fs.set_acl_default_deny(true).await.unwrap();
    // Provisioning writes the first grant with no actor, which is why the
    // unattributed `grant` has to stay.
    fs.grant(alice, "/proj", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    (fs, alice, agent)
}

fn denied(e: &OrigoFSError) -> bool {
    matches!(e, OrigoFSError::Denied(_))
}

// --- the escalation this exists to stop -------------------------------------

#[tokio::test]
async fn an_actor_cannot_grant_itself_rights_it_does_not_hold() {
    let (fs, _alice, agent) = fixture().await;
    fs.grant(agent, "/proj", Perms::READ | Perms::PROPOSE, None)
        .await
        .unwrap();
    let ctx = WriteCtx::actor(agent);

    // Propose-only here, so it cannot administer here at all.
    let e = fs
        .grant_as(ctx, agent, "/proj", Perms::READ | Perms::WRITE)
        .await
        .unwrap_err();
    assert!(denied(&e), "{e:?}");
    assert_eq!(
        fs.effective_perms(agent, "/proj/f").await.unwrap(),
        Perms::READ | Perms::PROPOSE,
        "the refused grant changed the ACL anyway"
    );
}

#[tokio::test]
async fn an_actor_cannot_grant_itself_the_whole_workspace() {
    // The worst case: escaping the subtree entirely.
    let (fs, _alice, agent) = fixture().await;
    fs.grant(agent, "/proj", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    let ctx = WriteCtx::actor(agent);

    let e = fs
        .grant_as(ctx, agent, "/", Perms::READ | Perms::WRITE)
        .await
        .unwrap_err();
    assert!(denied(&e), "{e:?}");
    assert_eq!(
        fs.effective_perms(agent, "/elsewhere/f").await.unwrap(),
        Perms::NONE
    );
}

/// The reason condition 1 is not enough on its own.
///
/// `WRITE` does not imply `READ` (pinned in `acl_read.rs`), so a write-only grant
/// is a real configuration. Gating delegation on `WRITE` alone would let its holder
/// grant itself `READ` and make that configuration meaningless.
#[tokio::test]
async fn a_write_only_actor_cannot_mint_itself_read() {
    let (fs, _alice, agent) = fixture().await;
    fs.grant(agent, "/proj", Perms::WRITE, None).await.unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();
    let ctx = WriteCtx::actor(agent);

    let e = fs
        .grant_as(ctx, agent, "/proj", Perms::READ | Perms::WRITE)
        .await
        .unwrap_err();
    assert!(denied(&e), "{e:?}");
    assert!(e.to_string().contains("does not have"), "{e}");
    assert!(
        !fs.effective_perms(agent, "/proj/f")
            .await
            .unwrap()
            .contains(Perms::READ)
    );
}

#[tokio::test]
async fn a_reader_cannot_share_what_it_can_read() {
    // Delegation is administrative: being able to read a subtree does not make you
    // the one who decides who else reads it.
    let (fs, _alice, agent) = fixture().await;
    let bob = fs.create_human("bob", None).await.unwrap();
    fs.grant(agent, "/proj", Perms::READ, None).await.unwrap();

    let e = fs
        .grant_as(WriteCtx::actor(agent), bob, "/proj", Perms::READ)
        .await
        .unwrap_err();
    assert!(denied(&e), "{e:?}");
}

// --- what delegation is *for* ------------------------------------------------

#[tokio::test]
async fn alice_can_narrow_her_agent_within_her_subtree() {
    // The pattern this whole feature serves: a propose-only agent, scoped by grant
    // rather than by the actor's write policy.
    let (fs, alice, agent) = fixture().await;
    let actx = WriteCtx::actor(alice);

    fs.grant_as(actx, agent, "/proj", Perms::READ | Perms::PROPOSE)
        .await
        .unwrap();
    assert_eq!(
        fs.effective_perms(agent, "/proj/f").await.unwrap(),
        Perms::READ | Perms::PROPOSE
    );

    // WRITE implies PROPOSE for delegation, which is what makes the line above
    // legal: alice holds READ|WRITE, not an explicit PROPOSE bit.
    assert!(
        !fs.effective_perms(alice, "/proj/f")
            .await
            .unwrap()
            .contains(Perms::PROPOSE)
    );

    // The agent proposes rather than writing, and cannot escalate.
    let ctx = WriteCtx::actor(agent);
    assert!(matches!(
        fs.write_or_propose(ctx, "/proj/f.md", b"x", None)
            .await
            .unwrap(),
        origofs_core::WriteOutcome::Proposed(_)
    ));
    assert!(
        fs.grant_as(ctx, agent, "/proj", Perms::WRITE)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn the_granter_is_recorded_from_the_context_not_the_caller() {
    // `granted_by` is no longer something a caller names — it is who the engine
    // authorized, which is the difference between an audit trail and a claim.
    let (fs, alice, agent) = fixture().await;
    fs.grant_as(WriteCtx::actor(alice), agent, "/proj", Perms::READ)
        .await
        .unwrap();

    let g = fs
        .list_grants(Some(agent))
        .await
        .unwrap()
        .into_iter()
        .find(|g| g.path_prefix == "/proj")
        .expect("grant written");
    assert_eq!(g.granted_by, Some(alice));
}

#[tokio::test]
async fn revoking_needs_write_where_the_grant_sits() {
    let (fs, alice, agent) = fixture().await;
    fs.grant(agent, "/proj", Perms::READ, None).await.unwrap();

    // The agent may not withdraw its own restriction's neighbour...
    let e = fs
        .revoke_as(WriteCtx::actor(agent), agent, "/proj")
        .await
        .unwrap_err();
    assert!(denied(&e), "{e:?}");
    // ...alice, who administers /proj, may.
    assert!(
        fs.revoke_as(WriteCtx::actor(alice), agent, "/proj")
            .await
            .unwrap()
    );
    assert_eq!(
        fs.effective_perms(agent, "/proj/f").await.unwrap(),
        Perms::NONE
    );
}

#[tokio::test]
async fn a_prefix_is_normalized_before_it_is_checked() {
    // The check and the stored row must agree on the prefix, or a trailing slash
    // would be checked at one path and written at another.
    let (fs, alice, agent) = fixture().await;
    fs.grant_as(WriteCtx::actor(alice), agent, "/proj/", Perms::READ)
        .await
        .unwrap();
    assert!(
        fs.effective_perms(agent, "/proj/f")
            .await
            .unwrap()
            .contains(Perms::READ)
    );
}

// --- the workspace switches --------------------------------------------------

#[tokio::test]
async fn the_workspace_switches_need_write_at_the_root() {
    // Ungated, an actor denied a read could turn read enforcement off and retry,
    // making the check in front of every read decorative.
    let (fs, _alice, agent) = fixture().await;
    fs.grant(agent, "/proj", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    let ctx = WriteCtx::actor(agent);

    assert!(denied(
        &fs.set_acl_enforce_reads_as(ctx, false).await.unwrap_err()
    ));
    assert!(denied(
        &fs.set_acl_default_deny_as(ctx, false).await.unwrap_err()
    ));
    assert!(denied(
        &fs.set_write_policy_as(ctx, agent, WritePolicy::Direct)
            .await
            .unwrap_err()
    ));
}

#[tokio::test]
async fn an_actor_with_root_write_may_change_the_switches() {
    let (fs, _alice, agent) = fixture().await;
    let admin = fs.create_human("admin", None).await.unwrap();
    fs.grant(admin, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    let ctx = WriteCtx::actor(admin);

    fs.set_acl_enforce_reads_as(ctx, true).await.unwrap();
    assert!(fs.acl_enforce_reads().await.unwrap());
    fs.set_acl_default_deny_as(ctx, false).await.unwrap();
    assert!(!fs.acl_default_deny().await.unwrap());
    fs.set_write_policy_as(ctx, agent, WritePolicy::Propose)
        .await
        .unwrap();
}

/// A propose-only actor cannot promote *itself* to `Direct` — the escalation the
/// path-less `ensure_may_write` would have allowed, since that check consults the
/// very policy being changed.
#[tokio::test]
async fn a_propose_only_actor_cannot_promote_itself() {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs: TestFs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    fs.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();

    let e = fs
        .set_write_policy_as(WriteCtx::actor(agent), agent, WritePolicy::Direct)
        .await
        .unwrap_err();
    assert!(denied(&e), "{e:?}");
    assert!(
        !fs.effective_perms(agent, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
}

// --- the raw primitives stay, and stay unchecked ------------------------------

/// Provisioning has no actor by construction — the first grant in a fresh
/// workspace is written before anyone can hold rights in it — so the raw form
/// stays, exactly as `remove`/`rename`/`mkdir_p` do on the write side.
#[tokio::test]
async fn the_unattributed_grant_is_still_unchecked() {
    let (fs, _alice, agent) = fixture().await;
    fs.grant(agent, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    assert!(
        fs.effective_perms(agent, "/anywhere")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
}
