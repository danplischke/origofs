//! The posture a workspace is **born** with (`origofs_core::defaults`).
//!
//! Every safety switch here defaults off, each for the same documented reason:
//! turning it on would change what an existing deployment already does. That is an
//! argument about upgrades, and it was being applied to new workspaces too — so a
//! system built on "every edit is recorded against the actor that made it" handed
//! you a fresh workspace with no access control at all.
//!
//! A default is therefore recorded at *creation*, with the epoch it was created
//! under beside it. Two properties carry the whole design, and the second is the
//! one that makes the first safe to have:
//!
//! * **A workspace created now starts with epoch 1's posture** — `acl.enforce_reads`
//!   on.
//! * **A workspace created before this keeps exactly what it had, forever.** Not
//!   "until it is next opened", not "unless it is migrated": opening is the
//!   migration runner and runs on every open, so an implementation keyed on "the
//!   config key is absent" would have flipped every existing workspace on upgrade.
//!   That is the bug this file exists to catch.

use origofs_core::{
    ConfigStore, Fs, MemStore, MetadataStore, Perms, SqliteMetadataStore, WriteCtx,
};
use std::sync::Arc;

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

fn store() -> Arc<dyn MetadataStore> {
    Arc::new(SqliteMetadataStore::open_in_memory().unwrap())
}

#[tokio::test]
async fn a_new_workspace_is_stamped_and_enforces_reads() {
    let fs: TestFs = Fs::new(store(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();

    assert_eq!(fs.defaults_epoch().await.unwrap(), Some(1));
    assert!(
        fs.acl_enforce_reads().await.unwrap(),
        "a workspace created now should start with read enforcement on"
    );
}

#[tokio::test]
async fn enforcement_on_a_new_workspace_denies_nobody_who_has_an_actor() {
    // The claim that makes epoch 1 cheap enough to be a default: with no grants at
    // all, `effective_perms` falls back to the actor's write policy, and both
    // policies carry READ. If this ever stops holding, a fresh workspace starts
    // refusing its own creator and the epoch must change, not this test.
    let fs: TestFs = Fs::new(store(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    assert!(fs.acl_enforce_reads().await.unwrap());

    let owner = fs.create_agent("owner", "opus", None).await.unwrap();
    let octx = WriteCtx::actor(owner);
    fs.grant(owner, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    fs.write_as(octx, "/doc.md", b"hello\n").await.unwrap();

    // `bob` holds no grant anywhere, and still reads — via the policy fallback.
    let bob = fs.create_agent("bob", "opus", None).await.unwrap();
    let bctx = WriteCtx::actor(bob);
    assert_eq!(
        fs.read_as(bctx, "/doc.md").await.unwrap().as_ref(),
        b"hello\n"
    );
    assert!(fs.stat_as(bctx, "/doc.md").await.is_ok());
    assert!(fs.ls_as(bctx, "/").await.is_ok());
}

#[tokio::test]
async fn reopening_an_existing_workspace_never_stamps_it() {
    // The migration invariant, and the reason `init` keys on the schema version
    // rather than on the key being absent. `init` *is* the migration runner: it
    // runs on every open, so "stamp when the key is missing" would flip every
    // workspace that predates the key — which is every existing workspace.
    let meta = store();
    let content = Arc::new(MemStore::new());

    let first: TestFs = Fs::new(meta.clone(), content.clone());
    first.init().await.unwrap();
    // Stand in for a workspace created before epoch 1: no stamp, enforcement off.
    first.set_acl_enforce_reads(false).await.unwrap();
    // No engine method unsets a config key, and one should not exist just for a
    // test — `backends()` is the named escape hatch for reaching around the engine
    // when the engine is what you are testing. An unparseable value reads back as
    // "no epoch", which is what a pre-epoch workspace has.
    first
        .backends()
        .meta
        .set_config("defaults.epoch", "")
        .await
        .unwrap();

    // Reopen it, twice, the way a restarted process would.
    for _ in 0..2 {
        let again: TestFs = Fs::new(meta.clone(), content.clone());
        again.init().await.unwrap();
        assert!(
            !again.acl_enforce_reads().await.unwrap(),
            "reopening a pre-epoch workspace must not turn enforcement on"
        );
    }
}

#[tokio::test]
async fn a_workspace_created_in_a_new_store_inherits_the_posture() {
    // A multi-tenant store creates workspaces at runtime. They must not be
    // governed differently from the `default` beside them.
    let fs: TestFs = Fs::new(store(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    assert_eq!(fs.defaults_epoch().await.unwrap(), Some(1));

    let (_id, tenant) = fs.open_workspace("tenant-a").await.unwrap();
    assert_eq!(tenant.defaults_epoch().await.unwrap(), Some(1));
    assert!(
        tenant.acl_enforce_reads().await.unwrap(),
        "a tenant workspace in a new store should inherit the posture"
    );
}

#[tokio::test]
async fn a_workspace_created_in_a_legacy_store_does_not() {
    // The other half, and the one that protects a running deployment: a store that
    // predates creation-time defaults must not start handing them to workspaces it
    // creates at runtime, or an upgrade changes the behaviour of tenants onboarded
    // after it.
    let fs: TestFs = Fs::new(store(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs.set_acl_enforce_reads(false).await.unwrap();
    fs.backends()
        .meta
        .set_config("defaults.epoch", "")
        .await
        .unwrap();

    let (_id, tenant) = fs.open_workspace("tenant-b").await.unwrap();
    assert!(
        !tenant.acl_enforce_reads().await.unwrap(),
        "a tenant workspace in a pre-epoch store must keep the legacy posture"
    );
}

#[tokio::test]
async fn opening_an_existing_workspace_again_does_not_restamp_it() {
    // `open_workspace` is lookup-or-create, and only the *create* path stamps. An
    // operator who turned enforcement off on one tenant must not have it turned
    // back on by the next process that opens that tenant.
    let fs: TestFs = Fs::new(store(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();

    let (_id, tenant) = fs.open_workspace("tenant-c").await.unwrap();
    assert!(tenant.acl_enforce_reads().await.unwrap());
    tenant.set_acl_enforce_reads(false).await.unwrap();

    let (_id, again) = fs.open_workspace("tenant-c").await.unwrap();
    assert!(
        !again.acl_enforce_reads().await.unwrap(),
        "reopening an existing workspace must not re-apply its creation posture"
    );
}
