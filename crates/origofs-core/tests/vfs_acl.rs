//! The path-scoped ACLs reach the **mount** inode ops (issue #141).
//!
//! `#123` gated every attributed mutation on `Fs`, and the mounts called none of
//! them: FUSE and NFS address everything by inode number through the `vfs_*`
//! layer, which took no actor at all. `CLAUDE.md` documented that as a deliberate
//! bypass, and while the only authorization was a per-actor write policy it cost
//! little — a mount was all-or-nothing either way. Prefix grants turned it into
//! false containment: an agent refused `WRITE` under `/src` over MCP took the
//! identical action through a mount and no check ran.
//!
//! These tests are written against the *engine*, because that is where the checks
//! live and must stay. The companion in `origofs-sdk/tests/mount_acl.rs` is what
//! stops a surface from reaching around them.

use origofs_core::{
    Fs, MemStore, MetadataStore, OrigoFSError, Owner, Perms, SqliteMetadataStore, WriteCtx,
};
use std::sync::Arc;

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

/// A workspace with `/src` and `/docs`, and an agent granted `WRITE` on `/docs`
/// only. Deny-by-default is on, so absence of a grant is a refusal rather than a
/// fallback to the actor's write policy.
async fn fixture() -> (TestFs, WriteCtx, i64, i64) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();

    fs.mkdir_p("/src").await.unwrap();
    fs.mkdir_p("/docs").await.unwrap();
    fs.write("/src/secret.rs", b"fn main() {}\n").await.unwrap();
    fs.write("/docs/ok.md", b"hello\n").await.unwrap();

    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let session = fs.create_session(agent, None).await.unwrap();
    fs.set_acl_default_deny(true).await.unwrap();
    fs.grant(agent, "/docs", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();

    let src = fs.stat("/src").await.unwrap().ino;
    let docs = fs.stat("/docs").await.unwrap().ino;
    (fs, WriteCtx::session(agent, session), src, docs)
}

fn denied(e: &OrigoFSError) -> bool {
    matches!(e, OrigoFSError::Denied(_))
}

/// The headline property, op by op: every mutating inode op a mount can issue is
/// refused for an actor without `WRITE` at the path it touches.
///
/// One test rather than fifteen because the interesting failure is a *gap* — one
/// op that forgot its guard — and a table makes the gap visible.
#[tokio::test]
async fn every_mount_mutation_is_refused_without_write_at_the_path() {
    let (fs, ctx, src, _docs) = fixture().await;
    let c = Some(ctx);
    let secret = fs.stat("/src/secret.rs").await.unwrap().ino;

    macro_rules! refused {
        ($what:expr, $call:expr) => {
            match $call.await {
                Err(e) if denied(&e) => {}
                Err(e) => panic!("{}: expected Denied, got {e:?}", $what),
                Ok(_) => panic!("{}: succeeded through the mount despite no WRITE", $what),
            }
        };
    }

    refused!("write", fs.vfs_write_as(c, secret, 0, b"pwned"));
    refused!("truncate", fs.vfs_truncate_as(c, secret, 0));
    refused!(
        "create",
        fs.vfs_create_as(c, src, "new.rs", 0o644, Owner::ROOT)
    );
    refused!("mkdir", fs.vfs_mkdir_as(c, src, "sub", 0o755, Owner::ROOT));
    refused!("unlink", fs.vfs_unlink_as(c, src, "secret.rs"));
    refused!("rmdir", fs.vfs_rmdir_as(c, src, "nothing"));
    refused!("link", fs.vfs_link_as(c, secret, src, "alias.rs"));
    refused!("rename", fs.vfs_rename_as(c, src, "secret.rs", src, "x.rs"));
    refused!(
        "symlink",
        fs.vfs_symlink_as(c, src, "l", "/etc/passwd", Owner::ROOT)
    );
    refused!("chmod", fs.vfs_chmod_as(c, secret, 0o777));
    refused!("chown", fs.vfs_chown_as(c, secret, Some(0), Some(0)));
    refused!("setxattr", fs.vfs_setxattr_as(c, secret, "user.x", b"1"));
    refused!("removexattr", fs.vfs_removexattr_as(c, secret, "user.x"));

    // Nothing landed.
    assert_eq!(
        &fs.read("/src/secret.rs").await.unwrap()[..],
        b"fn main() {}\n"
    );
    assert!(fs.stat("/src/new.rs").await.is_err());
    assert!(fs.stat("/src/sub").await.is_err());
    assert!(fs.stat("/src/alias.rs").await.is_err());
}

/// The other half: a grant that *does* cover the path still works, so the guard
/// refuses rather than simply breaking the mount.
#[tokio::test]
async fn a_granted_actor_still_mutates_through_the_mount() {
    let (fs, ctx, _src, docs) = fixture().await;
    let c = Some(ctx);
    let ok = fs.stat("/docs/ok.md").await.unwrap().ino;

    fs.vfs_write_as(c, ok, 0, b"HELLO").await.unwrap();
    fs.vfs_create_as(c, docs, "new.md", 0o644, Owner::ROOT)
        .await
        .unwrap();
    fs.vfs_mkdir_as(c, docs, "sub", 0o755, Owner::ROOT)
        .await
        .unwrap();
    fs.vfs_rename_as(c, docs, "new.md", docs, "moved.md")
        .await
        .unwrap();
    fs.vfs_unlink_as(c, docs, "moved.md").await.unwrap();

    assert_eq!(&fs.read("/docs/ok.md").await.unwrap()[..], b"HELLO\n");
    assert!(fs.stat("/docs/sub").await.is_ok());
}

/// A rename is refused when **either** end is out of reach — the source, so a
/// file cannot be moved out of a tree the actor may not touch, and the
/// destination, so it cannot be moved into one.
#[tokio::test]
async fn a_rename_is_checked_at_both_ends() {
    let (fs, ctx, src, docs) = fixture().await;
    let c = Some(ctx);

    // Granted source, ungranted destination.
    let e = fs
        .vfs_rename_as(c, docs, "ok.md", src, "stolen.md")
        .await
        .unwrap_err();
    assert!(denied(&e), "moving into an ungranted tree: {e:?}");

    // Ungranted source, granted destination.
    let e = fs
        .vfs_rename_as(c, src, "secret.rs", docs, "leaked.rs")
        .await
        .unwrap_err();
    assert!(denied(&e), "moving out of an ungranted tree: {e:?}");

    assert!(fs.stat("/docs/ok.md").await.is_ok());
    assert!(fs.stat("/src/secret.rs").await.is_ok());
}

/// An anonymous mount — `None` — behaves exactly as every mount did before this
/// existed. That is what keeps `origofs mount` working for the single-user case,
/// and it is the bypass that must stay *visible* rather than implicit.
#[tokio::test]
async fn an_anonymous_mount_still_bypasses_the_acls() {
    let (fs, _ctx, src, _docs) = fixture().await;
    let secret = fs.stat("/src/secret.rs").await.unwrap().ino;

    fs.vfs_write_as(None, secret, 0, b"XXXXXXXXXXXXX")
        .await
        .unwrap();
    fs.vfs_create_as(None, src, "anon.rs", 0o644, Owner::ROOT)
        .await
        .unwrap();
    assert!(fs.stat("/src/anon.rs").await.is_ok());
}

/// Reads are gated only where the workspace opted in, exactly like every other
/// read check — so adding these guards changed nothing for existing deployments.
#[tokio::test]
async fn reads_are_open_until_the_workspace_enforces_them() {
    let (fs, ctx, _src, _docs) = fixture().await;
    let c = Some(ctx);
    let secret = fs.stat("/src/secret.rs").await.unwrap().ino;

    // Off by default: the actor has no READ on /src and is served anyway.
    fs.vfs_read_as(c, secret, 0, 64).await.unwrap();
    fs.vfs_getattr_as(c, secret).await.unwrap();

    fs.set_acl_enforce_reads(true).await.unwrap();
    assert!(denied(&fs.vfs_read_as(c, secret, 0, 64).await.unwrap_err()));
    assert!(denied(&fs.vfs_getattr_as(c, secret).await.unwrap_err()));
    // The granted tree still reads.
    let ok = fs.stat("/docs/ok.md").await.unwrap().ino;
    fs.vfs_read_as(c, ok, 0, 64).await.unwrap();
}

/// A listing must not name what a `stat` would refuse, or the refusal is
/// decoration: the listing *is* the existence probe.
#[tokio::test]
async fn readdir_hides_entries_the_actor_may_not_read() {
    let (fs, ctx, _src, _docs) = fixture().await;
    let c = Some(ctx);
    fs.write("/docs/a.md", b"a").await.unwrap();
    fs.write("/docs/b.md", b"b").await.unwrap();
    fs.grant(ctx.actor, "/docs/b.md", Perms::from_bits(0), None)
        .await
        .unwrap();
    fs.set_acl_enforce_reads(true).await.unwrap();

    let docs = fs.stat("/docs").await.unwrap().ino;
    let names: Vec<String> = fs
        .vfs_readdir_page_as(c, docs, None, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert!(names.contains(&"a.md".to_string()), "{names:?}");
    assert!(
        !names.contains(&"b.md".to_string()),
        "an unreadable entry was listed: {names:?}"
    );

    // And the attribute-carrying form filters identically.
    let page = fs
        .vfs_readdir_page_with_attrs_as(c, docs, None, 100)
        .await
        .unwrap();
    assert!(
        !page.entries.iter().any(|e| e.entry.name == "b.md"),
        "the with-attrs form leaked an entry the plain form hides"
    );
}

/// The filtered listing must not report a premature end.
///
/// This is the trap in filtering a *paged* scan: the caller resumes from the last
/// name it was handed, so a page that filters to empty reads as end-of-directory.
/// With a page size of 1 and the only readable entry sorting last, a naive filter
/// returns nothing and the directory looks empty.
#[tokio::test]
async fn a_filtered_page_never_reports_a_premature_end() {
    let (fs, ctx, _src, _docs) = fixture().await;
    let c = Some(ctx);
    for n in ["a", "b", "c"] {
        fs.write(&format!("/docs/{n}.md"), b"x").await.unwrap();
    }
    // Everything but the last-sorting name is unreadable.
    for n in ["a", "b", "ok"] {
        fs.grant(
            ctx.actor,
            &format!("/docs/{n}.md"),
            Perms::from_bits(0),
            None,
        )
        .await
        .unwrap();
    }
    fs.set_acl_enforce_reads(true).await.unwrap();

    let docs = fs.stat("/docs").await.unwrap().ino;
    let names: Vec<String> = fs
        .vfs_readdir_page_as(c, docs, None, 1)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(
        names,
        vec!["c.md".to_string()],
        "the scan stopped at the first invisible page instead of paging past it"
    );
}

/// An inode with no path — an orphan, or another workspace's root — is refused
/// rather than allowed. A prefix grant cannot express permission for something
/// outside the tree, so "no path" must not read as "no restriction".
#[tokio::test]
async fn an_unreachable_inode_is_refused_rather_than_allowed() {
    let (fs, ctx, _src, _docs) = fixture().await;
    let e = fs
        .vfs_write_as(Some(ctx), 999_999, 0, b"x")
        .await
        .unwrap_err();
    assert!(
        denied(&e),
        "an unreachable inode must not be writable: {e:?}"
    );
}

// --- the structural guard ---------------------------------------------------

/// Every inode op has a checked counterpart, or is named here with a reason.
///
/// The behavioural tests above cover the ops that exist today. The bug they
/// cannot catch is the next one: a `vfs_thing` added to the engine with no
/// `vfs_thing_as` beside it is a hole that no test fails on, because no test
/// knows to call it. This is the moment someone has to think about it — either
/// they add the guard, or they write down why the op does not need one.
///
/// This is the engine half. `origofs-sdk/tests/mount_acl.rs` is the surface half:
/// having a checked counterpart is worth nothing if a mount calls the unchecked
/// one.
#[test]
fn every_inode_op_has_a_checked_counterpart() {
    /// Ops with no `_as` form, each entry a claim that it needs no check.
    const NO_CHECK_NEEDED: &[(&str, &str)] = &[
        (
            "vfs_readdir",
            "Superseded by the paged forms, which are the ones the mounts call \
             and the ones that filter. Left unchecked deliberately: giving it a \
             guard would imply it is a mount entry point, and the fix if a mount \
             ever calls it is to move that mount onto the paged form.",
        ),
        (
            "vfs_dentry_name",
            "Translates an NFSv3 readdir resume cookie (an inode number) into the \
             name cursor the paged listing takes. It answers only about a child of \
             a directory the caller is already listing, and that listing is \
             gated and filtered by `vfs_readdir_page_as`.",
        ),
        (
            "vfs_path_of",
            "The inverse of lookup, and what the guards themselves are built on — \
             gating it would be circular. It is not a mount entry point: the \
             mounts address by inode and never ask for a path.",
        ),
    ];

    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/vfs.rs"),
    )
    .unwrap();

    let mut ops: Vec<String> = Vec::new();
    let needle = "pub async fn vfs_";
    let mut from = 0usize;
    while let Some(at) = src[from..].find(needle) {
        let start = from + at + "pub async fn ".len();
        let name: String = src[start..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        from = start;
        ops.push(name);
    }
    ops.sort();
    ops.dedup();

    // A scan that silently stopped matching would pass this test while checking
    // nothing, so make the scan itself falsifiable.
    assert!(
        ops.len() >= 24,
        "the source scan found only {} `vfs_*` ops — the scan is broken, not the \
         engine: {ops:?}",
        ops.len()
    );

    let mut ungated = Vec::new();
    for op in &ops {
        if op.ends_with("_as") {
            continue;
        }
        if NO_CHECK_NEEDED.iter().any(|(name, _)| name == op) {
            continue;
        }
        if !ops.contains(&format!("{op}_as")) {
            ungated.push(op.clone());
        }
    }
    assert!(
        ungated.is_empty(),
        "these inode ops have no ACL-checked counterpart and are not exempt: \
         {ungated:?}\n\nAdd `<op>_as` beside each (see the guards at the bottom of \
         src/vfs.rs), or add it to NO_CHECK_NEEDED with a reason it cannot be \
         reached by a mount."
    );

    // An exemption for an op that no longer exists is a stale claim; drop it.
    for (name, _) in NO_CHECK_NEEDED {
        assert!(
            ops.iter().any(|o| o == name),
            "NO_CHECK_NEEDED names `{name}`, which no longer exists"
        );
    }
}
