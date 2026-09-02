//! The mounts reach the engine only through the ACL-checked inode ops (#141).
//!
//! The surface half of `origofs-core/tests/vfs_acl.rs`. That one proves the
//! checked ops refuse; this one proves FUSE and NFS actually call them, which is
//! the part no behavioural test can see: a new `Filesystem` callback wired to the
//! *unchecked* `vfs_*` op behaves perfectly in every test anyone would write for
//! it, and is a silent hole in the ACLs.
//!
//! It reads the source rather than running a mount, for the same reason
//! `api_read_acl.rs` does: mounting needs a kernel, a mountpoint and privileges
//! that CI does not have on every leg — and the property is about which function
//! the code names, which is a fact about the text.
//!
//! Deliberately **not** feature- or platform-gated. The files exist on disk
//! whatever this build compiles, so the guard also runs on the Windows leg, where
//! neither module is built and a regression would otherwise sail through.

/// Ops a mount may call unchecked, each entry a claim about why.
const UNCHECKED_OK: &[(&str, &str)] = &[(
    "vfs_dentry_name",
    "NFSv3 resumes a readdir by inode number; this turns that cookie back into \
     the name cursor the paged listing takes. It reveals only whether an inode is \
     a child of a directory the caller is already listing, and that listing goes \
     through the gated, per-entry-filtered `vfs_readdir_page_with_attrs_as`.",
)];

fn scan(file: &str) -> Vec<(usize, String)> {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join(file),
    )
    .unwrap_or_else(|e| panic!("cannot read src/{file}: {e}"));

    let mut found = Vec::new();
    for (i, line) in src.lines().enumerate() {
        let mut from = 0usize;
        while let Some(at) = line[from..].find(".vfs_") {
            let start = from + at + 1;
            let name: String = line[start..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            from = start;
            // `.vfs_x` only counts as a call when it is one.
            if line[start + name.len()..].starts_with('(') {
                found.push((i + 1, name));
            }
        }
    }
    found
}

#[test]
fn the_mount_surfaces_call_only_acl_checked_inode_ops() {
    let mut calls = Vec::new();
    for file in ["fuse.rs", "nfs.rs"] {
        for (line, name) in scan(file) {
            calls.push((file, line, name));
        }
    }

    // A scan that stopped matching would pass while checking nothing.
    assert!(
        calls.len() >= 30,
        "the source scan found only {} `vfs_*` call sites across the mounts — \
         the scan is broken, not the surfaces",
        calls.len()
    );

    let bad: Vec<String> = calls
        .iter()
        .filter(|(_, _, name)| {
            !name.ends_with("_as") && !UNCHECKED_OK.iter().any(|(ok, _)| ok == name)
        })
        .map(|(file, line, name)| format!("{file}:{line} calls {name}"))
        .collect();

    assert!(
        bad.is_empty(),
        "a mount surface reaches the engine through an unchecked inode op, so the \
         path-scoped ACLs do not apply to it:\n  {}\n\nCall the `_as` form and pass \
         the mount's `self.ctx` (see the guards at the bottom of \
         origofs-core/src/vfs.rs), or add the op to UNCHECKED_OK with a reason a \
         mount cannot use it to reach a path the actor may not touch.",
        bad.join("\n  ")
    );

    // A stale exemption is a claim nobody is checking any more.
    for (name, _) in UNCHECKED_OK {
        assert!(
            calls.iter().any(|(_, _, n)| n == name),
            "UNCHECKED_OK names `{name}`, which no mount calls any more"
        );
    }
}

/// Both mounts must keep a way to be started *with* an actor, and a way to be
/// started without one.
///
/// The engine guards are inert unless something supplies a `WriteCtx`, so
/// deleting these entry points would leave every check in place and never firing.
#[test]
fn both_mounts_expose_an_actor_bound_entry_point() {
    for (file, needles) in [
        ("fuse.rs", ["pub fn spawn_as(", "pub fn spawn("]),
        ("nfs.rs", ["pub async fn serve_as(", "pub async fn serve("]),
    ] {
        let src = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .join(file),
        )
        .unwrap();
        for needle in needles {
            assert!(
                src.contains(needle),
                "src/{file} no longer defines `{needle}` — a mount that cannot be \
                 given an actor cannot be governed by the ACLs"
            );
        }
    }
}
