//! The mounts reach the engine only through the ACL-checked inode ops (#141).
//!
//! The surface half of `origofs-core/tests/vfs_acl.rs`. That one proves the
//! checked ops refuse; this one is about surfaces calling them — which is the
//! part no behavioural test can see, because a `Filesystem` callback wired to the
//! *unchecked* op behaves perfectly in every test anyone would write for it.
//!
//! **This used to be a substring scan of `fuse.rs` and `nfs.rs` for `.vfs_`.** It
//! is now a fact the compiler enforces: the unchecked `vfs_*` primitives are
//! `pub(crate)` in origofs-core, so outside that crate only the `_as` forms
//! exist. A mount that called the wrong one would not build.
//!
//! The scan is gone for cause, not for tidiness. It could not see a rename, a
//! macro, a helper that forwarded the call, a trait method, or a file it was not
//! pointed at — and it was pointed at two. Seven unchecked calls in the SDK façade
//! (`chmod`, `chown`, `link`, and the four xattr methods) and seven more in the
//! Python bindings sat outside its view the whole time, running no authorization
//! at all. Making the ops `pub(crate)` surfaced all fourteen as build errors in
//! one pass.
//!
//! What remains here is the part that is still a choice rather than a type: which
//! `_as` calls pass the mount's actor rather than `None`, since `None` is the
//! anonymous mount and legitimately bypasses.

use std::path::Path;

fn source(file: &str) -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join(file))
        .unwrap_or_else(|e| panic!("cannot read src/{file}: {e}"))
}

/// A mount holds one actor for its lifetime and must pass it to every checked op.
///
/// Passing a literal `None` compiles and silently reverts that mount to the
/// anonymous, unchecked behaviour the `_as` forms exist to replace — the one
/// remaining way to lose the check without the compiler noticing.
#[test]
fn the_mounts_pass_their_actor_rather_than_none() {
    let mut bad = Vec::new();
    for file in ["fuse.rs", "nfs.rs"] {
        for (i, line) in source(file).lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.contains("_as(None") || trimmed.contains("_as(\n") {
                continue;
            }
            if trimmed.contains("vfs_") && trimmed.contains("(None,") {
                bad.push(format!("{file}:{}: {trimmed}", i + 1));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "a mount passes `None` to a checked inode op, which is the anonymous \
         mount: the ACL check is skipped even though the mount was given an \
         actor.\n  {}\n\nPass `self.ctx` (see `fuse::spawn_as` / `nfs::serve_as`).",
        bad.join("\n  ")
    );
}

/// The mounts hold an actor at all. If this field disappears there is nothing to
/// pass, and every `_as` call degenerates to the anonymous form.
#[test]
fn both_mounts_carry_a_write_context() {
    for file in ["fuse.rs", "nfs.rs"] {
        let src = source(file);
        assert!(
            src.contains("ctx: Option<WriteCtx>")
                || src.contains("ctx: Option<origofs_core::WriteCtx>"),
            "src/{file} no longer holds a `WriteCtx` for the mount's lifetime, so \
             the ACL-checked inode ops have no actor to check against"
        );
    }
}
