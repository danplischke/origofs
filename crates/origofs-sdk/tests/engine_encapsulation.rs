//! No surface reaches around `Fs` into a raw backend.
//!
//! `Fs::meta`/`Fs::content` were public fields. Every ACL check, quota check,
//! attribution record and `/.origofs` guard in the engine is a *method* on `Fs`,
//! so a caller holding a backend ran none of them — `ws.fs().meta.add_dentry(..)`
//! was a complete bypass of the authorization surface, and the SDK itself reached
//! past `Fs` fourteen times for operations that simply had no method.
//!
//! The fields are now `pub(crate)`, which the compiler enforces for every crate
//! outside `origofs-core`, and the administrative operations that legitimately
//! need a backend are methods on `Fs`. That is the real guard; this test covers
//! the one thing the compiler cannot see.
//!
//! The remaining hole is `Fs::backends()`, gated behind origofs-core's
//! `test-support` feature. A dev-dependency's features do not reach a consumer's
//! build, so no surface can call it in a normal build — but Cargo unifies
//! features across a single `cargo test --workspace` invocation, which is exactly
//! when a surface calling it would still compile and pass. This scan closes that
//! window.
//!
//! Deliberately not feature-gated: the files exist on disk whatever this build
//! compiles, so the guard runs on every platform leg including Windows, where
//! several of these modules are not built at all.

use std::path::Path;

/// Every source file under the SDK, recursively.
fn sdk_sources() -> Vec<(String, String)> {
    fn walk(dir: &Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs")
                && let Ok(s) = std::fs::read_to_string(&p)
            {
                out.push((p.display().to_string(), s));
            }
        }
    }
    let mut out = Vec::new();
    walk(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut out);
    out
}

#[test]
fn no_sdk_surface_reaches_the_raw_backends() {
    let files = sdk_sources();
    assert!(
        files.len() >= 5,
        "the source walk found only {} files under origofs-sdk/src — the walk is \
         broken, not the surfaces",
        files.len()
    );

    let mut bad = Vec::new();
    for (path, src) in &files {
        for (i, line) in src.lines().enumerate() {
            if line.contains(".backends()") {
                bad.push(format!("{path}:{}: {}", i + 1, line.trim()));
            }
        }
    }

    assert!(
        bad.is_empty(),
        "a surface reaches the metadata or content store directly, so none of the \
         engine's ACL, quota, attribution or internal-path guards run on what it \
         does:\n  {}\n\n`Fs::backends()` exists for origofs-core's own integration \
         suites, which are testing the engine and so may reach around it. A surface \
         needing something a backend can do needs a method on `Fs` that performs \
         the check — see the administration section of `engine.rs`.",
        bad.join("\n  ")
    );
}

/// The `Workspace` façade is the SDK's own front door and the biggest single
/// consumer of the engine, so it is worth asserting separately that it goes
/// through the administrative methods rather than the fields it used to touch.
#[test]
fn the_workspace_facade_uses_the_fs_administration_methods() {
    let src = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )
    .expect("origofs-sdk/src/lib.rs");

    for forbidden in [".fs.meta.", ".fs.content.", "fs().meta.", "fs().content."] {
        assert!(
            !src.contains(forbidden),
            "origofs-sdk/src/lib.rs still contains `{forbidden}` — the engine's \
             backends are private, so this would not compile; if it does, the \
             fields have been made public again."
        );
    }
}
