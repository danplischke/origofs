//! Git interop surface (`git` feature) — drive an origofs workspace with the real
//! `git` (`docs/DESIGN.md` §4c, the git-interop layer; roadmap M5).
//!
//! origofs stays BLAKE3-native internally; this module bridges its opt-in commit DAG
//! to genuine git objects in both directions:
//!
//! - [`export_git`] re-encodes an origofs branch as real git objects under a `.git`
//!   directory the actual `git` binary reads (`log`, `diff`, `blame`,
//!   `checkout`, `fsck`) — in SHA-1 (GitHub-compatible) or SHA-256 (origofs's native
//!   256-bit ids), with large files optionally written as git-LFS pointers.
//! - [`import_git`] reads a real git repository's history back into origofs commits,
//!   trees, and blobs, then checks the branch out.
//!
//! Because git records only commit-granular authorship, the finer per-line
//! human-vs-agent attribution (`docs/DESIGN.md` §4d) stays in origofs's own tables;
//! git interop neither needs nor disturbs it.
//!
//! The `git-remote-origofs` binary (shipped by `origofs-cli`) builds on this, so the
//! real `git` can `clone`/`fetch`/`push` an origofs workspace over `origofs://`. Reading
//! packed (non-loose) objects on import remains a follow-up.

mod export;
mod import;
mod object;

pub use export::{ExportOptions, GitExport, export_git};
pub use import::import_git;
pub use object::ObjectFormat;
