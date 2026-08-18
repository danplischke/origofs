//! Path-scoped access grants (`docs/PERMISSIONS.md` §3b, issue #123).
//!
//! The per-actor [`WritePolicy`](crate::WritePolicy) (migration V10) is
//! workspace-wide and binary: an actor may write everywhere, or propose
//! everywhere. That covers "this agent is untrusted" and nothing finer. It cannot
//! say *"this agent may write under `/src/parser` and nowhere else"*, which is the
//! first thing anyone pointing several agents at one workspace asks for.
//!
//! A [`Grant`] refines the policy for a subtree. Resolution is **longest matching
//! prefix wins**, falling back to the actor's write policy at the root:
//!
//! ```text
//!   write_policy = Direct                    → the fallback: write everywhere
//!   grant("/",         READ)                 → …unless a root grant overrides it
//!   grant("/src",      READ | WRITE)         → …and a deeper one overrides that
//!   grant("/src/vendor", READ)               → …and a deeper one overrides *that*
//!
//!   /docs/x.md      → "/"           → READ         (may not write)
//!   /src/main.rs    → "/src"        → READ | WRITE (may write)
//!   /src/vendor/z.c → "/src/vendor" → READ         (may not write)
//! ```
//!
//! # Why the fallback rather than a backfill
//!
//! An actor with **no** grants resolves to its `write_policy` at every path, which
//! is exactly the behaviour that existed before this module. So migration V18 needs
//! no backfill, `set_write_policy` keeps meaning what it always meant, and — the
//! part a backfill could not have given us — an actor created *after* the migration
//! is governed the same way. Grants are purely additive refinement.
//!
//! Deny-by-default is expressible without a separate mode: grant `/` nothing (or
//! read-only) and grant the subtrees the actor should reach.
//!
//! # Prefix matching is directory-boundary matching
//!
//! `/tenant-a` must not cover `/tenant-abc`. This is the classic bug in
//! prefix-scoped authorization, and `origofs.fastapi` already got it right for its
//! request scoping — [`covers`] is the same rule in the engine, where every surface
//! gets it for free instead of each one re-deriving it.
//!
//! # What this is *not*
//!
//! Not POSIX `mode`. `mode`/`uid`/`gid` are recorded and reported and never
//! consulted for authorization (`docs/PERMISSIONS.md` §2); origofs's principals are
//! **actors**, not uids. The two systems are deliberately separate.
//!
//! Not enforceable through a mount. FUSE and NFS have no actor context — a
//! deliberate bypass recorded in `CLAUDE.md` — so a grant restricts the SDK, the
//! HTTP API, MCP and the CLI, and a mount remains as unrestricted as it was before
//! grants existed. See `docs/PERMISSIONS.md` §5.

use crate::error::{OrigoFSError, Result};

/// What an actor may do with a path.
///
/// A bitset rather than an enum because the useful combinations are genuinely
/// combinations: read-only, read+write, and read+propose are all distinct, and
/// "propose but not read" is meaningless rather than forbidden — a proposer has to
/// see what it is amending.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Perms(u32);

impl Perms {
    /// No access at all.
    pub const NONE: Perms = Perms(0);
    /// May read the path's bytes and metadata.
    pub const READ: Perms = Perms(1);
    /// May mutate the path directly.
    pub const WRITE: Perms = Perms(2);
    /// May queue a change for review by a different actor.
    pub const PROPOSE: Perms = Perms(4);

    /// Read + write: what an unrestricted (`Direct`) actor has.
    pub const RW: Perms = Perms(1 | 2);
    /// Read + propose: what a `Propose`-only actor has.
    pub const RP: Perms = Perms(1 | 4);

    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Build from stored bits, rejecting any this build does not define.
    ///
    /// A row written by a newer origofs could carry a permission this one has never
    /// heard of. Silently masking it off would *quietly widen or narrow* an
    /// operator's intent — the one thing an authorization decision must never do —
    /// so an unknown bit fails loudly and the fix is to upgrade, not to guess.
    ///
    /// Not `UnsupportedVersion`: that variant describes an object *header* whose
    /// format this build cannot decode, and carries a `u8` version to say so. This
    /// is a metadata row whose contents this build cannot interpret, which is what
    /// `Metadata` means.
    pub fn from_bits(bits: u32) -> Result<Perms> {
        const KNOWN: u32 = 1 | 2 | 4;
        if bits & !KNOWN != 0 {
            return Err(OrigoFSError::Metadata(format!(
                "access grant carries permission bits {:#x} this build does not \
                 understand; upgrade origofs rather than acting on a guess",
                bits & !KNOWN
            )));
        }
        Ok(Perms(bits))
    }

    /// Whether every bit in `other` is present.
    pub const fn contains(self, other: Perms) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn union(self, other: Perms) -> Perms {
        Perms(self.0 | other.0)
    }

    /// A human-readable rendering (`"read,write"`), for error messages and CLI output.
    pub fn as_str(self) -> String {
        if self.0 == 0 {
            return "none".to_string();
        }
        let mut parts = Vec::new();
        if self.contains(Perms::READ) {
            parts.push("read");
        }
        if self.contains(Perms::WRITE) {
            parts.push("write");
        }
        if self.contains(Perms::PROPOSE) {
            parts.push("propose");
        }
        parts.join(",")
    }

    /// Parse a comma-separated rendering (`"read,write"`, `"none"`).
    pub fn parse(s: &str) -> Result<Perms> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("none") || s.is_empty() {
            return Ok(Perms::NONE);
        }
        let mut out = Perms::NONE;
        for part in s.split(',') {
            out = out.union(match part.trim().to_ascii_lowercase().as_str() {
                "read" | "r" => Perms::READ,
                "write" | "w" => Perms::WRITE,
                "propose" | "p" => Perms::PROPOSE,
                other => {
                    return Err(OrigoFSError::InvalidArgument(format!(
                        "unknown permission {other:?}; expected read, write, propose, or none"
                    )));
                }
            });
        }
        Ok(out)
    }
}

/// One stored grant: what `actor_id` may do at or below `path_prefix`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub actor_id: i64,
    pub path_prefix: String,
    pub perms: Perms,
}

/// Whether `prefix` covers `path`, matching on **directory boundaries**.
///
/// `/tenant-a` covers `/tenant-a` and `/tenant-a/x`, and does **not** cover
/// `/tenant-abc` — which is precisely the neighbour a scope exists to exclude. A
/// plain `starts_with` gets that wrong, and getting it wrong here would hand one
/// tenant's agent the next tenant's subtree.
pub fn covers(prefix: &str, path: &str) -> bool {
    let prefix = prefix.trim_end_matches('/');
    // The root covers everything; it is the only prefix that trims to empty.
    if prefix.is_empty() {
        return true;
    }
    path == prefix || path.starts_with(&format!("{prefix}/"))
}

/// Normalize a grant prefix: absolute, no trailing slash, no `.`/`..` components.
///
/// `..` is refused rather than resolved. `validate_component` already stops a
/// poisoned name being *stored* as a path, but that is a different guarantee from
/// stopping a *grant* escaping its intended subtree — `/src/../etc` normalizing to
/// `/etc` would silently widen a grant the operator wrote narrowly.
pub fn normalize_prefix(prefix: &str) -> Result<String> {
    let p = prefix.trim();
    if !p.starts_with('/') {
        return Err(OrigoFSError::InvalidArgument(format!(
            "grant prefix must be absolute (start with '/'); got {prefix:?}"
        )));
    }
    if p.split('/').any(|c| c == ".." || c == ".") {
        return Err(OrigoFSError::InvalidArgument(format!(
            "grant prefix may not contain '.' or '..'; got {prefix:?}"
        )));
    }
    let trimmed = p.trim_end_matches('/');
    Ok(if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    })
}

/// Resolve a caller-supplied path **inside** `root` (`docs/PERMISSIONS.md` §1g,
/// issue #125).
///
/// The caller's path is always relative to the root, so a client cannot address
/// anything outside its scope by asking for one — there is no representable request
/// for `/other-tenant/secrets`, because the root is **prepended** rather than
/// compared against. That is the property that makes surface scoping robust, and it
/// is why this is not simply a `covers` check after the fact.
///
/// A `..` component is refused outright. [`crate::engine::validate_component`]
/// already refuses to *store* one, but that is a different guarantee: it stops a
/// poisoned name being persisted, not a path being resolved out of its scope here.
///
/// Ported from `origofs.fastapi`'s `_scoped`, which was the only working
/// implementation in the tree, so both surfaces now share one rather than each
/// re-deriving it.
pub fn scope_path(root: &str, path: &str) -> Result<String> {
    let p = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if p.split('/').any(|c| c == "..") {
        return Err(OrigoFSError::InvalidArgument(
            "path may not contain '..'".into(),
        ));
    }
    let root = root.trim_end_matches('/');
    if root.is_empty() {
        return Ok(p);
    }
    Ok(if p == "/" {
        root.to_string()
    } else {
        format!("{root}{p}")
    })
}

/// Whether a record naming `path` is visible within `root`.
///
/// A `None` path is **not** in scope. A record that names no path — an idle
/// presence row — still tells a scoped reader that a neighbour exists, which is
/// exactly what a scope is for. `origofs.fastapi` learned this one the hard way and
/// documents it; keeping the rule here means the Rust surfaces cannot rediscover it
/// independently.
pub fn in_scope(root: &str, path: Option<&str>) -> bool {
    match path {
        Some(p) => covers(root, p),
        None => root.trim_end_matches('/').is_empty(),
    }
}

/// Pick the grant that governs `path`: the longest covering prefix.
///
/// Returns `None` when no grant covers the path, which is the caller's signal to
/// fall back to the actor's [`WritePolicy`](crate::WritePolicy).
pub fn resolve<'a>(grants: &'a [Grant], path: &str) -> Option<&'a Grant> {
    grants
        .iter()
        .filter(|g| covers(&g.path_prefix, path))
        // Longest prefix wins; `/src/vendor` beats `/src` beats `/`.
        .max_by_key(|g| g.path_prefix.trim_end_matches('/').len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prefix_covers_only_whole_path_components() {
        assert!(covers("/tenant-a", "/tenant-a"));
        assert!(covers("/tenant-a", "/tenant-a/notes.txt"));
        assert!(covers("/tenant-a", "/tenant-a/deep/nested/file"));
        // The bug this function exists to avoid.
        assert!(!covers("/tenant-a", "/tenant-abc"));
        assert!(!covers("/tenant-a", "/tenant-abc/secrets"));
        assert!(!covers("/src", "/srcs/x"));
        // The root covers everything, spelled either way.
        assert!(covers("/", "/anything/at/all"));
        assert!(covers("/", "/"));
    }

    #[test]
    fn the_longest_covering_prefix_wins() {
        let grants = vec![
            Grant {
                actor_id: 1,
                path_prefix: "/".into(),
                perms: Perms::READ,
            },
            Grant {
                actor_id: 1,
                path_prefix: "/src".into(),
                perms: Perms::RW,
            },
            Grant {
                actor_id: 1,
                path_prefix: "/src/vendor".into(),
                perms: Perms::READ,
            },
        ];
        assert_eq!(resolve(&grants, "/docs/x").unwrap().path_prefix, "/");
        assert_eq!(
            resolve(&grants, "/src/main.rs").unwrap().path_prefix,
            "/src"
        );
        assert_eq!(
            resolve(&grants, "/src/vendor/z.c").unwrap().path_prefix,
            "/src/vendor"
        );
    }

    #[test]
    fn no_covering_grant_resolves_to_none() {
        let grants = vec![Grant {
            actor_id: 1,
            path_prefix: "/src".into(),
            perms: Perms::RW,
        }];
        assert!(resolve(&grants, "/docs/x").is_none());
    }

    #[test]
    fn prefixes_normalize_and_refuse_traversal() {
        assert_eq!(normalize_prefix("/src/").unwrap(), "/src");
        assert_eq!(normalize_prefix("  /src  ").unwrap(), "/src");
        assert_eq!(normalize_prefix("/").unwrap(), "/");
        assert!(normalize_prefix("src").is_err()); // not absolute
        // Normalizing this to `/etc` would widen a grant written narrowly.
        assert!(normalize_prefix("/src/../etc").is_err());
        assert!(normalize_prefix("/src/./x").is_err());
    }

    #[test]
    fn a_scoped_path_is_prepended_not_compared() {
        // The property: out-of-scope paths are unrepresentable, not rejected.
        assert_eq!(
            scope_path("/tenant-a", "/notes.txt").unwrap(),
            "/tenant-a/notes.txt"
        );
        assert_eq!(
            scope_path("/tenant-a", "notes.txt").unwrap(),
            "/tenant-a/notes.txt"
        );
        assert_eq!(scope_path("/tenant-a", "/").unwrap(), "/tenant-a");
        // A client asking for another tenant just gets it under its own root.
        assert_eq!(
            scope_path("/tenant-a", "/tenant-b/secrets").unwrap(),
            "/tenant-a/tenant-b/secrets"
        );
        // Traversal is the one way out, and it is refused.
        assert!(scope_path("/tenant-a", "/../tenant-b/secrets").is_err());
        // An empty root is the whole workspace.
        assert_eq!(scope_path("", "/x").unwrap(), "/x");
        assert_eq!(scope_path("/", "/x").unwrap(), "/x");
    }

    #[test]
    fn a_record_naming_no_path_is_out_of_scope() {
        assert!(in_scope("/tenant-a", Some("/tenant-a/x")));
        assert!(!in_scope("/tenant-a", Some("/tenant-abc/x")));
        // An idle presence row tells a scoped reader a neighbour exists.
        assert!(!in_scope("/tenant-a", None));
        // …but an unscoped reader sees everything, including path-less records.
        assert!(in_scope("", None));
        assert!(in_scope("/", None));
    }

    #[test]
    fn perms_round_trip_and_reject_unknown_bits() {
        assert_eq!(Perms::parse("read,write").unwrap(), Perms::RW);
        assert_eq!(Perms::RW.as_str(), "read,write");
        assert_eq!(Perms::parse("none").unwrap(), Perms::NONE);
        assert_eq!(Perms::NONE.as_str(), "none");
        assert!(Perms::parse("execute").is_err());

        assert_eq!(Perms::from_bits(3).unwrap(), Perms::RW);
        // A row from a newer origofs must not be silently masked into something
        // narrower or wider than the operator wrote.
        // Loud, not silently masked: an authorization decision must never quietly
        // widen or narrow what the operator wrote.
        assert!(Perms::from_bits(1 | 8).is_err());
    }
}
