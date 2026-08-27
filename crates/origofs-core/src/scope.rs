//! Path scoping: restricting a *surface* to a subtree of a workspace (issue #125).
//!
//! This is the engine-side home of the four properties `origofs.fastapi`'s router
//! worked out, which were for a long time the **only** working per-path access
//! control in the repository — and Python-only, which is the more dangerous
//! direction of a parity gap, because a Rust embedder reading the Python docs
//! would reasonably assume the scoping lived in the shared layer.
//!
//! # Scoping is not authorization
//!
//! A [`Scope`] restricts *what a surface can address*. An ACL restricts *what an
//! actor may do* (issue #123). A deployment wants both, and they are enforced in
//! different places for a reason: a scope is a property of the connection or the
//! router that served the request, so it can be applied before any engine call and
//! cannot be forgotten by an individual handler; an ACL is a property of the actor
//! and belongs at the engine's write chokepoint.
//!
//! # The four properties
//!
//! Each of these is load-bearing, and three of them are things a naive
//! implementation gets wrong:
//!
//! 1. **Directory-boundary matching, not `starts_with`.** `/tenant-a` must not
//!    cover `/tenant-abc` — precisely the neighbour a scope exists to exclude.
//! 2. **Prepend, do not compare.** A caller's path is resolved *inside* the root,
//!    so a request for another tenant's data is not representable at all rather
//!    than being representable and rejected.
//! 3. **A `None` path is outside every scope.** A record naming no path — an idle
//!    presence row — still tells a scoped reader that a neighbour exists.
//! 4. **Out of scope is "not found", never "forbidden".** A scoped caller must not
//!    be able to tell "this exists but is not yours" from "this does not exist".
//!    That is a decision for the surface to honour when it maps the refusal to a
//!    status code; see [`ScopeError`].

use crate::error::{OrigoFSError, Result};

/// A surface's view of a workspace: everything, or one subtree.
///
/// Cheap to clone and to pass per request. [`Scope::whole`] is the unscoped case
/// and short-circuits every check, so a surface can hold one unconditionally
/// rather than threading an `Option` through every handler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scope {
    /// Absolute, no trailing slash. Empty means the whole workspace.
    root: String,
}

impl Default for Scope {
    fn default() -> Self {
        Self::whole()
    }
}

/// Why a path was refused. Distinct from [`OrigoFSError`] so a surface can map the
/// two cases to different statuses without string-matching.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScopeError {
    /// The path was malformed — it contained a `..` component.
    ///
    /// A caller error, so a surface should answer `400`. Distinguishable from
    /// [`OutOfScope`](Self::OutOfScope) on purpose: this reveals nothing about
    /// what exists, because it is refused before any lookup.
    Traversal,
    /// The path or record lies outside the scope.
    ///
    /// A surface **must** answer `404` here, not `403`: a `403` confirms that
    /// something exists at a path the caller may not see, which is exactly the
    /// inference a scope exists to prevent.
    OutOfScope,
}

impl Scope {
    /// The whole workspace — no scoping. Every path resolves to itself and every
    /// record is in scope.
    pub fn whole() -> Self {
        Scope {
            root: String::new(),
        }
    }

    /// A scope rooted at `root`, which **must be absolute**.
    ///
    /// A trailing slash is accepted and ignored, so `"/tenant-a"` and
    /// `"/tenant-a/"` are the same scope, and `"/"` is [`whole`](Self::whole).
    ///
    /// A *relative* root is a caller error rather than something quietly read as
    /// absolute. Reinterpreting `"tenant-a"` as `/tenant-a` looks helpful and is
    /// the wrong shape for this type: the root decides what a surface can reach at
    /// all, so guessing at an ambiguous one risks silently scoping to a subtree
    /// the caller did not mean — and a scope that is wrong in that direction fails
    /// open. Callers that genuinely want normalization can do it themselves, where
    /// the decision is visible.
    pub fn at(root: &str) -> Result<Self> {
        let trimmed = root.trim();
        if !trimmed.starts_with('/') && !trimmed.is_empty() {
            return Err(OrigoFSError::InvalidArgument(format!(
                "scope root must be absolute, got {root:?}"
            )));
        }
        let normalized = trimmed.trim_end_matches('/').to_string();
        // A root with `..` in it would let the scope itself escape, so it is
        // refused where the scope is *built* rather than on every request.
        if normalized.split('/').any(|c| c == "..") {
            return Err(OrigoFSError::InvalidPath(format!(
                "scope root may not contain '..': {root:?}"
            )));
        }
        if normalized.contains('\0') {
            return Err(OrigoFSError::InvalidPath(
                "scope root may not contain NUL".into(),
            ));
        }
        Ok(Scope { root: normalized })
    }

    /// Whether this scope covers the whole workspace.
    pub fn is_whole(&self) -> bool {
        self.root.is_empty()
    }

    /// The normalized root, or `""` for the whole workspace.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Whether `path` is the root itself or sits beneath it.
    ///
    /// **Directory-boundary matching, not `starts_with`**: `/tenant-a` does not
    /// cover `/tenant-abc`. A `None` path is under no scope but the whole one —
    /// see the module docs on why an idle presence row is a leak.
    pub fn contains(&self, path: Option<&str>) -> bool {
        if self.is_whole() {
            return true;
        }
        let Some(path) = path else {
            return false;
        };
        path == self.root
            || path
                .strip_prefix(&self.root)
                .is_some_and(|rest| rest.starts_with('/'))
    }

    /// Resolve a caller-supplied path *inside* this scope.
    ///
    /// The caller's path is always relative to the root, so nothing outside the
    /// scope is representable: there is no request that reaches
    /// `/other-tenant/secrets`, because the root is prepended rather than compared
    /// against.
    ///
    /// A `..` component is refused. `validate_component` in the engine already
    /// refuses to *store* one, but that is a different guarantee — it stops a
    /// poisoned name being persisted, not a path resolving out of its scope here.
    pub fn resolve(&self, path: &str) -> std::result::Result<String, ScopeError> {
        let abs = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        if abs.split('/').any(|c| c == "..") {
            return Err(ScopeError::Traversal);
        }
        if self.is_whole() {
            return Ok(abs);
        }
        Ok(if abs == "/" {
            self.root.clone()
        } else {
            format!("{}{}", self.root, abs)
        })
    }

    /// Resolve an optional path, leaving `None` alone.
    ///
    /// A `None` filter means "no path filter", which under a scope must become
    /// "everything in my scope" rather than "everything" — so callers pair this
    /// with [`contains`](Self::contains) on the results.
    pub fn resolve_opt(
        &self,
        path: Option<&str>,
    ) -> std::result::Result<Option<String>, ScopeError> {
        path.map(|p| self.resolve(p)).transpose()
    }

    /// Refuse a record that lies outside this scope.
    ///
    /// Use on anything addressed by something *other* than a path — a suggestion
    /// id, a lock id — where the caller could otherwise probe for a neighbour's
    /// records by guessing ids. Suggestion ids are workspace-global, so knowing an
    /// id was enough.
    pub fn require(&self, path: Option<&str>) -> std::result::Result<(), ScopeError> {
        if self.contains(path) {
            Ok(())
        } else {
            Err(ScopeError::OutOfScope)
        }
    }

    /// Keep only the records that fall inside this scope.
    ///
    /// For the workspace-wide listings — the change feed, presence, suggestions,
    /// locks, conflicts — which are the side doors a path-only scope would leave
    /// open. `key` extracts each item's path; an item with no path is dropped.
    pub fn filter<T>(&self, items: Vec<T>, key: impl Fn(&T) -> Option<&str>) -> Vec<T> {
        if self.is_whole() {
            return items;
        }
        items
            .into_iter()
            .filter(|i| self.contains(key(i)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_root_is_normalized() {
        for spelling in ["/tenant-a", "/tenant-a/", "  /tenant-a  "] {
            assert_eq!(
                Scope::at(spelling).unwrap().root(),
                "/tenant-a",
                "{spelling:?}"
            );
        }
        assert!(Scope::at("/").unwrap().is_whole());
        assert!(Scope::at("").unwrap().is_whole());
    }

    /// A relative root is refused rather than read as absolute. Guessing at an
    /// ambiguous root risks scoping to a subtree the caller did not mean, and a
    /// scope that is wrong in that direction fails open.
    #[test]
    fn a_relative_root_is_refused() {
        assert!(Scope::at("tenant-a").is_err());
        assert!(Scope::at("a/b").is_err());
    }

    #[test]
    fn a_root_may_not_traverse() {
        assert!(Scope::at("/a/../b").is_err());
        assert!(Scope::at("/..").is_err());
    }

    /// Property 1. The regression a `starts_with` implementation has.
    #[test]
    fn matching_is_on_a_directory_boundary() {
        let s = Scope::at("/tenant-a").unwrap();
        assert!(s.contains(Some("/tenant-a")));
        assert!(s.contains(Some("/tenant-a/f.txt")));
        assert!(s.contains(Some("/tenant-a/deep/f.txt")));
        assert!(
            !s.contains(Some("/tenant-abc")),
            "`/tenant-a` must not cover `/tenant-abc` — the exact neighbour a \
             scope exists to exclude"
        );
        assert!(!s.contains(Some("/tenant-abc/f.txt")));
        assert!(!s.contains(Some("/other")));
        assert!(!s.contains(Some("/")));
    }

    /// Property 2. Out-of-scope paths are not representable, not merely refused.
    #[test]
    fn resolution_prepends_rather_than_compares() {
        let s = Scope::at("/tenant-a").unwrap();
        assert_eq!(s.resolve("/f.txt").unwrap(), "/tenant-a/f.txt");
        assert_eq!(s.resolve("f.txt").unwrap(), "/tenant-a/f.txt");
        assert_eq!(s.resolve("/").unwrap(), "/tenant-a");
        // The interesting one: asking for another tenant lands inside your own.
        assert_eq!(
            s.resolve("/other-tenant/secrets").unwrap(),
            "/tenant-a/other-tenant/secrets",
            "a path naming another tenant must resolve inside the caller's own \
             root, not reach the neighbour"
        );
    }

    #[test]
    fn traversal_is_refused() {
        let s = Scope::at("/tenant-a").unwrap();
        assert_eq!(s.resolve("/../other").unwrap_err(), ScopeError::Traversal);
        assert_eq!(s.resolve("/a/../../b").unwrap_err(), ScopeError::Traversal);
        // Even unscoped: a `..` is malformed regardless of scoping.
        assert_eq!(
            Scope::whole().resolve("/a/../b").unwrap_err(),
            ScopeError::Traversal
        );
        // `..` as a *substring* is fine — only whole components traverse.
        assert_eq!(s.resolve("/a..b").unwrap(), "/tenant-a/a..b");
    }

    /// Property 3. A record naming no path still tells a scoped reader that a
    /// neighbour exists.
    #[test]
    fn a_pathless_record_is_outside_every_scope() {
        let s = Scope::at("/tenant-a").unwrap();
        assert!(!s.contains(None));
        assert_eq!(s.require(None).unwrap_err(), ScopeError::OutOfScope);
        // ...but the whole-workspace scope has nothing to hide.
        assert!(Scope::whole().contains(None));
    }

    #[test]
    fn the_whole_scope_short_circuits() {
        let s = Scope::whole();
        assert!(s.contains(Some("/anything")));
        assert_eq!(s.resolve("/anything").unwrap(), "/anything");
        assert!(s.require(Some("/anything")).is_ok());
    }

    #[test]
    fn filtering_drops_neighbours_and_pathless_rows() {
        let s = Scope::at("/tenant-a").unwrap();
        let rows = vec![
            Some("/tenant-a/f"),
            Some("/tenant-abc/f"),
            None,
            Some("/tenant-a"),
            Some("/other"),
        ];
        let kept = s.filter(rows, |r| *r);
        assert_eq!(kept, vec![Some("/tenant-a/f"), Some("/tenant-a")]);
    }
}
