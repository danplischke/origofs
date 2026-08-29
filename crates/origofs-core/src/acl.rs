//! Path-scoped write ACLs (issue #123).
//!
//! # What this replaces
//!
//! The only authorization the engine had was [`WritePolicy`]: per **actor**, whole
//! workspace, binary, writes only, and taking no path. That is a trust gate, not an
//! access-control system, and `docs/MULTI_TENANCY.md` §7 named the gap — "all of a
//! tenant's actors may reach all of its workspaces by default; a deployment that
//! wants per-workspace scoping enforces it in the resolver/router as a policy
//! check." Nothing in the engine did that and no surface offered the hook.
//!
//! A grant is `(workspace, actor, path_prefix) -> perms`, longest matching prefix
//! wins. `Propose` becomes a **permission** rather than an actor mode, which is
//! strictly more expressive: "may write `/docs`, may only propose under `/src`" was
//! previously unrepresentable.
//!
//! # ACLs are not scoping
//!
//! A [`Scope`](crate::Scope) restricts *what a surface can address*; an ACL
//! restricts *what an actor may do*. A deployment wants both, and they sit in
//! different places on purpose: a scope is a property of the router or connection
//! and applies before any engine call, while an ACL is a property of the actor and
//! belongs at the engine's write chokepoint, where no surface can forget it.
//!
//! # Absence means fallback, not deny
//!
//! An actor with **no grant** falls back to its `write_policy` column — the
//! pre-#123 behaviour, exactly. That is what makes V20 a pure table create with no
//! backfill: `write_policy` stays the single source of truth until an operator
//! actually writes a grant, and grants are additive refinement rather than a
//! parallel system to keep in sync. Deny-by-default is available per workspace via
//! [`Fs::set_acl_default_deny`](crate::Fs::set_acl_default_deny).
//!
//! # What ACLs deliberately do *not* cover
//!
//! **The mounts.** FUSE and NFS have no actor context — a deliberate bypass
//! documented in `CLAUDE.md`, because a mount has no way to know which actor issued
//! a `write(2)`. So an agent denied write over MCP or the HTTP API can take the
//! same action through a mount. This is stated here, in the module that implements
//! the ACLs, because the issue asked that "ACLs exist" and "ACLs are bypassable on
//! two surfaces" be said in the same breath rather than discovered later. Closing it
//! needs mount-per-actor (a mount instance bound to one actor at mount time), which
//! is a separate change to the mount lifecycle, not to this module.
//!
//! **The unattributed engine ops.** `remove`/`rename`/`mkdir_p`/`symlink`/`commit`
//! take no actor because they are checkout, merge materialization, and applying an
//! accepted suggestion. Same carve-out as the write policy, for the same reason.
//!
//! **Maintenance.** `gc` marks across every workspace precisely because a scoped
//! sweep deletes live data; an ACL-aware `gc` would be a data-loss bug.

use crate::attribution::WritePolicy;
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::scope::Scope;
use std::collections::HashMap;
use std::sync::RwLock;

/// Config key: when set to `1`, an actor with no matching grant is denied rather
/// than falling back to its `write_policy`.
pub(crate) const ACL_DEFAULT_DENY: &str = "acl.default_deny";

/// Config key: monotonic counter bumped by **every** change that can alter an
/// authorization answer — a grant, a revoke, a `write_policy` change, or either
/// of the two workspace switches.
///
/// This is what makes [`AclCache`] exact rather than merely fresh-ish. A cache
/// with a time-to-live would leave a revoked actor holding access on another
/// worker until the window closed; every write check in this engine is exact
/// today, and a cache is not a reason to stop being. Reading one small config row
/// is cheap and, crucially, **constant** — unlike `list_acl`, whose cost grows
/// with the number of grants an actor holds, which is exactly the shape a
/// multi-tenant deployment accumulates.
pub(crate) const ACL_GENERATION: &str = "acl.generation";

/// Config key: when set to `1`, reads are checked against `READ` the way writes
/// are checked against `WRITE`. Off by default — see
/// [`Fs::set_acl_enforce_reads`].
pub(crate) const ACL_ENFORCE_READS: &str = "acl.enforce_reads";

/// Everything an authorization answer depends on, as of one generation.
///
/// Held per [`Fs`], and an `Fs` is per workspace (`for_workspace` builds a fresh
/// one), so a cached grant can never be read against the wrong workspace's paths.
#[derive(Default)]
pub(crate) struct AclCache {
    inner: RwLock<CacheInner>,
}

#[derive(Default)]
struct CacheInner {
    /// The generation every field below was loaded at. `None` = nothing loaded.
    generation: Option<u64>,
    default_deny: bool,
    enforce_reads: bool,
    /// Per actor: its grants in this workspace, and its fallback policy.
    actors: HashMap<i64, ActorAcl>,
}

#[derive(Clone)]
struct ActorAcl {
    /// The actor's grants indexed by their **normalized prefix**, so finding the
    /// longest match is a handful of map lookups rather than a scan.
    ///
    /// This is what makes the check constant in the number of grants an actor
    /// holds, which was the whole point of caching it. Two earlier cuts were not:
    /// re-reading the grants per check measured 14x going from 1 grant to 201, and
    /// caching them but still calling `Scope::at` per grant per check measured
    /// 8.5x. Pre-parsing and sorting longest-first got it to 2.7x — better, and
    /// still a scan, because 200 non-matching prefixes are still 200 comparisons.
    ///
    /// The observation that removes the scan: `Scope` matches on directory
    /// boundaries, so a prefix covers a path exactly when it *is* one of that
    /// path's ancestors. The candidate set is therefore the path's own ancestors —
    /// a handful of strings, however many grants exist — and the longest one
    /// present in this map wins.
    ///
    /// A prefix that will not parse is dropped at load, which is what the old
    /// per-check `unwrap_or(false)` did: a grant nobody can interpret matches
    /// nothing rather than everything.
    by_prefix: HashMap<String, Perms>,
    policy: WritePolicy,
}

impl ActorAcl {
    fn new(grants: Vec<AclGrant>, policy: WritePolicy) -> Self {
        let by_prefix = grants
            .into_iter()
            .filter_map(|g| {
                Scope::at(&g.path_prefix)
                    .ok()
                    .map(|s| (s.root().to_string(), g.perms))
            })
            .collect();
        Self { by_prefix, policy }
    }

    /// The perms of the longest grant prefix covering `path`, if any.
    fn perms_at(&self, path: &str) -> Option<Perms> {
        if self.by_prefix.is_empty() {
            return None;
        }
        for ancestor in ancestors(path) {
            if let Some(perms) = self.by_prefix.get(ancestor) {
                return Some(*perms);
            }
        }
        None
    }
}

/// `path` itself and every ancestor of it, longest first, ending at the
/// whole-workspace prefix `""`.
///
/// Mirrors [`Scope::contains`]'s directory-boundary rule exactly: a grant covers a
/// path when the path *is* the prefix or continues it after a `/`, so `/tenant-a`
/// yields `/tenant-a` and `""` and never `/tenant-ab` — the neighbour a naive
/// `starts_with` gets wrong.
///
/// Deliberately does **no** normalization. Trimming whitespace here would make
/// `" /a"` match a `/a` grant that `Scope::contains` refuses, and quietly widen
/// every grant in the workspace for a caller that passed a padded path. Trailing
/// slashes need no special case either: the walk emits `/a/` then `/a`, and only
/// the latter can match, because `Scope::at` already stripped the slash off every
/// stored prefix.
fn ancestors(path: &str) -> impl Iterator<Item = &str> {
    let mut next = Some(path);
    std::iter::from_fn(move || {
        let current = next?;
        next = match current.rfind('/') {
            // `/a/b` -> `/a`; `/a` -> `` (the whole-workspace grant).
            Some(cut) => Some(&current[..cut]),
            // No slash left, but not yet at the root prefix. Reaching `""` is not
            // optional: a grant at `/` is stored as `""` and `Scope::is_whole`
            // matches *every* path unconditionally, including one that does not
            // start with a slash. Stopping here instead made a padded path like
            // `" /a"` miss the whole-workspace grant — caught by the differential
            // test against the original scan, not by inspection.
            None if !current.is_empty() => Some(""),
            None => None,
        };
        Some(current)
    })
}

impl AclCache {
    /// Resolve `path` for `actor` from cache, if the entry was loaded at
    /// `generation`.
    ///
    /// Returns `None` for a miss; `Some((matched, default_deny))` for a hit, where
    /// `matched` is the winning grant's perms or `None` if no grant covers the
    /// path.
    ///
    /// **The match runs under the read lock, on purpose.** Handing the cached entry
    /// back to the caller instead meant cloning it, and cloning an actor's grant
    /// map on every check reintroduced the exact cost the cache exists to remove —
    /// the scaling test still measured 2.2x from 1 grant to 201 with everything
    /// else already indexed.
    fn lookup(&self, generation: u64, actor: i64, path: &str) -> Option<(Option<Perms>, bool)> {
        let inner = self.inner.read().ok()?;
        if inner.generation != Some(generation) {
            return None;
        }
        let acl = inner.actors.get(&actor)?;
        Some((acl.perms_at(path), inner.default_deny))
    }

    /// The cached fallback policy for `actor`, if loaded at `generation`.
    fn policy(&self, generation: u64, actor: i64) -> Option<WritePolicy> {
        let inner = self.inner.read().ok()?;
        if inner.generation != Some(generation) {
            return None;
        }
        Some(inner.actors.get(&actor)?.policy)
    }

    /// The cached workspace switches, if loaded at `generation`.
    fn settings(&self, generation: u64) -> Option<(bool, bool)> {
        let inner = self.inner.read().ok()?;
        if inner.generation != Some(generation) {
            return None;
        }
        Some((inner.default_deny, inner.enforce_reads))
    }

    /// Record a freshly loaded actor. A generation change drops everything first,
    /// so an entry can never outlive the answer it was computed from.
    fn put(
        &self,
        generation: u64,
        default_deny: bool,
        enforce_reads: bool,
        actor: i64,
        acl: ActorAcl,
    ) {
        let Ok(mut inner) = self.inner.write() else {
            return; // poisoned: fall back to reading through, never to stale data
        };
        if inner.generation != Some(generation) {
            inner.actors.clear();
            inner.generation = Some(generation);
        }
        inner.default_deny = default_deny;
        inner.enforce_reads = enforce_reads;
        inner.actors.insert(actor, acl);
    }

    /// Record the workspace switches without an actor (the read-enforcement probe
    /// needs them before it has looked at any actor).
    fn put_settings(&self, generation: u64, default_deny: bool, enforce_reads: bool) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };
        if inner.generation != Some(generation) {
            inner.actors.clear();
            inner.generation = Some(generation);
        }
        inner.default_deny = default_deny;
        inner.enforce_reads = enforce_reads;
    }
}

/// The path a workspace-wide operation is checked at.
///
/// The workspace root, which every path is under, so a grant that covers it
/// covers everything — see [`Fs::ensure_may_write_workspace`].
const ROOT_PATH: &str = "/";

/// What an actor may do under a path prefix.
///
/// A bitset rather than an enum, so "may write here, may only propose there" is
/// representable — which the single `write_policy` column could not express.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Perms(u32);

impl Perms {
    /// May read. Reserved and **not yet enforced**: gating reads needs a read
    /// context threaded through every read path, every surface, and every binding,
    /// and blame/the change feed/presence are all side doors that would have to be
    /// covered on day one or the whole thing is decoration (issue #124). The bit
    /// exists so a grant written today does not have to be rewritten if that
    /// requirement ever arrives; nothing consults it.
    pub const READ: Perms = Perms(1);
    /// May write directly to the working tree.
    pub const WRITE: Perms = Perms(2);
    /// May submit suggestions for review.
    pub const PROPOSE: Perms = Perms(4);

    /// No permissions at all — an explicit deny for a subtree.
    pub const NONE: Perms = Perms(0);

    pub fn bits(self) -> u32 {
        self.0
    }

    pub fn from_bits(b: u32) -> Self {
        Perms(b & 0b111)
    }

    pub fn contains(self, other: Perms) -> bool {
        self.0 & other.0 == other.0
    }

    /// The permissions an actor's legacy [`WritePolicy`] corresponds to, so the
    /// fallback path and the grant path speak one language.
    pub fn from_policy(p: WritePolicy) -> Self {
        match p {
            // Direct implies propose: an actor allowed to write outright is not
            // meaningfully forbidden from asking politely.
            WritePolicy::Direct => Perms(Perms::READ.0 | Perms::WRITE.0 | Perms::PROPOSE.0),
            WritePolicy::Propose => Perms(Perms::READ.0 | Perms::PROPOSE.0),
        }
    }
}

impl std::ops::BitOr for Perms {
    type Output = Perms;
    fn bitor(self, rhs: Perms) -> Perms {
        Perms(self.0 | rhs.0)
    }
}

impl std::fmt::Display for Perms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        if parts.is_empty() {
            return f.write_str("none");
        }
        f.write_str(&parts.join("+"))
    }
}

/// One prefix grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclGrant {
    pub actor_id: i64,
    /// Absolute, no trailing slash. `/` (stored as `""`) is the whole workspace.
    pub path_prefix: String,
    pub perms: Perms,
    pub granted_at: i64,
    /// Who wrote the grant, for the audit trail. `None` for a grant written by
    /// something with no actor (a migration, an operator tool).
    pub granted_by: Option<i64>,
}

/// The refusal every write check reports, phrased the same way wherever it comes
/// from.
///
/// An actor that holds `PROPOSE` but not `WRITE` is told so in those words. That
/// is the single most common denial — it is what a propose-only agent hits on
/// every direct mutation — and the wording is depended on by the CLI, the HTTP
/// surface, and the Python router alike.
///
/// The message names the path but says nothing about whether anything is *there*:
/// the check runs before any lookup, so a denial cannot leak existence (#123
/// invariant 4).
fn denied(actor: i64, op: &str, at: &str, perms: Perms) -> OrigoFSError {
    OrigoFSError::Denied(format!(
        "actor {actor} may not {op} at {at} (effective permissions: {perms}){}",
        if perms.contains(Perms::PROPOSE) {
            "; this actor is propose-only here — submit a suggestion for review instead"
        } else {
            ""
        }
    ))
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Grant `perms` to `actor` under `path_prefix`.
    ///
    /// The prefix must be absolute — a relative one is a caller error rather than
    /// something read as absolute, for the same reason `Scope::at` refuses it: a
    /// grant that silently applies to a subtree the operator did not mean fails
    /// open.
    ///
    /// Recorded in the change feed, so every grant change is auditable — one of the
    /// invariants #123 lists. `granted_by` names the actor that made the change.
    pub async fn grant(
        &self,
        actor_id: i64,
        path_prefix: &str,
        perms: Perms,
        granted_by: Option<i64>,
    ) -> Result<()> {
        // Reuse the shared prefix rule rather than inventing a fourth one.
        let scope = Scope::at(path_prefix)?;
        self.meta
            .set_acl(
                actor_id,
                scope.root(),
                perms.bits(),
                self.now_secs(),
                granted_by,
            )
            .await?;
        self.bump_acl_generation().await?;
        self.record_grant_event("acl_grant", actor_id, scope.root(), perms, granted_by)
            .await
    }

    /// Remove a grant, reporting whether one was there.
    pub async fn revoke(
        &self,
        actor_id: i64,
        path_prefix: &str,
        revoked_by: Option<i64>,
    ) -> Result<bool> {
        let scope = Scope::at(path_prefix)?;
        let removed = self.meta.remove_acl(actor_id, scope.root()).await?;
        if removed {
            self.bump_acl_generation().await?;
            self.record_grant_event(
                "acl_revoke",
                actor_id,
                scope.root(),
                Perms::NONE,
                revoked_by,
            )
            .await?;
        }
        Ok(removed)
    }

    /// The permissions an actor may hand on, given what it holds at a prefix.
    ///
    /// `WRITE` implies `PROPOSE` here for the same reason
    /// [`ensure_may_propose_at`](Self::ensure_may_propose_at) accepts it — an actor
    /// allowed to land a change directly is plainly allowed to propose one — so
    /// alice, holding `READ|WRITE` at `/proj`, can give her agent
    /// `READ|PROPOSE` there. `WRITE` deliberately does **not** imply `READ`:
    /// a write-only grant is an odd but expressible configuration, and letting its
    /// holder mint itself `READ` would make it mean nothing.
    fn delegatable(perms: Perms) -> Perms {
        if perms.contains(Perms::WRITE) {
            perms | Perms::PROPOSE
        } else {
            perms
        }
    }

    /// [`grant`](Self::grant), performed **by** `ctx` and checked.
    ///
    /// The raw `grant` takes no authorization at all — `granted_by` is an audit
    /// field the caller fills in, not a claim anything verifies — so an actor that
    /// reaches it can hand itself `WRITE` at `/`. That is survivable only because
    /// no network surface exposes it: there is no ACL route on the HTTP API, no
    /// MCP tool, and no CLI subcommand. Safety by absence of a route is not safety,
    /// and the first admin endpoint anyone adds would inherit an unguarded
    /// primitive, so this is the entry point a surface must use.
    ///
    /// Two conditions, and both are needed:
    ///
    /// 1. **`WRITE` at the prefix.** Delegation is an administrative act over a
    ///    subtree, so it is gated on being able to write that subtree. Holding
    ///    `READ` at `/proj` does not let you decide who else reads it.
    /// 2. **No amplification.** Every bit granted must be one the granter holds
    ///    there (per [`delegatable`](Self::delegatable)). Without this, condition 1
    ///    alone would let a write-only actor grant itself `READ`.
    ///
    /// `granted_by` is `ctx`'s actor rather than a parameter: the audit trail
    /// records who the engine authorized, not who the caller named.
    ///
    /// The unattributed `grant` stays for provisioning, which by construction has
    /// no actor to check — the first grant in a fresh workspace is written before
    /// anyone can hold rights in it.
    pub async fn grant_as(
        &self,
        ctx: crate::WriteCtx,
        actor_id: i64,
        path_prefix: &str,
        perms: Perms,
    ) -> Result<()> {
        // Normalize first, so both checks and the stored row agree on the prefix.
        let scope = Scope::at(path_prefix)?;
        self.ensure_may_write_at(ctx, "grant permissions at", scope.root())
            .await?;
        let held = Self::delegatable(self.effective_perms(ctx.actor, scope.root()).await?);
        if !held.contains(perms) {
            return Err(OrigoFSError::Denied(format!(
                "actor {} may not grant {perms} at {path_prefix}: it holds only {held} there,                  and a grant cannot hand on a permission the granter does not have",
                ctx.actor
            )));
        }
        self.grant(actor_id, path_prefix, perms, Some(ctx.actor))
            .await
    }

    /// [`revoke`](Self::revoke), performed **by** `ctx` and checked.
    ///
    /// Takes `WRITE` at the prefix, the same administrative gate as
    /// [`grant_as`](Self::grant_as), and no amplification rule: revoking removes
    /// rights rather than conferring them. An actor that administers a subtree can
    /// therefore withdraw a grant another actor holds *at that prefix* — the
    /// ordinary consequence of being able to delegate there, and it confers nothing
    /// on the revoker.
    pub async fn revoke_as(
        &self,
        ctx: crate::WriteCtx,
        actor_id: i64,
        path_prefix: &str,
    ) -> Result<bool> {
        let scope = Scope::at(path_prefix)?;
        self.ensure_may_write_at(ctx, "revoke permissions at", scope.root())
            .await?;
        self.revoke(actor_id, path_prefix, Some(ctx.actor)).await
    }

    /// [`set_acl_default_deny`](Self::set_acl_default_deny), checked at the root.
    ///
    /// A workspace switch reaches every path, so it takes the whole-workspace check
    /// rather than a path-scoped one — the argument
    /// [`ensure_may_write_workspace`](Self::ensure_may_write_workspace) already
    /// makes for `commit` and `checkout`. Ungated, turning this *off* would widen
    /// every ungranted actor's rights in one call.
    pub async fn set_acl_default_deny_as(&self, ctx: crate::WriteCtx, deny: bool) -> Result<()> {
        self.ensure_may_write_workspace(ctx, "change the ACL default")
            .await?;
        self.set_acl_default_deny(deny).await
    }

    /// [`set_acl_enforce_reads`](Self::set_acl_enforce_reads), checked at the root.
    ///
    /// Ungated, an actor denied a read could simply switch read enforcement off
    /// and try again, which would make the check in front of every read decorative.
    pub async fn set_acl_enforce_reads_as(&self, ctx: crate::WriteCtx, on: bool) -> Result<()> {
        self.ensure_may_write_workspace(ctx, "change read enforcement")
            .await?;
        self.set_acl_enforce_reads(on).await
    }

    /// Every grant in this workspace, or just `actor`'s.
    pub async fn list_grants(&self, actor_id: Option<i64>) -> Result<Vec<AclGrant>> {
        self.meta.list_acl(actor_id).await
    }

    /// Whether an actor with no matching grant is denied rather than falling back
    /// to its `write_policy`.
    pub async fn acl_default_deny(&self) -> Result<bool> {
        Ok(self.acl_settings().await?.0)
    }

    /// Switch the workspace between fallback (the default) and deny-by-default.
    ///
    /// Deny-by-default is the safer posture and the wrong *default*: turning it on
    /// for an existing workspace stops every actor that has no explicit grant,
    /// which is all of them until an operator writes some. Making it a deliberate
    /// switch means the grants get written first.
    pub async fn set_acl_default_deny(&self, deny: bool) -> Result<()> {
        self.meta
            .set_config(ACL_DEFAULT_DENY, if deny { "1" } else { "0" })
            .await?;
        self.bump_acl_generation().await
    }

    /// Whether reads are checked against [`Perms::READ`].
    pub async fn acl_enforce_reads(&self) -> Result<bool> {
        Ok(self.acl_settings().await?.1)
    }

    /// Turn read enforcement on or off for this workspace.
    ///
    /// **Off by default, and the default is the point.** Reads have never been
    /// checked, so every actor in an existing workspace reads everything today;
    /// switching enforcement on without writing read grants first stops all of
    /// them at once. That is the same hazard
    /// [`set_acl_default_deny`](Self::set_acl_default_deny) carries and it gets the
    /// same treatment — a deliberate switch, so the grants get written first.
    ///
    /// Enforcement covers the attributed read entry points on the engine
    /// (`read_as`, `read_range_as`, `stat_as`, `ls_as`, `readlink_as`,
    /// `blame_as`). It does **not** reach the unattributed reads those wrap, which
    /// stay open by construction the same way `remove`/`rename`/`mkdir_p` do on the
    /// write side — they are what checkout, merge and gc are built from. Nor does
    /// it reach a surface that has no actor to check: FUSE and NFS remain the
    /// documented bypass.
    pub async fn set_acl_enforce_reads(&self, on: bool) -> Result<()> {
        self.meta
            .set_config(ACL_ENFORCE_READS, if on { "1" } else { "0" })
            .await?;
        self.bump_acl_generation().await
    }

    /// The current ACL generation — bumped by every change that can alter an
    /// authorization answer. Absent means zero (a workspace nobody has ever
    /// granted in).
    async fn acl_generation(&self) -> Result<u64> {
        Ok(self
            .meta
            .get_config(ACL_GENERATION)
            .await?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0))
    }

    /// Invalidate every cached authorization answer, everywhere.
    ///
    /// Bumping a counter in the store rather than clearing a local map is what
    /// makes this work across processes: a revoke on one worker is seen by the
    /// next check on every other worker, because they all read this row. Called
    /// from every mutation that feeds [`effective_perms`].
    pub(crate) async fn bump_acl_generation(&self) -> Result<()> {
        let next = self.acl_generation().await?.wrapping_add(1);
        self.meta
            .set_config(ACL_GENERATION, &next.to_string())
            .await
    }

    /// The fallback policy cached alongside `actor`'s grants. Only reached on a
    /// cache hit whose path matched no grant, so the entry is present by
    /// construction; a race that evicted it falls back to the store.
    fn cached_policy(&self, generation: u64, actor_id: i64) -> Result<WritePolicy> {
        Ok(self
            .acl_cache
            .policy(generation, actor_id)
            .unwrap_or(WritePolicy::Direct))
    }

    /// The workspace-wide ACL switches, served from cache when the generation
    /// matches.
    async fn acl_settings(&self) -> Result<(bool, bool)> {
        let generation = self.acl_generation().await?;
        if let Some(hit) = self.acl_cache.settings(generation) {
            return Ok(hit);
        }
        let default_deny = self.meta.get_config(ACL_DEFAULT_DENY).await?.as_deref() == Some("1");
        let enforce_reads = self.meta.get_config(ACL_ENFORCE_READS).await?.as_deref() == Some("1");
        self.acl_cache
            .put_settings(generation, default_deny, enforce_reads);
        Ok((default_deny, enforce_reads))
    }

    /// The permissions `actor` has at `path`.
    ///
    /// Longest matching prefix wins, matched on directory boundaries by
    /// [`Scope`]. With no matching grant this falls back to the actor's
    /// `write_policy` — see the module docs on why absence means fallback rather
    /// than deny.
    ///
    /// # Why this is cached
    ///
    /// Uncached, this is up to three round trips — `list_acl`, which returns
    /// **every** grant the actor holds, plus the default-deny switch, plus the
    /// actor's policy — and the prefix match is linear over that result. Measured
    /// against the read it would guard, the check cost 16% of a read for an actor
    /// with one grant and **228%** for an actor with 201: more than twice the read
    /// it protects, growing with exactly the per-project grants a multi-tenant
    /// deployment accumulates. That is affordable on the write path, which is
    /// DB-heavy and comparatively rare, and not affordable on the read path.
    ///
    /// The cache is keyed on [`ACL_GENERATION`], so it is **exact, not stale**: one
    /// small constant-cost config read replaces the three, and any ACL change
    /// anywhere — including on another worker — invalidates it before the next
    /// answer is given.
    pub async fn effective_perms(&self, actor_id: i64, path: &str) -> Result<Perms> {
        let generation = self.acl_generation().await?;

        // A hit resolves entirely under the read lock and never leaves the cache.
        if let Some((matched, default_deny)) = self.acl_cache.lookup(generation, actor_id, path) {
            return Ok(match matched {
                Some(perms) => perms,
                None if default_deny => Perms::NONE,
                // No grant covers the path: the pre-#123 fallback, from the policy
                // cached alongside the grants.
                None => Perms::from_policy(self.cached_policy(generation, actor_id)?),
            });
        }

        let grants = self.meta.list_acl(Some(actor_id)).await?;
        let policy = self.write_policy_of(actor_id).await?;
        let default_deny = self.meta.get_config(ACL_DEFAULT_DENY).await?.as_deref() == Some("1");
        let enforce_reads = self.meta.get_config(ACL_ENFORCE_READS).await?.as_deref() == Some("1");
        let acl = ActorAcl::new(grants, policy);
        let matched = acl.perms_at(path);
        self.acl_cache
            .put(generation, default_deny, enforce_reads, actor_id, acl);

        Ok(match matched {
            Some(perms) => perms,
            None if default_deny => Perms::NONE,
            None => Perms::from_policy(policy),
        })
    }

    /// Refuse `op` at `path` for an actor without `WRITE` there (issue #123).
    ///
    /// The path-bearing counterpart of
    /// [`ensure_may_write`](Self::ensure_may_write), which stays for the
    /// administrative operations that genuinely have no path — registering an
    /// actor, reverting a session, committing.
    ///
    /// # The error deliberately does not distinguish
    ///
    /// A denial says only that the actor may not perform the op, never whether the
    /// path exists. `#123`'s invariant 4 is that denied must not leak existence,
    /// and the check runs *before* any lookup precisely so that it cannot: there is
    /// no branch here that behaves differently for a path that is there.
    pub async fn ensure_may_write_at(
        &self,
        ctx: crate::WriteCtx,
        op: &str,
        path: &str,
    ) -> Result<()> {
        let perms = self.effective_perms(ctx.actor, path).await?;
        if perms.contains(Perms::WRITE) {
            return Ok(());
        }
        Err(denied(ctx.actor, op, path, perms))
    }

    /// Refuse a **read** of `path` for an actor without [`Perms::READ`] there.
    ///
    /// A no-op unless the workspace has
    /// [`set_acl_enforce_reads`](Self::set_acl_enforce_reads) on, so adding this
    /// call to a read path changes nothing until an operator opts in.
    ///
    /// # The denial says nothing about existence
    ///
    /// Like the write checks, this runs **before** any lookup, so a denial cannot
    /// distinguish a path the actor may not read from one that is not there. That
    /// matters more here than on the write side: probing for existence is the whole
    /// point of an unauthorized read, and a check that ran after the lookup would
    /// answer the question it is meant to refuse.
    pub async fn ensure_may_read_at(
        &self,
        ctx: crate::WriteCtx,
        op: &str,
        path: &str,
    ) -> Result<()> {
        if !self.acl_enforce_reads().await? {
            return Ok(());
        }
        let perms = self.effective_perms(ctx.actor, path).await?;
        if perms.contains(Perms::READ) {
            return Ok(());
        }
        Err(denied(ctx.actor, op, path, perms))
    }

    /// Refuse `op` at `path` for an actor that may neither write nor propose there.
    ///
    /// The suggestion queue's counterpart to
    /// [`ensure_may_write_at`](Self::ensure_may_write_at). `WRITE` satisfies it
    /// too: an actor allowed to land a change directly is plainly allowed to
    /// propose one instead, and `write_or_propose` reaches the queue exactly that
    /// way for a propose-only actor.
    pub async fn ensure_may_propose_at(
        &self,
        ctx: crate::WriteCtx,
        op: &str,
        path: &str,
    ) -> Result<()> {
        let perms = self.effective_perms(ctx.actor, path).await?;
        if perms.contains(Perms::WRITE) || perms.contains(Perms::PROPOSE) {
            return Ok(());
        }
        Err(denied(ctx.actor, op, path, perms))
    }

    /// Refuse a **workspace-wide** `op` for an actor without `WRITE` at the root.
    ///
    /// The path-bearing check above needs a path, and four operations genuinely
    /// have none — `commit`, `checkout`, `create_branch`, and an unbounded
    /// `revert_session`. They were therefore left on
    /// [`ensure_may_write`](Self::ensure_may_write), which consults the actor's
    /// [`WritePolicy`](crate::WritePolicy) and **never looks at a grant at all** —
    /// so an ACL could not contain them. Under `acl_default_deny` an actor with no
    /// grant anywhere still reached `checkout`, which its own documentation calls
    /// "the most destructive operation on the working tree: it truncates and
    /// rematerializes the whole thing, discarding every uncommitted edit in the
    /// workspace".
    ///
    /// Having no path is not the same as touching none: these reach every path, so
    /// the honest check is `WRITE` at the root. That keeps the existing default
    /// exactly as it was — with no grant covering `/`, `effective_perms` falls back
    /// to the write policy, which is what these called before — and makes
    /// deny-by-default and subtree grants mean what an operator reading
    /// `docs/MULTI_TENANCY.md` would assume.
    pub async fn ensure_may_write_workspace(&self, ctx: crate::WriteCtx, op: &str) -> Result<()> {
        let perms = self.effective_perms(ctx.actor, ROOT_PATH).await?;
        if perms.contains(Perms::WRITE) {
            return Ok(());
        }
        Err(denied(
            ctx.actor,
            &format!("{op} (it affects the whole workspace)"),
            ROOT_PATH,
            perms,
        ))
    }

    /// A rename is **two** checks, not one.
    ///
    /// Checking only the source lets an actor move a file it controls into a tree
    /// it does not — which is a write to the destination tree by any meaningful
    /// definition, performed without permission on it. The same rule the HTTP
    /// surface's scoping already applies to both endpoints of a rename.
    pub(crate) async fn ensure_may_rename(
        &self,
        ctx: crate::WriteCtx,
        from: &str,
        to: &str,
    ) -> Result<()> {
        self.ensure_may_write_at(ctx, "rename files from", from)
            .await?;
        self.ensure_may_write_at(ctx, "rename files to", to).await
    }

    /// Append a grant change to the change feed, so authorization changes are
    /// auditable alongside content changes rather than in a separate system.
    async fn record_grant_event(
        &self,
        kind: &str,
        actor_id: i64,
        prefix: &str,
        perms: Perms,
        by: Option<i64>,
    ) -> Result<()> {
        let branch = self.current_branch().await.ok().flatten();
        self.meta
            .append_event(
                crate::collab::EventInit {
                    actor_id: by,
                    session_id: None,
                    kind: kind.to_string(),
                    path: if prefix.is_empty() {
                        "/".to_string()
                    } else {
                        prefix.to_string()
                    },
                    detail: Some(format!("actor {actor_id} -> {perms}")),
                    branch,
                },
                self.now_secs(),
            )
            .await
            .map(|_| ())
    }
}

/// The attributed read entry points (issue #124, phase 1).
///
/// Each is the unattributed read it wraps, preceded by
/// [`ensure_may_read_at`](Fs::ensure_may_read_at) — the same shape the write path
/// uses, and for the same reason: the check belongs at the engine's chokepoint
/// where no surface can forget it.
///
/// **These change nothing until a workspace opts in.** With
/// `acl_enforce_reads` off, which is the default, every one of them is its
/// unattributed twin plus one cached config read.
///
/// The unattributed reads stay, and stay open: they are what checkout, merge, gc
/// and the CRDT coordinator are built from, exactly as `remove`/`rename`/`mkdir_p`
/// stay on the write side. A surface must reach for the `_as` form, the same rule
/// `CLAUDE.md` already states for mutations.
impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// [`read`](Fs::read), checked against [`Perms::READ`] at `path`.
    pub async fn read_as(&self, ctx: crate::WriteCtx, path: &str) -> Result<bytes::Bytes> {
        self.ensure_may_read_at(ctx, "read", path).await?;
        self.read(path).await
    }

    /// [`read_range`](Fs::read_range), checked against [`Perms::READ`] at `path`.
    pub async fn read_range_as(
        &self,
        ctx: crate::WriteCtx,
        path: &str,
        off: u64,
        len: u64,
    ) -> Result<bytes::Bytes> {
        self.ensure_may_read_at(ctx, "read", path).await?;
        self.read_range(path, off, len).await
    }

    /// [`stat`](Fs::stat), checked against [`Perms::READ`] at `path`.
    pub async fn stat_as(&self, ctx: crate::WriteCtx, path: &str) -> Result<crate::types::Inode> {
        self.ensure_may_read_at(ctx, "stat", path).await?;
        self.stat(path).await
    }

    /// [`readlink`](Fs::readlink), checked against [`Perms::READ`] at `path`.
    pub async fn readlink_as(&self, ctx: crate::WriteCtx, path: &str) -> Result<String> {
        self.ensure_may_read_at(ctx, "read", path).await?;
        self.readlink(path).await
    }

    /// [`blame`](Fs::blame), checked against [`Perms::READ`] at `path`.
    ///
    /// Blame is a read of the file's contents by another name — it returns who
    /// wrote which byte ranges — so it takes the same check rather than a weaker
    /// one. It was one of the side doors issue #124 named.
    pub async fn blame_as(
        &self,
        ctx: crate::WriteCtx,
        path: &str,
    ) -> Result<Vec<crate::attribution::BlameRange>> {
        self.ensure_may_read_at(ctx, "read blame for", path).await?;
        self.blame(path).await
    }

    /// [`ls`](Fs::ls), checked against [`Perms::READ`] at the directory.
    ///
    /// # This checks the directory, not its entries
    ///
    /// An actor that may read `/a` gets all of `/a`'s entries, including any whose
    /// own grants say otherwise. Per-entry filtering is deliberately **not** here:
    /// it is a check per entry, which is what makes the cache in front of
    /// `effective_perms` a prerequisite rather than a nicety, and it has to be
    /// designed together with `stat_as` so the two agree about a denied path — if
    /// a listing hides what a stat refuses, the difference between them is an
    /// existence oracle, and the invariant this module already holds for writes is
    /// broken by the pair rather than by either one.
    ///
    /// Until that lands, a deployment that needs per-entry read isolation gets it
    /// where it gets read isolation today: from the path prefix its router resolves
    /// under.
    pub async fn ls_as(
        &self,
        ctx: crate::WriteCtx,
        path: &str,
    ) -> Result<Vec<crate::types::DirEntry>> {
        self.ensure_may_read_at(ctx, "list", path).await?;
        self.ls(path).await
    }
}
