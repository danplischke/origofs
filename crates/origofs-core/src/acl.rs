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

/// Config key: when set to `1`, an actor with no matching grant is denied rather
/// than falling back to its `write_policy`.
pub(crate) const ACL_DEFAULT_DENY: &str = "acl.default_deny";

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

    /// Every grant in this workspace, or just `actor`'s.
    pub async fn list_grants(&self, actor_id: Option<i64>) -> Result<Vec<AclGrant>> {
        self.meta.list_acl(actor_id).await
    }

    /// Whether an actor with no matching grant is denied rather than falling back
    /// to its `write_policy`.
    pub async fn acl_default_deny(&self) -> Result<bool> {
        Ok(self.meta.get_config(ACL_DEFAULT_DENY).await?.as_deref() == Some("1"))
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
            .await
    }

    /// The permissions `actor` has at `path`.
    ///
    /// Longest matching prefix wins, matched on directory boundaries by
    /// [`Scope`]. With no matching grant this falls back to the actor's
    /// `write_policy` — see the module docs on why absence means fallback rather
    /// than deny.
    pub async fn effective_perms(&self, actor_id: i64, path: &str) -> Result<Perms> {
        let grants = self.meta.list_acl(Some(actor_id)).await?;
        let best = grants
            .iter()
            .filter(|g| {
                Scope::at(&g.path_prefix)
                    .map(|s| s.contains(Some(path)))
                    .unwrap_or(false)
            })
            // Longest prefix wins. The root grant (stored as "") is length 0, so it
            // loses to every more specific one, which is exactly right.
            .max_by_key(|g| g.path_prefix.len());

        match best {
            Some(g) => Ok(g.perms),
            None if self.acl_default_deny().await? => Ok(Perms::NONE),
            None => Ok(Perms::from_policy(self.write_policy_of(actor_id).await?)),
        }
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
