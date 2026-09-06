//! The posture a workspace is **born** with, versioned so it can improve.
//!
//! Every safety switch in this system defaults off — `acl.enforce_reads`,
//! `acl.default_deny`, `write.require_attribution`, trash retention, POSIX locks
//! — and each one's docs give the same reason: turning it on would change what an
//! existing deployment already does. That reasoning is sound, and it is about
//! *upgrades*. It was being applied to *new workspaces* as well, which is a
//! different population: a workspace created a minute ago has no scripts to break
//! and no operator expectations to violate. The result was that a system whose
//! premise is "every edit is recorded against the actor that made it" handed you a
//! fresh workspace with no access control at all.
//!
//! So a default is recorded **at creation**, the way `versioning` already is, and
//! the epoch it was created under is written beside it:
//!
//! - A workspace created by this build carries `defaults.epoch`, and starts with
//!   the posture that epoch names.
//! - A workspace that predates this has **no** epoch key, and nothing here ever
//!   writes one to it. It keeps exactly the behaviour it has today, forever.
//!
//! That is the whole mechanism, and it is what lets the recommended posture move
//! in a later release without the ground shifting under anyone. The alternative —
//! flipping a global default — is the thing every one of those switches already
//! refuses to do, for good reason.
//!
//! **Epochs are append-only.** Adding a setting to epoch 1 after this ships would
//! silently change what an already-created epoch-1 workspace means; add an epoch 2
//! instead. Nothing re-stamps a workspace, so an existing one keeps whichever
//! epoch it was born with even as newer ones appear.

use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::Result;
use crate::metadata::MetadataStore;

/// Config key: the defaults epoch this workspace was created under.
///
/// Absent means "created before creation-time defaults existed" — the legacy
/// posture, which is every switch off.
pub(crate) const DEFAULTS_EPOCH: &str = "defaults.epoch";

/// The epoch a workspace created by *this* build is stamped with.
///
/// Epoch 1: `acl.enforce_reads` on. It costs a registered actor nothing —
/// `effective_perms` falls back to the actor's write policy when no grant covers
/// the path, and both policies carry `READ` — so a workspace with no grants at all
/// behaves exactly as before for anyone holding an actor id. What it changes is
/// the *anonymous* door: the HTTP API's `ReadAuth` serves an unauthenticated read
/// while enforcement is off and answers 401 once it is on. For a workspace being
/// created now, closed is the right starting position, and it is one
/// `origofs acl enforce-reads off` away for anyone who disagrees.
///
/// Deliberately **not** in epoch 1:
/// - `acl.default_deny` — with no grants yet it denies every actor, so a fresh
///   workspace would refuse its own creator.
/// - `write.require_attribution` — it would make `origofs write /x` without
///   `--actor` an error, which is the README's first command and the whole
///   single-user local flow. It is guarded where it matters instead: `origofs
///   serve` refuses a non-loopback bind without it.
/// - trash retention — turning it on changes when space is reclaimed, and the
///   first anyone would learn of it is a storage bill.
/// - POSIX locks — answering `setlk` takes locking away from the kernel's local
///   handling, which works today.
pub(crate) const CURRENT_EPOCH: i64 = 1;

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// The epoch this workspace was created under, or `None` if it predates them.
    pub async fn defaults_epoch(&self) -> Result<Option<i64>> {
        Ok(self
            .meta
            .get_config(DEFAULTS_EPOCH)
            .await?
            .and_then(|v| v.parse::<i64>().ok()))
    }

    /// Stamp a **newly created** workspace with `epoch`'s posture.
    ///
    /// Idempotent by construction: every caller checks that the workspace is new
    /// first, and a re-stamp would in any case write the values it already has.
    /// It deliberately does *not* skip when the key is present — a caller that got
    /// here for an existing workspace has a bug, and quietly doing nothing would
    /// hide it — so the "is this new?" decision lives at the two call sites that
    /// can actually answer it.
    pub(crate) async fn stamp_creation_defaults(&self, epoch: i64) -> Result<()> {
        if epoch >= 1 {
            self.set_acl_enforce_reads(true).await?;
        }
        self.meta
            .set_config(DEFAULTS_EPOCH, &epoch.to_string())
            .await?;
        Ok(())
    }

    /// Give a workspace just created inside an existing store the same posture as
    /// the workspace it was created from.
    ///
    /// Multi-workspace stores are the case that would otherwise drift: the store's
    /// `default` workspace is stamped when the store itself is created, and every
    /// workspace opened from it inherits, so a tenant workspace is not quietly
    /// governed differently from the one beside it. A store created before this
    /// existed has no epoch on `default`, so its tenants keep the legacy posture
    /// too — which is the point.
    pub(crate) async fn inherit_creation_defaults(&self, from: &Self) -> Result<()> {
        if let Some(epoch) = from.defaults_epoch().await? {
            self.stamp_creation_defaults(epoch).await?;
        }
        Ok(())
    }
}
