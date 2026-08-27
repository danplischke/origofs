//! Usage accounting, `statfs`, and quotas (issues #116, #119).
//!
//! Before this, `MetadataStore::child_count` was the only per-directory stat in
//! the tree: no recursive stats, no `du`, no quota, and no `statfs` anywhere — so
//! `df` on a mount reported nothing meaningful, which real tooling notices.
//!
//! The two halves depend on each other and so landed together. `statfs` needs
//! something to report, which usage accounting provides; a quota needs something
//! to check, which is the same number.
//!
//! # What "used" means here
//!
//! Everything in this module reports **logical** bytes — the sum of `inode.size` —
//! not the deduplicated, chunked, possibly-compressed on-disk footprint. That is
//! deliberate and it is what `df` and `du` should say:
//!
//! * it is the number a user can act on ("this file is 4 GiB"), whereas the
//!   physical footprint moves when an unrelated workspace happens to write the
//!   same bytes;
//! * a quota expressed in physical bytes would be unpredictable for exactly that
//!   reason — the same write could fit or not depending on what someone else
//!   stored;
//! * and the physical figure is a property of the *content store*, which the
//!   metadata store cannot see. Asking a remote bucket to total itself on every
//!   `statfs` is not a thing a mount can do.
//!
//! An inode reachable by several names (a hard link, since #119) counts once.

use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::types::Ino;

/// Config key holding the workspace's capacity limit, in bytes. Absent = no limit.
const QUOTA_BYTES: &str = "quota.bytes";
/// Config key holding the workspace's inode limit. Absent = no limit.
const QUOTA_INODES: &str = "quota.inodes";

/// The block size `statfs` reports and denominates its counts in.
///
/// origofs stores content-defined chunks, not fixed blocks, so it has no native
/// block size; this is purely the unit `statfs` speaks. 4 KiB is what callers
/// expect and what makes `df` output look ordinary.
pub const STATFS_BLOCK_SIZE: u32 = 4096;

/// What a subtree (or a whole workspace) occupies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// Number of inodes, counting an inode with several names once.
    pub inodes: u64,
    /// Summed logical size of those inodes. See the module docs.
    pub bytes: u64,
}

/// A workspace's capacity limits (issue #116). `None` in either field is "no
/// limit", which is the default and what every existing workspace has.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Quota {
    pub bytes: Option<u64>,
    pub inodes: Option<u64>,
}

impl Quota {
    /// Whether either limit is set at all — the fast path for the write hook.
    pub fn is_unlimited(&self) -> bool {
        self.bytes.is_none() && self.inodes.is_none()
    }
}

/// The answer to a `statfs(2)` (issue #119).
///
/// A workspace has no intrinsic capacity, so with no quota set the "total" figures
/// are synthesized: see [`Fs::statfs`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FsStat {
    pub block_size: u32,
    pub total_blocks: u64,
    pub free_blocks: u64,
    pub total_inodes: u64,
    pub free_inodes: u64,
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Usage of the whole current workspace — one aggregate query, cheap enough to
    /// sit behind `statfs` on a mount.
    pub async fn usage(&self) -> Result<Usage> {
        let (inodes, bytes) = self.meta.workspace_usage().await?;
        Ok(Usage { inodes, bytes })
    }

    /// Recursive usage of the subtree at `path` — the `du` primitive (issue #116).
    ///
    /// Runs as a single recursive query in the store rather than a walk from here,
    /// so it costs one round trip rather than one per directory level. It is still
    /// proportional to the size of the subtree, so it is a reporting call and not
    /// something to put on a hot path.
    pub async fn du(&self, path: &str) -> Result<Usage> {
        let ino = self.resolve(path).await?;
        self.du_ino(ino).await
    }

    /// [`du`](Self::du) by inode, for the mount surfaces.
    pub async fn du_ino(&self, ino: Ino) -> Result<Usage> {
        let (inodes, bytes) = self.meta.subtree_usage(ino).await?;
        Ok(Usage { inodes, bytes })
    }

    /// The workspace's quota, or an all-`None` [`Quota`] if none is set.
    pub async fn quota(&self) -> Result<Quota> {
        let parse = |v: Option<String>| -> Option<u64> { v.and_then(|s| s.parse::<u64>().ok()) };
        Ok(Quota {
            bytes: parse(self.meta.get_config(QUOTA_BYTES).await?),
            inodes: parse(self.meta.get_config(QUOTA_INODES).await?),
        })
    }

    /// Set (or clear) the workspace's quota.
    ///
    /// Setting a limit **below** current usage is allowed and is not retroactive:
    /// nothing is deleted and no existing file becomes unreadable — further growth
    /// is simply refused until usage falls back under the limit. That is the same
    /// thing every filesystem quota does, and the alternative (refusing to set it)
    /// would make a quota impossible to introduce on a workspace that already has
    /// data, which is the only interesting case.
    pub async fn set_quota(&self, quota: Quota) -> Result<()> {
        // There is no `delete_config` on the store, and an empty value is a
        // perfectly good "unset": `"".parse::<u64>()` fails, which `quota()`
        // already reads as no limit. Writing the key rather than leaving a stale
        // one behind is what makes clearing a quota actually clear it.
        let clear = String::new();
        let b = quota.bytes.map(|v| v.to_string()).unwrap_or(clear.clone());
        let i = quota.inodes.map(|v| v.to_string()).unwrap_or(clear);
        self.meta.set_config(QUOTA_BYTES, &b).await?;
        self.meta.set_config(QUOTA_INODES, &i).await?;
        Ok(())
    }

    /// Answer a `statfs(2)` (issue #119).
    ///
    /// With a quota set, the totals are the quota — which is the honest answer and
    /// makes `df` show a real percentage.
    ///
    /// With no quota, a workspace has no capacity to report: its ceiling is the
    /// object store's, which is effectively unbounded and not knowable from here.
    /// Reporting zero would make `df` print a 100%-full filesystem and make some
    /// installers refuse to run — the exact class of "weird failure in ordinary
    /// tools" #119 is about. So the total is synthesized as a fixed nominal
    /// capacity: the used column is real, free shrinks as the workspace grows, and
    /// `df` looks and behaves like `df`.
    ///
    /// The nominal figure is a floor, not a cap — past it the total grows to keep a
    /// headroom, so a workspace larger than the nominal capacity still never
    /// reports full. (Making the total `used + headroom` unconditionally, which is
    /// the obvious first move, is wrong for the opposite reason: free would then be
    /// a constant and `df` would never move no matter how much was written.)
    pub async fn statfs(&self) -> Result<FsStat> {
        /// Nominal capacity reported when no quota is set: 1 TiB, in bytes.
        const NOMINAL: u64 = 1 << 40;

        let used = self.usage().await?;
        let quota = self.quota().await?;

        let bs = STATFS_BLOCK_SIZE as u64;
        // Headroom is a fraction of the nominal rather than an absolute, so it is
        // right in *both* axes: an absolute sized for bytes dwarfs the inode
        // nominal, which puts that axis permanently on the `used + headroom`
        // branch — and free inodes then never move, the very thing this shape
        // exists to avoid.
        let synth = |used: u64, nominal: u64| nominal.max(used.saturating_add(nominal / 64));
        let total_bytes = quota.bytes.unwrap_or_else(|| synth(used.bytes, NOMINAL));
        let total_inodes = quota
            .inodes
            .unwrap_or_else(|| synth(used.inodes, NOMINAL / bs));

        Ok(FsStat {
            block_size: STATFS_BLOCK_SIZE,
            total_blocks: total_bytes.div_ceil(bs),
            // saturating: a quota set below current usage reports zero free rather
            // than underflowing into an enormous one.
            free_blocks: total_bytes.saturating_sub(used.bytes).div_ceil(bs),
            total_inodes,
            free_inodes: total_inodes.saturating_sub(used.inodes),
        })
    }

    /// Refuse a write that would take the workspace past its quota (issue #116).
    ///
    /// Called from the write and create paths with what the operation is about to
    /// add. `Ok(())` when no quota is set, which is the default and therefore the
    /// overwhelmingly common case — one config read, no aggregate query.
    ///
    /// # Why this is not in `ensure_may_write`
    ///
    /// That would have been the tidy place, and it is wrong. `ensure_may_write`
    /// gates *attributed* mutations only, and is deliberately exempt for the
    /// unattributed internal machinery — checkout, merge materialization, applying
    /// an accepted suggestion. The issue flagged that those two exemptions are "not
    /// obviously the same exemption", and they are not:
    ///
    /// * the write policy exempts internal ops because they have no actor to judge;
    /// * a quota must exempt them because they materialize data the workspace has
    ///   **already accepted**. A checkout that fails half way because the commit it
    ///   is restoring does not fit under a quota set afterwards leaves the working
    ///   tree wrecked and the user with no way out — the quota would have made the
    ///   workspace unusable rather than bounded.
    ///
    /// So quota binds *new* bytes entering the workspace, and never the
    /// re-materialization of bytes already committed.
    /// Quota check for a whole-file write to `path` that will end up `new_size`
    /// bytes (issue #116).
    ///
    /// A write **replaces** a body, so what it adds is the growth over what is
    /// already there, not the whole new size — otherwise rewriting one byte of an
    /// existing file inside a full workspace would be refused. A new file also adds
    /// an inode; an overwrite does not.
    ///
    /// The old size is looked up **only when a quota is actually set**, so a
    /// workspace without one (the default, and the overwhelming majority) pays two
    /// config reads and no extra inode query on the write path.
    pub(crate) async fn check_quota_for_path(&self, path: &str, new_size: u64) -> Result<()> {
        if self.quota().await?.is_unlimited() {
            return Ok(());
        }
        let old = match self.resolve(path).await {
            Ok(ino) => self.meta.get_inode(ino).await?.map(|i| i.size),
            // Not found: this write creates the file.
            Err(_) => None,
        };
        self.check_quota_delta(old, new_size).await
    }

    /// [`check_quota_for_path`](Self::check_quota_for_path) for an inode that is
    /// known to exist — the mount write/truncate paths, which never create here.
    pub(crate) async fn check_quota_for_ino(&self, ino: Ino, new_size: u64) -> Result<()> {
        if self.quota().await?.is_unlimited() {
            return Ok(());
        }
        let old = self.meta.get_inode(ino).await?.map(|i| i.size);
        self.check_quota_delta(old, new_size).await
    }

    /// Shared tail of the two above: `old` is `None` when the write creates.
    async fn check_quota_delta(&self, old: Option<u64>, new_size: u64) -> Result<()> {
        let add_bytes = new_size.saturating_sub(old.unwrap_or(0));
        let add_inodes = u64::from(old.is_none());
        self.check_quota(add_bytes, add_inodes).await
    }

    pub(crate) async fn check_quota(&self, add_bytes: u64, add_inodes: u64) -> Result<()> {
        let quota = self.quota().await?;
        if quota.is_unlimited() {
            return Ok(());
        }
        let used = self.usage().await?;
        if let Some(limit) = quota.bytes {
            let after = used.bytes.saturating_add(add_bytes);
            if after > limit {
                return Err(OrigoFSError::TooLarge(format!(
                    "workspace quota exceeded: {after} bytes would exceed the {limit}-byte \
                     limit (currently {} used)",
                    used.bytes
                )));
            }
        }
        if let Some(limit) = quota.inodes {
            let after = used.inodes.saturating_add(add_inodes);
            if after > limit {
                return Err(OrigoFSError::TooLarge(format!(
                    "workspace inode quota exceeded: {after} inodes would exceed the \
                     {limit} limit (currently {} used)",
                    used.inodes
                )));
            }
        }
        Ok(())
    }
}
