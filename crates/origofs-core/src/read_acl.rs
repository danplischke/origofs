//! Attributed (permission-checked) reads (`docs/PERMISSIONS.md` §3c, issue #124).
//!
//! Writes got a path-scoped gate in #123 because every attributed mutation already
//! funnelled through one function. Reads had no equivalent: [`Fs::read`],
//! [`Fs::ls`], [`Fs::stat`] and [`Fs::blame`] take no actor at all, so there was
//! nothing to check against and no way for a surface to ask.
//!
//! This module adds the `*_as` counterparts — the same split the write path has
//! used since M4. The unattributed originals stay, ungated, because they are what
//! checkout, merge, gc, recovery and the mounts are built on; gating those would
//! make internal machinery depend on whose request happened to trigger it.
//!
//! # Denials are `NotFound`, not `Denied`
//!
//! A `403` on a path confirms the path exists, which is exactly the leak a read
//! grant closes. An actor that may not see `/tenant-b/secrets` must not be able to
//! tell it from a path that was never there. `origofs.fastapi` already applies this
//! rule to its scoped records; here it lives in the engine, so every surface
//! inherits it instead of re-deciding.
//!
//! The write path deliberately does the opposite — [`Fs::write_as`] returns
//! `Denied` — because a writer that can *read* the path already knows it exists,
//! and telling it "denied" is more useful than pretending the file vanished.
//!
//! # Enumerations filter; they do not fail
//!
//! `ls`, the suggestion queue, and the change feed return *sets*. One unreadable
//! entry must drop out of the result, not turn the whole call into an error — an
//! erroring `ls` would itself signal that something unreadable is in there.
//!
//! # What is deliberately not gated
//!
//! [`Fs::log`] returns commit metadata — hash, author, message, timestamp — and no
//! paths, so it is left ungated rather than given a misleading `_as` twin. A commit
//! *message* can of course mention a path; that is a disclosure a per-path grant
//! was never going to police, and pretending otherwise would be worse than saying
//! so. Same for branch names and the reflog.

use crate::acl::Perms;
use crate::attribution::{BlameRange, WriteCtx};
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::objectgraph::DiffEntry;
use crate::types::{DirEntry, Inode};
use bytes::Bytes;

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// [`read`](Fs::read), refused as `NotFound` unless `ctx` may read `path`.
    pub async fn read_as(&self, ctx: WriteCtx, path: &str) -> Result<Bytes> {
        self.ensure_readable(ctx, path).await?;
        self.read(path).await
    }

    /// [`read_range`](Fs::read_range), permission-checked. The check is on the
    /// path, not the range: a grant is about the file, and a caller that may read
    /// none of it may read no part of it.
    pub async fn read_range_as(
        &self,
        ctx: WriteCtx,
        path: &str,
        off: u64,
        len: u64,
    ) -> Result<Bytes> {
        self.ensure_readable(ctx, path).await?;
        self.read_range(path, off, len).await
    }

    /// [`open_for_range`](Fs::open_for_range), permission-checked — the entry point
    /// a streaming surface resolves through before sending a byte.
    pub async fn open_for_range_as(
        &self,
        ctx: WriteCtx,
        path: &str,
    ) -> Result<(Option<crate::chunk::Manifest>, u64)> {
        self.ensure_readable(ctx, path).await?;
        self.open_for_range(path).await
    }

    /// [`stat`](Fs::stat), permission-checked.
    pub async fn stat_as(&self, ctx: WriteCtx, path: &str) -> Result<Inode> {
        self.ensure_readable(ctx, path).await?;
        self.stat(path).await
    }

    /// [`blame`](Fs::blame), permission-checked.
    ///
    /// Worth its own gate rather than riding on `read`'s: blame answers *who wrote
    /// which lines*, which is a disclosure about people as much as about content,
    /// and it was one of the side doors #124 named explicitly.
    pub async fn blame_as(&self, ctx: WriteCtx, path: &str) -> Result<Vec<BlameRange>> {
        self.ensure_readable(ctx, path).await?;
        self.blame(path).await
    }

    /// [`ls`](Fs::ls), with unreadable children **omitted** rather than erroring.
    ///
    /// The directory itself must be readable — otherwise its very existence, and
    /// the shape of what is under it, leaks. Individual children are then filtered,
    /// so a grant on a subtree can hide siblings without breaking the listing.
    pub async fn ls_as(&self, ctx: WriteCtx, path: &str) -> Result<Vec<DirEntry>> {
        self.ensure_readable(ctx, path).await?;
        let entries = self.ls(path).await?;
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            let child = join_path(path, &e.name);
            if self.may_read(ctx, &child).await? {
                out.push(e);
            }
        }
        Ok(out)
    }

    /// [`status`](Fs::status), filtered to the paths `ctx` may read.
    pub async fn status_as(&self, ctx: WriteCtx) -> Result<Vec<DiffEntry>> {
        self.filter_diff(ctx, self.status().await?).await
    }

    /// [`diff`](Fs::diff), filtered to the paths `ctx` may read.
    pub async fn diff_as(&self, ctx: WriteCtx, from: &str, to: &str) -> Result<Vec<DiffEntry>> {
        self.filter_diff(ctx, self.diff(from, to).await?).await
    }

    /// [`diff_file`](Fs::diff_file), permission-checked.
    pub async fn diff_file_as(
        &self,
        ctx: WriteCtx,
        from: &str,
        to: &str,
        path: &str,
    ) -> Result<String> {
        self.ensure_readable(ctx, path).await?;
        self.diff_file(from, to, path).await
    }

    async fn filter_diff(&self, ctx: WriteCtx, entries: Vec<DiffEntry>) -> Result<Vec<DiffEntry>> {
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            if self.may_read(ctx, &e.path).await? {
                out.push(e);
            }
        }
        Ok(out)
    }

    /// `NotFound` unless `ctx` may read `path`. See the module docs for why this is
    /// `NotFound` and not `Denied`.
    ///
    /// Public so a surface can gate a collection the engine does not model — the
    /// co-editing room registry, say. Prefer a `*_as` variant wherever one exists;
    /// [`may_read`](Fs::may_read) is the boolean form, for filtering.
    pub async fn ensure_readable(&self, ctx: WriteCtx, path: &str) -> Result<()> {
        if self.may_read(ctx, path).await? {
            return Ok(());
        }
        Err(crate::error::OrigoFSError::NotFound(path.to_string()))
    }

    /// What `ctx` may do at `path`, as a convenience for a surface that wants to
    /// report it (an `X-Origofs-Perms` header, a UI's read-only banner).
    pub async fn perms_for(&self, ctx: WriteCtx, path: &str) -> Result<Perms> {
        self.effective_perms(ctx.actor, path).await
    }
}

/// Join a directory path and a child name into an absolute path.
fn join_path(dir: &str, name: &str) -> String {
    if dir == "/" {
        format!("/{name}")
    } else {
        format!("{}/{name}", dir.trim_end_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use super::join_path;

    #[test]
    fn children_of_the_root_do_not_get_a_double_slash() {
        // `//notes` would not match a grant on `/notes`, so the listing filter
        // would silently hide everything at the top level.
        assert_eq!(join_path("/", "notes"), "/notes");
        assert_eq!(join_path("/src", "main.rs"), "/src/main.rs");
        assert_eq!(join_path("/src/", "main.rs"), "/src/main.rs");
    }
}
