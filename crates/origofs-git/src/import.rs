//! Import a real git repository's history into an origofs workspace (`docs/DESIGN.md`
//! §4c, interop item 1, reverse direction).
//!
//! Reads loose objects from `<dir>/.git` (or a bare `<dir>`), translating each
//! git commit/tree/blob into origofs commits/trees/blobs, then points the target
//! branch at the imported head and checks it out. git-LFS pointer blobs are
//! resolved back to their stashed bytes so an export/import round-trip is
//! lossless. Packed objects are out of scope for this milestone — freshly
//! committed and freshly exported repos keep their objects loose.

use crate::object::{parse_commit, parse_tree, read_loose, ObjectFormat};
use async_recursion::async_recursion;
use origofs_core::error::{OrigoFSError, Result};
use origofs_core::objectgraph::{Commit, Tree, TreeEntry, TreeKind};
use origofs_core::types::Hash;
use origofs_sdk::Workspace;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Import `branch` from the git repository at `dir` into the workspace, then
/// check it out. Returns the origofs commit hash of the imported head.
pub async fn import_git(ws: &Workspace, dir: &Path, branch: &str) -> Result<Hash> {
    let git_dir = if dir.join(".git").is_dir() {
        dir.join(".git")
    } else {
        dir.to_path_buf()
    };
    let head_hex = resolve_head(&git_dir, branch)?;
    let fmt = ObjectFormat::from_hex_len(head_hex.len())
        .ok_or_else(|| OrigoFSError::Content(format!("unrecognized object id: {head_hex}")))?;

    let mut im = Importer {
        ws,
        git_dir,
        fmt,
        commits: HashMap::new(),
        trees: HashMap::new(),
    };
    let head = im.import_commit(&head_hex).await?;

    ws.fs().set_branch(branch, head).await?;
    ws.checkout(branch).await?;
    Ok(head)
}

/// Resolve a branch to its head object id, via a loose ref or `packed-refs`.
fn resolve_head(git_dir: &Path, branch: &str) -> Result<String> {
    let loose = git_dir.join("refs").join("heads").join(branch);
    if let Ok(s) = std::fs::read_to_string(&loose) {
        return Ok(s.trim().to_string());
    }
    if let Ok(packed) = std::fs::read_to_string(git_dir.join("packed-refs")) {
        let want = format!("refs/heads/{branch}");
        for line in packed.lines() {
            if line.starts_with('#') || line.starts_with('^') {
                continue;
            }
            if let Some((oid, name)) = line.split_once(' ') {
                if name.trim() == want {
                    return Ok(oid.trim().to_string());
                }
            }
        }
    }
    Err(OrigoFSError::NotFound(format!("branch {branch}")))
}

struct Importer<'a> {
    ws: &'a Workspace,
    git_dir: PathBuf,
    fmt: ObjectFormat,
    /// git commit oid hex -> origofs commit hash.
    commits: HashMap<String, Hash>,
    /// git tree oid hex -> origofs tree hash.
    trees: HashMap<String, Hash>,
}

impl Importer<'_> {
    #[async_recursion]
    async fn import_commit(&mut self, oid_hex: &str) -> Result<Hash> {
        if let Some(h) = self.commits.get(oid_hex) {
            return Ok(*h);
        }
        let (kind, payload) = read_loose(&self.git_dir, oid_hex)?;
        if kind != "commit" {
            return Err(OrigoFSError::Content(format!(
                "{oid_hex} is a {kind}, not a commit"
            )));
        }
        let parsed = parse_commit(&payload)?;

        let mut parents = Vec::with_capacity(parsed.parents.len());
        for p in &parsed.parents {
            parents.push(self.import_commit(p).await?);
        }
        let tree = self.import_tree(&parsed.tree).await?;

        let commit = Commit {
            tree,
            parents,
            author: parsed.author,
            message: parsed.message.trim_end_matches('\n').to_string(),
            timestamp: parsed.timestamp,
        };
        let hash = self.ws.fs().put_object(&commit.encode()).await?;
        self.commits.insert(oid_hex.to_string(), hash);
        Ok(hash)
    }

    #[async_recursion]
    async fn import_tree(&mut self, oid_hex: &str) -> Result<Hash> {
        if let Some(h) = self.trees.get(oid_hex) {
            return Ok(*h);
        }
        let (kind, payload) = read_loose(&self.git_dir, oid_hex)?;
        if kind != "tree" {
            return Err(OrigoFSError::Content(format!(
                "{oid_hex} is a {kind}, not a tree"
            )));
        }
        let mut entries = Vec::new();
        for e in parse_tree(&payload, self.fmt)? {
            let entry = match e.mode.as_str() {
                "40000" | "040000" => TreeEntry {
                    name: e.name,
                    mode: 0o40755,
                    kind: TreeKind::Dir,
                    hash: self.import_tree(&e.oid_hex).await?,
                },
                "120000" => {
                    let target = self.read_blob(&e.oid_hex)?;
                    TreeEntry {
                        name: e.name,
                        mode: 0o120777,
                        kind: TreeKind::Symlink,
                        hash: self.ws.fs().put_object(&target).await?,
                    }
                }
                "160000" => continue, // gitlink (submodule): unsupported, skip
                mode => {
                    let bytes = self.read_blob(&e.oid_hex)?;
                    let m = if mode == "100755" { 0o100755 } else { 0o100644 };
                    TreeEntry {
                        name: e.name,
                        mode: m,
                        kind: TreeKind::File,
                        hash: self.ws.fs().store_blob_bytes(&bytes).await?,
                    }
                }
            };
            entries.push(entry);
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let hash = self.ws.fs().put_object(&Tree { entries }.encode()).await?;
        self.trees.insert(oid_hex.to_string(), hash);
        Ok(hash)
    }

    /// Read a blob's bytes, transparently resolving a git-LFS pointer to its
    /// stashed object under `.git/lfs/objects`.
    fn read_blob(&self, oid_hex: &str) -> Result<Vec<u8>> {
        let (kind, payload) = read_loose(&self.git_dir, oid_hex)?;
        if kind != "blob" {
            return Err(OrigoFSError::Content(format!(
                "{oid_hex} is a {kind}, not a blob"
            )));
        }
        if let Some(lfs_oid) = lfs_pointer_oid(&payload) {
            let path = self
                .git_dir
                .join("lfs")
                .join("objects")
                .join(&lfs_oid[..2])
                .join(&lfs_oid[2..4])
                .join(&lfs_oid);
            return std::fs::read(&path)
                .map_err(|_| OrigoFSError::NotFound(format!("lfs object {lfs_oid}")));
        }
        Ok(payload)
    }
}

/// If `payload` is a git-LFS pointer, return its `sha256:` oid.
fn lfs_pointer_oid(payload: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(payload).ok()?;
    if !text.starts_with("version https://git-lfs.github.com/spec/v1") {
        return None;
    }
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("oid sha256:") {
            let oid = rest.trim();
            // Only accept a well-formed 64-char hex sha256. An unvalidated oid is
            // spliced into `.git/lfs/objects/<oid[..2]>/<oid[2..4]>/<oid>` and
            // read from disk, so a value like `../../../../etc/passwd` would read
            // an arbitrary host file into the store (and a short/non-ASCII oid
            // would panic the `[..2]` slice). Reject anything else.
            if oid.len() == 64 && oid.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Some(oid.to_string());
            }
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // SEC (security audit #7 / critic C1+C3): a git-LFS pointer's oid is spliced
    // into a filesystem path and read, so a traversal/malformed oid must not be
    // accepted (arbitrary host-file read) or slice-panic the process.
    #[test]
    fn lfs_pointer_oid_rejects_traversal_and_malformed() {
        let ptr = |oid: &str| {
            format!("version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize 12\n")
                .into_bytes()
        };
        // valid 64-hex sha256 is accepted
        let good = "a".repeat(64);
        assert_eq!(lfs_pointer_oid(&ptr(&good)), Some(good));
        // traversal, short, and non-hex oids are all rejected
        for bad in [
            "../../../../etc/passwd",
            "x",
            "..",
            &"g".repeat(64), // non-hex
            &"a".repeat(63), // wrong length
            "a/b",
        ] {
            assert_eq!(
                lfs_pointer_oid(&ptr(bad)),
                None,
                "oid {bad:?} must be rejected"
            );
        }
        // not an LFS pointer at all
        assert_eq!(lfs_pointer_oid(b"just a normal blob"), None);
    }
}
