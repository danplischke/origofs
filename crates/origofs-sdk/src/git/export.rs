//! Export an origofs branch into a real on-disk git repository (`docs/DESIGN.md`
//! §4c, interop items 1 & 3).
//!
//! Walks the origofs commit DAG from a branch head and re-encodes every commit,
//! tree, and file as a genuine git object under `<dir>/.git`, then writes the
//! branch ref and `HEAD`. The result is a repository the actual `git` binary
//! reads directly — `git log`, `git diff`, `git checkout`, `git fsck`. Files
//! above `lfs_threshold` are written as git-LFS pointer blobs, with their bytes
//! stashed as LFS objects, so real git clients clone quickly.

use super::object::{
    GitObject, GitTreeEntry, ObjectFormat, git_ident, make_object, sha256_hex, tree_payload,
    write_loose,
};
use crate::Workspace;
use async_recursion::async_recursion;
use origofs_core::error::{OrigoFSError, Result};
use origofs_core::objectgraph::TreeKind;
use origofs_core::types::Hash;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Options controlling a git export.
pub struct ExportOptions {
    /// Object id format for the exported repo.
    pub format: ObjectFormat,
    /// Branch to export (defaults to the workspace's current branch).
    pub branch: Option<String>,
    /// Files at least this many bytes are written as git-LFS pointers.
    pub lfs_threshold: Option<u64>,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            format: ObjectFormat::Sha1,
            branch: None,
            lfs_threshold: None,
        }
    }
}

/// The result of an export.
pub struct GitExport {
    pub branch: String,
    /// Hex object id of the exported branch head commit.
    pub head: String,
    pub commits: usize,
    pub lfs_objects: usize,
}

/// Export a workspace branch into a git repository rooted at `dir`.
pub async fn export_git(ws: &Workspace, dir: &Path, opts: &ExportOptions) -> Result<GitExport> {
    let branch = match &opts.branch {
        Some(b) => b.clone(),
        None => ws.current_branch().await?.ok_or_else(|| {
            OrigoFSError::InvalidArgument("HEAD is detached; pass a branch".into())
        })?,
    };
    let head = ws.fs().branch_head(&branch).await?.ok_or_else(|| {
        OrigoFSError::InvalidArgument(format!("branch {branch} has no commits to export"))
    })?;

    let git_dir = dir.join(".git");
    init_git_dir(&git_dir, opts.format, &branch)?;

    let mut ex = Exporter {
        ws,
        git_dir: git_dir.clone(),
        fmt: opts.format,
        lfs_threshold: opts.lfs_threshold,
        trees: HashMap::new(),
        commits: HashMap::new(),
        lfs_objects: 0,
    };
    let head_oid = ex.export_commit(head).await?;
    let commits = ex.commits.len();
    let lfs_objects = ex.lfs_objects;

    // Point the branch ref at the exported head.
    std::fs::write(
        git_dir.join("refs").join("heads").join(&branch),
        format!("{head_oid}\n"),
    )?;

    Ok(GitExport {
        branch,
        head: head_oid,
        commits,
        lfs_objects,
    })
}

/// Lay out a fresh git dir: `objects/`, `refs/heads/`, `HEAD`, and a `config`
/// declaring the object format (SHA-256 needs repo format v1 + an extension).
fn init_git_dir(git_dir: &Path, fmt: ObjectFormat, branch: &str) -> Result<()> {
    std::fs::create_dir_all(git_dir.join("objects"))?;
    std::fs::create_dir_all(git_dir.join("refs").join("heads"))?;
    std::fs::write(git_dir.join("HEAD"), format!("ref: refs/heads/{branch}\n"))?;
    let config = match fmt {
        ObjectFormat::Sha1 => "[core]\n\trepositoryformatversion = 0\n\tbare = false\n".to_string(),
        ObjectFormat::Sha256 => concat!(
            "[core]\n\trepositoryformatversion = 1\n\tbare = false\n",
            "[extensions]\n\tobjectformat = sha256\n"
        )
        .to_string(),
    };
    std::fs::write(git_dir.join("config"), config)?;
    Ok(())
}

struct Exporter<'a> {
    ws: &'a Workspace,
    git_dir: PathBuf,
    fmt: ObjectFormat,
    lfs_threshold: Option<u64>,
    /// origofs tree hash -> git tree oid hex.
    trees: HashMap<Hash, String>,
    /// origofs commit hash -> git commit oid hex.
    commits: HashMap<Hash, String>,
    lfs_objects: usize,
}

impl Exporter<'_> {
    #[async_recursion]
    async fn export_commit(&mut self, origofs_hash: Hash) -> Result<String> {
        if let Some(oid) = self.commits.get(&origofs_hash) {
            return Ok(oid.clone());
        }
        let commit = self.ws.fs().commit_object(&origofs_hash).await?;

        // Parents first, so their oids are known when we encode this commit.
        let mut parent_oids = Vec::with_capacity(commit.parents.len());
        for p in &commit.parents {
            parent_oids.push(self.export_commit(*p).await?);
        }
        let tree_oid = self.export_tree(commit.tree).await?;

        let ident = git_ident(&commit.author);
        let mut payload = format!("tree {tree_oid}\n");
        for p in &parent_oids {
            payload.push_str(&format!("parent {p}\n"));
        }
        payload.push_str(&format!(
            "author {ident} {ts} +0000\ncommitter {ident} {ts} +0000\n\n{msg}\n",
            ts = commit.timestamp,
            msg = commit.message,
        ));
        let obj = make_object(self.fmt, "commit", payload.as_bytes());
        write_loose(&self.git_dir, &obj)?;
        self.commits.insert(origofs_hash, obj.oid_hex.clone());
        Ok(obj.oid_hex)
    }

    #[async_recursion]
    async fn export_tree(&mut self, origofs_hash: Hash) -> Result<String> {
        if let Some(oid) = self.trees.get(&origofs_hash) {
            return Ok(oid.clone());
        }
        let tree = self.ws.fs().tree_object(&origofs_hash).await?;
        let mut entries = Vec::with_capacity(tree.entries.len());
        for e in &tree.entries {
            let (mode, oid_hex): (&'static str, String) = match e.kind {
                TreeKind::Dir => ("40000", self.export_tree(e.hash).await?),
                TreeKind::File => {
                    let bytes = self.ws.fs().read_blob_bytes(&e.hash).await?;
                    let mode = if e.mode & 0o111 != 0 {
                        "100755"
                    } else {
                        "100644"
                    };
                    (mode, self.export_file_blob(&bytes)?)
                }
                TreeKind::Symlink => {
                    let target = self.ws.fs().get_object(&e.hash).await?;
                    let obj = make_object(self.fmt, "blob", &target);
                    write_loose(&self.git_dir, &obj)?;
                    ("120000", obj.oid_hex)
                }
            };
            entries.push(GitTreeEntry {
                mode,
                name: e.name.clone(),
                oid: hex::decode(&oid_hex).map_err(|_| OrigoFSError::Content("bad oid".into()))?,
            });
        }
        let obj = make_object(self.fmt, "tree", &tree_payload(entries));
        write_loose(&self.git_dir, &obj)?;
        self.trees.insert(origofs_hash, obj.oid_hex.clone());
        Ok(obj.oid_hex)
    }

    /// Encode a file body as a git blob, or as a git-LFS pointer (stashing the
    /// real bytes as an LFS object) when it exceeds the threshold.
    fn export_file_blob(&mut self, bytes: &[u8]) -> Result<String> {
        if let Some(threshold) = self.lfs_threshold
            && bytes.len() as u64 >= threshold
        {
            return self.export_lfs_pointer(bytes);
        }
        let obj = make_object(self.fmt, "blob", bytes);
        write_loose(&self.git_dir, &obj)?;
        Ok(obj.oid_hex)
    }

    fn export_lfs_pointer(&mut self, bytes: &[u8]) -> Result<String> {
        let oid = sha256_hex(bytes);
        // Stash the object under .git/lfs/objects/<oid[0:2]>/<oid[2:4]>/<oid>.
        let obj_path = self
            .git_dir
            .join("lfs")
            .join("objects")
            .join(&oid[..2])
            .join(&oid[2..4])
            .join(&oid);
        if !obj_path.exists() {
            if let Some(parent) = obj_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&obj_path, bytes)?;
            self.lfs_objects += 1;
        }
        let pointer = format!(
            "version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {}\n",
            bytes.len()
        );
        let blob: GitObject = make_object(self.fmt, "blob", pointer.as_bytes());
        write_loose(&self.git_dir, &blob)?;
        Ok(blob.oid_hex)
    }
}
