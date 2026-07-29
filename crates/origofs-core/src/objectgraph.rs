//! The versioning object graph: `tree` and `commit` objects (`docs/DESIGN.md`
//! §4c).
//!
//! Together with the `blob` manifests from [`crate::chunk`], these form a
//! git-style content-addressed Merkle DAG stored in the [`ContentStore`]. A
//! `tree` snapshots a directory (referencing child trees, blob manifests, and
//! symlink blobs); a `commit` references a root tree plus its parent commits.
//!
//! [`ContentStore`]: crate::ContentStore

use crate::error::{OrigoFSError, Result};
use crate::types::Hash;

const TREE_MAGIC: &[u8; 5] = b"ORGT\x01";
const COMMIT_MAGIC: &[u8; 5] = b"ORGC\x01";
const REFS_MAGIC: &[u8; 5] = b"ORGR\x01";

/// The kind of a tree entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeKind {
    File,
    Dir,
    Symlink,
}

impl TreeKind {
    fn code(self) -> u8 {
        match self {
            TreeKind::File => 0,
            TreeKind::Dir => 1,
            TreeKind::Symlink => 2,
        }
    }
    fn from_code(c: u8) -> Option<Self> {
        match c {
            0 => Some(TreeKind::File),
            1 => Some(TreeKind::Dir),
            2 => Some(TreeKind::Symlink),
            _ => None,
        }
    }
}

/// One entry in a directory tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: String,
    pub mode: u32,
    pub kind: TreeKind,
    /// Child tree hash (dir), blob-manifest hash (file), or symlink-target blob (symlink).
    pub hash: Hash,
}

/// A directory snapshot: entries are stored sorted by name for a canonical hash.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Tree {
    pub entries: Vec<TreeEntry>,
}

impl Tree {
    /// `magic | count(u32) | [ kind(u8) | mode(u32) | name_len(u16) | name | hash(32) ]*`
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(TREE_MAGIC);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            out.push(e.kind.code());
            out.extend_from_slice(&e.mode.to_le_bytes());
            out.extend_from_slice(&(e.name.len() as u16).to_le_bytes());
            out.extend_from_slice(e.name.as_bytes());
            out.extend_from_slice(e.hash.as_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Tree> {
        let bad = || OrigoFSError::Content("malformed tree object".to_string());
        if bytes.len() < 9 || &bytes[0..5] != TREE_MAGIC {
            return Err(bad());
        }
        let count = u32::from_le_bytes(bytes[5..9].try_into().map_err(|_| bad())?) as usize;
        // Cap the pre-alloc by what the remaining bytes could actually hold (min
        // 39 bytes/entry: 7-byte header + 32-byte hash) so a crafted count can't
        // drive a multi-GB allocation and abort the process (cf. Manifest::decode).
        let mut entries = Vec::with_capacity(count.min(bytes.len().saturating_sub(9) / 39));
        let mut off = 9;
        for _ in 0..count {
            if off + 7 > bytes.len() {
                return Err(bad());
            }
            let kind = TreeKind::from_code(bytes[off]).ok_or_else(bad)?;
            let mode = u32::from_le_bytes(bytes[off + 1..off + 5].try_into().map_err(|_| bad())?);
            let name_len =
                u16::from_le_bytes(bytes[off + 5..off + 7].try_into().map_err(|_| bad())?) as usize;
            off += 7;
            if off + name_len + 32 > bytes.len() {
                return Err(bad());
            }
            let name = String::from_utf8(bytes[off..off + name_len].to_vec()).map_err(|_| bad())?;
            off += name_len;
            let mut h = [0u8; 32];
            h.copy_from_slice(&bytes[off..off + 32]);
            off += 32;
            entries.push(TreeEntry {
                name,
                mode,
                kind,
                hash: Hash::from_array(h),
            });
        }
        Ok(Tree { entries })
    }
}

/// A commit: a root tree, zero or more parents, author, message, timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    pub tree: Hash,
    pub parents: Vec<Hash>,
    pub author: String,
    pub message: String,
    pub timestamp: i64,
}

impl Commit {
    /// `magic | tree(32) | parent_count(u32) | parents(32)* | ts(i64) |
    ///  author_len(u16) | author | msg_len(u32) | msg`
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(COMMIT_MAGIC);
        out.extend_from_slice(self.tree.as_bytes());
        out.extend_from_slice(&(self.parents.len() as u32).to_le_bytes());
        for p in &self.parents {
            out.extend_from_slice(p.as_bytes());
        }
        out.extend_from_slice(&self.timestamp.to_le_bytes());
        out.extend_from_slice(&(self.author.len() as u16).to_le_bytes());
        out.extend_from_slice(self.author.as_bytes());
        out.extend_from_slice(&(self.message.len() as u32).to_le_bytes());
        out.extend_from_slice(self.message.as_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Commit> {
        let bad = || OrigoFSError::Content("malformed commit object".to_string());
        if bytes.len() < 5 + 32 + 4 || &bytes[0..5] != COMMIT_MAGIC {
            return Err(bad());
        }
        let mut off = 5;
        let mut tree = [0u8; 32];
        tree.copy_from_slice(&bytes[off..off + 32]);
        off += 32;
        let pc = u32::from_le_bytes(bytes[off..off + 4].try_into().map_err(|_| bad())?) as usize;
        off += 4;
        // Cap the pre-alloc by the remaining bytes (32 bytes/parent) so a hostile
        // count can't force a huge allocation (cf. Manifest::decode).
        let mut parents = Vec::with_capacity(pc.min(bytes.len().saturating_sub(off) / 32));
        for _ in 0..pc {
            if off + 32 > bytes.len() {
                return Err(bad());
            }
            let mut p = [0u8; 32];
            p.copy_from_slice(&bytes[off..off + 32]);
            off += 32;
            parents.push(Hash::from_array(p));
        }
        if off + 8 + 2 > bytes.len() {
            return Err(bad());
        }
        let timestamp = i64::from_le_bytes(bytes[off..off + 8].try_into().map_err(|_| bad())?);
        off += 8;
        let alen = u16::from_le_bytes(bytes[off..off + 2].try_into().map_err(|_| bad())?) as usize;
        off += 2;
        if off + alen + 4 > bytes.len() {
            return Err(bad());
        }
        let author = String::from_utf8(bytes[off..off + alen].to_vec()).map_err(|_| bad())?;
        off += alen;
        let mlen = u32::from_le_bytes(bytes[off..off + 4].try_into().map_err(|_| bad())?) as usize;
        off += 4;
        if off + mlen > bytes.len() {
            return Err(bad());
        }
        let message = String::from_utf8(bytes[off..off + mlen].to_vec()).map_err(|_| bad())?;
        Ok(Commit {
            tree: Hash::from_array(tree),
            parents,
            author,
            message,
            timestamp,
        })
    }
}

/// A commit plus its own hash, for `log` output.
#[derive(Clone, Debug)]
pub struct CommitInfo {
    pub hash: Hash,
    pub commit: Commit,
}

/// A mirror of the whole ref table (branches, tags, HEAD) written into the
/// [`ContentStore`] alongside the object graph, so a bare content store can
/// recover its branch names and tips without the metadata DB (`docs/DESIGN.md`
/// §7). Refs otherwise live only in the DB, which is the fragile part of a
/// recovery story; mirroring closes that gap.
///
/// `generation` is a monotonic counter stamped from the DB at write time, so a
/// recovery scan that finds several snapshots (those written since the last GC)
/// can pick the newest unambiguously.
///
/// [`ContentStore`]: crate::ContentStore
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RefSnapshot {
    pub generation: u64,
    /// `(name, value)` for every ref: a branch/tag maps to a commit hex, `HEAD`
    /// maps to `ref:<branch>` (or a commit hex when detached).
    pub refs: Vec<(String, String)>,
}

impl RefSnapshot {
    /// `magic | generation(u64) | count(u32) | [ name_len(u16) | name | val_len(u32) | val ]*`
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(REFS_MAGIC);
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&(self.refs.len() as u32).to_le_bytes());
        for (name, value) in &self.refs {
            out.extend_from_slice(&(name.len() as u16).to_le_bytes());
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(&(value.len() as u32).to_le_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<RefSnapshot> {
        let bad = || OrigoFSError::Content("malformed ref snapshot".to_string());
        if bytes.len() < 17 || &bytes[0..5] != REFS_MAGIC {
            return Err(bad());
        }
        let generation = u64::from_le_bytes(bytes[5..13].try_into().map_err(|_| bad())?);
        let count = u32::from_le_bytes(bytes[13..17].try_into().map_err(|_| bad())?) as usize;
        // Cap the pre-alloc by the remaining bytes (min 6 bytes/entry: a 2-byte
        // name length + a 4-byte value length) so a hostile count can't force a
        // huge allocation and abort the process (cf. Manifest::decode).
        let mut refs = Vec::with_capacity(count.min(bytes.len().saturating_sub(17) / 6));
        let mut off = 17;
        for _ in 0..count {
            if off + 2 > bytes.len() {
                return Err(bad());
            }
            let nlen =
                u16::from_le_bytes(bytes[off..off + 2].try_into().map_err(|_| bad())?) as usize;
            off += 2;
            if off + nlen + 4 > bytes.len() {
                return Err(bad());
            }
            let name = String::from_utf8(bytes[off..off + nlen].to_vec()).map_err(|_| bad())?;
            off += nlen;
            let vlen =
                u32::from_le_bytes(bytes[off..off + 4].try_into().map_err(|_| bad())?) as usize;
            off += 4;
            if off + vlen > bytes.len() {
                return Err(bad());
            }
            let value = String::from_utf8(bytes[off..off + vlen].to_vec()).map_err(|_| bad())?;
            off += vlen;
            refs.push((name, value));
        }
        Ok(RefSnapshot { generation, refs })
    }
}

/// Versioning mode for a workspace (`docs/DESIGN.md` §4c).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VersioningMode {
    /// No commits — a plain shared filesystem.
    Off,
    /// origofs's native content-addressed commit DAG.
    Native,
    /// Native commit DAG plus git interop: the `origofs-sdk` `git` module exports/imports
    /// genuine git objects (SHA-1 or SHA-256) so the real `git` CLI can drive it.
    Git,
}

impl VersioningMode {
    pub fn as_str(self) -> &'static str {
        match self {
            VersioningMode::Off => "off",
            VersioningMode::Native => "native",
            VersioningMode::Git => "git",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "off" => Some(VersioningMode::Off),
            "native" => Some(VersioningMode::Native),
            "git" => Some(VersioningMode::Git),
            _ => None,
        }
    }
    pub fn commits_enabled(self) -> bool {
        !matches!(self, VersioningMode::Off)
    }
}

/// A single change between two snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiffEntry {
    pub path: String,
    pub status: DiffStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiffStatus {
    Added,
    Modified,
    Deleted,
}

impl DiffStatus {
    pub fn sigil(self) -> char {
        match self {
            DiffStatus::Added => 'A',
            DiffStatus::Modified => 'M',
            DiffStatus::Deleted => 'D',
        }
    }
}
