//! Git's on-disk object model: loose object framing, hashing, and the tree
//! entry ordering the real `git` requires (`docs/DESIGN.md` §4c, interop item 1).
//!
//! A git object is `"<type> <len>\0<payload>"`; its id is the hash of that
//! framing, and on disk it is zlib-compressed under `objects/<id[0:2]>/<id[2:]>`.
//! Git supports two object formats that differ only in the hash function and the
//! binary id width inside trees: **SHA-1** (what GitHub accepts today) and
//! **SHA-256** (origofs's native 256-bit story). We encode and decode both, and pick
//! per export so a workspace can target either ecosystem.

use origofs_core::error::{OrigoFSError, Result};
use std::cmp::Ordering;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// A git object-id hash function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ObjectFormat {
    /// 160-bit SHA-1 ids — compatible with GitHub and today's git hosts.
    Sha1,
    /// 256-bit SHA-256 ids — matches origofs's 256-bit content addressing.
    Sha256,
}

impl ObjectFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            ObjectFormat::Sha1 => "sha1",
            ObjectFormat::Sha256 => "sha256",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "sha1" => Some(ObjectFormat::Sha1),
            "sha256" => Some(ObjectFormat::Sha256),
            _ => None,
        }
    }

    /// Binary id width: 20 bytes (SHA-1) or 32 bytes (SHA-256).
    pub fn oid_len(self) -> usize {
        match self {
            ObjectFormat::Sha1 => 20,
            ObjectFormat::Sha256 => 32,
        }
    }

    /// Infer the format from a hex object-id's length (40 vs 64 chars).
    pub fn from_hex_len(hex_len: usize) -> Option<Self> {
        match hex_len {
            40 => Some(ObjectFormat::Sha1),
            64 => Some(ObjectFormat::Sha256),
            _ => None,
        }
    }

    fn digest(self, data: &[u8]) -> Vec<u8> {
        match self {
            ObjectFormat::Sha1 => {
                use sha1::{Digest, Sha1};
                Sha1::digest(data).to_vec()
            }
            ObjectFormat::Sha256 => {
                use sha2::{Digest, Sha256};
                Sha256::digest(data).to_vec()
            }
        }
    }
}

/// Raw SHA-256 of `data` (git-LFS object ids are always SHA-256, independent of
/// the repository's object format).
pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(data))
}

/// Frame a payload as a git object: `"<type> <len>\0" ++ payload`.
pub fn frame(kind: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = format!("{kind} {}\0", payload.len()).into_bytes();
    out.extend_from_slice(payload);
    out
}

/// A framed git object plus its hex id under a given format.
pub struct GitObject {
    pub oid_hex: String,
    pub framed: Vec<u8>,
}

/// Frame `payload` and compute its object id.
pub fn make_object(fmt: ObjectFormat, kind: &str, payload: &[u8]) -> GitObject {
    let framed = frame(kind, payload);
    let oid_hex = hex::encode(fmt.digest(&framed));
    GitObject { oid_hex, framed }
}

/// One entry destined for a git tree object.
pub struct GitTreeEntry {
    /// Git mode string, e.g. `100644`, `100755`, `120000`, `40000`.
    pub mode: &'static str,
    pub name: String,
    /// Binary object id (`fmt.oid_len()` bytes).
    pub oid: Vec<u8>,
}

/// Encode a git tree payload with entries in git's canonical order.
///
/// Git sorts by name with directories compared as if their name had a trailing
/// `/` (see `base_name_compare`); `git fsck` rejects any other order.
pub fn tree_payload(mut entries: Vec<GitTreeEntry>) -> Vec<u8> {
    entries.sort_by(|a, b| git_name_cmp(&a.name, a.is_dir(), &b.name, b.is_dir()));
    let mut out = Vec::new();
    for e in &entries {
        out.extend_from_slice(e.mode.as_bytes());
        out.push(b' ');
        out.extend_from_slice(e.name.as_bytes());
        out.push(0);
        out.extend_from_slice(&e.oid);
    }
    out
}

impl GitTreeEntry {
    fn is_dir(&self) -> bool {
        self.mode == "40000"
    }
}

fn git_name_cmp(a: &str, a_dir: bool, b: &str, b_dir: bool) -> Ordering {
    // Compare with an implicit trailing '/' on directory names, matching git.
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    let n = ab.len().min(bb.len());
    match ab[..n].cmp(&bb[..n]) {
        Ordering::Equal => {
            let ac = ab.get(n).copied().unwrap_or(if a_dir { b'/' } else { 0 });
            let bc = bb.get(n).copied().unwrap_or(if b_dir { b'/' } else { 0 });
            ac.cmp(&bc)
        }
        other => other,
    }
}

/// Inflate ceiling for a single loose object: bounds a zlib decompression bomb
/// (a few KB expanding to many GB) so import / the remote helper can't be OOM'd.
///
/// The whole inflated object is held in memory at once, so this is also the peak
/// RSS one hostile object can force. 2 GiB was nominally a bound but not a useful
/// one — a single object could still OOM-kill a modestly-sized container. 256 MiB
/// is far above any real source blob (git itself warns past 512 MiB, and anything
/// approaching this belongs in LFS, which this importer resolves separately).
/// Override for a repository that genuinely needs more.
const MAX_GIT_OBJECT_DEFAULT: u64 = 256 * 1024 * 1024;

/// `ORIGOFS_GIT_MAX_OBJECT_BYTES` overrides [`MAX_GIT_OBJECT_DEFAULT`].
fn max_git_object() -> u64 {
    std::env::var("ORIGOFS_GIT_MAX_OBJECT_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(MAX_GIT_OBJECT_DEFAULT)
}

/// Validate a hex object id before it is used to build a filesystem path or
/// sliced positionally. Git ids are hex of a fixed width (40 for SHA-1, 64 for
/// SHA-256); anything else — a `..`, an embedded separator, a short/oversized or
/// non-hex string — is rejected. This closes a path traversal (an unvalidated id
/// like `../../etc/passwd` reading an arbitrary host file into the store on
/// import) and the `&oid[..2]` slice panic on a malformed id.
pub fn validate_oid(oid_hex: &str) -> Result<()> {
    let ok = (oid_hex.len() == 40 || oid_hex.len() == 64)
        && oid_hex.bytes().all(|b| b.is_ascii_hexdigit());
    if !ok {
        return Err(OrigoFSError::Content(format!(
            "invalid git object id: {oid_hex:?}"
        )));
    }
    Ok(())
}

/// Path to a loose object within a git dir.
pub fn loose_path(git_dir: &Path, oid_hex: &str) -> PathBuf {
    git_dir
        .join("objects")
        .join(&oid_hex[..2])
        .join(&oid_hex[2..])
}

/// Write a framed object as a zlib-compressed loose file (idempotent).
pub fn write_loose(git_dir: &Path, obj: &GitObject) -> Result<()> {
    let path = loose_path(git_dir, &obj.oid_hex);
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(&obj.framed)?;
    let compressed = enc.finish()?;
    std::fs::write(&path, compressed)?;
    Ok(())
}

/// Read and inflate a loose object, returning `(kind, payload)`.
///
/// The object is verified against the id it was fetched by. An oid is a *claim*
/// until it is checked: a hand-crafted `.git` can put arbitrary bytes at any oid
/// filename, and nothing else in the import path would notice. Checking makes the
/// imported object graph acyclic by construction — a commit naming itself as its
/// own parent, or two trees referencing each other, would each require knowing a
/// hash before computing it — which is what stops a crafted repository from
/// driving the importer into unbounded recursion.
pub fn read_loose(git_dir: &Path, oid_hex: &str) -> Result<(String, Vec<u8>)> {
    validate_oid(oid_hex)?;
    let path = loose_path(git_dir, oid_hex);
    let compressed = std::fs::read(&path)
        .map_err(|_| OrigoFSError::NotFound(format!("git object {oid_hex}")))?;
    let max = max_git_object();
    let mut framed = Vec::new();
    // Bounded inflate: cap the output so a decompression bomb can't OOM us.
    flate2::read::ZlibDecoder::new(&compressed[..])
        .take(max + 1)
        .read_to_end(&mut framed)
        .map_err(|e| OrigoFSError::Content(format!("inflate {oid_hex}: {e}")))?;
    if framed.len() as u64 > max {
        return Err(OrigoFSError::Content(format!(
            "git object {oid_hex} exceeds {max} bytes (possible zip bomb)"
        )));
    }
    // `validate_oid` already fixed the width at 40 (SHA-1) or 64 (SHA-256).
    let fmt = ObjectFormat::from_hex_len(oid_hex.len())
        .ok_or_else(|| OrigoFSError::Content(format!("unrecognized object id: {oid_hex}")))?;
    let actual = hex::encode(fmt.digest(&framed));
    if !actual.eq_ignore_ascii_case(oid_hex) {
        return Err(OrigoFSError::Corrupt(format!(
            "git object {oid_hex} hashes to {actual}"
        )));
    }
    parse_framed(&framed)
}

/// Split a framed object into `(kind, payload)`.
pub fn parse_framed(framed: &[u8]) -> Result<(String, Vec<u8>)> {
    let bad = || OrigoFSError::Content("malformed git object header".to_string());
    let sp = framed.iter().position(|&b| b == b' ').ok_or_else(bad)?;
    let nul = framed.iter().position(|&b| b == 0).ok_or_else(bad)?;
    if nul < sp {
        return Err(bad());
    }
    let kind = String::from_utf8(framed[..sp].to_vec()).map_err(|_| bad())?;
    Ok((kind, framed[nul + 1..].to_vec()))
}

/// A decoded git tree entry (from [`parse_tree`]).
pub struct ParsedTreeEntry {
    pub mode: String,
    pub name: String,
    pub oid_hex: String,
}

/// Parse a git tree payload into its entries.
pub fn parse_tree(payload: &[u8], fmt: ObjectFormat) -> Result<Vec<ParsedTreeEntry>> {
    let bad = || OrigoFSError::Content("malformed git tree".to_string());
    let oid_len = fmt.oid_len();
    let mut out = Vec::new();
    let mut i = 0;
    while i < payload.len() {
        let sp = payload[i..]
            .iter()
            .position(|&b| b == b' ')
            .ok_or_else(bad)?
            + i;
        let nul = payload[i..].iter().position(|&b| b == 0).ok_or_else(bad)? + i;
        if nul < sp || nul + 1 + oid_len > payload.len() {
            return Err(bad());
        }
        let mode = String::from_utf8(payload[i..sp].to_vec()).map_err(|_| bad())?;
        let name = String::from_utf8(payload[sp + 1..nul].to_vec()).map_err(|_| bad())?;
        let oid_hex = hex::encode(&payload[nul + 1..nul + 1 + oid_len]);
        out.push(ParsedTreeEntry {
            mode,
            name,
            oid_hex,
        });
        i = nul + 1 + oid_len;
    }
    Ok(out)
}

/// The fields of a git commit we round-trip.
pub struct ParsedCommit {
    pub tree: String,
    pub parents: Vec<String>,
    /// `Name <email>` without the trailing timestamp/zone.
    pub author: String,
    pub timestamp: i64,
    pub message: String,
}

/// Parse a git commit payload.
pub fn parse_commit(payload: &[u8]) -> Result<ParsedCommit> {
    let bad = || OrigoFSError::Content("malformed git commit".to_string());
    let text = String::from_utf8(payload.to_vec()).map_err(|_| bad())?;
    let (headers, message) = text.split_once("\n\n").unwrap_or((text.as_str(), ""));
    let mut tree = None;
    let mut parents = Vec::new();
    let mut author = String::new();
    let mut timestamp = 0i64;
    for line in headers.lines() {
        if let Some(v) = line.strip_prefix("tree ") {
            tree = Some(v.to_string());
        } else if let Some(v) = line.strip_prefix("parent ") {
            parents.push(v.to_string());
        } else if let Some(v) = line.strip_prefix("author ") {
            let (ident, ts) = split_ident(v);
            author = ident;
            timestamp = ts;
        }
    }
    Ok(ParsedCommit {
        tree: tree.ok_or_else(bad)?,
        parents,
        author,
        timestamp,
        message: message.to_string(),
    })
}

/// Split a git ident line `"Name <email> <ts> <tz>"` into `(ident, timestamp)`.
fn split_ident(v: &str) -> (String, i64) {
    let toks: Vec<&str> = v.rsplitn(3, ' ').collect(); // [tz, ts, ident]
    if toks.len() == 3 {
        let ts = toks[1].parse().unwrap_or(0);
        (toks[2].to_string(), ts)
    } else {
        (v.to_string(), 0)
    }
}

/// Render an origofs author string as a git ident (`Name <email>`), synthesizing an
/// email when the origofs author carries none so `git fsck` stays happy.
pub fn git_ident(author: &str) -> String {
    if author.contains('<') && author.contains('>') {
        author.to_string()
    } else {
        format!("{author} <{author}@origofs.local>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // SEC (security audit critic C3): an object id becomes `objects/<id[..2]>/<id[2..]>`
    // and is read from disk, so a malformed id must be rejected before it can
    // traverse the filesystem or panic the positional slice.
    #[test]
    fn validate_oid_accepts_only_fixed_width_hex() {
        assert!(validate_oid(&"a".repeat(40)).is_ok()); // SHA-1
        assert!(validate_oid(&"0".repeat(64)).is_ok()); // SHA-256
        for bad in [
            "..",
            "../../etc/passwd",
            "x", // too short, would panic `[..2]`
            "",
            &"a".repeat(41), // wrong width
            &"z".repeat(40), // non-hex
            "abc/def",
        ] {
            assert!(validate_oid(bad).is_err(), "oid {bad:?} must be rejected");
        }
    }
}
