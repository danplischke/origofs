//! Core value types shared across the metadata store, content store, and engine.

use std::fmt;

/// Inode number. The root directory is always [`INO_ROOT`].
pub type Ino = i64;

/// The root directory inode. Every path resolves starting here.
pub const INO_ROOT: Ino = 1;

/// A BLAKE3-256 content address (32 bytes), hex-formatted for storage and display.
///
/// In M0 a file body is stored as a single content-addressed blob. M1 replaces
/// the single blob with a FastCDC chunk manifest addressed the same way.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Content address of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Hash(*blake3::hash(bytes).as_bytes())
    }

    pub fn from_array(b: [u8; 32]) -> Self {
        Hash(b)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse a 64-char lowercase hex string, or `None` if malformed.
    pub fn from_hex(s: &str) -> Option<Self> {
        let v = hex::decode(s).ok()?;
        let arr: [u8; 32] = v.try_into().ok()?;
        Some(Hash(arr))
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", self.to_hex())
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// The kind of a filesystem object. A minimal, POSIX-flavored set for M0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    File,
    Dir,
    Symlink,
}

impl FileKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FileKind::File => "file",
            FileKind::Dir => "dir",
            FileKind::Symlink => "symlink",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "file" => Some(FileKind::File),
            "dir" => Some(FileKind::Dir),
            "symlink" => Some(FileKind::Symlink),
            _ => None,
        }
    }
}

/// Inode metadata (an M0 subset of the POSIX inode in `docs/DESIGN.md` §5).
#[derive(Clone, Debug)]
pub struct Inode {
    pub ino: Ino,
    pub kind: FileKind,
    pub mode: u32,
    pub nlink: i64,
    pub size: u64,
    /// Content address of the whole body (M0). `None` for empty files, dirs, symlinks.
    pub content: Option<Hash>,
    pub mtime: i64,
    pub ctime: i64,
}

/// The fields required to allocate a new inode.
#[derive(Clone, Debug)]
pub struct InodeInit {
    pub kind: FileKind,
    pub mode: u32,
}

/// One entry within a directory listing.
#[derive(Clone, Debug)]
pub struct DirEntry {
    pub name: String,
    pub ino: Ino,
    pub kind: FileKind,
}
