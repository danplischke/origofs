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
/// `Ord` is the lexicographic order of the 32 address bytes. It carries no
/// meaning about the content — it exists so a hash can key an ordered collection
/// (the LRU index in [`TieredStore`](crate::TieredStore)) and so tie-breaks that
/// need to be deterministic across runs, such as `merge_base`'s choice among
/// equally-good candidates, can be.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

    /// Parse a 64-char **lowercase** hex string, or `None` if malformed.
    ///
    /// Uppercase is rejected rather than accepted, even though `hex::decode`
    /// would take it. A hash is a *storage key*: `to_hex` emits lowercase and
    /// every backend derives an object's path from it, so accepting `AB…` here
    /// would mint a `Hash` whose `path_for` points somewhere other than where the
    /// name came from. The concrete failure is in `list()`, which reconstructs
    /// hashes from directory entries — an uppercase name would parse, be reported
    /// as present, and then be undeletable, because `delete` would look for the
    /// lowercase path. One canonical spelling, checked at the boundary.
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.contains(|c: char| c.is_ascii_uppercase()) {
            return None;
        }
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

/// POSIX ownership of an inode: a numeric user and group id (issue #122).
///
/// Ownership is a **mount-surface** concept and is deliberately *not* origofs's
/// notion of identity. A uid says which local account the kernel should evaluate
/// its permission checks against; an [`Actor`](crate::Actor) says who a blame
/// trail attributes bytes to. `docs/PERMISSIONS.md` §2 sets out why the two must
/// not be collapsed — a uid is reused across machines, reassigned, and shared by
/// every process a user runs, none of which is true of an actor.
///
/// This type exists so the mounts can stop reporting every inode as root-owned.
/// That was coherent (the FUSE mount sets `DefaultPermissions`, and
/// `fuse_mountable()` requires root, so every check passed) but it is exactly why
/// `allow_other` and non-root mounts could not work: a non-root caller would be
/// evaluated against uid 0 in the *other* class and lose write access to the
/// whole tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Owner {
    pub uid: u32,
    pub gid: u32,
}

impl Owner {
    /// uid 0 / gid 0 — what every inode reported before #122, and what existing
    /// rows are backfilled to so the migration changes no observable behaviour.
    pub const ROOT: Owner = Owner { uid: 0, gid: 0 };

    pub fn new(uid: u32, gid: u32) -> Self {
        Owner { uid, gid }
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
    /// Owning user id (issue #122). 0 for anything created before the ownership
    /// migration, and for anything created off a mount.
    pub uid: u32,
    /// Owning group id (issue #122). See [`uid`](Self::uid).
    pub gid: u32,
}

impl Inode {
    /// This inode's ownership as an [`Owner`].
    pub fn owner(&self) -> Owner {
        Owner {
            uid: self.uid,
            gid: self.gid,
        }
    }
}

/// The fields required to allocate a new inode.
#[derive(Clone, Debug)]
pub struct InodeInit {
    pub kind: FileKind,
    pub mode: u32,
    /// Ownership for the new inode. [`Owner::ROOT`] for the internal machinery
    /// (checkout, merge materialization, git import), which has no caller whose
    /// uid would be meaningful; the mounts pass the requesting process's.
    pub owner: Owner,
}

impl InodeInit {
    /// A new inode owned by root — the default for every non-mount caller.
    ///
    /// Most `create_inode` call sites are internal machinery materializing a tree,
    /// where there is no requesting process and therefore no uid worth recording.
    /// They use this so adding ownership did not require each of them to invent an
    /// answer.
    pub fn new(kind: FileKind, mode: u32) -> Self {
        InodeInit {
            kind,
            mode,
            owner: Owner::ROOT,
        }
    }

    /// A new inode owned by `owner` — what the mount surfaces use, passing the
    /// uid/gid of the process that issued the `create`/`mkdir`/`symlink`.
    pub fn owned_by(kind: FileKind, mode: u32, owner: Owner) -> Self {
        InodeInit { kind, mode, owner }
    }
}

/// One entry within a directory listing.
#[derive(Clone, Debug)]
pub struct DirEntry {
    pub name: String,
    pub ino: Ino,
    pub kind: FileKind,
}

/// A directory entry with its inode attributes already resolved.
///
/// A `readdir` that also needs attributes (NFSv3 `READDIRPLUS`-style replies)
/// would otherwise issue one `getattr` per entry; the attrs here come from a
/// single batched inode fetch instead (M16).
#[derive(Clone, Debug)]
pub struct DirEntryAttr {
    pub entry: DirEntry,
    pub inode: Inode,
}

/// One keyset page of a directory read (M16).
///
/// Pages are ordered by name and resumed by name — [`next_after`](Self::next_after)
/// is the cursor to hand back as `after_name` for the following page. It is the
/// name of the last *dentry* on the page, which is deliberately not the same as
/// the last element of `entries`: an inode that vanished between the dentry query
/// and the attribute fetch is dropped from `entries`, and the cursor must still
/// advance past it or the read would loop forever.
#[derive(Clone, Debug, Default)]
pub struct DirPage {
    /// The page's entries, in name order, each with its attributes.
    pub entries: Vec<DirEntryAttr>,
    /// Keyset cursor for the next page, or `None` when the page was empty.
    pub next_after: Option<String>,
    /// The store returned fewer rows than the requested limit, so this is the
    /// last page. A page that exactly fills the limit reports `false` even when
    /// the directory happens to end there — the caller confirms with one more
    /// (empty) page, exactly as a keyset scan must.
    pub end: bool,
}
