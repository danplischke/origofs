//! The object header every structured origofs object carries: a 4-byte **type
//! tag** plus a 1-byte **format version** (`docs/DESIGN.md` §4a, §4c).
//!
//! ```text
//! ORGM 01 …   blob manifest       ORGC 01 …   commit
//! ORGT 01 …   tree                ORGR 01 …   ref-mirror snapshot
//! ```
//!
//! **Why the version byte matters here and not in the metadata DB.** The DB
//! migrates forward in place ([`crate::migrations`]); content cannot. Objects are
//! immutable and addressed by the BLAKE3 hash of their bytes, so a format change
//! produces *new* objects at *new* addresses while every old object stays valid
//! and readable forever. Evolution therefore costs nothing — **provided readers
//! stay backwards-compatible**, which is the discipline this module exists to
//! enforce.
//!
//! ## Rules for changing an object format
//!
//! 1. **Never change an existing version's encoding.** The bytes are the address:
//!    re-encoding an unchanged tree under the same version silently changes its
//!    hash, which forks the DAG and destroys dedup against everything already
//!    stored. The golden fixtures in `tests/format.rs` pin the v1 bytes *and*
//!    their hashes so this fails in CI rather than in a bucket.
//! 2. **Add a version, keep the old decoder.** Give `decode` a new `match` arm
//!    (`2 => Self::decode_v2(bytes)`) and leave `decode_v1` untouched.
//! 3. **Ship the reader before the writer.** Bump [`ObjectKind::max_read_version`]
//!    in one release and [`ObjectKind::write_version`] in a later one, so a
//!    mixed-version fleet has already learned to read v2 by the time anything
//!    writes it. `write_version <= max_read_version` is asserted in tests.
//!
//! A reader that meets a version above its `max_read_version` reports
//! [`OrigoFSError::UnsupportedVersion`] — deliberately *not*
//! [`Corrupt`](OrigoFSError::Corrupt), because the bytes are fine and the fix is
//! to upgrade origofs, not to restore a backup.

use crate::error::{OrigoFSError, Result};

/// Header length: `tag(4) | version(1)`.
pub(crate) const HEADER_LEN: usize = 5;

/// The header contract for one structured object kind.
pub(crate) struct ObjectKind {
    /// The 4-byte type tag. Stable **forever**: it names the kind, not the layout.
    tag: &'static [u8; 4],
    /// Name used in error messages.
    name: &'static str,
    /// The format version this build writes.
    write_version: u8,
    /// The highest format version this build can decode. Always
    /// `>= write_version` — see rule 3 in the module docs.
    max_read_version: u8,
}

/// Blob manifest — the ordered chunk list of a file ([`crate::chunk::Manifest`]).
pub(crate) const MANIFEST: ObjectKind = ObjectKind {
    tag: b"ORGM",
    name: "manifest",
    write_version: 1,
    max_read_version: 1,
};

/// Directory snapshot ([`crate::objectgraph::Tree`]).
pub(crate) const TREE: ObjectKind = ObjectKind {
    tag: b"ORGT",
    name: "tree",
    write_version: 1,
    max_read_version: 1,
};

/// Commit ([`crate::objectgraph::Commit`]).
pub(crate) const COMMIT: ObjectKind = ObjectKind {
    tag: b"ORGC",
    name: "commit",
    write_version: 1,
    max_read_version: 1,
};

/// Ref-mirror snapshot ([`crate::objectgraph::RefSnapshot`]).
pub(crate) const REFS: ObjectKind = ObjectKind {
    tag: b"ORGR",
    name: "ref snapshot",
    write_version: 1,
    max_read_version: 1,
};

/// Store descriptor — the named slot stamped at the store's root ([`StoreDescriptor`]).
pub(crate) const STORE: ObjectKind = ObjectKind {
    tag: b"ORGS",
    name: "store descriptor",
    write_version: 1,
    max_read_version: 1,
};

/// Pack object footer ([`crate::pack::PackStore`]).
pub(crate) const PACK: ObjectKind = ObjectKind {
    tag: b"ORGP",
    name: "pack",
    write_version: 1,
    max_read_version: 1,
};

/// Pack index entry ([`crate::pack::PackStore`]).
pub(crate) const PACK_INDEX: ObjectKind = ObjectKind {
    tag: b"ORGI",
    name: "pack index entry",
    write_version: 1,
    max_read_version: 1,
};

/// Every object kind whose version a *store* has to account for. Excludes the
/// store descriptor itself (it describes them; it cannot describe itself) and the
/// pack encoding (a private detail of one backend, not of the object graph).
const GRAPH_KINDS: [&ObjectKind; 4] = [&MANIFEST, &TREE, &COMMIT, &REFS];

/// The highest object-graph format version this build ever writes.
pub(crate) fn current_format_version() -> u8 {
    GRAPH_KINDS
        .iter()
        .map(|k| k.write_version)
        .max()
        .unwrap_or(1)
}

/// The highest object-graph format version this build can read.
pub(crate) fn max_readable_format_version() -> u8 {
    GRAPH_KINDS
        .iter()
        .map(|k| k.max_read_version)
        .max()
        .unwrap_or(1)
}

impl ObjectKind {
    /// The 5 header bytes an encoder emits.
    pub(crate) fn header(&self) -> [u8; HEADER_LEN] {
        let mut h = [0u8; HEADER_LEN];
        h[..4].copy_from_slice(self.tag);
        h[4] = self.write_version;
        h
    }

    /// Whether `bytes` carries this kind's type tag, **ignoring the version**.
    ///
    /// Use this to classify an object before decoding it, so "an object of this
    /// kind I'm too old to read" stays distinguishable from "not this kind at
    /// all". Note that a raw data chunk can begin with these four bytes by
    /// coincidence, so a positive answer is a claim, not proof.
    pub(crate) fn tagged(&self, bytes: &[u8]) -> bool {
        bytes.len() >= HEADER_LEN && &bytes[..4] == self.tag
    }

    /// Validate the header and return the object's format version.
    ///
    /// A wrong tag, a truncated header, or version `0` (never written) is
    /// [`malformed`](Self::malformed); a version this build is too old to decode
    /// is [`OrigoFSError::UnsupportedVersion`].
    pub(crate) fn version_of(&self, bytes: &[u8]) -> Result<u8> {
        if !self.tagged(bytes) {
            return Err(self.malformed());
        }
        match bytes[4] {
            0 => Err(self.malformed()),
            v if v > self.max_read_version => Err(self.unsupported(v)),
            v => Ok(v),
        }
    }

    /// This kind's "the bytes aren't a valid object of this kind" error.
    pub(crate) fn malformed(&self) -> OrigoFSError {
        OrigoFSError::Content(format!("malformed {} object", self.name))
    }

    /// This kind's "written by a newer origofs" error.
    pub(crate) fn unsupported(&self, found: u8) -> OrigoFSError {
        OrigoFSError::UnsupportedVersion {
            kind: self.name,
            found,
            max_supported: self.max_read_version,
        }
    }

    /// This kind's name, as it appears in errors and reports.
    pub(crate) fn name(&self) -> &'static str {
        self.name
    }
}

/// The name of the [`StoreDescriptor`] slot, under
/// [`ContentStore::get_meta`](crate::ContentStore::get_meta).
pub(crate) const STORE_DESCRIPTOR_SLOT: &str = "format";

/// What a content store says about the object formats inside it.
///
/// Stamped into a named slot at the store's root by [`Fs::init`](crate::Fs::init)
/// and checked on every open, so a build that is too old to read a store learns
/// it **once, at open**, instead of N objects later with N confusing per-object
/// errors — or worse, not at all, from a code path that treats an undecodable
/// object as absent.
///
/// The slot is *not* content-addressed: it lives outside the object namespace, so
/// `list` never returns it and `gc` can never sweep it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StoreDescriptor {
    /// The highest object-graph format version any writer has put in this store.
    pub format_version: u8,
    /// The lowest reader that can read this store *completely*. Equal to
    /// `format_version` today; they diverge only if a future version is additive
    /// enough that an older reader can still see everything that matters, which is
    /// a judgement the writer makes when it bumps `format_version`.
    pub min_reader_version: u8,
}

impl StoreDescriptor {
    /// What this build stamps on a store it is the first to touch.
    pub(crate) fn current() -> Self {
        Self {
            format_version: current_format_version(),
            min_reader_version: current_format_version(),
        }
    }

    /// `ORGS | version | format_version(u8) | min_reader_version(u8)`
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = STORE.header().to_vec();
        out.push(self.format_version);
        out.push(self.min_reader_version);
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        match STORE.version_of(bytes)? {
            1 => {
                if bytes.len() < HEADER_LEN + 2 {
                    return Err(STORE.malformed());
                }
                Ok(Self {
                    format_version: bytes[HEADER_LEN],
                    min_reader_version: bytes[HEADER_LEN + 1],
                })
            }
            v => Err(STORE.unsupported(v)),
        }
    }

    /// Whether this build can read the store the descriptor describes.
    pub(crate) fn check_readable(&self) -> Result<()> {
        let max = max_readable_format_version();
        if self.min_reader_version > max {
            return Err(OrigoFSError::UnsupportedVersion {
                kind: "store",
                found: self.min_reader_version,
                max_supported: max,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [&ObjectKind; 7] = [&MANIFEST, &TREE, &COMMIT, &REFS, &STORE, &PACK, &PACK_INDEX];

    #[test]
    fn readers_are_never_behind_writers() {
        // Rule 3: support for reading a version ships before anything writes it.
        for k in ALL {
            assert!(
                k.write_version <= k.max_read_version,
                "{}: writes v{} but only reads up to v{}",
                k.name,
                k.write_version,
                k.max_read_version
            );
            assert!(
                k.write_version >= 1,
                "{}: v0 is not a valid version",
                k.name
            );
        }
    }

    #[test]
    fn type_tags_are_distinct() {
        // Recovery classifies objects by tag alone, so a collision would make two
        // kinds indistinguishable in the store.
        for (i, a) in ALL.iter().enumerate() {
            for b in &ALL[i + 1..] {
                assert_ne!(a.tag, b.tag, "{} and {} share a type tag", a.name, b.name);
            }
        }
    }

    #[test]
    fn version_of_separates_too_new_from_malformed() {
        let mut buf = TREE.header().to_vec();
        assert_eq!(TREE.version_of(&buf).unwrap(), 1);

        // A future version: a distinct, actionable error — not "corrupt".
        buf[4] = 2;
        let e = TREE.version_of(&buf).unwrap_err();
        assert!(matches!(
            e,
            OrigoFSError::UnsupportedVersion {
                kind: "tree",
                found: 2,
                max_supported: 1
            }
        ));
        assert_eq!(e.code(), "unsupported_version");

        // v0 was never written, and a foreign/truncated tag is just malformed.
        buf[4] = 0;
        assert_eq!(TREE.version_of(&buf).unwrap_err().code(), "content_error");
        assert_eq!(
            TREE.version_of(b"ORGC\x01").unwrap_err().code(),
            "content_error"
        );
        assert_eq!(TREE.version_of(b"ORG").unwrap_err().code(), "content_error");
    }

    #[test]
    fn store_descriptor_round_trips_and_gates_on_min_reader() {
        let d = StoreDescriptor::current();
        assert_eq!(StoreDescriptor::decode(&d.encode()).unwrap(), d);
        d.check_readable().expect("a store we stamped is readable");

        // A store only a future origofs can read fully: one error, at open.
        let future = StoreDescriptor {
            format_version: 9,
            min_reader_version: 9,
        };
        let e = future.check_readable().unwrap_err();
        assert!(matches!(
            e,
            OrigoFSError::UnsupportedVersion {
                kind: "store",
                found: 9,
                ..
            }
        ));

        // Additive change: written by a newer build, still fully readable here.
        StoreDescriptor {
            format_version: 9,
            min_reader_version: 1,
        }
        .check_readable()
        .expect("min_reader_version is what gates, not format_version");
    }
}
