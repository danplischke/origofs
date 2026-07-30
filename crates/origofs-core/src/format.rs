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

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [&ObjectKind; 4] = [&MANIFEST, &TREE, &COMMIT, &REFS];

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
}
