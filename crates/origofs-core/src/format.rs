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
///
/// v2 appends an addressing flag: whether the value stored under this key is
/// `BLAKE3(value)` (so `repack` can re-hash it) or was written by a transforming
/// layer that owns the address ([`EncryptedStore`](crate::encrypt::EncryptedStore)
/// storing ciphertext under the plaintext hash). v1 entries carry no flag and are
/// decoded as *unknown* — see `pack.rs`.
///
/// Rule 3 ("ship the reader before the writer") is deliberately taken in one step
/// here, unlike the object-graph kinds. A pack index is a private detail of one
/// backend — excluded from [`GRAPH_KINDS`] for exactly that reason — and it is
/// node-local rather than shared through the bucket, so there is no mixed-version
/// fleet reading one another's entries. An older binary that does meet a v2 entry
/// reports `UnsupportedVersion` ("upgrade origofs"), never corruption, and v1
/// entries stay readable forever.
pub(crate) const PACK_INDEX: ObjectKind = ObjectKind {
    tag: b"ORGI",
    name: "pack index entry",
    write_version: 2,
    max_read_version: 2,
};

/// The envelope wrapped around every encrypted object
/// ([`EncryptedStore`](crate::encrypt::EncryptedStore), `encryption` feature).
///
/// Everything else in a store could already be evolved; the ciphertext could not.
/// `EncryptedStore` wrote the bare AEAD output, so the cipher, the nonce
/// derivation and the KDF were pinned by the *binary* rather than recorded in the
/// bytes — and there was no way to change any of them without making every
/// existing object unreadable. Worse, the failure would have surfaced as
/// `Corrupt("wrong key or corrupt data")`, which is what a wrong passphrase looks
/// like: an upgrade would have read as an operator error.
///
/// The header names the scheme, so a v2 arm can define a different one and v1
/// objects keep decrypting forever. It costs 5 bytes per object and changes no
/// address — the address is the *plaintext* hash (convergent encryption), never
/// the stored bytes.
///
/// **Objects written before this existed carry no header**, and the decoder
/// accepts them for good: see `encrypt::EncryptedStore::decrypt` for how the two
/// are told apart, and why the AEAD tag — not the header — is what ultimately
/// decides.
#[cfg(feature = "encryption")]
pub(crate) const ENCRYPTED: ObjectKind = ObjectKind {
    tag: b"ORGE",
    name: "encrypted object",
    write_version: 1,
    max_read_version: 1,
};

/// The key-derivation descriptor stored beside the Argon2id salt
/// (`origofs-sdk`'s `kdf` sidecar, `encryption` feature).
///
/// Argon2id parameters were the `argon2` crate's `Params::default()` — a constant
/// owned by a dependency, not by origofs and not by the store. That default has
/// already moved once (0.4 → 0.5 raised `m_cost` from 4096 to 19456), and if it
/// moves again every passphrase-derived key changes and every object in every
/// encrypted store becomes undecryptable, presenting as "wrong passphrase".
///
/// Recording the parameters next to the salt they are used with makes the
/// derivation a property of the *store* instead of the build: a store carries the
/// cost it was created with, forever, and a future origofs is free to raise the
/// default for new stores without touching existing ones. A store with no
/// descriptor predates this and used [`crate::encrypt`]'s pinned legacy
/// parameters, which is exactly what the absent case falls back to.
#[cfg(feature = "encryption")]
pub(crate) const KDF: ObjectKind = ObjectKind {
    tag: b"ORGK",
    name: "key-derivation descriptor",
    write_version: 1,
    max_read_version: 1,
};

/// Co-edit CRDT sidecar, flat (`Y.Text`) shape (`crate::coedit`, `coedit` feature).
///
/// Unlike every other kind here the sidecar is not a content-store object — it is
/// an ordinary working-tree *file body*, chunked and committed like any other. It
/// carries a header anyway, for the reason `coedit::parse_sidecar` spells
/// out: a sidecar this build cannot parse is treated as **absent**, and an absent
/// sidecar is a silent fallback rather than an error. Without a version byte, the
/// first symptom of a format change is lost editing history.
#[cfg(feature = "coedit")]
pub(crate) const COEDIT_SIDECAR: ObjectKind = ObjectKind {
    tag: b"ORGY",
    name: "co-edit sidecar",
    write_version: 1,
    max_read_version: 1,
};

/// Co-edit CRDT sidecar, tree (`Y.XmlFragment`) shape (`crate::coedit_tree`, `coedit` feature).
///
/// The same framing argument as [`COEDIT_SIDECAR`], with more at stake: the flat
/// shape can rebuild a document from the file's text, and this one cannot —
/// parsing bytes back into nodes needs the host's schema. An unreadable tree
/// sidecar therefore opens an **empty** document, so a format change that this
/// build silently declined to recognize would discard every live document's
/// history and let a host checkpoint an empty body over the file.
#[cfg(feature = "coedit")]
pub(crate) const COEDIT_TREE_SIDECAR: ObjectKind = ObjectKind {
    tag: b"ORGX",
    name: "co-edit tree sidecar",
    write_version: 1,
    max_read_version: 1,
};

/// Every object kind whose version a *store* has to account for. Excludes the
/// store descriptor itself (it describes them; it cannot describe itself) and the
/// pack encoding (a private detail of one backend, not of the object graph).
///
/// The co-edit sidecars are excluded too, and that is a judgement rather than an
/// oversight. A store-level bump locks an older build out of the **whole** store
/// at open — every ordinary file read included — and co-editing is an opt-in
/// feature (`coedit`) most workspaces never write a byte of. Paying a store-wide
/// lockout for it is disproportionate. What replaces it is that a sidecar written
/// by a newer origofs is a loud `UnsupportedVersion` at the one document that
/// meets it, never the "unparseable, so absent" fallback the version byte exists
/// to prevent.
///
/// [`ENCRYPTED`] and [`KDF`] are excluded for a different reason again: the store
/// descriptor is stamped on **every** store, and folding an encryption-only kind
/// into it would raise the format version of unencrypted stores that can never
/// contain one. An envelope bump is caught loudly at the first object read, and
/// encryption is opt-in — the same trade as the sidecars, from the other side.
const GRAPH_KINDS: [&ObjectKind; 4] = [&MANIFEST, &TREE, &COMMIT, &REFS];

/// The oldest build that can read **everything this build writes**.
///
/// Deliberately its own constant rather than `current_format_version()`, because
/// the two answer different questions and conflating them breaks a mixed-version
/// fleet at exactly the moment rule 3 exists to protect it. `write_version` says
/// what this build emits; this says what a *reader* must be to keep up. They
/// diverge whenever a bump is additive — a new object kind, a field older readers
/// can ignore, a version only some paths ever produce — which is the common case.
///
/// It is stamped into the store descriptor's `min_reader_version`, and that is the
/// field that **locks older builds out of the whole store at open**. So raising
/// this is the deliberate, fleet-breaking act: it belongs in the same release that
/// starts writing genuinely non-additive objects, never in the release that merely
/// *learns* to write a new version. Everything shipped so far is v1, so it is 1.
const MIN_READER_VERSION: u8 = 1;

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
            min_reader_version: MIN_READER_VERSION,
        }
    }

    /// The descriptor to store once `self` (what the store advertises) has been
    /// met by `current` (what this build is): each field raised to the higher of
    /// the two, never lowered.
    ///
    /// The two fields are raised **independently**, and that is the whole point.
    /// They used to move together — `current()` set `min_reader_version` to
    /// `format_version`, and `check_store_format` wrote the pair whenever the
    /// format version advanced — so the first node in a fleet to upgrade stamped a
    /// `min_reader_version` every other node failed on, at *open*, before a single
    /// object of the new version existed anywhere in the bucket. That inverts rule
    /// 3 ("ship the reader before the writer"): an upgrade that was supposed to be
    /// invisible took the store away from everyone who had not taken it yet.
    ///
    /// Now `format_version` rises on its own — it is advisory, it says what may be
    /// *inside* — and `min_reader_version` rises only when
    /// [`MIN_READER_VERSION`] does, which is a decision a human makes when a bump
    /// genuinely cannot be read by older builds. In that case the lockout at open
    /// is the correct behaviour, and it is what the descriptor is for.
    ///
    /// Neither field is ever lowered: an older build re-opening a store must not
    /// erase a newer one's warning to everybody else.
    pub(crate) fn raised_over(self, current: Self) -> Self {
        Self {
            format_version: self.format_version.max(current.format_version),
            min_reader_version: self.min_reader_version.max(current.min_reader_version),
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

    /// The kinds that exist only with `coedit` on. Gated like the consts they
    /// name, so a `--no-default-features` build of this crate still compiles its
    /// tests — CI runs clippy on exactly that shape.
    #[cfg(feature = "coedit")]
    const COEDIT_KINDS: [&ObjectKind; 2] = [&COEDIT_SIDECAR, &COEDIT_TREE_SIDECAR];
    #[cfg(not(feature = "coedit"))]
    const COEDIT_KINDS: [&ObjectKind; 0] = [];

    /// Likewise for `encryption`.
    #[cfg(feature = "encryption")]
    const ENCRYPTION_KINDS: [&ObjectKind; 2] = [&ENCRYPTED, &KDF];
    #[cfg(not(feature = "encryption"))]
    const ENCRYPTION_KINDS: [&ObjectKind; 0] = [];

    /// Every kind this build knows.
    fn all() -> Vec<&'static ObjectKind> {
        ALL.iter()
            .chain(COEDIT_KINDS.iter())
            .chain(ENCRYPTION_KINDS.iter())
            .copied()
            .collect()
    }

    #[test]
    fn readers_are_never_behind_writers() {
        // Rule 3: support for reading a version ships before anything writes it.
        for k in all() {
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
        let all = all();
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
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

    /// The two fields move independently, and a fleet's ability to survive its
    /// own rollout depends on it.
    ///
    /// `current()` used to set `min_reader_version` to `format_version`, so the
    /// first node to upgrade stamped a descriptor every *other* node failed to
    /// open — before any object of the new version existed. An additive bump has
    /// to leave the gate where it is.
    #[test]
    fn an_additive_bump_raises_the_advisory_field_and_not_the_gate() {
        let stored = StoreDescriptor {
            format_version: 1,
            min_reader_version: 1,
        };
        // A build that writes v2 objects older builds can still read.
        let additive = StoreDescriptor {
            format_version: 2,
            min_reader_version: 1,
        };
        assert_eq!(stored.raised_over(additive), additive);
        stored
            .raised_over(additive)
            .check_readable()
            .expect("an additive bump must not lock this build out");

        // A build whose objects older readers genuinely cannot use: the gate moves,
        // and locking them out at open is then the correct behaviour.
        let breaking = StoreDescriptor {
            format_version: 2,
            min_reader_version: 2,
        };
        assert_eq!(stored.raised_over(breaking), breaking);
        assert!(breaking.check_readable().is_err());
    }

    /// An older build re-opening a store must not erase the warning a newer one
    /// left for everybody else.
    #[test]
    fn neither_field_is_ever_lowered() {
        let stored = StoreDescriptor {
            format_version: 4,
            min_reader_version: 3,
        };
        assert_eq!(stored.raised_over(StoreDescriptor::current()), stored);
    }

    /// What this build actually stamps on a fresh store. `MIN_READER_VERSION` is
    /// a separate decision from `write_version`, so it can lag it — but it can
    /// never exceed what this build can read, or a store would be unopenable by
    /// the build that created it.
    #[test]
    fn the_stamp_this_build_writes_is_self_consistent() {
        let d = StoreDescriptor::current();
        assert!(d.min_reader_version <= d.format_version);
        assert!(d.min_reader_version <= max_readable_format_version());
        d.check_readable().expect("we can read what we stamp");
    }
}
