//! Golden fixtures for the on-disk object format (`origofs_core::format`).
//!
//! Every structured object in the content store is addressed by the BLAKE3 hash
//! of its own bytes, so **the encoding is the address**. Changing what `encode`
//! emits for an existing format version — even in a way that round-trips
//! perfectly — re-addresses every object: the DAG forks, dedup against everything
//! already stored is lost, and two builds of origofs stop agreeing on what a given
//! tree *is*. That failure is silent in unit tests that only check `decode(encode(x)) == x`.
//!
//! These fixtures pin the v1 bytes and their resulting hashes so it fails here
//! instead. **If one of these assertions fails, do not update the constant** —
//! add a new format version alongside v1 (see the rules in `src/format.rs`).
//!
//! The rest of the file covers the other half of the contract: an object written
//! by a *newer* origofs must be reported as such, not as corruption.

use origofs_core::chunk::{ChunkRef, Manifest, chunk_bounds};
use origofs_core::error::OrigoFSError;
use origofs_core::objectgraph::{Commit, RefSnapshot, Tree, TreeEntry, TreeKind};
use origofs_core::types::Hash;

fn h(b: u8) -> Hash {
    Hash::from_array([b; 32])
}

fn hex_to_bytes(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
        .collect()
}

// --- the fixtures ------------------------------------------------------------

// Deliberately unbroken single-line literals: a line continuation that silently
// drops or duplicates a nibble would be a broken fixture, not a broken format.

/// `Manifest { size: 40, chunks: [(0xaa…, 16), (0xbb…, 24)] }`
const MANIFEST_V1: &str = "4f52474d01280000000000000002000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa10000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb18000000";
const MANIFEST_V1_HASH: &str = "ba6a498ec75e763b3c4cbd6d7b42315ce805f268db9c59737c2aefaa6d8b3b08";

/// A tree with one file, one dir, and one symlink entry.
const TREE_V1: &str = "4f524754010300000000a48100000500612e747874111111111111111111111111111111111111111111111111111111111111111101ed4100000300646972222222222222222222222222222222222222222222222222222222222222222202ffa1000004006c696e6b3333333333333333333333333333333333333333333333333333333333333333";
const TREE_V1_HASH: &str = "15f7279402469d7fd9b0f3c2c67f12b458fcad111ddae3d63d06de4d64c9d13b";

/// A two-parent (merge) commit.
const COMMIT_V1: &str = "4f524743014444444444444444444444444444444444444444444444444444444444444444020000005555555555555555555555555555555555555555555555555555555555555555666666666666666666666666666666666666666666666666666666666666666600f15365000000000500616c6963650b0000007365656420636f6d6d6974";
const COMMIT_V1_HASH: &str = "973e2f470e67ac8a605f155cb691c2372a7e3e199e5fb3e8834cd26e8afd0293";

/// A ref mirror at generation 7 with `HEAD` -> `main`.
const REFS_V1: &str = "4f52475201070000000000000002000000040048454144080000007265663a6d61696e04006d61696e4000000037373737373737373737373737373737373737373737373737373737373737373737373737373737373737373737373737373737373737373737373737373737";
const REFS_V1_HASH: &str = "084628a33593c8114984cfe24063c78cf7c5b73e60d71016faaa8dca24e6988d";

fn fixture_manifest() -> Manifest {
    Manifest {
        size: 40,
        chunks: vec![
            ChunkRef {
                hash: h(0xaa),
                len: 16,
            },
            ChunkRef {
                hash: h(0xbb),
                len: 24,
            },
        ],
    }
}

fn fixture_tree() -> Tree {
    Tree {
        entries: vec![
            TreeEntry {
                name: "a.txt".into(),
                mode: 0o100644,
                kind: TreeKind::File,
                hash: h(0x11),
            },
            TreeEntry {
                name: "dir".into(),
                mode: 0o040755,
                kind: TreeKind::Dir,
                hash: h(0x22),
            },
            TreeEntry {
                name: "link".into(),
                mode: 0o120777,
                kind: TreeKind::Symlink,
                hash: h(0x33),
            },
        ],
    }
}

fn fixture_commit() -> Commit {
    Commit {
        tree: h(0x44),
        parents: vec![h(0x55), h(0x66)],
        author: "alice".into(),
        message: "seed commit".into(),
        timestamp: 1_700_000_000,
    }
}

fn fixture_refs() -> RefSnapshot {
    RefSnapshot {
        generation: 7,
        refs: vec![
            ("HEAD".into(), "ref:main".into()),
            ("main".into(), h(0x77).to_hex()),
        ],
    }
}

// --- byte-exact encoding -----------------------------------------------------

/// Encoding must be byte-for-byte stable: these bytes *are* the object's address.
#[test]
fn v1_encodings_are_frozen() {
    for (name, actual, golden) in [
        (
            "manifest",
            fixture_manifest().encode().unwrap(),
            MANIFEST_V1,
        ),
        ("tree", fixture_tree().encode().unwrap(), TREE_V1),
        ("commit", fixture_commit().encode().unwrap(), COMMIT_V1),
        ("refs", fixture_refs().encode(), REFS_V1),
    ] {
        assert_eq!(
            hex::encode(&actual),
            golden,
            "{name} v1 encoding changed — this re-addresses every stored {name}. \
             Add a v2 instead of editing v1 (see src/format.rs)."
        );
    }
}

/// The addresses themselves, spelled out — the thing a bucket full of objects is
/// keyed by, and what a stale reader would fail to find after a silent re-encode.
#[test]
fn v1_addresses_are_frozen() {
    for (name, actual, golden) in [
        (
            "manifest",
            fixture_manifest().encode().unwrap(),
            MANIFEST_V1_HASH,
        ),
        ("tree", fixture_tree().encode().unwrap(), TREE_V1_HASH),
        ("commit", fixture_commit().encode().unwrap(), COMMIT_V1_HASH),
        ("refs", fixture_refs().encode(), REFS_V1_HASH),
    ] {
        assert_eq!(
            Hash::of(&actual).to_hex(),
            golden,
            "{name} v1 address changed"
        );
    }
}

/// A build must still decode bytes it did not just encode — the direction that
/// actually matters for reading an existing store.
#[test]
fn v1_bytes_decode_to_the_fixtures() {
    assert_eq!(
        Manifest::decode(&hex_to_bytes(MANIFEST_V1)).unwrap(),
        fixture_manifest()
    );
    assert_eq!(
        Tree::decode(&hex_to_bytes(TREE_V1)).unwrap(),
        fixture_tree()
    );
    assert_eq!(
        Commit::decode(&hex_to_bytes(COMMIT_V1)).unwrap(),
        fixture_commit()
    );
    assert_eq!(
        RefSnapshot::decode(&hex_to_bytes(REFS_V1)).unwrap(),
        fixture_refs()
    );
}

/// Every object carries the same 5-byte header: a 4-byte type tag + version `1`.
#[test]
fn every_object_is_tagged_and_versioned() {
    for (tag, hexs) in [
        (b"ORGM", MANIFEST_V1),
        (b"ORGT", TREE_V1),
        (b"ORGC", COMMIT_V1),
        (b"ORGR", REFS_V1),
    ] {
        let bytes = hex_to_bytes(hexs);
        assert_eq!(&bytes[..4], tag, "type tag");
        assert_eq!(bytes[4], 1, "format version byte");
    }
}

// --- version dispatch --------------------------------------------------------

/// The whole point of the version byte: an object from a newer origofs reports
/// `UnsupportedVersion` — with the kind and versions spelled out — instead of
/// "malformed", which reads like bit rot and sends an operator to their backups.
#[test]
fn a_future_version_is_reported_as_unsupported_not_corrupt() {
    fn bump(hexs: &str) -> Vec<u8> {
        let mut b = hex_to_bytes(hexs);
        b[4] = 2;
        b
    }
    let cases: Vec<(&str, OrigoFSError)> = vec![
        (
            "manifest",
            Manifest::decode(&bump(MANIFEST_V1)).unwrap_err(),
        ),
        ("tree", Tree::decode(&bump(TREE_V1)).unwrap_err()),
        ("commit", Commit::decode(&bump(COMMIT_V1)).unwrap_err()),
        (
            "ref snapshot",
            RefSnapshot::decode(&bump(REFS_V1)).unwrap_err(),
        ),
    ];
    for (kind, err) in cases {
        assert!(
            err.is_unsupported_version(),
            "{kind}: expected UnsupportedVersion, got {err}"
        );
        assert_eq!(err.code(), "unsupported_version");
        assert!(
            matches!(err, OrigoFSError::UnsupportedVersion { kind: k, found: 2, max_supported: 1 } if k == kind),
            "{kind}: wrong payload: {err:?}"
        );
        // The message has to tell the operator what to do about it.
        assert!(err.to_string().contains("newer origofs"), "{err}");
    }
}

/// Version `0` was never written, and a foreign tag is not "too new" — both are
/// plain malformed. Keeping these distinct is what stops the unsupported-version
/// signal from crying wolf on random bytes.
#[test]
fn v0_and_foreign_tags_stay_malformed() {
    let mut zeroed = hex_to_bytes(TREE_V1);
    zeroed[4] = 0;
    let err = Tree::decode(&zeroed).unwrap_err();
    assert!(
        !err.is_unsupported_version(),
        "v0 is malformed, not too new"
    );
    assert_eq!(err.code(), "content_error");

    // A commit's bytes are not a tree, however well-formed they are.
    let err = Tree::decode(&hex_to_bytes(COMMIT_V1)).unwrap_err();
    assert!(!err.is_unsupported_version());
    assert_eq!(err.code(), "content_error");

    for empty in [&b""[..], b"ORG", b"ORGT"] {
        assert!(Tree::decode(empty).is_err());
    }
}

/// Truncation is corruption, not a version problem.
#[test]
fn truncated_objects_are_not_reported_as_unsupported() {
    let full = hex_to_bytes(COMMIT_V1);
    for cut in [6, 20, full.len() - 1] {
        let err = Commit::decode(&full[..cut]).unwrap_err();
        assert!(
            !err.is_unsupported_version(),
            "truncating to {cut} should not look like a version problem: {err}"
        );
    }
}

// --- chunking stability ------------------------------------------------------

/// Chunk boundaries are not a correctness contract (a manifest records each
/// chunk's length, so any boundary set reads back fine) but they *are* a dedup
/// contract: change the FastCDC parameters and a re-write of an unchanged file
/// stores a whole new set of chunks that dedup against nothing already in the
/// store. Pinned here so that cost is a deliberate decision.
#[test]
fn fastcdc_boundaries_are_stable() {
    // A deterministic xorshift corpus — no fixture file, same bytes everywhere.
    let mut data = Vec::with_capacity(1 << 20);
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    while data.len() < (1 << 20) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        data.extend_from_slice(&x.to_le_bytes());
    }

    let bounds = chunk_bounds(&data);
    assert_eq!(bounds.len(), 13, "chunk count for the 1 MiB corpus");

    // Boundaries must tile the input exactly, with no gap or overlap.
    let mut next = 0usize;
    for (off, len) in &bounds {
        assert_eq!(*off, next, "chunk boundaries must be contiguous");
        next += len;
    }
    assert_eq!(next, data.len(), "chunks must cover the whole input");

    let mut flat = Vec::new();
    for (o, l) in &bounds {
        flat.extend_from_slice(&(*o as u64).to_le_bytes());
        flat.extend_from_slice(&(*l as u64).to_le_bytes());
    }
    assert_eq!(
        Hash::of(&flat).to_hex(),
        "ded0ae1ff520ea680a875ae6c0e63e690b44a1e1d0f362fbdaa6db9567e5bf09",
        "FastCDC boundaries changed — every re-written file will re-chunk and \
         stop deduplicating against what is already stored"
    );
}
