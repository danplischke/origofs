//! The co-edit CRDT sidecars carry a versioned header, so their framing can be
//! evolved without the change reading as "this document has no history".
//!
//! Both shapes treat an unparseable sidecar as an **absent** one — the flat shape
//! rebuilds from the file, the tree shape opens empty. That fallback is right for
//! junk and catastrophic for bytes a newer origofs wrote, which is the whole
//! reason for the version byte: a future version has to be an error the reader
//! *reports*, never a document it silently reconstructs without its history.
//! Requires the `coedit` feature.
#![cfg(feature = "coedit")]

use origofs_core::{
    CoeditDoc, CoeditTreeDoc, Fs, MemStore, OrigoFSError, SqliteMetadataStore, TreeSpan, WriteCtx,
    coedit_sidecar_path, coedit_tree_sidecar_path,
};
use std::sync::Arc;
use yrs::types::xml::{XmlElementPrelim, XmlFragment, XmlTextPrelim};
use yrs::{Doc, ReadTxn, StateVector, Transact};

const ROOT: &str = "content";
/// `tag(4) | version(1)`, mirroring `origofs_core::format::HEADER_LEN`.
const HEADER_LEN: usize = 5;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();
    fs
}

/// An actor + session to write as.
async fn actor(fs: &Fs<SqliteMetadataStore, Arc<MemStore>>) -> WriteCtx {
    let a = fs.create_human("alice", None).await.unwrap();
    let s = fs.create_session(a, None).await.unwrap();
    WriteCtx::session(a, s)
}

/// A flat document checkpointed at `/doc`, returning its text and state vector so
/// a resume can be compared against them.
async fn flat_document(
    fs: &Fs<SqliteMetadataStore, Arc<MemStore>>,
    ctx: WriteCtx,
) -> (String, Vec<u8>) {
    let doc = CoeditDoc::new();
    doc.insert(ctx, 0, "hello ");
    doc.insert(ctx, 6, "world");
    fs.checkpoint_coedit(ctx, "/doc", &doc).await.unwrap();
    (doc.text(), doc.state_vector())
}

/// A tree document checkpointed at `/notes.md`.
async fn tree_document(
    fs: &Fs<SqliteMetadataStore, Arc<MemStore>>,
    ctx: WriteCtx,
) -> CoeditTreeDoc {
    let doc = fs.open_coedit_tree(ctx, "/notes.md", ROOT).await.unwrap();
    // Seed the way a real editor does: an opaque update from a plain `yrs` doc
    // bound to the same fragment root.
    let editor = Doc::new();
    let frag = editor.get_or_insert_xml_fragment(ROOT);
    {
        let mut txn = editor.transact_mut();
        let p = frag.push_back(&mut txn, XmlElementPrelim::empty("p"));
        p.push_back(&mut txn, XmlTextPrelim::new("hello"));
    }
    let update = editor
        .transact()
        .encode_state_as_update_v1(&StateVector::default());
    doc.apply_update_as(ctx, &update).unwrap();
    fs.checkpoint_coedit_tree(ctx, "/notes.md", &doc, b"hello\n", &[] as &[TreeSpan])
        .await
        .unwrap();
    doc
}

async fn sidecar(fs: &Fs<SqliteMetadataStore, Arc<MemStore>>, path: &str) -> Vec<u8> {
    fs.read(path).await.unwrap().to_vec()
}

// What a build writes today: the versioned framing, with distinct tags so neither
// shape can ever be decoded as the other.
#[tokio::test]
async fn a_sidecar_written_today_carries_its_tag_and_version() {
    let fs = fixture().await;
    let ctx = actor(&fs).await;
    flat_document(&fs, ctx).await;
    tree_document(&fs, ctx).await;

    let flat = sidecar(&fs, &coedit_sidecar_path("/doc")).await;
    assert_eq!(&flat[..4], b"ORGY");
    assert_eq!(flat[4], 1);

    let tree = sidecar(&fs, &coedit_tree_sidecar_path("/notes.md")).await;
    assert_eq!(&tree[..4], b"ORGX");
    assert_eq!(tree[4], 1);
}

// The pre-versioning framing (`[1]`/`[2]` + payload) is what every bucket in the
// wild holds, so it stays readable — with its history, not merely its text.
#[tokio::test]
async fn a_pre_versioning_flat_sidecar_still_resumes() {
    let fs = fixture().await;
    let ctx = actor(&fs).await;
    let (text, sv) = flat_document(&fs, ctx).await;

    // Re-frame the sidecar the way 0.0.4 wrote it: one magic byte, no version.
    let framed = sidecar(&fs, &coedit_sidecar_path("/doc")).await;
    let mut legacy = vec![1u8];
    legacy.extend_from_slice(&framed[HEADER_LEN..]);
    fs.write(&coedit_sidecar_path("/doc"), &legacy)
        .await
        .unwrap();

    let resumed = fs.open_coedit(ctx, "/doc").await.unwrap();
    assert_eq!(resumed.text(), text);
    // The state vector is the proof it was *resumed* rather than rebuilt: a
    // rebuild from the file would produce the same text under a fresh client id.
    assert_eq!(resumed.state_vector(), sv);
}

#[tokio::test]
async fn a_pre_versioning_tree_sidecar_still_resumes() {
    let fs = fixture().await;
    let ctx = actor(&fs).await;
    let doc = tree_document(&fs, ctx).await;
    let sv = doc.state_vector();

    let framed = sidecar(&fs, &coedit_tree_sidecar_path("/notes.md")).await;
    let mut legacy = vec![2u8];
    legacy.extend_from_slice(&framed[HEADER_LEN..]);
    fs.write(&coedit_tree_sidecar_path("/notes.md"), &legacy)
        .await
        .unwrap();

    let resumed = fs.load_coedit_tree("/notes.md", ROOT).await.unwrap();
    assert!(resumed.resumed(), "a legacy tree sidecar must still resume");
    assert_eq!(resumed.state_vector(), sv);
}

// The point of the exercise. A sidecar from a newer origofs is an upgrade
// problem, and must not be mistaken for one that isn't there.
#[tokio::test]
async fn a_future_flat_sidecar_is_an_upgrade_error_not_a_silent_rebuild() {
    let fs = fixture().await;
    let ctx = actor(&fs).await;
    flat_document(&fs, ctx).await;

    let mut future = sidecar(&fs, &coedit_sidecar_path("/doc")).await;
    future[4] = 2;
    fs.write(&coedit_sidecar_path("/doc"), &future)
        .await
        .unwrap();

    match fs.open_coedit(ctx, "/doc").await {
        Err(OrigoFSError::UnsupportedVersion {
            kind,
            found,
            max_supported,
        }) => {
            assert_eq!((kind, found, max_supported), ("co-edit sidecar", 2, 1));
        }
        Ok(_) => panic!("a future sidecar was silently rebuilt instead of reported"),
        Err(e) => panic!("expected UnsupportedVersion, got {e:?}"),
    }
}

#[tokio::test]
async fn a_future_tree_sidecar_is_an_upgrade_error_not_an_empty_document() {
    let fs = fixture().await;
    let ctx = actor(&fs).await;
    tree_document(&fs, ctx).await;

    let mut future = sidecar(&fs, &coedit_tree_sidecar_path("/notes.md")).await;
    future[4] = 2;
    fs.write(&coedit_tree_sidecar_path("/notes.md"), &future)
        .await
        .unwrap();

    match fs.load_coedit_tree("/notes.md", ROOT).await {
        Err(OrigoFSError::UnsupportedVersion { kind, found, .. }) => {
            assert_eq!((kind, found), ("co-edit tree sidecar", 2));
        }
        Ok(_) => panic!("a future tree sidecar opened as an empty document"),
        Err(e) => panic!("expected UnsupportedVersion, got {e:?}"),
    }
    // The same error on the root probe, which reads the sidecar for its root name.
    assert!(matches!(
        fs.coedit_tree_root("/notes.md").await,
        Err(OrigoFSError::UnsupportedVersion { .. })
    ));
}

// The fallback the version byte protects is still there for bytes that are
// genuinely not a sidecar: junk costs a rebuild, not an error.
#[tokio::test]
async fn junk_is_still_a_cache_miss_on_both_shapes() {
    let fs = fixture().await;
    let ctx = actor(&fs).await;
    let (text, sv) = flat_document(&fs, ctx).await;
    tree_document(&fs, ctx).await;

    fs.write(&coedit_sidecar_path("/doc"), b"not a sidecar")
        .await
        .unwrap();
    let rebuilt = fs.open_coedit(ctx, "/doc").await.unwrap();
    assert_eq!(rebuilt.text(), text, "rebuilt from the file's own bytes");
    assert_ne!(rebuilt.state_vector(), sv, "rebuilt, not resumed");

    fs.write(&coedit_tree_sidecar_path("/notes.md"), b"not a sidecar")
        .await
        .unwrap();
    let empty = fs.load_coedit_tree("/notes.md", ROOT).await.unwrap();
    assert!(!empty.resumed(), "the tree shape opens empty and says so");
}

/// A bounded randomized sweep over both framings: arbitrary bytes must be
/// *reported*, never fatal.
///
/// The real coverage is `cargo fuzz run sidecar_decode` / `tree_sidecar_decode`,
/// which CI only `cargo check`s — so without something like this the totality
/// claim is only ever argued, never executed, on an ordinary test run. This is
/// the cheap standing version of that argument: deterministic, seeded, and it
/// leans on the shapes a purely random generator would almost never produce (a
/// valid tag, a plausible length prefix) because those are the inputs that reach
/// past the first guard.
///
/// # Why this stops at the framing
///
/// It deliberately does **not** drive `CoeditDoc::load` / `CoeditTreeDoc::load`
/// on the same bytes, which is the obvious next step and is what the
/// `coedit_state_decode` fuzz target does instead. Those reach `yrs`'s
/// `Update::decode_v1`, and on malformed input `yrs 0.23.5` builds a `str` from
/// unvalidated bytes and then iterates it — undefined behaviour, which aborts the
/// process rather than unwinding, so a test cannot contain it. Writing this sweep
/// is what found that; it is tracked separately and is not origofs's bug to fix
/// here. The framing parsers below are origofs's own, and they are total.
#[test]
fn arbitrary_bytes_are_reported_rather_than_fatal() {
    fn xorshift(x: &mut u64) -> u64 {
        *x ^= *x << 13;
        *x ^= *x >> 7;
        *x ^= *x << 17;
        *x
    }

    let prefixes: [&[u8]; 6] = [b"ORGY", b"ORGX", &[1], &[2], &[0], b"ORG"];
    let mut seed = 0x5eed_1234_u64;
    for case in 0..4000u32 {
        let mut blob: Vec<u8> = Vec::new();
        // Half the cases start with something the parsers will actually accept as
        // a tag, so the sweep gets past the cheap rejection and into the length
        // and UTF-8 handling.
        if case % 2 == 0 {
            blob.extend_from_slice(prefixes[(case as usize / 2) % prefixes.len()]);
        }
        let len = (xorshift(&mut seed) % 80) as usize;
        while blob.len() < len {
            blob.extend_from_slice(&xorshift(&mut seed).to_le_bytes());
        }
        blob.truncate(len.max(blob.len().min(len)));

        // The contract is only "no panic"; either outcome is fine.
        let _ = origofs_core::fuzz_support::parse_flat_sidecar(&blob);
        let _ = origofs_core::fuzz_support::parse_tree_sidecar(&blob);
    }
}
