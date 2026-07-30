//! The store-level format descriptor: one loud failure at open, instead of N
//! confusing ones later.
//!
//! Per-object version bytes only help a caller that reaches the object. Several
//! code paths deliberately treat bytes they can't parse as *absent* — recovery
//! classification, the co-edit sidecar's rebuild-on-unparseable fallback — so a
//! store written by a newer origofs could otherwise be silently under-read. The
//! descriptor is stamped at `init` into a named slot beside the objects (never
//! content-addressed, so `gc` cannot sweep it) and checked on every open.

use origofs_core::{ContentStore, Fs, LocalCasStore, MemStore, SqliteMetadataStore};
use std::sync::Arc;

const SLOT: &str = "format";
/// `ORGS | version 1 | format_version | min_reader_version`
const DESCRIPTOR_V1: &[u8] = b"ORGS\x01\x01\x01";

async fn open(
    store: Arc<MemStore>,
) -> origofs_core::Result<Fs<SqliteMetadataStore, Arc<MemStore>>> {
    let fs = Fs::new(SqliteMetadataStore::open_in_memory().unwrap(), store);
    fs.init().await?;
    Ok(fs)
}

/// `Fs` isn't `Debug`, so `unwrap_err` is out; this keeps the tests readable.
async fn open_err(store: Arc<MemStore>) -> origofs_core::OrigoFSError {
    match open(store).await {
        Err(e) => e,
        Ok(_) => panic!("expected the open to fail"),
    }
}

#[tokio::test]
async fn init_stamps_a_fresh_store() {
    let store = Arc::new(MemStore::new());
    open(store.clone()).await.unwrap();

    assert_eq!(
        store.get_meta(SLOT).await.unwrap().as_deref(),
        Some(DESCRIPTOR_V1),
        "a fresh store must be stamped with the format this build writes"
    );
}

/// A store from before descriptors existed holds only v1 objects, so it opens
/// normally — and comes away stamped, so the *next* reader gets the check.
#[tokio::test]
async fn an_unstamped_store_opens_and_gets_stamped() {
    // A store from the pre-descriptor world: real objects in it, no slot.
    let store = Arc::new(MemStore::new());
    store.put(b"an object written long ago").await.unwrap();
    assert!(store.get_meta(SLOT).await.unwrap().is_none());

    let fs = open(store.clone()).await.unwrap();
    fs.write("/a.txt", b"hello").await.unwrap();

    assert_eq!(
        store.get_meta(SLOT).await.unwrap().as_deref(),
        Some(DESCRIPTOR_V1),
        "opening an unstamped store must stamp it, so the next reader gets checked"
    );
}

/// The headline case: the store says it needs a reader this build doesn't have.
#[tokio::test]
async fn open_fails_on_a_store_written_by_a_newer_origofs() {
    let store = Arc::new(MemStore::new());
    store
        .put_meta(SLOT, b"ORGS\x01\x02\x02") // format v2, needs a v2 reader
        .await
        .unwrap();

    let err = open_err(store).await;
    assert!(err.is_unsupported_version(), "got {err}");
    assert_eq!(err.code(), "unsupported_version");
    assert!(err.to_string().contains("store"), "{err}");
    assert!(err.to_string().contains("newer origofs"), "{err}");
}

/// `min_reader_version` is what gates, not `format_version`: a future writer that
/// judges its change additive can leave older readers working.
#[tokio::test]
async fn a_newer_format_that_stays_readable_still_opens() {
    let store = Arc::new(MemStore::new());
    store.put_meta(SLOT, b"ORGS\x01\x02\x01").await.unwrap();

    let fs = open(store).await.unwrap();
    fs.write("/a.txt", b"hello").await.unwrap();
    assert_eq!(&fs.read("/a.txt").await.unwrap()[..], b"hello");
}

/// A descriptor slot written in a format *the descriptor itself* postdates.
#[tokio::test]
async fn a_too_new_descriptor_encoding_is_also_caught() {
    let store = Arc::new(MemStore::new());
    store.put_meta(SLOT, b"ORGS\x02\x01\x01").await.unwrap();

    let err = open_err(store).await;
    assert!(err.is_unsupported_version(), "got {err}");
    assert!(err.to_string().contains("store descriptor"), "{err}");
}

/// Garbage: loud, not ignored. A store whose descriptor we can't parse is not a
/// store we should start writing objects into.
#[tokio::test]
async fn a_corrupt_descriptor_fails_the_open() {
    let store = Arc::new(MemStore::new());
    store.put_meta(SLOT, b"not a descriptor").await.unwrap();

    let err = open_err(store).await;
    assert!(!err.is_unsupported_version());
    assert_eq!(err.code(), "content_error");
}

/// The slot must be invisible to everything that walks the object namespace —
/// otherwise `gc` (mark-and-sweep over `list`) would collect it as unreachable.
#[tokio::test]
async fn the_slot_is_outside_the_object_namespace() {
    let store = Arc::new(MemStore::new());
    let fs = open(store.clone()).await.unwrap();
    fs.write("/a.txt", b"hello").await.unwrap();
    fs.commit("dan", "c1").await.unwrap();

    let listed = store.list().await.unwrap();
    let descriptor = origofs_core::Hash::of(DESCRIPTOR_V1);
    assert!(
        !listed.contains(&descriptor),
        "the descriptor must not appear as a content object"
    );

    fs.gc().await.unwrap();
    assert_eq!(
        store.get_meta(SLOT).await.unwrap().as_deref(),
        Some(DESCRIPTOR_V1),
        "gc must not be able to sweep the descriptor"
    );
    assert_eq!(&fs.read("/a.txt").await.unwrap()[..], b"hello");
}

/// A slot name becomes a path component or object key, so it is validated the way
/// every other name-shaped input into the storage layer is.
#[tokio::test]
async fn slot_names_cannot_escape_the_store_root() {
    let store = MemStore::new();
    for bad in ["..", ".", "", "../escape", "a/b", "UPPER", "sl\0t"] {
        assert!(
            store.put_meta(bad, b"x").await.is_err(),
            "put_meta accepted {bad:?}"
        );
        assert!(
            store.get_meta(bad).await.is_err(),
            "get_meta accepted {bad:?}"
        );
    }
}

/// On a real filesystem the slot lands in `<root>/meta/`, a sibling of `objects/`
/// — which is what keeps `list` (an `objects/` walk) from seeing it.
#[tokio::test]
async fn local_store_keeps_slots_beside_the_objects() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalCasStore::open(dir.path()).await.unwrap();

    assert!(store.get_meta(SLOT).await.unwrap().is_none());
    store.put_meta(SLOT, DESCRIPTOR_V1).await.unwrap();
    assert_eq!(
        store.get_meta(SLOT).await.unwrap().as_deref(),
        Some(DESCRIPTOR_V1)
    );
    // Overwrites in place, unlike a content-addressed put.
    store.put_meta(SLOT, b"ORGS\x01\x01\x01\x00").await.unwrap();
    assert_eq!(store.get_meta(SLOT).await.unwrap().unwrap().len(), 8);

    assert!(dir.path().join("meta").join(SLOT).exists());
    assert!(
        store.list().await.unwrap().is_empty(),
        "slots are not objects"
    );
}
