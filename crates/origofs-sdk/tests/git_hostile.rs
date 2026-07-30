//! Importing a git repository the real `git` would never produce.
//!
//! `git_interop.rs` covers the honest round-trips against the actual `git`
//! binary. This file covers the other direction: a `.git` directory someone
//! hand-built. Loose objects are just zlib-compressed bytes at a path named after
//! an object id, so nothing stops that id from being a lie — and the importer
//! used to take it on trust, memoize a commit only *after* descending into its
//! parents, and recurse once per commit.
#![cfg(feature = "git")]

use origofs_sdk::Workspace;
use origofs_sdk::git::import_git;
use std::io::Write;
use std::path::{Path, PathBuf};

async fn workspace(dir: &Path) -> Workspace {
    Workspace::open_local(dir.join("ws.db"), dir.join("ws-cas"))
        .await
        .unwrap()
}

/// `"<kind> <len>\0" ++ payload` — git's object framing.
fn frame(kind: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = format!("{kind} {}\0", payload.len()).into_bytes();
    out.extend_from_slice(payload);
    out
}

fn sha1_hex(data: &[u8]) -> String {
    use sha1::{Digest, Sha1};
    hex::encode(Sha1::digest(data))
}

/// Write `framed` as a loose object under `oid_hex` — *without* checking that it
/// hashes to that id, which is exactly what a hostile repository does.
fn write_loose_raw(git_dir: &Path, oid_hex: &str, framed: &[u8]) {
    let path: PathBuf = git_dir
        .join("objects")
        .join(&oid_hex[..2])
        .join(&oid_hex[2..]);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(framed).unwrap();
    std::fs::write(&path, enc.finish().unwrap()).unwrap();
}

/// An honest loose object: framed, hashed, stored under its real id.
fn write_object(git_dir: &Path, kind: &str, payload: &[u8]) -> String {
    let framed = frame(kind, payload);
    let oid = sha1_hex(&framed);
    write_loose_raw(git_dir, &oid, &framed);
    oid
}

fn init_git_dir(dir: &Path, branch: &str, head_oid: &str) -> PathBuf {
    let git_dir = dir.join(".git");
    std::fs::create_dir_all(git_dir.join("refs").join("heads")).unwrap();
    std::fs::create_dir_all(git_dir.join("objects")).unwrap();
    std::fs::write(git_dir.join("HEAD"), format!("ref: refs/heads/{branch}\n")).unwrap();
    std::fs::write(
        git_dir.join("refs").join("heads").join(branch),
        format!("{head_oid}\n"),
    )
    .unwrap();
    git_dir
}

fn commit_payload(tree: &str, parents: &[&str], msg: &str) -> Vec<u8> {
    let mut s = format!("tree {tree}\n");
    for p in parents {
        s.push_str(&format!("parent {p}\n"));
    }
    s.push_str("author A <a@e> 1700000000 +0000\n");
    s.push_str("committer A <a@e> 1700000000 +0000\n");
    s.push_str(&format!("\n{msg}\n"));
    s.into_bytes()
}

/// A commit naming *itself* as its own parent.
///
/// The importer memoized a commit only after importing its parents, so this
/// recursed forever — a stack overflow, which is a `SIGSEGV` the test harness
/// cannot catch, not an error anyone can handle. It must come back as an `Err`.
#[tokio::test]
async fn self_parenting_commit_is_an_error_not_a_stack_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let git_dir = init_git_dir(&repo, "main", "0".repeat(40).as_str());

    let tree = write_object(&git_dir, "tree", b"");
    // Pick an id first, then write a commit that claims that id as its own parent
    // and store it there. No honest hash could ever satisfy this.
    let oid = "a".repeat(40);
    let payload = commit_payload(&tree, &[&oid], "i am my own parent");
    write_loose_raw(&git_dir, &oid, &frame("commit", &payload));
    std::fs::write(
        git_dir.join("refs").join("heads").join("main"),
        format!("{oid}\n"),
    )
    .unwrap();

    let err = import_git(&ws, &repo, "main").await;
    assert!(
        err.is_err(),
        "a self-parenting commit must be rejected, got {err:?}"
    );
}

/// Bytes stored under an id they don't hash to must be refused.
///
/// This is what makes the imported graph acyclic by construction: a cycle needs
/// an object to reference an id that depends on its own content.
#[tokio::test]
async fn object_not_matching_its_oid_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let git_dir = init_git_dir(&repo, "main", "0".repeat(40).as_str());

    let tree = write_object(&git_dir, "tree", b"");
    let honest = commit_payload(&tree, &[], "honest");
    let oid = sha1_hex(&frame("commit", &honest));

    // Same id, different bytes.
    let tampered = commit_payload(&tree, &[], "tampered");
    write_loose_raw(&git_dir, &oid, &frame("commit", &tampered));
    std::fs::write(
        git_dir.join("refs").join("heads").join("main"),
        format!("{oid}\n"),
    )
    .unwrap();

    let err = import_git(&ws, &repo, "main").await;
    assert!(
        err.is_err(),
        "an object that doesn't hash to its id must be refused, got {err:?}"
    );
}

/// A long *linear* history is not hostile at all — it is what any real repository
/// looks like. One nested future per commit made import depth-limited by the
/// stack, so this is the benign case the recursive walk also broke.
#[tokio::test]
async fn deep_linear_history_imports_without_exhausting_the_stack() {
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let git_dir = init_git_dir(&repo, "main", "0".repeat(40).as_str());

    let tree = write_object(&git_dir, "tree", b"");
    const N: usize = 4000;
    let mut prev: Option<String> = None;
    for i in 0..N {
        let parents: Vec<&str> = prev.iter().map(|s| s.as_str()).collect();
        let payload = commit_payload(&tree, &parents, &format!("commit {i}"));
        prev = Some(write_object(&git_dir, "commit", &payload));
    }
    let head = prev.unwrap();
    std::fs::write(
        git_dir.join("refs").join("heads").join("main"),
        format!("{head}\n"),
    )
    .unwrap();

    import_git(&ws, &repo, "main")
        .await
        .expect("a 4000-commit linear history must import");
    assert_eq!(ws.log().await.unwrap().len(), N);
}
