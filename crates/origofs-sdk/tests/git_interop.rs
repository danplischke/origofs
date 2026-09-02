//! Git interop: origofs history <-> real git objects, verified against the actual
//! `git` binary in both object formats, plus a git-LFS pointer round-trip.
#![cfg(feature = "git")]

use origofs_sdk::Workspace;
use origofs_sdk::git::{ExportOptions, ObjectFormat, export_git, import_git};
use std::path::Path;
use std::process::Command;

async fn workspace(dir: &Path, name: &str) -> Workspace {
    Workspace::open_local(
        dir.join(format!("{name}.db")),
        dir.join(format!("{name}-cas")),
    )
    .await
    .unwrap()
}

fn git(dir: &Path, args: &[&str]) -> (bool, String, String) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        // Keep the working tree byte-exact, on every platform. Git for Windows
        // ships `core.autocrlf=true` in its *system* config, so a checkout there
        // rewrites the LF in our exported blobs to CRLF and the working-tree
        // assertion below reads back `fn main() {}\r\n`. That is the host's
        // checkout filter doing exactly what it is configured to do — the blob
        // itself is correct, which `git show main:pkg/main.rs` above proves — so
        // the fix belongs here rather than in the exporter. Pinning it also stops
        // a developer's own global `core.autocrlf` from deciding whether this
        // suite passes. A no-op wherever autocrlf is already off.
        .args(["-c", "core.autocrlf=false", "-c", "core.eol=lf"])
        .args(args)
        .output()
        .expect("git must be installed for interop tests");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// git with a fixed identity + no signing, for creating source repos.
fn git_authored(dir: &Path, args: &[&str]) -> (bool, String, String) {
    let mut full = vec![
        "-c",
        "user.name=Tester",
        "-c",
        "user.email=tester@example.com",
        "-c",
        "commit.gpgsign=false",
    ];
    full.extend_from_slice(args);
    git(dir, &full)
}

// --- origofs -> git -> origofs, no external git binary involved ---------------------

async fn roundtrip_for(fmt: ObjectFormat) {
    let tmp = tempfile::tempdir().unwrap();
    let src = workspace(tmp.path(), "src").await;
    src.mkdir_p("/dir").await.unwrap();
    src.write("/readme.md", b"# hello\n").await.unwrap();
    src.write("/dir/nested.txt", b"deep\n").await.unwrap();
    src.symlink("/readme.md", "/link").await.unwrap();
    src.commit("Alice <alice@example.com>", "first commit")
        .await
        .unwrap();
    src.write("/readme.md", b"# hello\nmore\n").await.unwrap();
    src.commit("Bob <bob@example.com>", "second commit")
        .await
        .unwrap();

    let repo = tmp.path().join("exported");
    let opts = ExportOptions {
        format: fmt,
        ..Default::default()
    };
    let export = export_git(&src, &repo, &opts).await.unwrap();
    assert_eq!(export.commits, 2);

    // Re-import into a pristine workspace and check content + history survive.
    let dst = workspace(tmp.path(), "dst").await;
    import_git(&dst, &repo, "main").await.unwrap();

    assert_eq!(
        &dst.read("/readme.md").await.unwrap()[..],
        b"# hello\nmore\n"
    );
    assert_eq!(&dst.read("/dir/nested.txt").await.unwrap()[..], b"deep\n");
    assert_eq!(dst.readlink("/link").await.unwrap(), "/readme.md");

    let log = dst.log().await.unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].commit.message, "second commit");
    assert_eq!(log[1].commit.message, "first commit");
    assert_eq!(log[0].commit.author, "Bob <bob@example.com>");
}

#[tokio::test]
async fn roundtrip_sha1() {
    roundtrip_for(ObjectFormat::Sha1).await;
}

#[tokio::test]
async fn roundtrip_sha256() {
    roundtrip_for(ObjectFormat::Sha256).await;
}

// --- the real git binary reads what we export ------------------------------

async fn real_git_reads_export(fmt: ObjectFormat) {
    let tmp = tempfile::tempdir().unwrap();
    let src = workspace(tmp.path(), "src").await;
    src.mkdir_p("/pkg").await.unwrap();
    src.write("/pkg/main.rs", b"fn main() {}\n").await.unwrap();
    src.write("/top.txt", b"top level\n").await.unwrap();
    src.commit("Dev <dev@example.com>", "initial import")
        .await
        .unwrap();
    src.write("/top.txt", b"top level v2\n").await.unwrap();
    src.commit("Dev <dev@example.com>", "update top")
        .await
        .unwrap();

    let repo = tmp.path().join("repo");
    let export = export_git(
        &src,
        &repo,
        &ExportOptions {
            format: fmt,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Every object we wrote is valid and connected.
    let (ok, _out, err) = git(&repo, &["fsck", "--full", "--strict"]);
    assert!(ok, "git fsck failed: {err}");

    // History, subjects, and head oid line up with what we exported.
    let (ok, head, _) = git(&repo, &["rev-parse", "main"]);
    assert!(ok);
    assert_eq!(head.trim(), export.head);

    let (ok, subjects, _) = git(&repo, &["log", "--format=%s", "main"]);
    assert!(ok);
    assert_eq!(
        subjects.lines().collect::<Vec<_>>(),
        ["update top", "initial import"]
    );

    // File contents are readable straight from the objects.
    let (ok, content, _) = git(&repo, &["show", "main:pkg/main.rs"]);
    assert!(ok);
    assert_eq!(content, "fn main() {}\n");
    let (ok, content, _) = git(&repo, &["show", "main:top.txt"]);
    assert!(ok);
    assert_eq!(content, "top level v2\n");

    // And a real checkout materializes a correct working tree.
    let (ok, _o, err) = git(&repo, &["reset", "--hard", "main"]);
    assert!(ok, "git reset --hard failed: {err}");
    assert_eq!(
        std::fs::read(repo.join("pkg/main.rs")).unwrap(),
        b"fn main() {}\n"
    );
}

#[tokio::test]
async fn real_git_reads_export_sha1() {
    real_git_reads_export(ObjectFormat::Sha1).await;
}

#[tokio::test]
async fn real_git_reads_export_sha256() {
    real_git_reads_export(ObjectFormat::Sha256).await;
}

// --- we import what the real git binary produced ---------------------------

async fn import_real_git(fmt: ObjectFormat) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("real");
    std::fs::create_dir_all(&repo).unwrap();

    let obj_fmt = format!("--object-format={}", fmt.as_str());
    let (ok, _o, err) = git(&repo, &["init", "-q", "-b", "main", &obj_fmt]);
    assert!(ok, "git init failed: {err}");

    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("src/lib.rs"), b"pub fn f() {}\n").unwrap();
    std::fs::write(repo.join("README"), b"a real repo\n").unwrap();
    let (ok, _o, err) = git_authored(&repo, &["add", "-A"]);
    assert!(ok, "git add failed: {err}");
    let (ok, _o, err) = git_authored(&repo, &["commit", "-qm", "real one"]);
    assert!(ok, "git commit failed: {err}");

    std::fs::write(repo.join("README"), b"a real repo, edited\n").unwrap();
    let (ok, _o, _e) = git_authored(&repo, &["commit", "-qam", "real two"]);
    assert!(ok);

    // Import it and read the imported working tree + history back.
    let ws = workspace(tmp.path(), "ws").await;
    import_git(&ws, &repo, "main").await.unwrap();

    assert_eq!(
        &ws.read("/src/lib.rs").await.unwrap()[..],
        b"pub fn f() {}\n"
    );
    assert_eq!(
        &ws.read("/README").await.unwrap()[..],
        b"a real repo, edited\n"
    );

    let log = ws.log().await.unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].commit.message, "real two");
    assert!(log[0].commit.author.contains("Tester"));
}

#[tokio::test]
async fn import_real_git_sha1() {
    import_real_git(ObjectFormat::Sha1).await;
}

#[tokio::test]
async fn import_real_git_sha256() {
    import_real_git(ObjectFormat::Sha256).await;
}

// --- large files ride git-LFS pointers -------------------------------------

#[tokio::test]
async fn lfs_pointer_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let src = workspace(tmp.path(), "src").await;
    let big = vec![b'x'; 4096];
    src.write("/big.bin", &big).await.unwrap();
    src.write("/small.txt", b"tiny\n").await.unwrap();
    src.commit("Dev <dev@example.com>", "with a big file")
        .await
        .unwrap();

    let repo = tmp.path().join("lfs-repo");
    let export = export_git(
        &src,
        &repo,
        &ExportOptions {
            format: ObjectFormat::Sha256,
            branch: None,
            lfs_threshold: Some(1024),
        },
    )
    .await
    .unwrap();
    assert_eq!(export.lfs_objects, 1);

    // git sees a small pointer blob, not the 4 KiB payload.
    let (ok, pointer, _) = git(&repo, &["show", "main:big.bin"]);
    assert!(ok);
    assert!(pointer.starts_with("version https://git-lfs.github.com/spec/v1"));
    assert!(pointer.contains("size 4096"));
    // The small file stayed a normal blob.
    let (ok, small, _) = git(&repo, &["show", "main:small.txt"]);
    assert!(ok);
    assert_eq!(small, "tiny\n");

    // Import resolves the pointer back to the real bytes.
    let dst = workspace(tmp.path(), "dst").await;
    import_git(&dst, &repo, "main").await.unwrap();
    assert_eq!(dst.read("/big.bin").await.unwrap().len(), 4096);
    assert_eq!(&dst.read("/big.bin").await.unwrap()[..], &big[..]);
}

// --- `/.origofs` never leaves the workspace (#143) --------------------------

/// The exporter drops origofs's own state from the root of every commit tree.
///
/// The co-edit sidecars are committed working-tree files under `/.origofs/ydoc/`,
/// so an unfiltered walk shipped one opaque blob per co-edited path per commit
/// into any repo somebody cloned — carrying the `(actor, session)` stamps and node
/// ids the CRDT issued. Written here as plain files rather than through the
/// co-editing API on purpose: the exporter sees a path and some bytes, the feature
/// that produced them is not part of the question, and this suite is gated on
/// `git`, not `coedit`.
///
/// The two lookalikes are the point of the test as much as the sidecar is. A bare
/// `starts_with` would eat `/.origofs-bench`, and a name match at every level
/// would eat a user's own nested `.origofs` directory; only the root path is
/// internal.
async fn export_omits_internal_state_for(fmt: ObjectFormat) {
    let tmp = tempfile::tempdir().unwrap();
    let src = workspace(tmp.path(), "src").await;

    src.write("/notes.md", b"# notes\n").await.unwrap();
    src.mkdir_p("/.origofs/ydoc").await.unwrap();
    src.write("/.origofs/ydoc/2f6e6f7465732e6d64", b"\x01ydoc-state")
        .await
        .unwrap();
    src.mkdir_p("/.origofs-bench").await.unwrap();
    src.write("/.origofs-bench/keep.txt", b"bench\n")
        .await
        .unwrap();
    src.mkdir_p("/src/.origofs").await.unwrap();
    src.write("/src/.origofs/user.txt", b"mine\n")
        .await
        .unwrap();
    src.commit("Dev <dev@example.com>", "with a sidecar")
        .await
        .unwrap();

    let repo = tmp.path().join("exported");
    let opts = ExportOptions {
        format: fmt,
        ..Default::default()
    };
    export_git(&src, &repo, &opts).await.unwrap();

    let (ok, out, err) = git(&repo, &["ls-tree", "-r", "--name-only", "main"]);
    assert!(ok, "ls-tree failed: {err}");
    let paths: Vec<&str> = out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();

    assert!(
        !paths
            .iter()
            .any(|p| *p == ".origofs" || p.starts_with(".origofs/")),
        "exported repo carries origofs internal state: {paths:?}"
    );
    for expected in [
        "notes.md",
        ".origofs-bench/keep.txt",
        "src/.origofs/user.txt",
    ] {
        assert!(
            paths.contains(&expected),
            "exporter dropped a user path {expected:?}: {paths:?}"
        );
    }

    // Exporting is a read: the workspace keeps its own state.
    assert_eq!(
        &src.read("/.origofs/ydoc/2f6e6f7465732e6d64").await.unwrap()[..],
        b"\x01ydoc-state"
    );
}

#[tokio::test]
async fn export_omits_internal_state_sha1() {
    export_omits_internal_state_for(ObjectFormat::Sha1).await;
}

#[tokio::test]
async fn export_omits_internal_state_sha256() {
    export_omits_internal_state_for(ObjectFormat::Sha256).await;
}

/// `/.origofs` is internal because of *where it sits*, so the same origofs tree can
/// need exporting two different ways — and the exporter memoizes by tree hash.
///
/// Moving the whole root under `/sub` makes that concrete: the tree that was the
/// first commit's root (where `.origofs` is origofs's own state, filtered) is the
/// second commit's `/sub` (where it is an ordinary user directory, kept). Both
/// commits are exported in one walk, head first, so a memo keyed on the hash alone
/// hands the older commit the `/sub` encoding and ships the sidecar after all.
#[tokio::test]
async fn export_filters_by_position_not_by_tree_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let src = workspace(tmp.path(), "src").await;

    src.mkdir_p("/.origofs/ydoc").await.unwrap();
    src.write("/.origofs/ydoc/a", b"\x01state").await.unwrap();
    src.write("/notes.md", b"# notes\n").await.unwrap();
    src.commit("Dev <dev@example.com>", "internal at the root")
        .await
        .unwrap();

    src.mkdir_p("/sub").await.unwrap();
    src.rename("/.origofs", "/sub/.origofs").await.unwrap();
    src.rename("/notes.md", "/sub/notes.md").await.unwrap();
    src.commit("Dev <dev@example.com>", "moved under /sub")
        .await
        .unwrap();

    // The aliasing is the premise of the test, so assert it rather than assume it:
    // if a future change stops the two trees colliding, this must fail loudly
    // instead of quietly testing nothing.
    let log = src.log().await.unwrap();
    let head_root = src.fs().tree_object(&log[0].commit.tree).await.unwrap();
    let sub = head_root
        .entries
        .iter()
        .find(|e| e.name == "sub")
        .expect("/sub must be in the head tree");
    assert_eq!(
        sub.hash, log[1].commit.tree,
        "premise: /sub must be the very same origofs tree as the older commit's root"
    );

    let repo = tmp.path().join("exported");
    export_git(&src, &repo, &ExportOptions::default())
        .await
        .unwrap();

    let listing = |rev: &str| {
        let (ok, out, err) = git(&repo, &["ls-tree", "-r", "--name-only", rev]);
        assert!(ok, "ls-tree {rev} failed: {err}");
        out.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };

    // Head: `.origofs` sits under `/sub`, which makes it a user path — it stays.
    let head = listing("main");
    assert!(
        head.iter().any(|p| p == "sub/.origofs/ydoc/a"),
        "a user directory named .origofs was dropped: {head:?}"
    );
    assert!(head.iter().any(|p| p == "sub/notes.md"), "{head:?}");

    // Parent: the identical tree was the root there, so it is internal and goes.
    let parent = listing("main~1");
    assert!(
        !parent.iter().any(|p| p.starts_with(".origofs/")),
        "the memoized tree leaked internal state into the older commit: {parent:?}"
    );
    assert!(parent.iter().any(|p| p == "notes.md"), "{parent:?}");
}
