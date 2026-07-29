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
