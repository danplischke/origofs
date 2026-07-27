//! Drive the real `git` binary against an origofs workspace through the
//! `git-remote-origofs` helper: clone from origofs, then edit + commit + push back, and
//! confirm the pushed history and content land in origofs.

use origofs_sdk::Workspace;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The freshly built helper binary; its directory goes on `PATH` so git finds it.
const HELPER: &str = env!("CARGO_BIN_EXE_git-remote-origofs");

fn helper_dir() -> PathBuf {
    Path::new(HELPER).parent().unwrap().to_path_buf()
}

/// Run git with the helper on PATH and a fixed, side-effect-free identity.
fn git(cwd: Option<&Path>, args: &[&str]) -> (bool, String) {
    let path = format!(
        "{}:{}",
        helper_dir().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut cmd = Command::new("git");
    cmd.env("PATH", path)
        .env("GIT_AUTHOR_NAME", "Git User")
        .env("GIT_AUTHOR_EMAIL", "git@example.com")
        .env("GIT_COMMITTER_NAME", "Git User")
        .env("GIT_COMMITTER_EMAIL", "git@example.com")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .args(["-c", "commit.gpgsign=false"])
        .args(args);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let out = cmd.output().expect("git must be installed");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_edit_push_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let ws_path = tmp.path().join("ws");

    // Seed an origofs workspace with two files and a commit, then release it so the
    // helper (a subprocess) can open the same SQLite database.
    {
        let ws = Workspace::open_local(ws_path.join("meta.db"), ws_path.join("cas"))
            .await
            .unwrap();
        ws.write("/readme.md", b"hello\n").await.unwrap();
        ws.mkdir_p("/src").await.unwrap();
        ws.write("/src/main.rs", b"fn main() {}\n").await.unwrap();
        ws.commit("Dan <dan@example.com>", "initial").await.unwrap();
    }

    let url = format!("origofs://{}", ws_path.display());
    let dest = tmp.path().join("clone");

    // --- clone from origofs -----------------------------------------------------
    let (ok, err) = git(None, &["clone", "-q", &url, dest.to_str().unwrap()]);
    assert!(ok, "git clone failed: {err}");
    assert_eq!(std::fs::read(dest.join("readme.md")).unwrap(), b"hello\n");
    assert_eq!(
        std::fs::read(dest.join("src/main.rs")).unwrap(),
        b"fn main() {}\n"
    );
    let (ok, err) = git(Some(&dest), &["log", "--format=%s"]);
    assert!(ok, "git log failed: {err}");

    // --- edit + commit + push back into origofs --------------------------------
    std::fs::write(dest.join("readme.md"), b"hello\nedited via git\n").unwrap();
    std::fs::write(dest.join("new.txt"), b"brand new\n").unwrap();
    let (ok, err) = git(Some(&dest), &["add", "-A"]);
    assert!(ok, "git add failed: {err}");
    let (ok, err) = git(Some(&dest), &["commit", "-qm", "from git client"]);
    assert!(ok, "git commit failed: {err}");
    let (ok, err) = git(Some(&dest), &["push", "-q", "origin", "main"]);
    assert!(ok, "git push failed: {err}");

    // --- origofs now reflects the push -----------------------------------------
    let ws = Workspace::open_local(ws_path.join("meta.db"), ws_path.join("cas"))
        .await
        .unwrap();
    assert_eq!(&ws.read("/new.txt").await.unwrap()[..], b"brand new\n");
    assert_eq!(
        &ws.read("/readme.md").await.unwrap()[..],
        b"hello\nedited via git\n"
    );
    let log = ws.log().await.unwrap();
    assert_eq!(
        log.len(),
        2,
        "the pushed commit is on top of the initial one"
    );
    assert_eq!(log[0].commit.message, "from git client");
    assert_eq!(log[1].commit.message, "initial");
}
