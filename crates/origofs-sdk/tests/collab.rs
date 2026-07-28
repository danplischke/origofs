//! Live collaboration at the workspace API: user/agent operations land on the
//! change feed with attribution, the feed tails by cursor, and presence lists
//! who is active.

use origofs_sdk::{Workspace, WriteCtx};

async fn workspace() -> (Workspace, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    (ws, dir)
}

#[tokio::test]
async fn operations_land_on_the_feed_with_attribution() {
    let (ws, _dir) = workspace().await;

    // A human and an agent share the workspace.
    let alice = ws.create_human("alice", None).await.unwrap();
    let alice_s = ws.create_session(alice, Some("cli")).await.unwrap();
    let bot = ws
        .create_agent("claude", "opus", Some(alice))
        .await
        .unwrap();
    let bot_s = ws.create_session(bot, Some("mcp")).await.unwrap();

    ws.write_as(
        WriteCtx::session(alice, alice_s),
        "/notes.txt",
        b"human line\n",
    )
    .await
    .unwrap();
    ws.write_as(
        WriteCtx::session(bot, bot_s),
        "/notes.txt",
        b"human line\nagent line\n",
    )
    .await
    .unwrap();
    ws.commit("alice", "snapshot").await.unwrap();

    let events = ws.watch(0).await.unwrap();
    let writes: Vec<_> = events.iter().filter(|e| e.kind == "write").collect();
    assert_eq!(writes.len(), 2, "both attributed writes are on the feed");
    assert_eq!(
        (writes[0].actor_id, writes[0].session_id),
        (Some(alice), Some(alice_s))
    );
    assert_eq!(
        (writes[1].actor_id, writes[1].session_id),
        (Some(bot), Some(bot_s))
    );
    let commit = events.iter().find(|e| e.kind == "commit").unwrap();
    assert_eq!(commit.detail.as_deref(), Some("snapshot"));

    // Cursor tailing: after the first event, only later ones return.
    let tail = ws.watch(events[0].seq).await.unwrap();
    assert_eq!(tail.len(), events.len() - 1);
    assert!(tail.iter().all(|e| e.seq > events[0].seq));
}

#[tokio::test]
async fn structural_ops_are_recorded() {
    let (ws, _dir) = workspace().await;
    ws.mkdir_p("/src").await.unwrap();
    ws.write("/src/a.txt", b"x").await.unwrap();
    ws.rename("/src/a.txt", "/src/b.txt").await.unwrap();
    ws.remove("/src/b.txt").await.unwrap();

    let kinds: Vec<String> = ws
        .watch(0)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.kind)
        .collect();
    assert_eq!(kinds, vec!["mkdir", "write", "rename", "remove"]);

    // rename carries its destination in `detail`.
    let rename = ws
        .watch(0)
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.kind == "rename")
        .unwrap();
    assert_eq!(rename.path, "/src/a.txt");
    assert_eq!(rename.detail.as_deref(), Some("/src/b.txt"));
}

#[tokio::test]
async fn presence_lists_active_collaborators() {
    let (ws, _dir) = workspace().await;
    let alice = ws.create_human("alice", None).await.unwrap();
    let a_s = ws.create_session(alice, Some("cli")).await.unwrap();
    let bot = ws.create_agent("claude", "opus", None).await.unwrap();
    let b_s = ws.create_session(bot, Some("mcp")).await.unwrap();

    ws.touch(alice, a_s, Some("/notes.txt")).await.unwrap();
    ws.touch(bot, b_s, Some("/src/main.rs")).await.unwrap();

    let present = ws.presence(60).await.unwrap();
    assert_eq!(present.len(), 2);
    let bot_p = present.iter().find(|p| p.display_name == "claude").unwrap();
    assert_eq!(bot_p.kind.as_str(), "agent");
    assert_eq!(bot_p.path.as_deref(), Some("/src/main.rs"));
    assert!(
        present
            .iter()
            .any(|p| p.display_name == "alice" && p.kind.as_str() == "human")
    );
}
