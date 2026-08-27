//! Live co-editing at the workspace API (roadmap M8): open a document, apply an
//! edit from a collaborator's client, checkpoint it, and see each collaborator's
//! exact character spans in the byte-range blame index. The y-sync wire protocol
//! itself is covered in `origofs-core`; this exercises the workspace surface —
//! `open_coedit` / `checkpoint_coedit` — end to end. Requires the `coedit` feature.
#![cfg(feature = "coedit")]

use origofs_sdk::{CoeditDoc, Workspace, WriteCtx};

async fn workspace() -> (Workspace, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    (ws, dir)
}

// A human opens a doc and seeds a line; an agent's client (seeded from the doc's
// state) appends a line and syncs it up. The server attributes the agent's
// content to the agent — not the opener — and a checkpoint lands both authors'
// exact byte spans in blame.
#[tokio::test]
async fn workspace_coedit_open_apply_checkpoint_blame() {
    let (ws, _dir) = workspace().await;
    let alice = ws.create_human("alice", None).await.unwrap();
    let alice_s = ws.create_session(alice, Some("web")).await.unwrap();
    let claude = ws
        .create_agent("claude", "opus", Some(alice))
        .await
        .unwrap();
    let claude_s = ws.create_session(claude, Some("mcp")).await.unwrap();

    // Alice opens a fresh document on the server and types a line.
    let server = ws
        .open_coedit(WriteCtx::session(alice, alice_s), "/doc")
        .await
        .unwrap();
    server.insert(WriteCtx::session(alice, alice_s), 0, "alice wrote this\n");

    // Claude's editor pulls the current state, appends a line, and syncs its
    // update up. The server attributes exactly Claude's insertion to Claude.
    let client = CoeditDoc::load(&server.state_update()).unwrap();
    client.insert(
        WriteCtx::session(claude, claude_s),
        17,
        "claude added this\n",
    );
    server
        .apply_update_as(WriteCtx::session(claude, claude_s), &client.state_update())
        .unwrap();
    assert_eq!(server.text(), "alice wrote this\nclaude added this\n");

    // Checkpoint (driven by Alice — the driver does not change authorship) and
    // read blame: Alice owns line 1's bytes, Claude owns line 2's.
    ws.checkpoint_coedit(WriteCtx::session(alice, alice_s), "/doc", &server)
        .await
        .unwrap();
    assert_eq!(
        &ws.read("/doc").await.unwrap()[..],
        b"alice wrote this\nclaude added this\n"
    );

    let b = ws.blame("/doc").await.unwrap();
    assert_eq!(b.len(), 2);
    assert_eq!(
        (b[0].actor.id, b[0].byte_start, b[0].byte_end),
        (alice, 0, 17)
    );
    assert_eq!(
        (b[1].actor.id, b[1].byte_start, b[1].byte_end),
        (claude, 17, 35)
    );
    assert_eq!(b[1].session, Some(claude_s));

    // The session is durable: reopening restores the live CRDT, not just text.
    let resumed = ws
        .open_coedit(WriteCtx::session(alice, alice_s), "/doc")
        .await
        .unwrap();
    assert_eq!(resumed.text(), "alice wrote this\nclaude added this\n");
}
