//! MCP tools for live co-editing (issue #75 §3.2, §3.4): proposing a CRDT merge
//! and reading a path's live/dirty flag.
//!
//! Agents are exactly the actors that should be *proposing* rather than writing,
//! and a live document is exactly where a byte proposal is wrong — its base is a
//! content hash that goes stale on somebody else's keystroke, and accepting it
//! replaces the whole body. These pin the two tools that close that gap, under the
//! same server-side attribution every other MCP tool call gets: the agent never
//! names itself, the server does.
#![cfg(all(feature = "mcp", feature = "coedit"))]

use origofs_sdk::mcp::McpServer;
use origofs_sdk::{SuggestionKind, Workspace, WriteCtx};
use serde_json::{Value, json};

/// A server plus a *clone* of the workspace it drives, so a test can inspect the
/// engine directly (the server owns its own handle; both point at one store).
async fn server_and_ws() -> (McpServer, Workspace) {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let server = McpServer::create(ws.clone(), "claude", "claude-opus-4-8")
        .await
        .unwrap();
    (server, ws)
}

fn call(name: &str, args: Value) -> Value {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": name, "arguments": args }
    })
}

fn text(resp: &Value) -> String {
    resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .to_string()
}

fn is_error(resp: &Value) -> bool {
    resp["result"]["isError"].as_bool().unwrap_or(false)
}

#[tokio::test]
async fn coedit_tools_are_advertised() {
    let (s, _ws) = server_and_ws().await;
    let list = s
        .handle(json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
        .await
        .unwrap();
    let tools = list["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"origofs_suggest_coedit"), "{names:?}");
    assert!(names.contains(&"origofs_live"), "{names:?}");

    // The schema shape matches the other tools': an object with typed properties
    // and an explicit required list. Note that neither names an actor — identity
    // is the server's, never the caller's.
    let sc = tools
        .iter()
        .find(|t| t["name"] == "origofs_suggest_coedit")
        .unwrap();
    assert_eq!(sc["inputSchema"]["type"], "object");
    assert_eq!(
        sc["inputSchema"]["required"],
        json!(["path", "old", "new"]),
        "{sc}"
    );
    assert_eq!(sc["inputSchema"]["properties"]["old"]["type"], "string");
    assert!(sc["inputSchema"]["properties"].get("actor").is_none());

    let live = tools.iter().find(|t| t["name"] == "origofs_live").unwrap();
    assert_eq!(live["inputSchema"]["required"], json!([]), "{live}");
}

#[tokio::test]
async fn suggest_coedit_proposes_a_crdt_merge_attributed_to_the_agent() {
    let (s, ws) = server_and_ws().await;

    // A human co-edits a document and checkpoints it, so there is a real live
    // document for the agent to propose against.
    let dan = ws.create_human("dan", None).await.unwrap();
    let dan_s = ws.create_session(dan, Some("editor")).await.unwrap();
    let h = WriteCtx::session(dan, dan_s);
    let doc = ws.open_coedit(h, "/notes.md").await.unwrap();
    doc.insert(h, 0, "alpha beta\n");
    ws.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();

    let r = s
        .handle(call(
            "origofs_suggest_coedit",
            json!({"path": "/notes.md", "old": "beta", "new": "gamma", "summary": "rename"}),
        ))
        .await
        .unwrap();
    assert!(!is_error(&r), "{}", text(&r));
    assert!(text(&r).contains("CRDT"), "{}", text(&r));

    // One pending suggestion, of kind `crdt`, credited to the *agent* — the
    // server's own actor, which the tool call never named.
    let pending = ws.list_suggestions(None, Some("/notes.md")).await.unwrap();
    assert_eq!(pending.len(), 1);
    let sug = &pending[0];
    assert_eq!(sug.kind, SuggestionKind::Crdt);
    assert_ne!(sug.actor_id, dan, "must not be credited to the human");
    let agent = ws.get_actor(sug.actor_id).await.unwrap().unwrap();
    assert_eq!(agent.display_name, "claude");
    assert_eq!(sug.summary.as_deref(), Some("rename"));
    // Both Yjs blobs are addressed in the content store, never inlined.
    assert!(sug.base_hash.is_some() && sug.proposed_hash.is_some());

    // Nothing landed in the working tree: this is propose-and-review.
    let before = String::from_utf8(ws.read("/notes.md").await.unwrap().to_vec()).unwrap();
    assert_eq!(before, "alpha beta\n");

    // The human accepts; the merge lands, credited to its author (the agent).
    ws.accept_suggestion(sug.id, h).await.unwrap();
    let after = String::from_utf8(ws.read("/notes.md").await.unwrap().to_vec()).unwrap();
    assert_eq!(after, "alpha gamma\n");
    let blame = ws.blame("/notes.md").await.unwrap();
    assert!(
        blame.iter().any(|b| b.actor.display_name == "claude"),
        "the merged text keeps the agent as its author: {blame:?}"
    );
}

#[tokio::test]
async fn suggest_coedit_rejects_an_ambiguous_or_empty_edit() {
    let (s, ws) = server_and_ws().await;
    let dan = ws.create_human("dan", None).await.unwrap();
    let h = WriteCtx::actor(dan);
    let doc = ws.open_coedit(h, "/notes.md").await.unwrap();
    doc.insert(h, 0, "x x\n");
    ws.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();
    ws.end_coedit("/notes.md").await.unwrap();

    for args in [
        json!({"path": "/notes.md", "old": "", "new": "y"}),
        json!({"path": "/notes.md", "old": "x", "new": "x"}),
        json!({"path": "/notes.md", "old": "nope", "new": "y"}),
        json!({"path": "/notes.md", "old": "x", "new": "y"}), // matches twice
    ] {
        let r = s
            .handle(call("origofs_suggest_coedit", args.clone()))
            .await
            .unwrap();
        assert!(is_error(&r), "{args} should be refused: {}", text(&r));
    }
    assert!(ws.list_suggestions(None, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn suggest_coedit_does_not_leave_a_live_marker_behind() {
    // The tool opens a throwaway replica to compute the update. Opening marks the
    // path live; since nothing was live before, the marker must be put back the
    // way it was found — otherwise every agent proposal would permanently tell
    // byte readers (and the git export) that the file may lag an open editor.
    let (s, ws) = server_and_ws().await;
    let dan = ws.create_human("dan", None).await.unwrap();
    let h = WriteCtx::actor(dan);
    let doc = ws.open_coedit(h, "/notes.md").await.unwrap();
    doc.insert(h, 0, "hello\n");
    ws.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();
    ws.end_coedit("/notes.md").await.unwrap();
    assert!(ws.live_doc("/notes.md").await.unwrap().is_none());

    let r = s
        .handle(call(
            "origofs_suggest_coedit",
            json!({"path": "/notes.md", "old": "hello", "new": "hi"}),
        ))
        .await
        .unwrap();
    assert!(!is_error(&r), "{}", text(&r));

    assert!(
        ws.live_doc("/notes.md").await.unwrap().is_none(),
        "a proposal must not leave the path marked live"
    );
    assert!(ws.live_paths().await.unwrap().is_empty());
}

#[tokio::test]
async fn suggest_coedit_leaves_a_real_room_marked_live() {
    // The other direction: if somebody genuinely has the document open, the
    // proposal must not clear their marker. A marker left set is the safe failure
    // direction; clearing a live one is the misleading one.
    let (s, ws) = server_and_ws().await;
    let dan = ws.create_human("dan", None).await.unwrap();
    let dan_s = ws.create_session(dan, Some("editor")).await.unwrap();
    let h = WriteCtx::session(dan, dan_s);
    let doc = ws.open_coedit(h, "/notes.md").await.unwrap();
    doc.insert(h, 0, "hello\n");
    ws.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();
    assert!(ws.live_doc("/notes.md").await.unwrap().is_some());

    let r = s
        .handle(call(
            "origofs_suggest_coedit",
            json!({"path": "/notes.md", "old": "hello", "new": "hi"}),
        ))
        .await
        .unwrap();
    assert!(!is_error(&r), "{}", text(&r));
    assert!(
        ws.live_doc("/notes.md").await.unwrap().is_some(),
        "an agent's proposal must not end somebody else's co-editing session"
    );
}

#[tokio::test]
async fn live_tool_reports_the_flag_for_a_path_and_lists_live_documents() {
    let (s, ws) = server_and_ws().await;
    let dan = ws.create_human("dan", None).await.unwrap();
    let dan_s = ws.create_session(dan, Some("editor")).await.unwrap();
    let h = WriteCtx::session(dan, dan_s);

    ws.write_as(h, "/quiet.md", b"just bytes\n").await.unwrap();

    // Nothing live yet.
    let r = s.handle(call("origofs_live", json!({}))).await.unwrap();
    assert_eq!(text(&r), "no live documents");
    let r = s
        .handle(call("origofs_live", json!({"path": "/quiet.md"})))
        .await
        .unwrap();
    assert!(text(&r).contains("not live"), "{}", text(&r));

    // Open one: the flag flips, and it shows up in the listing.
    let doc = ws.open_coedit(h, "/notes.md").await.unwrap();
    doc.insert(h, 0, "typing\n");
    ws.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();

    let r = s
        .handle(call("origofs_live", json!({"path": "/notes.md"})))
        .await
        .unwrap();
    let out = text(&r);
    assert!(out.contains("LIVE"), "{out}");
    assert!(out.contains("may lag"), "{out}");

    let listed = text(&s.handle(call("origofs_live", json!({}))).await.unwrap());
    assert!(listed.contains("/notes.md"), "{listed}");
    assert!(!listed.contains("/quiet.md"), "{listed}");

    // A live path still *reads* — surfacing staleness never blocks a reader.
    let r = s
        .handle(call("origofs_read", json!({"path": "/notes.md"})))
        .await
        .unwrap();
    assert!(!is_error(&r), "{}", text(&r));
    assert_eq!(text(&r), "typing\n");

    // And clearing the marker leaves the bytes alone.
    ws.end_coedit("/notes.md").await.unwrap();
    let r = s
        .handle(call("origofs_live", json!({"path": "/notes.md"})))
        .await
        .unwrap();
    assert!(text(&r).contains("not live"), "{}", text(&r));
}

// The same tool over a **non-ASCII** document.
//
// `CoeditDoc` indexes UTF-8 bytes; this tool converted to UTF-16 code units
// first, so on any document containing multi-byte characters the splice landed
// at the wrong offset and removed the wrong number of bytes — replacing "world"
// in "ééééé world\n" spliced at byte 6 instead of 11 and cut 5 bytes instead of
// 11, yielding "éééorigofsworld\nééééé world\n". The tool reported success, the
// suggestion was reviewable, and accepting it corrupted the file. This is the
// only production caller that indexes a co-edit document directly.
#[tokio::test]
async fn suggest_coedit_splices_at_the_right_offset_in_a_non_ascii_document() {
    let (s, ws) = server_and_ws().await;

    let dan = ws.create_human("dan", None).await.unwrap();
    let dan_s = ws.create_session(dan, Some("editor")).await.unwrap();
    let h = WriteCtx::session(dan, dan_s);
    let doc = ws.open_coedit(h, "/notes.md").await.unwrap();
    doc.insert(h, 0, "ééééé world\n"); // 5 two-byte chars: byte 11 != UTF-16 6
    ws.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();
    ws.end_coedit("/notes.md").await.unwrap();

    let r = s
        .handle(call(
            "origofs_suggest_coedit",
            json!({"path": "/notes.md", "old": "world", "new": "origofs", "summary": "rename"}),
        ))
        .await
        .unwrap();
    assert!(!is_error(&r), "{}", text(&r));

    let pending = ws.list_suggestions(None, Some("/notes.md")).await.unwrap();
    assert_eq!(pending.len(), 1);
    ws.accept_suggestion(pending[0].id, h).await.unwrap();

    let after = String::from_utf8(ws.read("/notes.md").await.unwrap().to_vec()).unwrap();
    assert_eq!(after, "ééééé origofs\n");
}
