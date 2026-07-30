//! MCP server: protocol handshake, tool listing, and attributed tool calls.
#![cfg(feature = "mcp")]

use origofs_sdk::mcp::McpServer;
use origofs_sdk::{SuggestionStatus, Workspace, WriteCtx, WritePolicy};
use serde_json::{Value, json};

async fn server() -> McpServer {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    McpServer::create(ws, "claude", "claude-opus-4-8")
        .await
        .unwrap()
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

#[tokio::test]
async fn initialize_and_list_tools() {
    let s = server().await;

    let init = s
        .handle(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}))
        .await
        .unwrap();
    assert_eq!(init["result"]["serverInfo"]["name"], "origofs");
    assert!(init["result"]["capabilities"]["tools"].is_object());

    // initialized is a notification -> no response
    assert!(
        s.handle(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await
            .is_none()
    );

    let list = s
        .handle(json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
        .await
        .unwrap();
    let names: Vec<&str> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"origofs_write"));
    assert!(names.contains(&"origofs_blame"));
}

#[tokio::test]
async fn writes_are_attributed_to_the_agent() {
    let s = server().await;

    let w = s
        .handle(call(
            "origofs_write",
            json!({"path":"/notes.txt","content":"one\ntwo\n"}),
        ))
        .await
        .unwrap();
    assert!(text(&w).contains("wrote"));
    assert_eq!(w["result"]["isError"], false);

    let r = s
        .handle(call("origofs_read", json!({"path":"/notes.txt"})))
        .await
        .unwrap();
    assert_eq!(text(&r), "one\ntwo\n");

    // the agent's write shows up in blame as an agent
    let b = s
        .handle(call("origofs_blame", json!({"path":"/notes.txt"})))
        .await
        .unwrap();
    assert!(text(&b).contains("agent:claude"), "blame was: {}", text(&b));
}

#[tokio::test]
async fn tool_errors_are_reported_not_thrown() {
    let s = server().await;
    let r = s
        .handle(call("origofs_read", json!({"path":"/missing"})))
        .await
        .unwrap();
    assert_eq!(r["result"]["isError"], true);
    assert!(text(&r).contains("error"));
}

#[tokio::test]
async fn serves_over_a_stream() {
    let s = server().await;
    let input = concat!(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"origofs_mkdir","arguments":{"path":"/d"}}}"#,
        "\n",
    );
    let reader = tokio::io::BufReader::new(input.as_bytes());
    let mut out: Vec<u8> = Vec::new();
    s.serve(reader, &mut out).await.unwrap();

    let responses: Vec<Value> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["result"]["serverInfo"]["name"], "origofs");
    assert!(
        responses[1]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("created /d")
    );
}

// A propose-only agent (the untrusted-agent posture from §6) can't land a direct
// write over MCP: `origofs_write` routes it into the suggestion queue, the file
// stays absent, and it takes a *different* actor to accept — the agent can't
// rubber-stamp its own proposal.
#[tokio::test]
async fn propose_only_agent_writes_become_suggestions() {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let agent = ws
        .create_agent("claude", "claude-opus-4-8", None)
        .await
        .unwrap();
    let session = ws.create_session(agent, Some("mcp")).await.unwrap();
    // Bound this agent to propose-only.
    ws.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    let s = McpServer::new(ws.clone(), agent, session);

    // The write is queued, not applied.
    let w = s
        .handle(call(
            "origofs_write",
            json!({"path":"/notes.txt","content":"proposed edit"}),
        ))
        .await
        .unwrap();
    assert_eq!(w["result"]["isError"], false);
    assert!(
        text(&w).contains("proposed suggestion"),
        "expected a proposal, got: {}",
        text(&w)
    );
    // The file doesn't exist yet.
    let r = s
        .handle(call("origofs_read", json!({"path":"/notes.txt"})))
        .await
        .unwrap();
    assert_eq!(r["result"]["isError"], true);

    // The suggestion is visible via the MCP review tools.
    let list = s
        .handle(call("origofs_suggestions", json!({})))
        .await
        .unwrap();
    assert!(
        text(&list).contains("/notes.txt"),
        "list was: {}",
        text(&list)
    );

    let sid = ws
        .list_suggestions(Some(SuggestionStatus::Pending), None)
        .await
        .unwrap()[0]
        .id;

    // The agent cannot accept its own proposal — review needs a different actor.
    let self_accept = s
        .handle(call("origofs_accept", json!({ "id": sid })))
        .await
        .unwrap();
    assert_eq!(self_accept["result"]["isError"], true);
    assert!(ws.read("/notes.txt").await.is_err()); // still not applied

    // A human reviewer accepts it — now it lands, credited to the agent.
    let reviewer = ws.create_human("dan", None).await.unwrap();
    let rs = ws.create_session(reviewer, None).await.unwrap();
    ws.accept_suggestion(sid, WriteCtx::session(reviewer, rs))
        .await
        .unwrap();
    assert_eq!(&ws.read("/notes.txt").await.unwrap()[..], b"proposed edit");
    let blame = ws.blame("/notes.txt").await.unwrap();
    assert!(blame.iter().all(|r| r.actor.id == agent));
}

// A direct agent (the default) still writes straight through — the policy is
// opt-in and nothing changes for a trusted agent.
#[tokio::test]
async fn direct_agent_still_writes_directly() {
    let s = server().await;
    let w = s
        .handle(call(
            "origofs_write",
            json!({"path":"/d.txt","content":"landed"}),
        ))
        .await
        .unwrap();
    assert!(text(&w).contains("wrote"), "got: {}", text(&w));
    let r = s
        .handle(call("origofs_read", json!({"path":"/d.txt"})))
        .await
        .unwrap();
    assert_eq!(text(&r), "landed");
}

// origofs_edit is exact string search-and-replace (the canonical str_replace
// contract): it replaces a unique `old` with `new`, refuses an ambiguous or
// missing match with a helpful error, and does every occurrence only when asked.
#[tokio::test]
async fn edit_replaces_exact_string_and_enforces_uniqueness() {
    let s = server().await;
    s.handle(call(
        "origofs_write",
        json!({"path":"/f.txt","content":"hello world\n"}),
    ))
    .await
    .unwrap();

    // a unique replacement lands
    let e = s
        .handle(call(
            "origofs_edit",
            json!({"path":"/f.txt","old":"world","new":"origofs"}),
        ))
        .await
        .unwrap();
    assert_eq!(e["result"]["isError"], false, "{}", text(&e));
    let r = s
        .handle(call("origofs_read", json!({"path":"/f.txt"})))
        .await
        .unwrap();
    assert_eq!(text(&r), "hello origofs\n");

    // a string that isn't there is a (recoverable) error, not a silent no-op
    let miss = s
        .handle(call(
            "origofs_edit",
            json!({"path":"/f.txt","old":"NOPE","new":"x"}),
        ))
        .await
        .unwrap();
    assert_eq!(miss["result"]["isError"], true);
    assert!(text(&miss).contains("not found"), "{}", text(&miss));

    // an ambiguous match is refused unless replace_all is set
    s.handle(call(
        "origofs_write",
        json!({"path":"/g.txt","content":"a a a"}),
    ))
    .await
    .unwrap();
    let amb = s
        .handle(call(
            "origofs_edit",
            json!({"path":"/g.txt","old":"a","new":"b"}),
        ))
        .await
        .unwrap();
    assert_eq!(amb["result"]["isError"], true);
    assert!(text(&amb).contains("matches 3 times"), "{}", text(&amb));

    let all = s
        .handle(call(
            "origofs_edit",
            json!({"path":"/g.txt","old":"a","new":"b","replace_all":true}),
        ))
        .await
        .unwrap();
    assert_eq!(all["result"]["isError"], false, "{}", text(&all));
    let rg = s
        .handle(call("origofs_read", json!({"path":"/g.txt"})))
        .await
        .unwrap();
    assert_eq!(text(&rg), "b b b");
}

// An edit is governed by the write policy exactly like a write: a propose-only
// agent's edit becomes a suggestion, leaving the file untouched until review.
#[tokio::test]
async fn edit_is_governed_by_write_policy() {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    // seed a file directly (a trusted human)
    let human = ws.create_human("dan", None).await.unwrap();
    let hs = ws.create_session(human, None).await.unwrap();
    ws.write_as(WriteCtx::session(human, hs), "/f.txt", b"hello world\n")
        .await
        .unwrap();

    // a propose-only agent edits it
    let agent = ws.create_agent("claude", "opus", None).await.unwrap();
    let asess = ws.create_session(agent, Some("mcp")).await.unwrap();
    ws.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    let s = McpServer::new(ws.clone(), agent, asess);

    let e = s
        .handle(call(
            "origofs_edit",
            json!({"path":"/f.txt","old":"world","new":"origofs"}),
        ))
        .await
        .unwrap();
    assert_eq!(e["result"]["isError"], false, "{}", text(&e));
    assert!(text(&e).contains("proposed suggestion"), "{}", text(&e));
    // unchanged until a different actor accepts
    assert_eq!(&ws.read("/f.txt").await.unwrap()[..], b"hello world\n");
}

/// A propose-only agent over MCP is stopped at **every** mutating tool, not just
/// `origofs_write` (issue #78).
///
/// Before the fix the agent could not overwrite `/keep.txt` but could delete it
/// and commit the deletion — the gate lengthened the destructive path instead of
/// closing it.
#[tokio::test]
async fn propose_only_agent_cannot_delete_mkdir_or_commit() {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let human = ws.create_human("dan", None).await.unwrap();
    ws.write("/keep.txt", b"precious").await.unwrap();

    let agent = ws.create_agent("claude", "opus", None).await.unwrap();
    let session = ws.create_session(agent, Some("mcp")).await.unwrap();
    ws.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    let s = McpServer::new(ws.clone(), agent, session);

    // `rm` has a propose-shaped path: queued for review, file untouched.
    let rm = s
        .handle(call("origofs_rm", json!({"path":"/keep.txt"})))
        .await
        .unwrap();
    assert_eq!(rm["result"]["isError"], false, "{}", text(&rm));
    assert!(
        text(&rm).contains("proposed deletion"),
        "expected a queued deletion, got: {}",
        text(&rm)
    );
    assert_eq!(
        &ws.read("/keep.txt").await.unwrap()[..],
        b"precious",
        "a propose-only agent must not be able to delete what it cannot overwrite"
    );

    // `mkdir` and `commit` have no propose-shaped equivalent: refused outright.
    let md = s
        .handle(call("origofs_mkdir", json!({"path":"/sneaky"})))
        .await
        .unwrap();
    assert_eq!(md["result"]["isError"], true, "{}", text(&md));
    let ci = s
        .handle(call("origofs_commit", json!({"message":"mine now"})))
        .await
        .unwrap();
    assert_eq!(ci["result"]["isError"], true, "{}", text(&ci));

    // A reviewer accepting the deletion is what actually removes the file, and
    // it stays attributed to the agent that asked.
    let sid = ws
        .list_suggestions(Some(SuggestionStatus::Pending), None)
        .await
        .unwrap()[0]
        .id;
    ws.accept_suggestion(sid, WriteCtx::actor(human))
        .await
        .unwrap();
    assert!(ws.stat("/keep.txt").await.is_err());
}

/// Every advertised MCP tool is accounted for as read-only, policy-gated, or
/// review-queued.
///
/// This is the regression guard for how `origofs_rm` shipped ungated: a new tool
/// is invisible to the behavioural tests above, so it would have slipped in the
/// same way. Adding one now fails here until it is classified — and the act of
/// classifying it is the moment to notice it needs a policy check.
#[tokio::test]
async fn every_mutating_mcp_tool_is_policy_classified() {
    // Read-only: no working-tree mutation, safe for any actor.
    const READ_ONLY: &[&str] = &[
        "origofs_read",
        "origofs_ls",
        "origofs_blame",
        "origofs_log",
        "origofs_live",
        "origofs_suggestions",
        "origofs_suggestion_diff",
    ];
    // The propose path itself: open to every actor by design — that is the whole
    // point of a review queue. Touches no working-tree state.
    const PROPOSE_PATH: &[&str] = &["origofs_suggest", "origofs_suggest_coedit"];
    // Mutating or review-resolving, and routed through the §6 write policy: queued
    // for a propose-only actor (`write`/`edit`/`rm`), or refused outright
    // (`mkdir`/`commit`, and `accept`/`reject` via `ensure_may_write`).
    const POLICY_GATED: &[&str] = &[
        "origofs_write",
        "origofs_edit",
        "origofs_rm",
        "origofs_mkdir",
        "origofs_commit",
        "origofs_accept",
        "origofs_reject",
    ];

    let s = server().await;
    let list = s
        .handle(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .await
        .unwrap();
    let names: Vec<String> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();

    for name in &names {
        let known = READ_ONLY.contains(&name.as_str())
            || PROPOSE_PATH.contains(&name.as_str())
            || POLICY_GATED.contains(&name.as_str());
        assert!(
            known,
            "MCP tool {name} is not classified in this test. If it mutates the \
             working tree it must route through the write policy (write_or_propose \
             / remove_or_propose / an `_as` variant); if it only reads, add it to \
             READ_ONLY. See issue #78."
        );
    }
}
