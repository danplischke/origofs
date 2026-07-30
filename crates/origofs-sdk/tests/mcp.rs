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

// The MCP agent's identity must survive a restart. `create` used to call
// `create_agent`, an unconditional INSERT, so every `origofs mcp` launch minted a
// brand-new actor with the same display name. That quietly defeated the review
// gate — a `propose` policy set on the agent applied to an actor no later run
// would ever use again — and scattered one agent's blame across a growing pile of
// indistinguishable actors.
#[tokio::test]
async fn agent_identity_is_stable_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let open = || async {
        Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap()
    };

    let first = McpServer::create(open().await, "claude", "claude-opus-4-8")
        .await
        .unwrap();
    let (actor, first_session) = (first.actor(), first.session());

    // An operator gates this agent between runs.
    open()
        .await
        .set_write_policy(actor, WritePolicy::Propose)
        .await
        .unwrap();

    let second = McpServer::create(open().await, "claude", "claude-opus-4-8")
        .await
        .unwrap();
    assert_eq!(
        second.actor(),
        actor,
        "the same agent name must resolve to the same actor across restarts"
    );
    assert_ne!(
        second.session(),
        first_session,
        "each process still gets its own session"
    );

    // The decisive check: the policy still applies, so the write is queued for
    // review rather than landing directly.
    let w = second
        .handle(call(
            "origofs_write",
            json!({"path":"/notes.txt","content":"after restart"}),
        ))
        .await
        .unwrap();
    assert_eq!(w["result"]["isError"], false);
    assert!(
        text(&w).contains("proposed suggestion"),
        "a policy set before the restart must still gate this write, got: {}",
        text(&w)
    );

    // A different agent name is still a different actor.
    let other = McpServer::create(open().await, "gemini", "gemini-3")
        .await
        .unwrap();
    assert_ne!(other.actor(), actor);

    // One actor per agent name, not one per launch.
    let claude_actors = open()
        .await
        .list_actors()
        .await
        .unwrap()
        .into_iter()
        .filter(|a| a.display_name == "claude")
        .count();
    assert_eq!(claude_actors, 1, "one actor per agent, not one per launch");
}
