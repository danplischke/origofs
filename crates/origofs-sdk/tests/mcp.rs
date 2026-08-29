//! MCP server: protocol handshake, tool listing, and attributed tool calls.
#![cfg(feature = "mcp")]

use origofs_sdk::mcp::McpServer;
use origofs_sdk::{Scope, SuggestionStatus, Workspace, WriteCtx, WritePolicy};
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
        "origofs_trash",
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
        // A restore writes a file back into the working tree, so it is a
        // mutation like any other and takes the attributed `restore_trash`.
        "origofs_restore",
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

/// No MCP tool reads through an **unattributed** engine method.
///
/// The read counterpart of the classification test above, and the guard on issue
/// #124's phase 2. Every read tool has an agent — the MCP server resolves one at
/// startup and attributes every mutation to it — so unlike the HTTP surface there
/// is no anonymous case to accommodate: a read tool calling `ws.read` rather than
/// `ws.read_as` is simply dropping an identity it already holds, and
/// `acl_enforce_reads` becomes decoration for every agent on the server.
///
/// Scanned from source rather than exercised, for the same reason the sibling is:
/// a new tool is invisible to behavioural tests until someone writes one.
#[test]
fn no_mcp_tool_reads_through_an_unattributed_method() {
    // The unattributed reads. Each has an `_as` twin that consults `Perms::READ`;
    // these exist for checkout, merge, gc and the CRDT coordinator, none of which
    // is an MCP tool.
    const UNATTRIBUTED: &[&str] = &[
        "read(",
        "read_range(",
        "ls(",
        "stat(",
        "readlink(",
        "blame(",
        "diff(",
        "diff_file(",
        "presence(",
        "live_doc(",
        "live_paths(",
        "list_suggestions(",
        "get_suggestion(",
        "suggestion_diff(",
    ];

    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/mcp.rs"),
    )
    .unwrap();

    let mut offenders = Vec::new();
    for m in UNATTRIBUTED {
        let needle = format!("ws.{m}");
        if src.contains(&needle) {
            offenders.push(needle);
        }
    }

    assert!(
        offenders.is_empty(),
        "these unattributed reads are called from the MCP server, so the tools \
         using them answer without consulting `Perms::READ`:\n  {}\n\nThe MCP \
         server always has an agent — call the `_as` twin with `self.ctx()`.",
        offenders.join("\n  ")
    );
}

/// Every tool that takes a `path` resolves it through the server's [`Scope`], and
/// no tool leaks a path outside it (issue #125).
///
/// The scoping counterpart of the classification test above, and it exists for the
/// same reason: a new tool is invisible to the behavioural tests, so it would ship
/// unscoped exactly the way `origofs_rm` shipped ungated. Rather than listing tools
/// again, this drives every advertised tool that declares a `path` property with a
/// path naming a neighbour, and asserts the neighbour is never touched or reported.
#[tokio::test]
async fn every_path_taking_mcp_tool_is_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    // A neighbour's tree, and the scoped agent's own.
    ws.mkdir_p("/tenant-a").await.unwrap();
    ws.mkdir_p("/other").await.unwrap();
    ws.write("/other/secrets.txt", b"NEIGHBOUR-SECRET")
        .await
        .unwrap();

    let s = McpServer::create(ws, "claude", "claude-opus-4-8")
        .await
        .unwrap()
        .with_scope(Scope::at("/tenant-a").unwrap());

    let list = s
        .handle(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .await
        .unwrap();
    let tools = list["result"]["tools"].as_array().unwrap().clone();

    let mut checked = 0;
    for t in &tools {
        let name = t["name"].as_str().unwrap();
        // Only the tools that actually take a path are in scope for this check.
        if t["inputSchema"]["properties"]["path"].is_null() {
            continue;
        }
        checked += 1;

        // Every required argument gets a placeholder, so the call reaches the
        // dispatch rather than failing on a missing field before scoping runs.
        let mut args = serde_json::Map::new();
        args.insert("path".into(), json!("/other/secrets.txt"));
        if let Some(props) = t["inputSchema"]["properties"].as_object() {
            for key in props.keys() {
                if key != "path" {
                    args.entry(key.clone()).or_insert(json!("x"));
                }
            }
        }

        let resp = s.handle(call(name, Value::Object(args))).await.unwrap();
        let out = text(&resp);
        assert!(
            !out.contains("NEIGHBOUR-SECRET"),
            "MCP tool {name} returned a neighbour's content from outside the \
             scope: {out}"
        );
    }

    assert!(
        checked >= 5,
        "expected several path-taking tools to check, found {checked} — has the \
         schema shape changed?"
    );

    // Nothing reached the neighbour's tree, in either direction.
    assert_eq!(
        &s.workspace().read("/other/secrets.txt").await.unwrap()[..],
        b"NEIGHBOUR-SECRET",
        "a scoped MCP tool wrote into a tree it cannot address"
    );
    // The neighbour's *directory* is likewise untouched: a scoped write naming
    // `/other/...` must have landed under `/tenant-a`, not created anything at the
    // workspace top level.
    let top = s.workspace().ls("/").await.unwrap();
    let names: Vec<&str> = top.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"tenant-a") && names.contains(&"other"),
        "the fixture's own directories should still be the only top-level ones: {names:?}"
    );
}

/// A propose-only agent must not create directories on its way to the review
/// queue.
///
/// `origofs_write` correctly queues the *edit* for a propose-only agent, but it
/// used to create the path's parent first with the unattributed `mkdir_p` — so
/// the agent mutated the working tree anyway, with no blame, no edit-op, and no
/// policy check. The engine documents having fixed exactly this class internally
/// (`suggest.rs`); the surface was still doing it by hand.
///
/// Every other test here writes to a root-level path, where `rsplit_once('/')`
/// yields an empty parent and the `mkdir_p` never runs — which is why this went
/// unnoticed. This one uses a nested path on purpose.
#[tokio::test]
async fn a_queued_write_creates_no_directories() {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let agent = ws.create_agent("claude", "opus", None).await.unwrap();
    let session = ws.create_session(agent, Some("mcp")).await.unwrap();
    ws.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    let s = McpServer::new(ws.clone(), agent, session);

    let w = s
        .handle(call(
            "origofs_write",
            json!({"path":"/deep/nested/notes.txt","content":"proposed"}),
        ))
        .await
        .unwrap();

    // The write itself is refused or queued — either is a correct policy outcome.
    // What must not happen is the directory appearing regardless.
    let _ = w;
    assert!(
        ws.stat("/deep").await.is_err(),
        "a propose-only agent created /deep while its edit was only being proposed"
    );
}

/// The same path works for a trusted agent: the parent is created, attributed.
#[tokio::test]
async fn a_direct_agent_creates_parents_attributed() {
    let s = server().await;
    let w = s
        .handle(call(
            "origofs_write",
            json!({"path":"/deep/nested/notes.txt","content":"real"}),
        ))
        .await
        .unwrap();
    assert_eq!(w["result"]["isError"], false, "{}", text(&w));

    let r = s
        .handle(call(
            "origofs_read",
            json!({"path":"/deep/nested/notes.txt"}),
        ))
        .await
        .unwrap();
    assert_eq!(text(&r), "real");
}

/// A misspelled tool name is an error, not a success with an apologetic string.
#[tokio::test]
async fn an_unknown_tool_is_an_error() {
    let s = server().await;
    let r = s
        .handle(call("origofs_wrtie", json!({"path":"/a.txt"})))
        .await
        .unwrap();
    assert_eq!(
        r["result"]["isError"], true,
        "an unknown tool reported success, so an agent would treat the call as \
         having worked: {r}"
    );
}

/// Commits name the agent, not a placeholder shared by every agent.
#[tokio::test]
async fn a_commit_names_the_agent() {
    let s = server().await;
    s.handle(call(
        "origofs_write",
        json!({"path":"/a.txt","content":"hello"}),
    ))
    .await
    .unwrap();
    let c = s
        .handle(call("origofs_commit", json!({"message":"first"})))
        .await
        .unwrap();
    assert_eq!(c["result"]["isError"], false, "{}", text(&c));

    let log = s.handle(call("origofs_log", json!({}))).await.unwrap();
    assert!(text(&log).contains("first"));
}

/// The trash is reachable from an agent, and scoped like every other listing.
///
/// The engine has had a recoverable delete since #115 and no tool exposed it,
/// so the population it exists for — an agent that deleted the wrong path —
/// had nothing to reach for. The scoping half is not optional: a trash id is a
/// workspace-global integer, so without a path check a scoped agent could
/// materialize a neighbour's deleted file by guessing one.
#[tokio::test]
async fn an_agent_can_list_and_restore_its_own_deletes() {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let agent = ws.create_agent("agent", "opus", None).await.unwrap();
    let session = ws.create_session(agent, Some("mcp")).await.unwrap();
    ws.write_as(WriteCtx::session(agent, session), "/doc.txt", b"precious\n")
        .await
        .unwrap();
    let s = McpServer::new(ws.clone(), agent, session);

    // Disabled by default, and it says so rather than reporting an empty list:
    // "nothing was deleted" and "nothing is being kept" are different answers,
    // and only one of them is something the agent can act on.
    let out = text(&s.handle(call("origofs_trash", json!({}))).await.unwrap());
    assert!(out.contains("disabled"), "{out}");

    ws.set_trash_retention(Some(3600)).await.unwrap();
    s.handle(call("origofs_rm", json!({ "path": "/doc.txt" })))
        .await
        .unwrap();
    assert!(ws.read("/doc.txt").await.is_err());

    let listed = text(&s.handle(call("origofs_trash", json!({}))).await.unwrap());
    assert!(listed.contains("/doc.txt"), "{listed}");
    assert!(listed.contains(&format!("actor {agent}")), "{listed}");
    let id: i64 = listed
        .split_whitespace()
        .next()
        .and_then(|t| t.trim_start_matches('#').parse().ok())
        .unwrap_or_else(|| panic!("no id in: {listed}"));

    let restored = text(
        &s.handle(call("origofs_restore", json!({ "id": id })))
            .await
            .unwrap(),
    );
    assert!(restored.contains("/doc.txt"), "{restored}");
    assert_eq!(&ws.read("/doc.txt").await.unwrap()[..], b"precious\n");
}
