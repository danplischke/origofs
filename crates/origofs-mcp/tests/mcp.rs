//! MCP server: protocol handshake, tool listing, and attributed tool calls.

use origofs_mcp::McpServer;
use origofs_sdk::Workspace;
use serde_json::{json, Value};

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
    assert!(s
        .handle(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .await
        .is_none());

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
    assert!(responses[1]["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("created /d"));
}
