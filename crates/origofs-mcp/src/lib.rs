//! origofs-mcp — expose an origofs workspace to agents over the Model Context Protocol
//! (`docs/DESIGN.md` §4e).
//!
//! A minimal JSON-RPC 2.0 server over newline-delimited stdio (the MCP stdio
//! transport). Every mutating tool call is attributed to the server's agent
//! actor, so blame + the edit-op audit come "for free" from how the agent works
//! — reading and writing files *is* the tool call.

use origofs_sdk::{OrigoFSError, Workspace, WriteCtx};
use serde_json::{json, Value};

type Result<T> = std::result::Result<T, OrigoFSError>;

const PROTOCOL_VERSION: &str = "2024-11-05";

/// An MCP server bound to a workspace and an agent actor/session.
pub struct McpServer {
    ws: Workspace,
    agent: i64,
    session: i64,
}

impl McpServer {
    pub fn new(ws: Workspace, agent: i64, session: i64) -> Self {
        Self { ws, agent, session }
    }

    /// Register an agent actor + session and bind a server to them.
    pub async fn create(ws: Workspace, agent_name: &str, model: &str) -> Result<Self> {
        let agent = ws.create_agent(agent_name, model, None).await?;
        let session = ws.create_session(agent, Some("mcp")).await?;
        Ok(Self::new(ws, agent, session))
    }

    fn ctx(&self) -> WriteCtx {
        WriteCtx::session(self.agent, self.session)
    }

    /// Handle one JSON-RPC message. Returns a response for requests, or `None`
    /// for notifications.
    pub async fn handle(&self, req: Value) -> Option<Value> {
        let id = req.get("id").cloned();
        let method = req.get("method").and_then(Value::as_str).unwrap_or("");
        match method {
            "initialize" => Some(ok(
                id,
                json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "origofs", "version": env!("CARGO_PKG_VERSION") },
                }),
            )),
            "notifications/initialized" => None,
            "ping" => Some(ok(id, json!({}))),
            "tools/list" => Some(ok(id, json!({ "tools": tool_defs() }))),
            "tools/call" => {
                let params = req.get("params").cloned().unwrap_or(Value::Null);
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                let args = params.get("arguments").cloned().unwrap_or(json!({}));
                Some(ok(id, self.call_tool(name, &args).await))
            }
            // Unknown request => JSON-RPC method-not-found; ignore notifications.
            _ if id.is_some() => Some(err(id, -32601, "method not found")),
            _ => None,
        }
    }

    async fn call_tool(&self, name: &str, args: &Value) -> Value {
        match self.dispatch(name, args).await {
            Ok(text) => content(&text, false),
            Err(e) => content(&format!("error: {e}"), true),
        }
    }

    async fn dispatch(&self, name: &str, args: &Value) -> Result<String> {
        let path = || args.get("path").and_then(Value::as_str).unwrap_or_default();
        match name {
            "origofs_read" => {
                let bytes = self.ws.read(path()).await?;
                Ok(String::from_utf8_lossy(&bytes).into_owned())
            }
            "origofs_write" => {
                let p = path();
                let data = args.get("content").and_then(Value::as_str).unwrap_or("");
                if let Some((parent, _)) = p.rsplit_once('/') {
                    if !parent.is_empty() {
                        self.ws.mkdir_p(parent).await?;
                    }
                }
                self.ws.write_as(self.ctx(), p, data.as_bytes()).await?;
                Ok(format!("wrote {} bytes to {p}", data.len()))
            }
            "origofs_ls" => {
                let entries = self.ws.ls(path()).await?;
                Ok(entries
                    .iter()
                    .map(|e| format!("{}\t{}", e.kind.as_str(), e.name))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "origofs_mkdir" => {
                self.ws.mkdir_p(path()).await?;
                Ok(format!("created {}", path()))
            }
            "origofs_rm" => {
                self.ws.remove(path()).await?;
                Ok(format!("removed {}", path()))
            }
            "origofs_blame" => {
                let ranges = self.ws.blame(path()).await?;
                Ok(ranges
                    .iter()
                    .map(|r| {
                        format!(
                            "L{}-{} {}:{}",
                            r.line_start,
                            r.line_end,
                            r.actor.kind.as_str(),
                            r.actor.display_name
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "origofs_commit" => {
                let message = args
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("commit via mcp");
                let hash = self.ws.commit("mcp-agent", message).await?;
                Ok(format!("committed {}", &hash.to_hex()[..12]))
            }
            "origofs_log" => {
                let log = self.ws.log().await?;
                Ok(log
                    .iter()
                    .map(|c| format!("{} {}", &c.hash.to_hex()[..12], c.commit.message))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            other => Ok(format!("unknown tool: {other}")),
        }
    }

    /// Serve MCP over the given async reader/writer (newline-delimited JSON).
    pub async fn serve<R, W>(&self, reader: R, mut writer: W) -> std::io::Result<()>
    where
        R: tokio::io::AsyncBufRead + Unpin,
        W: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            let Ok(req) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if let Some(resp) = self.handle(req).await {
                let mut bytes = serde_json::to_vec(&resp).unwrap_or_default();
                bytes.push(b'\n');
                writer.write_all(&bytes).await?;
                writer.flush().await?;
            }
        }
        Ok(())
    }

    /// Serve over stdio.
    pub async fn serve_stdio(&self) -> std::io::Result<()> {
        let stdin = tokio::io::BufReader::new(tokio::io::stdin());
        self.serve(stdin, tokio::io::stdout()).await
    }
}

fn ok(id: Option<Value>, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn err(id: Option<Value>, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn content(text: &str, is_error: bool) -> Value {
    json!({ "content": [ { "type": "text", "text": text } ], "isError": is_error })
}

fn tool(name: &str, description: &str, props: Value, required: &[&str]) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": props,
            "required": required,
        }
    })
}

fn tool_defs() -> Vec<Value> {
    let path_prop = json!({ "path": { "type": "string", "description": "absolute origofs path" } });
    vec![
        tool(
            "origofs_read",
            "Read a file's contents.",
            path_prop.clone(),
            &["path"],
        ),
        tool(
            "origofs_write",
            "Write a file (attributed to this agent; records blame).",
            json!({
                "path": { "type": "string" },
                "content": { "type": "string" },
            }),
            &["path", "content"],
        ),
        tool(
            "origofs_ls",
            "List a directory.",
            path_prop.clone(),
            &["path"],
        ),
        tool(
            "origofs_mkdir",
            "Create a directory (and parents).",
            path_prop.clone(),
            &["path"],
        ),
        tool(
            "origofs_rm",
            "Remove a file or empty directory.",
            path_prop.clone(),
            &["path"],
        ),
        tool(
            "origofs_blame",
            "Per-line authorship (human vs agent) for a file.",
            path_prop,
            &["path"],
        ),
        tool(
            "origofs_commit",
            "Snapshot the working tree into a commit.",
            json!({ "message": { "type": "string" } }),
            &["message"],
        ),
        tool("origofs_log", "Show commit history.", json!({}), &[]),
    ]
}
