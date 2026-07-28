//! origofs-mcp — expose an origofs workspace to agents over the Model Context Protocol
//! (`docs/DESIGN.md` §4e).
//!
//! A minimal JSON-RPC 2.0 server over newline-delimited stdio (the MCP stdio
//! transport). Every mutating tool call is attributed to the server's agent
//! actor, so blame + the edit-op audit come "for free" from how the agent works
//! — reading and writing files *is* the tool call.

use origofs_sdk::{OrigoFSError, SuggestionStatus, Workspace, WriteCtx, WriteOutcome};
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
                // Governed by this agent's write policy: a direct agent writes
                // straight to the tree; a propose-only agent's edit is queued for
                // review instead of landing.
                let summary = format!("write {p} via mcp agent");
                match self
                    .ws
                    .write_or_propose(self.ctx(), p, data.as_bytes(), Some(&summary))
                    .await?
                {
                    WriteOutcome::Wrote => Ok(format!("wrote {} bytes to {p}", data.len())),
                    WriteOutcome::Proposed(id) => Ok(format!(
                        "proposed suggestion #{id} for {p} ({} bytes) — pending review; \
                         this agent is propose-only",
                        data.len()
                    )),
                }
            }
            "origofs_edit" => {
                // Exact string search-and-replace — the canonical edit contract
                // (Anthropic's text_editor `str_replace`, Aider/Cursor SEARCH-REPLACE):
                // content-based, never line numbers, and `old` must be unique so an
                // edit can't land in the wrong place. Governed by the write policy,
                // like `origofs_write`.
                let p = path();
                let old = args.get("old").and_then(Value::as_str).unwrap_or("");
                let new = args.get("new").and_then(Value::as_str).unwrap_or("");
                let replace_all = args
                    .get("replace_all")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if old.is_empty() {
                    return Err(OrigoFSError::InvalidArgument(
                        "edit: `old` must be non-empty (use origofs_write to create/replace a file)"
                            .into(),
                    ));
                }
                if old == new {
                    return Err(OrigoFSError::InvalidArgument(
                        "edit: `old` and `new` are identical — nothing to change".into(),
                    ));
                }
                let bytes = self.ws.read(p).await?;
                let text = std::str::from_utf8(&bytes).map_err(|_| {
                    OrigoFSError::InvalidArgument(format!(
                        "{p}: not a UTF-8 text file; edit works on text"
                    ))
                })?;
                let count = text.matches(old).count();
                if count == 0 {
                    return Err(OrigoFSError::InvalidArgument(format!(
                        "edit: `old` not found in {p}"
                    )));
                }
                if count > 1 && !replace_all {
                    return Err(OrigoFSError::InvalidArgument(format!(
                        "edit: `old` matches {count} times in {p}; include more surrounding \
                         context to make it unique, or set replace_all=true"
                    )));
                }
                let updated = if replace_all {
                    text.replace(old, new)
                } else {
                    text.replacen(old, new, 1)
                };
                let summary = format!("edit {p} via mcp agent");
                match self
                    .ws
                    .write_or_propose(self.ctx(), p, updated.as_bytes(), Some(&summary))
                    .await?
                {
                    WriteOutcome::Wrote => Ok(format!(
                        "edited {p} ({count} replacement{})",
                        if count == 1 { "" } else { "s" }
                    )),
                    WriteOutcome::Proposed(id) => Ok(format!(
                        "proposed suggestion #{id} for {p} (edit) — pending review; \
                         this agent is propose-only"
                    )),
                }
            }
            "origofs_suggest" => {
                let p = path();
                let data = args.get("content").and_then(Value::as_str).unwrap_or("");
                let summary = args.get("summary").and_then(Value::as_str);
                if let Some((parent, _)) = p.rsplit_once('/') {
                    if !parent.is_empty() {
                        self.ws.mkdir_p(parent).await?;
                    }
                }
                let id = self
                    .ws
                    .suggest(self.ctx(), p, data.as_bytes(), summary)
                    .await?;
                Ok(format!(
                    "proposed suggestion #{id} for {p} (pending review)"
                ))
            }
            "origofs_suggestions" => {
                let path_filter = args.get("path").and_then(Value::as_str);
                let list = self
                    .ws
                    .list_suggestions(Some(SuggestionStatus::Pending), path_filter)
                    .await?;
                if list.is_empty() {
                    return Ok("no pending suggestions".to_string());
                }
                Ok(list
                    .iter()
                    .map(|s| {
                        format!(
                            "#{}\t{}\tby actor {}\t{}",
                            s.id,
                            s.path,
                            s.actor_id,
                            s.summary.as_deref().unwrap_or("")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "origofs_suggestion_diff" => {
                let id = args.get("id").and_then(Value::as_i64).unwrap_or(0);
                self.ws.suggestion_diff(id).await
            }
            "origofs_accept" => {
                let id = args.get("id").and_then(Value::as_i64).unwrap_or(0);
                self.ws.accept_suggestion(id, self.ctx()).await?;
                Ok(format!("accepted suggestion #{id}"))
            }
            "origofs_reject" => {
                let id = args.get("id").and_then(Value::as_i64).unwrap_or(0);
                self.ws.reject_suggestion(id, self.ctx()).await?;
                Ok(format!("rejected suggestion #{id}"))
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
            "Write a file, attributed to this agent (records blame). If this agent is \
             propose-only, the edit is queued as a suggestion for review instead of \
             landing — the result says which happened.",
            json!({
                "path": { "type": "string" },
                "content": { "type": "string" },
            }),
            &["path", "content"],
        ),
        tool(
            "origofs_edit",
            "Edit a text file by exact string replacement: replace `old` with `new`. \
             `old` must appear exactly once — include enough surrounding context to be \
             unique — unless replace_all is set. Prefer this over origofs_write for a \
             small change: it sends only the changed text and credits only the changed \
             lines. Governed by this agent's write policy, like origofs_write.",
            json!({
                "path": { "type": "string" },
                "old": { "type": "string", "description": "exact text to replace; must be unique unless replace_all" },
                "new": { "type": "string", "description": "replacement text" },
                "replace_all": { "type": "boolean", "description": "replace every occurrence (default false)" },
            }),
            &["path", "old", "new"],
        ),
        tool(
            "origofs_suggest",
            "Propose an edit to a file for review instead of writing it directly. The \
             bytes are stored now; the file changes only when a different actor accepts.",
            json!({
                "path": { "type": "string" },
                "content": { "type": "string" },
                "summary": { "type": "string", "description": "optional note for the reviewer" },
            }),
            &["path", "content"],
        ),
        tool(
            "origofs_suggestions",
            "List pending suggestions awaiting review (optionally filtered to a path).",
            json!({ "path": { "type": "string", "description": "optional path filter" } }),
            &[],
        ),
        tool(
            "origofs_suggestion_diff",
            "Show a suggestion's unified diff (base to proposed).",
            json!({ "id": { "type": "integer" } }),
            &["id"],
        ),
        tool(
            "origofs_accept",
            "Accept a pending suggestion, landing it attributed to its author. Refused \
             if this agent is the suggestion's own author (review requires a different actor).",
            json!({ "id": { "type": "integer" } }),
            &["id"],
        ),
        tool(
            "origofs_reject",
            "Reject a pending suggestion without applying it.",
            json!({ "id": { "type": "integer" } }),
            &["id"],
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
