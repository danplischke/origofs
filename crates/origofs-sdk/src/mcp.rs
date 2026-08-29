//! MCP surface (`mcp` feature) — expose an origofs workspace to agents over the
//! Model Context Protocol (`docs/DESIGN.md` §4e).
//!
//! A minimal JSON-RPC 2.0 server over newline-delimited stdio (the MCP stdio
//! transport). Every mutating tool call is attributed to the server's agent
//! actor, so blame + the edit-op audit come "for free" from how the agent works
//! — reading and writing files *is* the tool call.

use crate::{OrigoFSError, Scope, ScopeError, SuggestionStatus, Workspace, WriteCtx, WriteOutcome};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, OrigoFSError>;

const PROTOCOL_VERSION: &str = "2024-11-05";

/// An MCP server bound to a workspace and an agent actor/session.
pub struct McpServer {
    ws: Workspace,
    agent: i64,
    session: i64,
    /// This server's view of the workspace (issue #125). [`Scope::whole`] unless
    /// [`with_scope`](Self::with_scope) narrowed it.
    scope: Scope,
}

impl McpServer {
    pub fn new(ws: Workspace, agent: i64, session: i64) -> Self {
        Self {
            ws,
            agent,
            session,
            scope: Scope::whole(),
        }
    }

    /// Restrict this server to one subtree of the workspace (issue #125).
    ///
    /// Every path a tool call supplies is then resolved *inside* `scope`, so an
    /// agent cannot address anything outside it — including by asking. Same
    /// primitive the HTTP surface uses, so the two cannot drift.
    ///
    /// This is scoping, not authorization: it bounds what this server can
    /// *address*, and is orthogonal to the `Propose` write policy that bounds what
    /// the agent may *do*. An agent typically wants both.
    pub fn with_scope(mut self, scope: Scope) -> Self {
        self.scope = scope;
        self
    }

    /// This server's scope.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// The workspace this server serves.
    pub fn workspace(&self) -> &Workspace {
        &self.ws
    }

    /// The agent actor every mutating tool call on this server is attributed to.
    /// Stable across restarts for a given agent name — see [`create`](Self::create).
    pub fn actor(&self) -> i64 {
        self.agent
    }

    /// This process's session. A fresh one per server, unlike the actor.
    pub fn session(&self) -> i64 {
        self.session
    }

    /// Bind a server to the agent actor named `agent_name`, creating it only on
    /// its first run, plus a fresh session for this process.
    ///
    /// The actor is *resolved* by a stable subject rather than minted per launch.
    /// This used to call `create_agent`, an unconditional INSERT, so every
    /// `origofs mcp` start produced a brand-new actor. Three consequences, all
    /// bad for a system whose point is attribution: a write policy set on
    /// yesterday's agent silently reverted to `Direct` today — so the review gate
    /// could never be applied to the MCP surface at all; the `actor` table grew a
    /// row per launch; and one logical agent's blame fragmented across dozens of
    /// identically-named actors a reviewer cannot tell apart.
    ///
    /// The *session* stays per-process — that is what a session is.
    ///
    /// An existing actor is returned as-is: `model` is used only when creating
    /// one, so re-running an agent under a new model keeps its identity (and its
    /// history) rather than forking it.
    pub async fn create(ws: Workspace, agent_name: &str, model: &str) -> Result<Self> {
        Self::create_as(ws, &format!("mcp:{agent_name}"), agent_name, model).await
    }

    /// [`create`](Self::create) with an explicit auth subject, for an embedder
    /// that resolves agent identity itself — an API key, an OIDC subject — instead
    /// of deriving it from a name off the command line.
    pub async fn create_as(
        ws: Workspace,
        auth_subject: &str,
        agent_name: &str,
        model: &str,
    ) -> Result<Self> {
        let agent = ws
            .find_or_create_agent(auth_subject, agent_name, model, None)
            .await?;
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
        // The single chokepoint for every path a tool call supplies, so scoping
        // cannot be forgotten by an individual tool — the same reason the HTTP
        // surface scopes in its extractor rather than per handler (issue #125).
        let path = || -> Result<String> {
            let raw = args.get("path").and_then(Value::as_str).unwrap_or_default();
            self.scope.resolve(raw).map_err(|e| match e {
                ScopeError::Traversal => {
                    OrigoFSError::InvalidPath("path may not contain '..'".into())
                }
                // "not found", never "denied": a scoped agent must not be able to
                // tell "exists but not yours" from "does not exist".
                ScopeError::OutOfScope => OrigoFSError::NotFound(raw.to_string()),
            })
        };
        match name {
            "origofs_read" => {
                let bytes = self.ws.read_as(self.ctx(), &path()?).await?;
                Ok(String::from_utf8_lossy(&bytes).into_owned())
            }
            "origofs_write" => {
                let p = &path()?;
                let data = args.get("content").and_then(Value::as_str).unwrap_or("");
                // `mkdir_as`, not the unattributed `mkdir_p`. Creating the
                // parent is a working-tree mutation like any other, so it has to
                // carry the agent and pass the write policy — otherwise a
                // propose-only agent whose *edit* is correctly queued for review
                // still silently creates directories on its way there. The engine
                // fixed this class internally (`suggest.rs`); the surface was
                // still doing it by hand.
                if let Some((parent, _)) = p.rsplit_once('/')
                    && !parent.is_empty()
                {
                    self.ws.mkdir_as(self.ctx(), parent).await?;
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
                let p = &path()?;
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
                let bytes = self.ws.read_as(self.ctx(), p).await?;
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
                let p = &path()?;
                let data = args.get("content").and_then(Value::as_str).unwrap_or("");
                let summary = args.get("summary").and_then(Value::as_str);
                // `mkdir_as`, not the unattributed `mkdir_p`. Creating the
                // parent is a working-tree mutation like any other, so it has to
                // carry the agent and pass the write policy — otherwise a
                // propose-only agent whose *edit* is correctly queued for review
                // still silently creates directories on its way there. The engine
                // fixed this class internally (`suggest.rs`); the surface was
                // still doing it by hand.
                if let Some((parent, _)) = p.rsplit_once('/')
                    && !parent.is_empty()
                {
                    self.ws.mkdir_as(self.ctx(), parent).await?;
                }
                let id = self
                    .ws
                    .suggest(self.ctx(), p, data.as_bytes(), summary)
                    .await?;
                Ok(format!(
                    "proposed suggestion #{id} for {p} (pending review)"
                ))
            }
            // Proposing against a *live* document is a CRDT merge, not a file
            // body: the base is a state vector rather than a content hash, so a
            // colleague's keystroke elsewhere in the file can neither stale the
            // proposal nor be clobbered by accepting it. Only with `coedit`.
            #[cfg(feature = "coedit")]
            "origofs_suggest_coedit" => {
                let p = &path()?;
                let old = args.get("old").and_then(Value::as_str).unwrap_or("");
                let new = args.get("new").and_then(Value::as_str).unwrap_or("");
                let summary = args.get("summary").and_then(Value::as_str);
                if old.is_empty() {
                    return Err(OrigoFSError::InvalidArgument(
                        "suggest_coedit: `old` must be non-empty (a CRDT proposal is an edit to \
                         an existing document, not a whole new body — use origofs_suggest for that)"
                            .into(),
                    ));
                }
                if old == new {
                    return Err(OrigoFSError::InvalidArgument(
                        "suggest_coedit: `old` and `new` are identical — nothing to propose".into(),
                    ));
                }
                // A throwaway replica to compute the proposal against — not a
                // co-editing session. `load_coedit_as` reconstructs the document
                // without claiming the path live (so no marker has to be restored
                // afterwards) and takes the *propose* check rather than the write
                // one, which is the whole point on this tool: proposing is exactly
                // what a propose-only actor is allowed to do.
                let doc = self.ws.load_coedit_as(self.ctx(), p).await?;
                let text = doc.text();
                let count = text.matches(old).count();
                if count == 0 {
                    return Err(OrigoFSError::InvalidArgument(format!(
                        "suggest_coedit: `old` not found in {p}"
                    )));
                }
                if count > 1 {
                    return Err(OrigoFSError::InvalidArgument(format!(
                        "suggest_coedit: `old` matches {count} times in {p}; include more \
                         surrounding context to make it unique"
                    )));
                }
                // `CoeditDoc` indexes UTF-8 bytes, not UTF-16 code units — see its
                // type docs. Converting to UTF-16 here placed every edit in a
                // non-ASCII document at the wrong offset and with the wrong
                // length, so the replacement straddled the match: replacing
                // "world" in "ééééé world" spliced at byte 6 instead of 11 and
                // removed 5 of the 11 bytes it should have, producing
                // "éééorigofsworld" — reported as a successful suggestion, and
                // corrupt once accepted.
                let index = text.find(old).unwrap_or(0) as u32;
                doc.remove(index, old.len() as u32);
                doc.insert(self.ctx(), index, new);
                let id = self.ws.suggest_coedit(self.ctx(), p, &doc, summary).await?;
                Ok(format!(
                    "proposed co-edit suggestion #{id} for {p} (a CRDT merge, pending review)"
                ))
            }
            "origofs_live" => match args.get("path").and_then(Value::as_str) {
                Some(p) if !p.is_empty() => {
                    match self.ws.live_doc_as(self.ctx(), &path()?).await? {
                        Some(l) => Ok(format!(
                            "{p} is LIVE (open since {} by actor {}): its durable bytes are a \
                             checkpoint and may lag the open document — origofs_read still \
                             answers, it just may be behind. Propose with \
                             origofs_suggest_coedit rather than origofs_write.",
                            l.since, l.actor_id
                        )),
                        None => Ok(format!(
                            "{p} is not live: its stored bytes are the whole truth"
                        )),
                    }
                }
                _ => {
                    // The live-document list is workspace-wide, so unscoped it
                    // reports which of a neighbour's paths are being edited right
                    // now — a side door around the path tools (issue #125).
                    let live = self
                        .scope
                        .filter(self.ws.live_paths_as(self.ctx()).await?, |l| {
                            Some(l.path.as_str())
                        });
                    if live.is_empty() {
                        return Ok("no live documents".to_string());
                    }
                    Ok(live
                        .iter()
                        .map(|l| {
                            format!("{}\tlive since {}\tactor {}", l.path, l.since, l.actor_id)
                        })
                        .collect::<Vec<_>>()
                        .join("\n"))
                }
            },
            "origofs_suggestions" => {
                // Both halves, as on the HTTP surface: resolving the filter stops
                // an agent *asking* about a neighbour, and filtering the results
                // stops an absent filter from *returning* one.
                let raw = args
                    .get("path")
                    .and_then(Value::as_str)
                    .filter(|p| !p.is_empty());
                let path_filter = match raw {
                    Some(_) => Some(path()?),
                    None => None,
                };
                let list = self
                    .ws
                    .list_suggestions_as(
                        self.ctx(),
                        Some(SuggestionStatus::Pending),
                        path_filter.as_deref(),
                    )
                    .await?;
                let list = self.scope.filter(list, |s| Some(s.path.as_str()));
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
                self.ws.suggestion_diff_as(self.ctx(), id).await
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
            "origofs_trash" => {
                let entries = self.ws.list_trash().await?;
                // Scoped like every other listing: unscoped, a scoped agent would
                // be told which of a neighbour's paths were deleted and when
                // (issue #125).
                let visible = self.scope.filter(entries, |e| Some(e.path.as_str()));
                if visible.is_empty() {
                    return Ok(match self.ws.trash_retention().await? {
                        Some(_) => "nothing in the trash".to_string(),
                        None => "trash is disabled for this workspace: deletes are \
                                 immediate and nothing can be restored"
                            .to_string(),
                    });
                }
                Ok(visible
                    .iter()
                    .map(|e| {
                        format!(
                            "#{}\t{}\t{}\tdeleted by actor {}",
                            e.id,
                            e.kind.as_str(),
                            e.path,
                            e.actor_id
                                .map(|a| a.to_string())
                                .unwrap_or_else(|| "-".into()),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "origofs_restore" => {
                let id = args.get("id").and_then(Value::as_i64).unwrap_or(0);
                // Scope-checked by *path* before restoring: a trash id is a
                // workspace-global handle, so without this a scoped agent could
                // materialize a neighbour's deleted file by guessing an integer.
                // Not-found rather than denied, for the reason the suggestion
                // routes give: a refusal would confirm the id exists.
                let entry = self
                    .ws
                    .list_trash()
                    .await?
                    .into_iter()
                    .find(|e| e.id == id)
                    .filter(|e| self.scope.contains(Some(e.path.as_str())))
                    .ok_or_else(|| OrigoFSError::NotFound(format!("trash entry #{id}")))?;
                let _ = &entry;
                let path = self.ws.restore_trash(id, self.ctx()).await?;
                Ok(format!("restored {path}"))
            }
            "origofs_ls" => {
                let entries = self.ws.ls_as(self.ctx(), &path()?).await?;
                Ok(entries
                    .iter()
                    .map(|e| format!("{}\t{}", e.kind.as_str(), e.name))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "origofs_mkdir" => {
                self.ws.mkdir_as(self.ctx(), &path()?).await?;
                Ok(format!("created {}", path()?))
            }
            "origofs_rm" => {
                // Policy-governed, like `origofs_write`: a propose-only agent's
                // removal is queued for review rather than destroying the file
                // it was already forbidden to overwrite (issue #78).
                let summary = format!("delete {}", path()?);
                match self
                    .ws
                    .remove_or_propose(self.ctx(), &path()?, Some(&summary))
                    .await?
                {
                    WriteOutcome::Wrote => Ok(format!("removed {}", path()?)),
                    WriteOutcome::Proposed(id) => Ok(format!(
                        "proposed deletion #{id} for {} (pending review)",
                        path()?
                    )),
                }
            }
            "origofs_blame" => {
                let ranges = self.ws.blame_as(self.ctx(), &path()?).await?;
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
                // Resolve the agent's real display name, as the HTTP surface
                // does. Hardcoding "mcp-agent" made every commit from every agent
                // anonymous and identical in `origofs log` — the one place a
                // reader looks to see who did what.
                let author = self
                    .ws
                    .get_actor(self.agent)
                    .await?
                    .map(|a| a.display_name)
                    .unwrap_or_else(|| format!("actor:{}", self.agent));
                let hash = self.ws.commit_as(self.ctx(), &author, message).await?;
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
            // An error, not an `Ok` string. Returning `Ok` set `isError: false`,
            // so a typo'd tool name looked to the agent exactly like a call that
            // had worked — and agents act on that.
            other => Err(crate::OrigoFSError::InvalidArgument(format!(
                "unknown tool: {other}"
            ))),
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
    #[cfg_attr(not(feature = "coedit"), allow(unused_mut))]
    let mut defs = vec![
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
            "Remove a file or empty directory. Governed by your write policy the \
             same way origofs_write is: if you are propose-only, this queues a \
             deletion for review instead of removing anything.",
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
            "origofs_live",
            "Whether a path has a live co-editing document open — i.e. whether its \
             stored bytes are the whole truth or a checkpoint that may lag what \
             people are typing right now. Omit `path` to list every live document. \
             Reading a live path always works; this only tells you how fresh the \
             answer is, and that a proposal there belongs in origofs_suggest_coedit.",
            json!({ "path": { "type": "string", "description": "optional: a single path to check" } }),
            &[],
        ),
        tool(
            "origofs_commit",
            "Snapshot the working tree into a commit.",
            json!({ "message": { "type": "string" } }),
            &["message"],
        ),
        tool("origofs_log", "Show commit history.", json!({}), &[]),
        // The recovery path for the failure this agent is most likely to cause.
        // The engine has had a trash since #115 and no tool exposed it, so an
        // agent that deleted the wrong file had nothing to reach for — and the
        // deletion it needs to undo is one *it* is recorded as having made.
        tool(
            "origofs_trash",
            "List deleted files that can still be restored, newest first. Each entry \
             shows an id, the path it was deleted from, and who deleted it. Trash is \
             off unless the workspace enabled it; when it is off this says so rather \
             than reporting an empty list, because 'nothing was deleted' and 'nothing \
             is being kept' are different answers.",
            json!({}),
            &[],
        ),
        tool(
            "origofs_restore",
            "Put a deleted file back at the path it was deleted from. Take the `id` \
             from origofs_trash. This is the undo for a delete that should not have \
             happened — including your own; the restore is credited to you and the \
             original deletion stays in the record.",
            json!({ "id": { "type": "integer" } }),
            &["id"],
        ),
    ];
    // Only offered when the server was built with `coedit` — advertising a tool
    // whose dispatch arm isn't compiled in would be a promise the server can't keep.
    #[cfg(feature = "coedit")]
    defs.push(tool(
        "origofs_suggest_coedit",
        "Propose a change to a live co-edited document as a CRDT merge instead of a \
         file body: replace `old` with `new` (exact text, must be unique). Prefer this \
         over origofs_suggest whenever origofs_live says the path is live — a byte \
         proposal there goes stale on somebody else's keystroke, and accepting it \
         replaces the whole body; this one merges, so a concurrent disjoint edit \
         survives. The document changes only when a different actor accepts, and the \
         merged text is credited to this agent.",
        json!({
            "path": { "type": "string" },
            "old": { "type": "string", "description": "exact text to replace; must be unique in the document" },
            "new": { "type": "string", "description": "replacement text" },
            "summary": { "type": "string", "description": "optional note for the reviewer" },
        }),
        &["path", "old", "new"],
    ));
    defs
}
