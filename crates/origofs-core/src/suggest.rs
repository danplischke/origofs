//! Agent-suggestion review queue (`docs/DESIGN.md` §6).
//!
//! An agent can *propose* an edit instead of writing it directly: the proposed
//! bytes are stored in the content-addressed store (dedup'd, and diffable like
//! anything else) and a review record is written to the `suggestion` table.
//! A human then reviews it — [`Fs::suggestion_diff`] renders it as a unified
//! diff of `base` → `proposed` — and [`Fs::accept_suggestion`] applies it (an
//! attributed write, so blame still credits the agent that authored the
//! content) or [`Fs::reject_suggestion`] discards it.
//!
//! Nothing here is a new storage path: suggestions reuse the CAS, the diff
//! machinery, the change feed, and attribution. Rejected/superseded proposals
//! leave orphaned chunks that ordinary GC reclaims.

use crate::attribution::WriteCtx;
use crate::collab::EventInit;
use crate::content::ContentStore;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::types::Hash;

/// The lifecycle state of a suggestion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuggestionStatus {
    /// Awaiting review.
    Pending,
    /// Applied to the working tree.
    Accepted,
    /// Discarded without applying.
    Rejected,
    /// The base moved out from under it before review (informational).
    Superseded,
}

impl SuggestionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SuggestionStatus::Pending => "pending",
            SuggestionStatus::Accepted => "accepted",
            SuggestionStatus::Rejected => "rejected",
            SuggestionStatus::Superseded => "superseded",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => SuggestionStatus::Pending,
            "accepted" => SuggestionStatus::Accepted,
            "rejected" => SuggestionStatus::Rejected,
            "superseded" => SuggestionStatus::Superseded,
            _ => return None,
        })
    }
}

/// A new suggestion to record.
#[derive(Clone, Debug)]
pub struct SuggestionInit {
    pub actor_id: i64,
    pub session_id: Option<i64>,
    pub branch: Option<String>,
    pub path: String,
    /// The content hash the proposal was computed against (`None` if the file
    /// did not exist), used to detect a stale base at accept time.
    pub base_hash: Option<String>,
    /// The content hash of the proposed body (`None` proposes a deletion).
    pub proposed_hash: Option<String>,
    pub summary: Option<String>,
}

/// A recorded suggestion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Suggestion {
    pub id: i64,
    pub actor_id: i64,
    pub session_id: Option<i64>,
    pub branch: Option<String>,
    pub path: String,
    pub base_hash: Option<String>,
    pub proposed_hash: Option<String>,
    pub summary: Option<String>,
    pub status: SuggestionStatus,
    pub created_ts: i64,
    pub resolved_ts: Option<i64>,
    pub resolved_by: Option<i64>,
}

/// A suggestion's **content** (not just a diff): the text at the proposal's base
/// and the proposed text. Lets a caller render an inline review straight from the
/// store, instead of stashing the proposed bytes app-side. `proposed` is `None`
/// when the suggestion proposes a deletion.
#[derive(Clone, Debug)]
pub struct SuggestionContent {
    pub base: String,
    pub proposed: Option<String>,
}

impl<M: MetadataStore, C: ContentStore> crate::engine::Fs<M, C> {
    /// Propose an edit to `path` without applying it. The bytes are stored in
    /// the CAS now; the working tree is untouched until the suggestion is
    /// accepted. Returns the new suggestion id. `data` empty with the intent to
    /// delete is expressed by [`Self::suggest_delete`].
    pub async fn suggest(
        &self,
        ctx: WriteCtx,
        path: &str,
        data: &[u8],
        summary: Option<&str>,
    ) -> Result<i64> {
        let base_hash = self.current_content_hex(path).await?;
        let (mhash, _size) = self.store_body(data).await?;
        let proposed_hash = match mhash {
            Some(h) => Some(h.to_hex()),
            // Empty proposed content is a real *empty file*, not a deletion.
            // `store_body("")` returns no manifest, so persist an explicit empty
            // manifest and reference it — otherwise `proposed_hash == None` would
            // be indistinguishable from `suggest_delete` and remove the file on
            // accept.
            None => Some(
                self.content
                    .put(&crate::chunk::Manifest::default().encode())
                    .await?
                    .to_hex(),
            ),
        };
        self.record_suggestion(ctx, path, base_hash, proposed_hash, summary)
            .await
    }

    /// Propose deleting `path` (a suggestion with no proposed content).
    pub async fn suggest_delete(
        &self,
        ctx: WriteCtx,
        path: &str,
        summary: Option<&str>,
    ) -> Result<i64> {
        // Existence is a namespace question, not "has content": an empty file
        // exists but has no content hash. `resolve` errors `NotFound` if the
        // path genuinely doesn't exist.
        self.resolve(path).await?;
        let base_hash = self.current_content_hex(path).await?;
        self.record_suggestion(ctx, path, base_hash, None, summary)
            .await
    }

    async fn record_suggestion(
        &self,
        ctx: WriteCtx,
        path: &str,
        base_hash: Option<String>,
        proposed_hash: Option<String>,
        summary: Option<&str>,
    ) -> Result<i64> {
        let branch = self.current_branch().await.ok().flatten();
        let id = self
            .meta
            .create_suggestion(
                SuggestionInit {
                    actor_id: ctx.actor,
                    session_id: ctx.session,
                    branch: branch.clone(),
                    path: path.to_string(),
                    base_hash,
                    proposed_hash,
                    summary: summary.map(str::to_string),
                },
                self.now_secs(),
            )
            .await?;
        self.record_event(EventInit {
            actor_id: Some(ctx.actor),
            session_id: ctx.session,
            kind: "suggest".to_string(),
            path: path.to_string(),
            detail: Some(format!("suggestion #{id}")),
            branch,
        })
        .await?;
        Ok(id)
    }

    /// The current content hash of `path` in hex, or `None` if it doesn't exist.
    async fn current_content_hex(&self, path: &str) -> Result<Option<String>> {
        match self.resolve(path).await {
            Ok(ino) => {
                let inode = self
                    .meta
                    .get_inode(ino)
                    .await?
                    .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
                Ok(inode.content.map(|h| h.to_hex()))
            }
            Err(OrigoFSError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// A suggestion by id.
    pub async fn get_suggestion(&self, id: i64) -> Result<Option<Suggestion>> {
        self.meta.get_suggestion(id).await
    }

    /// Suggestions, optionally filtered by `status` and/or `path`, newest first.
    pub async fn list_suggestions(
        &self,
        status: Option<SuggestionStatus>,
        path: Option<&str>,
    ) -> Result<Vec<Suggestion>> {
        self.meta.list_suggestions(status, path).await
    }

    /// Render a suggestion as a unified line diff (`base` → `proposed`).
    pub async fn suggestion_diff(&self, id: i64) -> Result<String> {
        let s = self
            .meta
            .get_suggestion(id)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("suggestion #{id}")))?;
        let old = self.hex_to_text(s.base_hash.as_deref()).await?;
        let new = self.hex_to_text(s.proposed_hash.as_deref()).await?;
        Ok(diffy::create_patch(&old, &new).to_string())
    }

    /// A suggestion's base and proposed **content**, read from the store — so a
    /// reviewer UI can render an inline diff without the app stashing the proposed
    /// bytes itself. `proposed` is `None` when the suggestion proposes a deletion.
    pub async fn suggestion_content(&self, id: i64) -> Result<SuggestionContent> {
        let s = self
            .meta
            .get_suggestion(id)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("suggestion #{id}")))?;
        let base = self.hex_to_text(s.base_hash.as_deref()).await?;
        let proposed = match s.proposed_hash.as_deref() {
            Some(h) => Some(self.hex_to_text(Some(h)).await?),
            None => None,
        };
        Ok(SuggestionContent { base, proposed })
    }

    async fn hex_to_text(&self, hex: Option<&str>) -> Result<String> {
        match hex {
            Some(h) => {
                let hash = Hash::from_hex(h)
                    .ok_or_else(|| OrigoFSError::Metadata("bad content hash".into()))?;
                let bytes = self.content_bytes(&hash).await?;
                Ok(String::from_utf8_lossy(&bytes).into_owned())
            }
            None => Ok(String::new()),
        }
    }

    /// Accept a pending suggestion: apply it to the working tree and mark it
    /// accepted. The applied write is attributed to the **original author**
    /// (so blame credits the agent), while `approver` is recorded as who
    /// accepted it. Fails with [`OrigoFSError::Conflict`] if the file changed since
    /// the suggestion was made (a stale base) — re-diff and re-suggest.
    pub async fn accept_suggestion(&self, id: i64, approver: WriteCtx) -> Result<()> {
        let s = self
            .meta
            .get_suggestion(id)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("suggestion #{id}")))?;
        if s.status != SuggestionStatus::Pending {
            return Err(OrigoFSError::InvalidArgument(format!(
                "suggestion #{id} is already {}",
                s.status.as_str()
            )));
        }

        // The review gate: a suggestion's own author cannot approve it. This is
        // what makes "an agent proposes, a different actor reviews" a real gate
        // rather than a rubber stamp the proposer can apply to itself.
        if approver.actor == s.actor_id {
            return Err(OrigoFSError::InvalidArgument(format!(
                "suggestion #{id} cannot be accepted by its author (actor {}); acceptance requires a different reviewer",
                s.actor_id
            )));
        }

        // Staleness: the file must still be what the proposal was based on.
        let current = self.current_content_hex(&s.path).await?;
        if current != s.base_hash {
            return Err(OrigoFSError::Conflict(format!(
                "suggestion #{id}: {} changed since it was proposed",
                s.path
            )));
        }

        let author = WriteCtx {
            actor: s.actor_id,
            session: s.session_id,
            tool_call: None,
        };
        // The base the proposal was diffed against, as the CAS expectation below.
        let expected_base = match &s.base_hash {
            Some(hex) => Some(
                Hash::from_hex(hex)
                    .ok_or_else(|| OrigoFSError::Metadata("bad base hash".into()))?,
            ),
            None => None,
        };
        match &s.proposed_hash {
            Some(hex) => {
                let hash = Hash::from_hex(hex)
                    .ok_or_else(|| OrigoFSError::Metadata("bad proposed hash".into()))?;
                let bytes = self.content_bytes(&hash).await?;
                // Apply atomically: the write only lands if the file is *still* at
                // the base it was proposed against, so a change that slipped in
                // after the staleness check above can't be silently clobbered.
                self.write_as_expecting(author, &s.path, &bytes, expected_base)
                    .await?;
            }
            None => {
                // Proposed deletion. (The staleness pre-check above guards it; a
                // conditional delete would close its narrower remaining window.)
                self.remove(&s.path).await?;
            }
        }

        self.meta
            .resolve_suggestion(
                id,
                SuggestionStatus::Accepted,
                Some(approver.actor),
                self.now_secs(),
            )
            .await?;
        self.record_event(EventInit {
            actor_id: Some(approver.actor),
            session_id: approver.session,
            kind: "accept".to_string(),
            path: s.path.clone(),
            detail: Some(format!("suggestion #{id}")),
            branch: self.current_branch().await.ok().flatten(),
        })
        .await?;
        Ok(())
    }

    /// Reject a pending suggestion without applying it.
    pub async fn reject_suggestion(&self, id: i64, approver: WriteCtx) -> Result<()> {
        let s = self
            .meta
            .get_suggestion(id)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("suggestion #{id}")))?;
        if s.status != SuggestionStatus::Pending {
            return Err(OrigoFSError::InvalidArgument(format!(
                "suggestion #{id} is already {}",
                s.status.as_str()
            )));
        }
        self.meta
            .resolve_suggestion(
                id,
                SuggestionStatus::Rejected,
                Some(approver.actor),
                self.now_secs(),
            )
            .await?;
        self.record_event(EventInit {
            actor_id: Some(approver.actor),
            session_id: approver.session,
            kind: "reject".to_string(),
            path: s.path.clone(),
            detail: Some(format!("suggestion #{id}")),
            branch: self.current_branch().await.ok().flatten(),
        })
        .await?;
        Ok(())
    }
}
