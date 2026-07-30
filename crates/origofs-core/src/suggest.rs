//! Suggestion / review queue (`docs/DESIGN.md` §6).
//!
//! Any actor can *propose* an edit instead of writing it directly: the proposed
//! bytes are stored in the content-addressed store (dedup'd, and diffable like
//! anything else) and a review record is written to the `suggestion` table.
//! A *different* actor then reviews it — [`Fs::suggestion_diff`] renders it as a
//! unified diff of `base` → `proposed` — and [`Fs::accept_suggestion`] applies it
//! (an attributed write, so blame still credits the actor that authored the
//! content) or [`Fs::reject_suggestion`] discards it.
//!
//! The mechanism is **actor-agnostic**: it serves an untrusted agent proposing for
//! human review *and* a change-request workflow between people. Whether an actor
//! *must* propose (vs. write directly) is its [`WritePolicy`](crate::WritePolicy),
//! enforced by [`Fs::write_or_propose`] — a bounded trust gate that is a property
//! of the actor, never its kind.
//!
//! Nothing here is a new storage path: suggestions reuse the CAS, the diff
//! machinery, the change feed, and attribution. Rejected/superseded proposals
//! leave orphaned chunks that ordinary GC reclaims.

use crate::attribution::{WriteCtx, WritePolicy};
use crate::collab::EventInit;
use crate::content::ContentStore;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::types::Hash;

/// The outcome of a policy-governed write (see [`Fs::write_or_propose`]).
// Deliberately NOT `#[non_exhaustive]`. This is an *outcome* enum: its whole
// purpose is to make the caller handle each case, so a new variant should be a
// compile error at every call site. `non_exhaustive` would force a wildcard arm
// that silently swallows it instead — the opposite of the intent. Adding a
// variant here is a breaking change on purpose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The actor writes directly; the edit landed in the working tree.
    Wrote,
    /// The actor is propose-only; the edit was queued as this suggestion for review.
    Proposed(i64),
}

/// What a suggestion proposes, and therefore how it is applied (issue #75 §3.2).
///
/// The two kinds differ in *what the base means*, which is what makes staleness
/// mean something different for each:
///
/// * [`Bytes`](Self::Bytes) — a whole file body. `base_hash` is the file's content
///   address when the proposal was computed, so accepting it is a conditional
///   whole-file write: if the file moved on, applying the proposal would silently
///   throw away the intervening change, so it must not be applied.
/// * [`Crdt`](Self::Crdt) — a *merge* into a co-edited document. `base_hash`
///   addresses the document's Yjs **state vector** and `proposed_hash` an opaque
///   `encodeStateAsUpdate` blob, so accepting it is `applyUpdate`. A CRDT merge is
///   defined for **any** pair of states, so a disjoint concurrent edit is not a
///   conflict and must not be rejected — the byte-suggestion staleness guard would
///   false-reject it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SuggestionKind {
    /// A whole-file body (the classic path).
    #[default]
    Bytes,
    /// A Yjs update to merge into a co-edited document.
    Crdt,
}

impl SuggestionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SuggestionKind::Bytes => "bytes",
            SuggestionKind::Crdt => "crdt",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "bytes" => SuggestionKind::Bytes,
            "crdt" => SuggestionKind::Crdt,
            _ => return None,
        })
    }
}

/// The lifecycle state of a suggestion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
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
    /// What the two hashes above *mean* — see [`SuggestionKind`].
    pub kind: SuggestionKind,
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
    pub kind: SuggestionKind,
    pub status: SuggestionStatus,
    pub created_ts: i64,
    pub resolved_ts: Option<i64>,
    pub resolved_by: Option<i64>,
}

/// A suggestion's **content** (not just a diff): the text at the proposal's base
/// and the proposed text. Lets a caller render an inline review straight from the
/// store, instead of stashing the proposed bytes app-side. `proposed` is `None`
/// when the suggestion proposes a deletion.
///
/// For a [`Crdt`](SuggestionKind::Crdt) suggestion the pair is instead the *effect
/// of the merge* — the live document's text now, and its text with the proposed
/// update applied — because neither of that kind's hashes addresses readable text
/// (they are a state vector and an opaque Yjs update). Same shape, same rendering
/// code in a reviewer UI; see [`Fs::suggestion_diff`](crate::Fs::suggestion_diff).
#[derive(Clone, Debug)]
pub struct SuggestionContent {
    pub base: String,
    pub proposed: Option<String>,
}

/// The error a CRDT suggestion raises in a build without the `coedit` feature:
/// the review row is readable, but applying (or previewing) it needs the CRDT
/// engine that feature compiles in.
#[cfg(not(feature = "coedit"))]
fn crdt_needs_feature(id: i64) -> OrigoFSError {
    OrigoFSError::InvalidArgument(format!(
        "suggestion #{id} is a CRDT suggestion; this build lacks the `coedit` feature"
    ))
}

impl<M: MetadataStore, C: ContentStore> crate::engine::Fs<M, C> {
    /// The actor's [`WritePolicy`], or the column default for an unknown actor.
    ///
    /// Unknown actors resolve to [`Direct`](WritePolicy::Direct) to match the
    /// column default — identity is resolved server-side before any of this runs,
    /// so in practice the actor always exists.
    pub(crate) async fn write_policy_of(&self, actor: i64) -> Result<WritePolicy> {
        Ok(self
            .meta
            .get_actor(actor)
            .await?
            .map(|a| a.write_policy)
            .unwrap_or(WritePolicy::Direct))
    }

    /// **The trust gate (§6).** Refuse `op` outright when `ctx`'s actor is
    /// [`Propose`](WritePolicy::Propose)-only.
    ///
    /// This is the one place a direct mutation is authorized, and every
    /// actor-attributed mutating entry point on [`Fs`](crate::Fs) calls it — so a
    /// new surface cannot forget the check by simply not knowing about it, which
    /// is exactly how `origofs_rm` shipped ungated (issue #78).
    ///
    /// Operations that have a propose-shaped equivalent (`write` →
    /// [`write_or_propose`](Self::write_or_propose), `remove` →
    /// [`remove_or_propose`](Self::remove_or_propose)) should prefer that, so a
    /// propose-only actor's request is *queued* rather than refused. This is the
    /// backstop for the ones that have no such equivalent — rename, mkdir,
    /// symlink, commit, accepting someone else's suggestion.
    ///
    /// Internal machinery is exempt **by construction**, not by flag: it calls the
    /// raw unattributed engine ops ([`remove`](crate::Fs::remove),
    /// [`rename`](crate::Fs::rename), [`write_as`](Self::write_as)), which do not
    /// route through here. Checkout, merge materialization and
    /// [`accept_suggestion`](Self::accept_suggestion) applying an approved edit are
    /// all system actions with no requesting actor to police.
    /// Refuse `op` for an actor whose [`WritePolicy`](crate::WritePolicy) is
    /// `Propose` (§6). `pub` so a *surface* can gate an administrative operation
    /// that has no attributed engine variant of its own — registering an actor,
    /// for instance, mutates the identity registry rather than the working tree,
    /// so there is nothing to attribute, but it still must not be open to an
    /// actor the operator has restricted.
    pub async fn ensure_may_write(&self, ctx: WriteCtx, op: &str) -> Result<()> {
        match self.write_policy_of(ctx.actor).await? {
            WritePolicy::Direct => Ok(()),
            WritePolicy::Propose => Err(OrigoFSError::Denied(format!(
                "actor {} is propose-only and may not {op} directly; \
                 submit a suggestion for review instead",
                ctx.actor
            ))),
        }
    }

    /// Submit a removal of `path` **governed by the actor's write policy** (§6) —
    /// the deletion counterpart of [`write_or_propose`](Self::write_or_propose).
    ///
    /// A [`Direct`](WritePolicy::Direct) actor removes the path immediately (as an
    /// attributed op); a [`Propose`](WritePolicy::Propose) actor's request becomes
    /// a pending deletion suggestion via [`suggest_delete`](Self::suggest_delete),
    /// which a different actor reviews. Without this, a propose-only actor blocked
    /// from *overwriting* a file could still delete it — the same destruction, one
    /// call further along (issue #78).
    pub async fn remove_or_propose(
        &self,
        ctx: WriteCtx,
        path: &str,
        summary: Option<&str>,
    ) -> Result<WriteOutcome> {
        match self.write_policy_of(ctx.actor).await? {
            WritePolicy::Direct => {
                self.remove_as(ctx, path).await?;
                Ok(WriteOutcome::Wrote)
            }
            WritePolicy::Propose => {
                let id = self.suggest_delete(ctx, path, summary).await?;
                Ok(WriteOutcome::Proposed(id))
            }
        }
    }

    /// Submit an edit to `path` **governed by the actor's write policy** (§6): a
    /// [`Direct`](WritePolicy::Direct) actor writes straight to the working tree; a
    /// [`Propose`](WritePolicy::Propose) actor's edit is instead queued as a
    /// suggestion for review by a *different* actor. This is the entry point
    /// untrusted surfaces (the MCP agent, the HTTP API) route writes through, so a
    /// propose-only actor can never land an unreviewed edit through the front door.
    /// Internal machinery (checkpoints, accepting a suggestion) uses the raw
    /// [`write_as`](Self::write_as) and is exempt by construction. Actor-agnostic —
    /// the gate is the actor's policy, never their kind.
    pub async fn write_or_propose(
        &self,
        ctx: WriteCtx,
        path: &str,
        data: &[u8],
        summary: Option<&str>,
    ) -> Result<WriteOutcome> {
        // An unknown actor has no policy on record: default to direct (matching the
        // column default). Identity is resolved server-side before we get here, so
        // in practice the actor always exists.
        let policy = self
            .meta
            .get_actor(ctx.actor)
            .await?
            .map(|a| a.write_policy)
            .unwrap_or(WritePolicy::Direct);
        match policy {
            WritePolicy::Direct => {
                // Missing parents are created here, *after* the policy decision.
                // Surfaces used to do it before calling in, so an edit that was
                // merely queued for review had already mutated the working tree —
                // the one thing a propose-only actor must not be able to do.
                // `accept_suggestion` creates them on the way in instead.
                if let Some((parent, _)) = path.rsplit_once('/')
                    && !parent.is_empty()
                {
                    self.mkdir_p(parent).await?;
                }
                self.write_as(ctx, path, data).await?;
                Ok(WriteOutcome::Wrote)
            }
            WritePolicy::Propose => {
                let id = self.suggest(ctx, path, data, summary).await?;
                Ok(WriteOutcome::Proposed(id))
            }
        }
    }

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
        self.record_suggestion(
            ctx,
            path,
            base_hash,
            proposed_hash,
            summary,
            SuggestionKind::Bytes,
        )
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
        self.record_suggestion(ctx, path, base_hash, None, summary, SuggestionKind::Bytes)
            .await
    }

    pub(crate) async fn record_suggestion(
        &self,
        ctx: WriteCtx,
        path: &str,
        base_hash: Option<String>,
        proposed_hash: Option<String>,
        summary: Option<&str>,
        kind: SuggestionKind,
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
                    kind,
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
    pub(crate) async fn current_content_hex(&self, path: &str) -> Result<Option<String>> {
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
    ///
    /// For a [`Crdt`](SuggestionKind::Crdt) suggestion neither hash addresses
    /// readable text (they are a state vector and an opaque update), so the diff is
    /// instead rendered from the *effect* of the merge: the live document's text
    /// now, versus its text with the proposed update applied to a throwaway copy.
    /// That is what a reviewer actually needs to see, and — because the merge is
    /// recomputed against the current document — it stays accurate as the document
    /// moves on underneath.
    pub async fn suggestion_diff(&self, id: i64) -> Result<String> {
        let s = self
            .meta
            .get_suggestion(id)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("suggestion #{id}")))?;
        let (old, new) = self.suggestion_texts(&s).await?;
        Ok(diffy::create_patch(&old, &new.unwrap_or_default()).to_string())
    }

    /// The `(base, proposed)` text a review renders, per suggestion kind. `proposed`
    /// is `None` only for a proposed deletion.
    async fn suggestion_texts(&self, s: &Suggestion) -> Result<(String, Option<String>)> {
        match s.kind {
            SuggestionKind::Bytes => {
                let base = self.hex_to_text(s.base_hash.as_deref()).await?;
                let proposed = match s.proposed_hash.as_deref() {
                    Some(h) => Some(self.hex_to_text(Some(h)).await?),
                    None => None,
                };
                Ok((base, proposed))
            }
            #[cfg(feature = "coedit")]
            SuggestionKind::Crdt => {
                let (before, after) = self.preview_coedit_suggestion(s).await?;
                Ok((before, Some(after)))
            }
            #[cfg(not(feature = "coedit"))]
            SuggestionKind::Crdt => Err(crdt_needs_feature(s.id)),
        }
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
        let (base, proposed) = self.suggestion_texts(&s).await?;
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

    /// Accept a pending suggestion: apply it and mark it accepted. The applied
    /// edit is attributed to the **original author** (so blame credits the agent),
    /// while `approver` is recorded as who accepted it — and must be a different
    /// actor.
    ///
    /// **Staleness depends on the kind** ([`SuggestionKind`]).
    ///
    /// * A [`Bytes`](SuggestionKind::Bytes) suggestion replaces the whole file, so
    ///   a moved base means applying it would silently discard the intervening
    ///   change. It is refused with [`OrigoFSError::Conflict`] *and* resolved to
    ///   [`Superseded`](SuggestionStatus::Superseded) — the honest terminal state
    ///   for "this proposal is about a version of the file that no longer exists".
    ///   Re-diff and re-suggest.
    /// * A [`Crdt`](SuggestionKind::Crdt) suggestion is a *merge*, and a CRDT merge
    ///   is defined for any pair of states: a disjoint concurrent edit is not a
    ///   conflict. Applying it can therefore never discard anything, so it is
    ///   **not** subject to the staleness guard — that guard false-rejected every
    ///   concurrent edit over an always-mergeable document.
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

        // The review gate, part one: an approver must itself be trusted to write.
        // Accepting lands a real attributed edit in the working tree, so letting a
        // propose-only actor approve would be a direct write wearing a review as a
        // costume — and two propose-only agents could rubber-stamp each other into
        // full write access (issue #78).
        self.ensure_may_write(approver, "accept suggestions")
            .await?;

        // Part two: a suggestion's own author cannot approve it. This is
        // what makes "an agent proposes, a different actor reviews" a real gate
        // rather than a rubber stamp the proposer can apply to itself.
        if approver.actor == s.actor_id {
            return Err(OrigoFSError::InvalidArgument(format!(
                "suggestion #{id} cannot be accepted by its author (actor {}); acceptance requires a different reviewer",
                s.actor_id
            )));
        }

        let author = WriteCtx {
            actor: s.actor_id,
            session: s.session_id,
            tool_call: None,
        };

        match s.kind {
            SuggestionKind::Bytes => self.apply_byte_suggestion(&s, author).await?,
            #[cfg(feature = "coedit")]
            SuggestionKind::Crdt => self.apply_coedit_suggestion(&s, author).await?,
            #[cfg(not(feature = "coedit"))]
            SuggestionKind::Crdt => return Err(crdt_needs_feature(s.id)),
        }

        // `resolve_suggestion` is a compare-and-set on `status = 'pending'`, and its
        // answer matters. Discarding it meant a *lost* CAS still reported success:
        // the dangerous shape is an accept racing a reject, where the reject claims
        // the row, the accept applies the proposed bytes anyway, and the caller is
        // told the acceptance worked while the suggestion reads "rejected". The
        // write has already landed by this point — that is inherent to applying
        // before resolving — so this cannot be undone here, but it must not be
        // silent.
        if !self
            .meta
            .resolve_suggestion(
                id,
                SuggestionStatus::Accepted,
                Some(approver.actor),
                self.now_secs(),
            )
            .await?
        {
            let now = self.meta.get_suggestion(id).await?;
            let state = now.as_ref().map(|s| s.status.as_str()).unwrap_or("deleted");
            return Err(OrigoFSError::Conflict(format!(
                "suggestion #{id} was resolved as {state} by another reviewer while                  this acceptance was being applied; {} now holds the proposed                  content and should be reviewed directly",
                s.path
            )));
        }
        self.record_event(EventInit {
            actor_id: Some(approver.actor),
            session_id: approver.session,
            kind: "accept".to_string(),
            path: s.path.clone(),
            detail: Some(format!("suggestion #{id}")),
            branch: self.current_branch().await.ok().flatten(),
        })
        .await?;
        // The accept moved the file, so any *other* pending byte proposal against
        // the old base is now about a version that no longer exists. Retire those
        // to `Superseded` instead of leaving them Pending forever for a reviewer to
        // discover one failed accept at a time.
        self.supersede_stale_byte_suggestions(&s.path, Some(id))
            .await?;
        Ok(())
    }

    /// Apply a whole-file byte suggestion, guarding the base it was proposed
    /// against. A stale base is refused *and* recorded as
    /// [`Superseded`](SuggestionStatus::Superseded) — never silently clobbered.
    async fn apply_byte_suggestion(&self, s: &Suggestion, author: WriteCtx) -> Result<()> {
        let current = self.current_content_hex(&s.path).await?;
        if current != s.base_hash {
            return Err(self.mark_superseded(s).await);
        }
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
                match self
                    .write_as_expecting(author, &s.path, &bytes, expected_base)
                    .await
                {
                    Ok(()) => Ok(()),
                    // Lost the race in the window between the check and the write:
                    // same situation, same terminal state.
                    Err(OrigoFSError::Conflict(_)) => Err(self.mark_superseded(s).await),
                    Err(e) => Err(e),
                }
            }
            None => {
                // Proposed deletion. (The staleness pre-check above guards it; a
                // conditional delete would close its narrower remaining window.)
                self.remove(&s.path).await
            }
        }
    }

    /// Retire `s` as [`Superseded`](SuggestionStatus::Superseded) and return the
    /// `Conflict` to report. Resolving it is best-effort: failing to *record* the
    /// outcome must not turn a clean "your base moved" into a backend error, and
    /// the caller's contract — "a stale byte suggestion is never applied" — holds
    /// either way.
    async fn mark_superseded(&self, s: &Suggestion) -> OrigoFSError {
        let _ = self
            .meta
            .resolve_suggestion(s.id, SuggestionStatus::Superseded, None, self.now_secs())
            .await;
        let _ = self
            .record_event(EventInit {
                actor_id: Some(s.actor_id),
                session_id: s.session_id,
                kind: "supersede".to_string(),
                path: s.path.clone(),
                detail: Some(format!("suggestion #{}", s.id)),
                branch: self.current_branch().await.ok().flatten(),
            })
            .await;
        OrigoFSError::Conflict(format!(
            "suggestion #{}: {} changed since it was proposed; marked superseded — re-diff and re-suggest",
            s.id, s.path
        ))
    }

    /// Resolve every pending **byte** suggestion on `path` whose base no longer
    /// matches the file to [`Superseded`](SuggestionStatus::Superseded), skipping
    /// `except`. CRDT suggestions are deliberately untouched: they merge into
    /// whatever the document has become, so a moved file does not invalidate them.
    /// Returns how many were retired.
    pub async fn supersede_stale_byte_suggestions(
        &self,
        path: &str,
        except: Option<i64>,
    ) -> Result<usize> {
        let current = self.current_content_hex(path).await?;
        let pending = self
            .meta
            .list_suggestions(Some(SuggestionStatus::Pending), Some(path))
            .await?;
        let mut n = 0;
        for s in pending {
            if Some(s.id) == except
                || s.kind != SuggestionKind::Bytes
                || s.base_hash == current
                || !self
                    .meta
                    .resolve_suggestion(s.id, SuggestionStatus::Superseded, None, self.now_secs())
                    .await?
            {
                continue;
            }
            n += 1;
            self.record_event(EventInit {
                actor_id: Some(s.actor_id),
                session_id: s.session_id,
                kind: "supersede".to_string(),
                path: s.path.clone(),
                detail: Some(format!("suggestion #{}", s.id)),
                branch: self.current_branch().await.ok().flatten(),
            })
            .await?;
        }
        Ok(n)
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
        // Reviewing is a trusted act in both directions. Rejecting mutates no
        // bytes, but it *disposes of someone else's proposal* — leaving it open to
        // propose-only actors would let one agent quietly clear another's work out
        // of the review queue. An actor that cannot write cannot review (#78).
        // Withdrawing your own proposal is a different operation and stays open.
        if approver.actor != s.actor_id {
            self.ensure_may_write(approver, "reject others' suggestions")
                .await?;
        }
        // Same CAS, same reason it must be checked — a reject that lost to a
        // concurrent accept previously reported success while the proposed bytes
        // were being written to the working tree.
        if !self
            .meta
            .resolve_suggestion(
                id,
                SuggestionStatus::Rejected,
                Some(approver.actor),
                self.now_secs(),
            )
            .await?
        {
            let now = self.meta.get_suggestion(id).await?;
            let state = now.as_ref().map(|s| s.status.as_str()).unwrap_or("deleted");
            return Err(OrigoFSError::Conflict(format!(
                "suggestion #{id} was already resolved as {state} by another reviewer"
            )));
        }
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
