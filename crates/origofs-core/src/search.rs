//! Content search: what to index, and what a query means.
//!
//! # The index is keyed by content, not by path
//!
//! Everything here follows from one decision: an indexed row is addressed by a
//! blob's **content hash**, never by the path it currently sits at. Content in
//! origofs is immutable and deduplicated, so a hash that has been indexed stays
//! correctly indexed forever, and the expensive half of search — reading a body
//! back through its chunks and tokenizing it — happens once per unique blob
//! rather than once per path per change.
//!
//! What that buys is not a micro-optimization, it is the absence of an
//! invalidation protocol:
//!
//! * **A rename costs nothing.** No indexed row mentions a name.
//! * **A delete costs nothing.** [`Fs::search`](crate::Fs::search) resolves hits
//!   to paths against the live working tree, so an unlinked file simply stops
//!   resolving. There are no tombstones to write and none to miss.
//! * **A branch checkout costs nothing.** It re-materializes paths whose content
//!   addresses the index already holds. This is the case that rules out the
//!   obvious alternatives: a per-path index or a per-inode version column both
//!   have to touch every file in the tree, and a change-feed row per path turns
//!   one checkout into thousands of log rows.
//! * **A stale index cannot serve a wrong answer.** It can only serve a *short*
//!   one, and [`SearchStatus`] says by how much. Since results are joined
//!   against the live tree, an unindexed blob is a missing hit, never a hit
//!   pointing at content that is no longer there.
//!
//! The last point is why indexing needs no reliable change signal, and why this
//! deliberately does not consume the change feed: the feed is emitted at the
//! `Workspace` API boundary and by design carries user intent rather than every
//! state mutation (see `collab`), so a write through a mount never appears in
//! it. An indexer built on it would drift silently. Instead the work queue is a
//! set difference — content addresses in the tree that `blob_index` has not
//! seen — which is self-healing by construction and cannot drift at all.
//!
//! # What this costs, stated plainly
//!
//! A one-byte edit to a large file produces a new whole-file hash and re-reads
//! the whole file. Chunk-level incrementality is not available here: FastCDC
//! boundaries fall wherever the content-defined rolling hash puts them, which is
//! mid-token, so indexing per chunk would split terms across chunk edges and
//! silently lose them. Whole-file re-extraction is the honest trade.
//!
//! # Scope of v1
//!
//! Whole-word terms, `AND` across the query's terms, no ranking, no phrases, no
//! regex, and the working tree only (not history). The inverted index is a plain
//! table rather than SQLite FTS5 or Postgres `tsvector` **so that both backends
//! answer identically** — the same reason the POSIX-lock resolver lives in one
//! place with the backends only running its decisions. A backend-native index
//! would rank differently on SQLite and Postgres, and "your dev box disagrees
//! with production about which files match" is a worse deal than no ranking.

/// Bodies larger than this are not indexed.
///
/// A cap has to exist or one multi-gigabyte file stalls the sweep and lands a
/// term list nothing can use. 4 MiB is chosen to comfortably cover source files,
/// notes and documents — the things a workspace's users actually search — while
/// excluding the assets they do not.
pub const MAX_INDEXED_BYTES: u64 = 4 * 1024 * 1024;

/// Terms shorter than this are dropped.
///
/// One- and two-character tokens match almost everything, so they cost the most
/// index space and carry the least information.
pub const MIN_TERM_LEN: usize = 3;

/// Terms longer than this are truncated rather than dropped.
///
/// Truncating keeps a long identifier findable by its prefix; dropping would
/// make a minified bundle or a base64 blob simply unsearchable. The bound exists
/// because a term is a primary-key column.
pub const MAX_TERM_LEN: usize = 64;

/// The most distinct terms kept from a single blob.
///
/// A bound on the row count one file can contribute, so a pathological input
/// (a dictionary, a wordlist) cannot dominate the table.
pub const MAX_TERMS_PER_BLOB: usize = 20_000;

/// How far into a body to look for a NUL before calling it text.
const BINARY_SNIFF_BYTES: usize = 8192;

/// The text of `bytes`, or `None` if this is not something to index.
///
/// Two rejections, both deliberate. **Not UTF-8** is not text origofs can
/// tokenize — there is no encoding detection here, and guessing one would put
/// mojibake in the index. **A NUL byte in the first [`BINARY_SNIFF_BYTES`]** is
/// the same heuristic `grep` and `git` use to call a file binary; a file can be
/// valid UTF-8 and still be a binary format, and indexing the ASCII runs inside
/// one produces hits nobody wants.
///
/// Size is checked by the caller against [`MAX_INDEXED_BYTES`], because the
/// caller knows it from the inode and can skip the read entirely.
pub fn extract_text(bytes: &[u8]) -> Option<&str> {
    let sniff = &bytes[..bytes.len().min(BINARY_SNIFF_BYTES)];
    if sniff.contains(&0) {
        return None;
    }
    std::str::from_utf8(bytes).ok()
}

/// Split `text` into the distinct terms to index, lowercased.
///
/// Deliberately simple: fold to lowercase, break on anything that is not a
/// letter or a digit, drop what is too short, truncate what is too long. No
/// stemming and no stop-word list — both are language-dependent, and a workspace
/// holding source code in one language and prose in another is the normal case
/// here, not the exception.
///
/// Returned sorted and deduplicated, so the caller writes one row per distinct
/// term and the result does not depend on where in the file a word appeared.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    for raw in text.split(|c: char| !c.is_alphanumeric()) {
        if raw.is_empty() {
            continue;
        }
        // Lowercase first: `is_alphanumeric` already accepted the characters, and
        // a fold can change length (`İ`), so the length bounds are applied after.
        let mut term = raw.to_lowercase();
        if term.chars().count() < MIN_TERM_LEN {
            continue;
        }
        if term.len() > MAX_TERM_LEN {
            // Truncate on a character boundary — `MAX_TERM_LEN` is a byte bound
            // and a multi-byte character must not be split, or the term is not
            // valid UTF-8 to store.
            let end = (0..=MAX_TERM_LEN)
                .rev()
                .find(|&i| term.is_char_boundary(i))
                .unwrap_or(0);
            term.truncate(end);
            if term.chars().count() < MIN_TERM_LEN {
                continue;
            }
        }
        terms.push(term);
    }
    terms.sort_unstable();
    terms.dedup();
    terms.truncate(MAX_TERMS_PER_BLOB);
    terms
}

/// The terms a query asks for, as [`tokenize`] would have produced them.
///
/// Running the query through the *same* function as the document is the point:
/// a query term that tokenization would never emit can never match, and having
/// one function makes that true by construction rather than by two definitions
/// agreeing. An empty result means the query held nothing searchable — a caller
/// must treat that as "no query", not as "match everything".
pub fn query_terms(query: &str) -> Vec<String> {
    tokenize(query)
}

/// One search hit: where it is now, and what it matched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchHit {
    /// The path this content is at **now**, resolved against the live tree.
    pub path: String,
    /// The content address that matched.
    pub content: crate::types::Hash,
    /// The inode the path resolved through.
    pub ino: crate::types::Ino,
}

/// How complete the index is, so a short answer is never mistaken for a whole one.
///
/// Returned alongside every search. `pending` is the number of distinct content
/// addresses in the working tree that have not been indexed yet: while it is
/// non-zero the results are a subset, and a caller that renders them as "no
/// matches" is lying to its user. This is the same distinction the trash listing
/// draws between "nothing deleted" and "not collecting" — only one of those is a
/// configuration answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchStatus {
    /// Distinct content addresses indexed for this workspace.
    pub indexed: i64,
    /// Distinct content addresses in the working tree still waiting to be indexed.
    pub pending: i64,
}

impl SearchStatus {
    /// Whether every indexable blob in the working tree has been indexed.
    pub fn complete(&self) -> bool {
        self.pending == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_lowercased_deduplicated_and_sorted() {
        assert_eq!(
            tokenize("Hello hello WORLD world"),
            vec!["hello".to_string(), "world".to_string()]
        );
    }

    #[test]
    fn punctuation_and_code_split_into_identifiers() {
        assert_eq!(
            tokenize("fn ensure_may_read_at(&self) -> Result<()>"),
            vec![
                "ensure".to_string(),
                "may".to_string(),
                "read".to_string(),
                "result".to_string(),
                "self".to_string(),
            ],
            "`at` and `fn` are below MIN_TERM_LEN; `_` is a separator, not a letter"
        );
    }

    #[test]
    fn short_terms_are_dropped() {
        assert!(tokenize("a an to of").is_empty());
    }

    #[test]
    fn a_long_term_is_truncated_on_a_character_boundary_not_dropped() {
        // A long identifier stays findable by its prefix rather than vanishing.
        let long = "a".repeat(MAX_TERM_LEN + 40);
        assert_eq!(tokenize(&long), vec!["a".repeat(MAX_TERM_LEN)]);

        // The bound is in bytes and the truncation must not split a character.
        let multi = "é".repeat(MAX_TERM_LEN);
        let out = tokenize(&multi);
        assert_eq!(out.len(), 1);
        assert!(out[0].len() <= MAX_TERM_LEN);
        assert!(std::str::from_utf8(out[0].as_bytes()).is_ok());
    }

    #[test]
    fn the_term_count_is_bounded() {
        let many: String = (0..MAX_TERMS_PER_BLOB + 500)
            .map(|i| format!("term{i} "))
            .collect();
        assert_eq!(tokenize(&many).len(), MAX_TERMS_PER_BLOB);
    }

    #[test]
    fn a_query_tokenizes_exactly_like_a_document() {
        // The property that makes a match possible at all: if these two could
        // disagree, a query term might be one no document could ever hold.
        let doc = "The Quick, brown_fox!";
        assert_eq!(query_terms(doc), tokenize(doc));
    }

    #[test]
    fn an_unsearchable_query_yields_no_terms_rather_than_matching_everything() {
        assert!(query_terms("a  ?? -").is_empty());
    }

    #[test]
    fn utf8_text_is_extracted_and_binary_is_refused() {
        assert_eq!(extract_text(b"hello world"), Some("hello world"));
        // A NUL in the sniff window is the grep/git heuristic for "binary".
        assert_eq!(extract_text(b"PK\x03\x04\x00\x00mostly text"), None);
        // Invalid UTF-8 is refused rather than lossily converted: guessing an
        // encoding would put mojibake in the index.
        assert_eq!(extract_text(&[0xff, 0xfe, 0x41]), None);
    }

    #[test]
    fn a_nul_beyond_the_sniff_window_does_not_make_a_file_binary() {
        // Sniffing is bounded, so this is a real (accepted) limit rather than an
        // oversight — assert the boundary so a change to it is deliberate.
        let mut v = vec![b'a'; BINARY_SNIFF_BYTES];
        v.push(0);
        // Still not indexed, because it is not valid UTF-8-with-no-NUL text? A
        // NUL *is* valid UTF-8, so this one survives the sniff and is returned.
        assert!(extract_text(&v).is_some());
    }

    #[test]
    fn an_empty_body_is_text_with_no_terms() {
        assert_eq!(extract_text(b""), Some(""));
        assert!(tokenize("").is_empty());
    }

    #[test]
    fn status_reports_incompleteness() {
        assert!(
            SearchStatus {
                indexed: 3,
                pending: 0
            }
            .complete()
        );
        assert!(
            !SearchStatus {
                indexed: 3,
                pending: 1
            }
            .complete()
        );
    }
}

use crate::content::ContentStore;
use crate::engine::{Fs, is_internal_path};
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::types::Ino;

/// What one call to [`Fs::index_pending`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexReport {
    /// Blobs read and tokenized.
    pub indexed: u64,
    /// Of those, ones that held no indexable text (binary, or no term long
    /// enough to keep). Recorded rather than skipped, so they are not re-read on
    /// every future sweep.
    pub skipped_binary: u64,
    /// Terms written across all of them.
    pub terms: u64,
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Index up to `limit` not-yet-indexed blobs from the working tree.
    ///
    /// The unit of work is a **content address**, so this is idempotent and
    /// resumable by construction: call it in a loop until it reports nothing,
    /// from a cron, or from a background task, and it converges. Nothing here
    /// consumes the change feed — see this module's header for why the queue is
    /// a set difference instead.
    ///
    /// A blob that cannot be read is left unindexed rather than marked, so a
    /// transient backend failure retries on the next sweep. A blob that reads
    /// fine but holds no text *is* marked, with zero terms, because re-reading
    /// it forever is the failure this distinction exists to prevent.
    pub async fn index_pending(&self, limit: i64) -> Result<IndexReport> {
        let pending = self.meta.unindexed_blobs(self.root_ino, limit).await?;
        let mut report = IndexReport::default();
        let now = self.now_secs();
        for (hash, size) in pending {
            let bytes = match self.read_blob_bytes(&hash).await {
                Ok(b) => b,
                Err(e) => {
                    // Transient or not, leaving it unindexed is the safe answer:
                    // the queue will offer it again.
                    tracing::warn!(hash = %hash.to_hex(), error = %e, "search: blob unreadable, leaving unindexed");
                    continue;
                }
            };
            let terms = match extract_text(&bytes) {
                Some(text) => tokenize(text),
                None => Vec::new(),
            };
            if terms.is_empty() {
                report.skipped_binary += 1;
            }
            report.terms += terms.len() as u64;
            report.indexed += 1;
            self.meta.index_blob(&hash, size, &terms, now).await?;
        }
        Ok(report)
    }

    /// Index everything outstanding, in batches, until nothing is left.
    ///
    /// Batched rather than one query so a large workspace does not build one
    /// enormous result set, and so progress is durable as it goes: an
    /// interrupted run leaves everything it finished indexed.
    pub async fn reindex(&self) -> Result<IndexReport> {
        const BATCH: i64 = 256;
        let mut total = IndexReport::default();
        loop {
            let r = self.index_pending(BATCH).await?;
            if r.indexed == 0 {
                return Ok(total);
            }
            total.indexed += r.indexed;
            total.skipped_binary += r.skipped_binary;
            total.terms += r.terms;
        }
    }

    /// How much of the working tree is indexed.
    pub async fn search_status(&self) -> Result<SearchStatus> {
        let (indexed, pending) = self.meta.index_status(self.root_ino).await?;
        Ok(SearchStatus { indexed, pending })
    }

    /// Paths whose content matches every term in `query`.
    ///
    /// **Unattributed**, like every other bare read on `Fs` — the machinery uses
    /// it and it performs no ACL check. `search_as` is the entry point a surface
    /// must call.
    ///
    /// Hits are resolved to paths against the **live** working tree, which is
    /// what makes a deleted or renamed file correct without the index knowing
    /// anything about it: the walk simply fails or produces the new path. It is
    /// also the workspace boundary — a hit whose walk does not reach this
    /// workspace's root belongs to another tenant and is dropped.
    pub async fn search(&self, query: &str, limit: i64) -> Result<Vec<SearchHit>> {
        let terms = query_terms(query);
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        // Over-fetch: rows are dropped by the path walk (another workspace, a
        // race with an unlink, an internal path), so asking for exactly `limit`
        // would under-fill a page that had every right to be full.
        let rows = self
            .meta
            .search_blobs(&terms, limit.saturating_mul(4).max(limit))
            .await?;
        let mut out = Vec::new();
        for (ino, content) in rows {
            if out.len() as i64 >= limit {
                break;
            }
            let Some(path) = self.path_of(ino).await? else {
                continue;
            };
            // origofs's own state is not user content. The co-edit CRDT sidecars
            // live in the working tree so they version and replicate like
            // anything else, which means code written for user files reaches
            // them — and these carry `(actor, session)` stamps and node ids, so
            // indexing them would make internal identifiers searchable.
            if is_internal_path(&path) {
                continue;
            }
            out.push(SearchHit { path, content, ino });
        }
        Ok(out)
    }

    /// The absolute path `ino` is at now, or `None` if it is not reachable from
    /// this workspace's root.
    ///
    /// One indexed lookup per level, up the `dentry(ino)` index — the same walk
    /// `rename` uses for its containment check. The `None` cases are all real:
    /// an inode unlinked between the index read and this walk, and an inode that
    /// belongs to a different workspace's subtree.
    async fn path_of(&self, ino: Ino) -> Result<Option<String>> {
        if ino == self.root_ino {
            return Ok(Some("/".to_string()));
        }
        let mut names: Vec<String> = Vec::new();
        let mut cur = ino;
        // Bounded so a cycle — which the dentry graph should make impossible, but
        // which a corrupted store could still present — cannot hang a search.
        for _ in 0..MAX_PATH_DEPTH {
            let Some(parent) = self.meta.parent_of(cur).await? else {
                return Ok(None); // reached a root that is not ours
            };
            let Some(name) = self.meta.dentry_name(parent, cur).await? else {
                return Ok(None); // unlinked underneath us
            };
            names.push(name);
            if parent == self.root_ino {
                names.reverse();
                return Ok(Some(format!("/{}", names.join("/"))));
            }
            cur = parent;
        }
        Ok(None)
    }
}

/// How far up a path walk will go before giving up.
///
/// A guard against a cycle in the dentry graph, which the write path forbids but
/// a corrupted store could still present. Deep enough that no real tree reaches
/// it.
const MAX_PATH_DEPTH: usize = 256;
