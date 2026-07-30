//! Retrieval corpus: splitting a workspace into provenance-carrying **passages**
//! (`docs/DESIGN.md` — the RAG surface).
//!
//! This is the technology-agnostic half of retrieval-augmented generation. It
//! turns files into passages that each know *where they came from* — their path,
//! byte range, a content hash, and per-byte **authorship** ([`BlameRange`]). It
//! deliberately contains **no embeddings, no vector store, and no framework
//! types**: embedding a passage and indexing the vector are userland concerns, so
//! any stack (LlamaIndex, LangChain, Haystack, a hand-rolled pipeline) consumes
//! the same [`Passage`] records.
//!
//! Two origofs-native properties make this more than a text splitter:
//!
//! * **Blame-per-passage.** Each passage carries the authorship of exactly its
//!   bytes, so a retrieved passage can say *who* wrote it. Content with no
//!   recorded authorship (a plain [`Fs::write`], or a binary) simply blames to
//!   nothing rather than lying.
//! * **Content-addressed, edit-stable passages.** Every passage is keyed by the
//!   BLAKE3 [`Hash`] of its own bytes, so identical passages dedupe to one
//!   embedding and only genuinely-changed passages need re-embedding. With
//!   [`Segmentation::ContentDefined`] the boundaries themselves move only where
//!   the surrounding bytes change — an edit near the top of a file doesn't shift
//!   every later passage's hash the way fixed-size windows do — which is what
//!   makes incremental re-embedding actually cheap.
//!
//! Passages are extracted over the current working tree; note the passage `hash`
//! so a caller can memoize embeddings across runs.

use crate::attribution::BlameRange;
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::types::{FileKind, Hash};
use bytes::Bytes;

/// How a document's bytes are split into passages.
///
/// The choice is a real tradeoff for retrieval, not just cosmetics — see the note
/// on each variant. All sizes are in **bytes** (or lines) and describe the passage
/// units, independent of the storage chunker.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Segmentation {
    /// One passage per file. Best for short documents you retrieve whole.
    WholeFile,
    /// Fixed-size byte windows with `overlap` bytes shared between neighbours.
    /// Predictable, but **edit-brittle**: inserting bytes near the start of a file
    /// shifts every later window, so every passage hash changes and the whole file
    /// must be re-embedded. Prefer [`Self::ContentDefined`] when you index often.
    FixedBytes { size: usize, overlap: usize },
    /// Line-aligned windows: up to `max_lines` lines each, sharing `overlap` lines.
    /// Keeps passages human-meaningful for line-oriented text (code, logs, prose).
    Lines { max_lines: usize, overlap: usize },
    /// Content-defined boundaries (FastCDC over the text). A boundary sits at a
    /// position determined by the local bytes, so an edit only disturbs the passage
    /// it lands in — the rest keep their hashes across revisions. This is the
    /// edit-stable segmentation that makes incremental embedding pay off. Sizes are
    /// retrieval-tuned and clamped to FastCDC's valid range.
    ContentDefined { min: usize, avg: usize, max: usize },
}

impl Default for Segmentation {
    fn default() -> Self {
        Segmentation::content_defined()
    }
}

impl Segmentation {
    /// A retrieval-tuned content-defined default (~1 KiB passages, edit-stable).
    pub fn content_defined() -> Self {
        Segmentation::ContentDefined {
            min: 256,
            avg: 1024,
            max: 4096,
        }
    }

    /// The `[start, end)` byte spans this strategy produces over `bytes`. Never
    /// yields an empty span, and always covers the whole input in order.
    fn spans(&self, bytes: &[u8]) -> Vec<(usize, usize)> {
        let n = bytes.len();
        if n == 0 {
            return Vec::new();
        }
        match *self {
            Segmentation::WholeFile => vec![(0, n)],
            Segmentation::FixedBytes { size, overlap } => {
                let size = size.max(1);
                let step = size - overlap.min(size - 1);
                let mut out = Vec::new();
                let mut start = 0;
                while start < n {
                    let end = (start + size).min(n);
                    out.push((start, end));
                    if end == n {
                        break;
                    }
                    start += step;
                }
                out
            }
            Segmentation::Lines { max_lines, overlap } => {
                let lines = line_spans(bytes);
                if lines.is_empty() {
                    return Vec::new();
                }
                let max_lines = max_lines.max(1);
                let step = max_lines - overlap.min(max_lines - 1);
                let mut out = Vec::new();
                let mut i = 0;
                while i < lines.len() {
                    let end_line = (i + max_lines).min(lines.len());
                    out.push((lines[i].0, lines[end_line - 1].1));
                    if end_line == lines.len() {
                        break;
                    }
                    i += step;
                }
                out
            }
            Segmentation::ContentDefined { min, avg, max } => {
                // FastCDC panics on out-of-range parameters, so clamp to its valid
                // envelope (min ≥ 64, avg ≥ 256, max ≥ 1024, min ≤ avg ≤ max).
                let min = (min as u64).clamp(64, 67_108_864) as u32;
                let max = ((max as u64).max(min as u64 + 1)).clamp(1024, 1_073_741_824) as u32;
                let avg = (avg as u64).clamp((min.max(256)) as u64, max as u64) as u32;
                fastcdc::v2020::FastCDC::new(bytes, min, avg, max)
                    .map(|c| (c.offset, c.offset + c.length))
                    .collect()
            }
        }
    }
}

/// One provenance-carrying passage of a document.
#[derive(Clone, Debug)]
pub struct Passage {
    /// The file this passage came from (absolute workspace path).
    pub path: String,
    /// The passage's byte range within the file, `[byte_start, byte_end)`.
    pub byte_start: u64,
    pub byte_end: u64,
    /// BLAKE3 of the passage bytes — the dedup / incremental-embedding key. Two
    /// passages with the same bytes (across files or revisions) share a hash.
    pub hash: Hash,
    /// The passage bytes, present iff [`PassageOptions::with_text`] was set.
    pub text: Option<Bytes>,
    /// Authorship of this passage's bytes, each span clipped to the passage range.
    /// Precise for attributed text; empty when the content has no recorded blame
    /// (a plain write, or a binary). Populated iff [`PassageOptions::with_blame`].
    pub blame: Vec<BlameRange>,
}

/// What to extract, and how.
#[derive(Clone, Debug)]
pub struct PassageOptions {
    /// Restrict extraction to this subtree (a directory) or a single file.
    /// Defaults to the whole tree (`/`).
    pub root: String,
    /// If set, keep only files whose lowercased extension (no dot) is listed —
    /// e.g. `["md", "txt"]`. `None` keeps every regular file.
    pub exts: Option<Vec<String>>,
    /// How to split each file into passages.
    pub segmentation: Segmentation,
    /// Include the passage bytes in [`Passage::text`] (set false for a cheap
    /// "manifest" pass that only needs paths + hashes, e.g. to diff two revisions).
    pub with_text: bool,
    /// Compute per-passage [`Passage::blame`] (one extra blame read per file).
    pub with_blame: bool,
    /// Skip files larger than this many bytes (`0` = no limit). A guard so a stray
    /// large binary can't be pulled whole into memory.
    pub max_file_bytes: u64,
}

impl Default for PassageOptions {
    fn default() -> Self {
        Self {
            root: "/".to_string(),
            exts: None,
            segmentation: Segmentation::default(),
            with_text: true,
            with_blame: true,
            max_file_bytes: 0,
        }
    }
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Extract retrieval [`Passage`]s from the working tree under
    /// [`PassageOptions::root`]. Files are visited in sorted path order; symlinks
    /// are not followed. Each file is read once, split per
    /// [`PassageOptions::segmentation`], and (optionally) annotated with the
    /// authorship of each passage's exact bytes.
    ///
    /// This is a read-only projection built from `read` + `blame`; it stores
    /// nothing and knows nothing about embeddings.
    pub async fn passages(&self, opts: &PassageOptions) -> Result<Vec<Passage>> {
        let files = self.corpus_files(&opts.root).await?;
        let mut out = Vec::new();
        for path in files {
            if let Some(exts) = &opts.exts
                && !ext_matches(&path, exts)
            {
                continue;
            }
            let bytes = self.read(&path).await?;
            if opts.max_file_bytes > 0 && bytes.len() as u64 > opts.max_file_bytes {
                continue;
            }
            if bytes.is_empty() {
                continue;
            }
            // One blame read per file; each passage takes the slice overlapping it.
            let file_blame = if opts.with_blame {
                self.blame(&path).await.unwrap_or_default()
            } else {
                Vec::new()
            };
            for (s, e) in opts.segmentation.spans(&bytes) {
                let slice = &bytes[s..e];
                out.push(Passage {
                    path: path.clone(),
                    byte_start: s as u64,
                    byte_end: e as u64,
                    hash: Hash::of(slice),
                    text: opts.with_text.then(|| Bytes::copy_from_slice(slice)),
                    blame: if opts.with_blame {
                        clip_blame(&file_blame, s as u64, e as u64)
                    } else {
                        Vec::new()
                    },
                });
            }
        }
        Ok(out)
    }

    /// Sorted absolute paths of every regular file under `root` (a subtree, or a
    /// single file). Symlinks are skipped so extraction can't be steered off-tree.
    async fn corpus_files(&self, root: &str) -> Result<Vec<String>> {
        let root = normalize_dir(root);
        let st = self.stat(&root).await?;
        if st.kind == FileKind::File {
            return Ok(vec![root]);
        }
        let mut files = Vec::new();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            for e in self.ls(&dir).await? {
                let child = join_path(&dir, &e.name);
                match e.kind {
                    FileKind::Dir => stack.push(child),
                    FileKind::File => files.push(child),
                    FileKind::Symlink => {}
                }
            }
        }
        files.sort();
        Ok(files)
    }
}

/// Intersect each blame span with `[s, e)`, clipping byte ranges to the passage.
fn clip_blame(blame: &[BlameRange], s: u64, e: u64) -> Vec<BlameRange> {
    blame
        .iter()
        .filter_map(|b| {
            let bs = b.byte_start.max(s);
            let be = b.byte_end.min(e);
            (bs < be).then(|| BlameRange {
                line_start: b.line_start,
                line_end: b.line_end,
                byte_start: bs,
                byte_end: be,
                actor: b.actor.clone(),
                session: b.session,
            })
        })
        .collect()
}

/// Byte spans of each line, `\n`-terminated (the final line may be unterminated).
fn line_spans(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\n' {
            spans.push((start, i + 1));
            start = i + 1;
        }
    }
    if start < bytes.len() {
        spans.push((start, bytes.len()));
    }
    spans
}

/// Whether `path`'s lowercased extension (no dot) is in `exts`.
fn ext_matches(path: &str, exts: &[String]) -> bool {
    match path.rsplit_once('.') {
        Some((_, ext)) if !ext.contains('/') => {
            let ext = ext.to_ascii_lowercase();
            exts.iter()
                .any(|e| e.trim_start_matches('.').eq_ignore_ascii_case(&ext))
        }
        _ => false,
    }
}

fn normalize_dir(path: &str) -> String {
    let t = path.trim().trim_end_matches('/');
    if t.is_empty() {
        "/".to_string()
    } else if t.starts_with('/') {
        t.to_string()
    } else {
        format!("/{t}")
    }
}

fn join_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}
