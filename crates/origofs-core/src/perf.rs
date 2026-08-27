//! Performance introspection: a per-file layout report and an end-to-end
//! benchmark (issue #118).
//!
//! Everything else in this crate is emit-only observability — `tracing` spans and
//! `metrics` observations that a *running* daemon exports. That leaves two
//! questions a person at a shell cannot answer at all:
//!
//! * **"why is this one file slow?"** — [`Fs::file_layout`]. A file's cost is its
//!   chunk layout: how many chunks a read has to fetch, how big they are, and
//!   whether the store still holds them. All of that is derivable from the
//!   manifest plus one `has` per chunk, and none of it was reachable.
//! * **"what does this bucket actually do?"** — [`Fs::bench`]. `benches/engine.rs`
//!   measures the engine against a local temp directory, which is the right thing
//!   for catching a regression in the chunker and says nothing about a user's own
//!   store at their own latency. The numbers that matter for tuning
//!   `ORIGOFS_UPLOAD_CONCURRENCY` / `ORIGOFS_FETCH_CONCURRENCY`, or for deciding
//!   whether a [`PackStore`](crate::PackStore) is worth it, can only be produced
//!   against the store in question.
//!
//! # Honesty is the feature
//!
//! Both reports exist so a change to the write path or the cache tier can be
//! argued about with a measurement instead of an intuition, which makes an
//! *overstated* number worse than no number. So each figure here is bounded by
//! what the engine can actually observe, and the types say which is which:
//!
//! * "dedup" is **self**-dedup — repeated content *within one file*. Whether a
//!   chunk is also shared with some other file cannot be known without scanning
//!   the whole store, so [`FileLayout`] does not claim it. See
//!   [`FileLayout::distinct_bytes`].
//! * "residency" is **store presence** ([`ContentStore::has`]), not cache
//!   residency. On a [`TieredStore`](crate::TieredStore) `has` is true when
//!   *either* tier holds the chunk, and the object-safe trait exposes no way to
//!   ask which — so [`Residency`] reports presence and says so rather than
//!   labelling a remote hit a cache hit.
//! * the benchmark's two read passes are "first" and "repeat", not "cold" and
//!   "warm": nothing here can drop a page cache or a cache tier, so the honest
//!   claim is only that the second pass runs against whatever the first one
//!   warmed. See [`BenchReport::reread`].
//! * the tuning knobs are reported as [`Tunable`]s that are `None` when unset,
//!   rather than as a duplicated copy of the engine's default — a duplicate would
//!   go on printing `16` the day the engine stopped using it.

use crate::chunk::{AVG_CHUNK, MAX_CHUNK, MIN_CHUNK, Manifest};
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::types::Hash;
use futures::StreamExt;
use std::collections::HashSet;
use std::time::{Duration, Instant};

// ── info ────────────────────────────────────────────────────────────────────

/// How many `has` probes [`Fs::file_layout`] keeps in flight.
///
/// One probe is one HEAD against the content backend, so a large file's report is
/// latency-bound in exactly the way a read of it would be, and the same bounded
/// window is the fix — a sequential probe loop over a 1 GiB file's ~13,700 chunks
/// at a 30 ms round trip is seven minutes of nothing.
///
/// Fixed rather than tied to `ORIGOFS_FETCH_CONCURRENCY`: that knob budgets
/// `MAX_CHUNK`-sized *bodies* held in memory, and a probe carries no body, so the
/// two have no reason to move together.
const PROBE_CONCURRENCY: usize = 32;

/// How many missing chunk addresses [`Residency`] keeps.
///
/// A file whose chunks are gone is usually *entirely* gone, so the interesting
/// output is the count plus enough addresses to go looking with — not a hundred
/// thousand hex strings scrolling past.
const MISSING_SAMPLE: usize = 8;

/// The chunk-size histogram's bucket upper bounds, in bytes (inclusive).
///
/// Derived from the chunker's own parameters rather than picked as round numbers,
/// so the buckets keep meaning something if `MIN_CHUNK`/`AVG_CHUNK`/`MAX_CHUNK`
/// are ever retuned: the interesting question is always "how far from the target
/// did FastCDC land", and that only reads as an answer when the target is a
/// bucket edge. Sorted and deduplicated so the bounds stay monotone whatever
/// relationship the three constants end up in.
pub fn histogram_bounds() -> Vec<u32> {
    let mut bounds = vec![
        MIN_CHUNK,
        AVG_CHUNK / 2,
        AVG_CHUNK,
        AVG_CHUNK.saturating_mul(2).min(MAX_CHUNK),
        MAX_CHUNK,
    ];
    bounds.sort_unstable();
    bounds.dedup();
    bounds
}

/// Which of a file's chunks the content store still holds.
///
/// **Presence, not cache residency.** [`ContentStore::has`] answers "can this
/// store serve this chunk", which is what tells a missing-content story apart from
/// a slow one. On a [`TieredStore`](crate::TieredStore) it is true when either
/// tier holds the chunk, and nothing on the object-safe trait distinguishes them,
/// so this deliberately does not claim to report a cache hit rate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Residency {
    /// Distinct chunks the store answered `has` for.
    pub present: u64,
    /// Their summed length, from the manifest.
    pub present_bytes: u64,
    /// Distinct chunks the store does not hold — a file that cannot be read.
    pub missing: u64,
    /// Up to [`MISSING_SAMPLE`] of the missing addresses, to go looking with.
    pub missing_sample: Vec<Hash>,
}

/// What a file costs to read: its chunk layout, size distribution, self-dedup, and
/// (optionally) whether the store still holds it. Produced by
/// [`Fs::file_layout`].
#[derive(Clone, Debug)]
pub struct FileLayout {
    /// Logical size in bytes — what `stat` reports.
    pub size: u64,
    /// Address of the manifest ("blob") object. `None` for an empty file, which
    /// has no body and therefore no manifest.
    pub manifest: Option<Hash>,
    /// Chunk references in the manifest. **This is the read-amplification
    /// number**: a whole-file read fetches exactly this many objects.
    pub chunks: u64,
    /// Distinct chunk addresses among them. Lower than [`chunks`](Self::chunks)
    /// only when the file repeats content within itself.
    pub distinct_chunks: u64,
    /// Summed length of the distinct chunks — what this file would occupy in an
    /// otherwise-empty store.
    ///
    /// **Not** its footprint in *this* store. Chunks are shared across every file
    /// in the workspace, and finding out how much of this file's content some
    /// other file also references would mean reading every other manifest. So
    /// `size - distinct_bytes` is the saving from repetition *inside this file*
    /// and is a lower bound on the real one; the cross-file share is strictly
    /// larger and is not measured here.
    pub distinct_bytes: u64,
    /// Shortest chunk, in bytes. `None` for an empty file.
    ///
    /// Can legitimately fall below `MIN_CHUNK`: the final chunk of a file is
    /// whatever is left over, and a file smaller than `MIN_CHUNK` is one short
    /// chunk.
    pub smallest: Option<u32>,
    /// Longest chunk, in bytes. `None` for an empty file. Never above
    /// `MAX_CHUNK`.
    pub largest: Option<u32>,
    /// Median chunk length. `None` for an empty file. The median rather than the
    /// mean because FastCDC's length distribution has a long tail at `MAX_CHUNK`
    /// (every cut the rolling hash failed to find lands there), and the mean
    /// reports that tail as if it were the typical case.
    pub median: Option<u32>,
    /// `(upper_bound_inclusive, count)` over all chunk references, using
    /// [`histogram_bounds`]. Counts every reference, not every distinct chunk —
    /// the question is what a read pulls.
    pub histogram: Vec<(u32, u64)>,
    /// Store presence, when it was probed. `None` when the caller skipped it —
    /// see [`Fs::file_layout`].
    pub residency: Option<Residency>,
    /// The chunker parameters this build uses, so a report carries the settings it
    /// was produced under rather than leaving the reader to guess.
    pub chunker: (u32, u32, u32),
}

impl FileLayout {
    /// Logical bytes per distinct stored byte — the **self**-dedup factor. `1.0`
    /// for a file with no internal repetition, which is the overwhelmingly common
    /// case. See [`distinct_bytes`](Self::distinct_bytes) for what it excludes.
    pub fn self_dedup(&self) -> f64 {
        if self.distinct_bytes == 0 {
            return 1.0;
        }
        self.size as f64 / self.distinct_bytes as f64
    }

    /// Mean chunk length, or `None` for an empty file. Read it next to
    /// [`median`](Self::median), never instead of it.
    pub fn mean(&self) -> Option<u64> {
        (self.chunks > 0).then(|| self.size / self.chunks)
    }
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Report what `path` costs to read: chunk count, size distribution,
    /// self-dedup, and optionally whether the store still holds the chunks
    /// (issue #118).
    ///
    /// Errors the way a read would — [`NotFound`](OrigoFSError::NotFound) for a
    /// missing path, [`IsADirectory`](OrigoFSError::IsADirectory) for a directory
    /// — so `info` and `read` disagree about a path only when the read path itself
    /// is broken.
    ///
    /// `probe_residency` costs **one `has` per distinct chunk**, which against
    /// object storage is one HEAD each (bounded by [`PROBE_CONCURRENCY`], so it is
    /// latency-parallel rather than latency-serial). That is the one part of this
    /// report that touches the network at all, so it is a parameter and not
    /// unconditional: everything else is derived from the manifest, which a read
    /// would have fetched anyway.
    pub async fn file_layout(&self, path: &str, probe_residency: bool) -> Result<FileLayout> {
        // `open_for_range` first, so a directory or a symlink produces the same
        // diagnosis here that it would on a read rather than a `stat`-shaped one.
        let (manifest, size) = self.open_for_range(path).await?;
        let manifest_hash = self.stat(path).await?.content;
        let manifest = manifest.unwrap_or_default();

        let layout = summarize(&manifest, manifest_hash, size);
        if !probe_residency {
            return Ok(layout);
        }
        let distinct = distinct_chunks(&manifest);
        Ok(FileLayout {
            residency: Some(self.probe_residency(&distinct).await?),
            ..layout
        })
    }

    /// `has` every entry of `distinct`, with a bounded window.
    ///
    /// A probe error is **not** swallowed into "absent": a backend that is
    /// unreachable would otherwise report every chunk of a perfectly healthy file
    /// as missing, which is precisely the wrong answer to give someone who came
    /// here because something looked broken.
    async fn probe_residency(&self, distinct: &[(Hash, u32)]) -> Result<Residency> {
        let probes =
            futures::stream::iter(distinct.iter().copied())
                .map(|(hash, len)| async move {
                    self.content.has(&hash).await.map(|ok| (hash, len, ok))
                })
                .buffer_unordered(PROBE_CONCURRENCY)
                .collect::<Vec<_>>()
                .await;

        let mut r = Residency {
            present: 0,
            present_bytes: 0,
            missing: 0,
            missing_sample: Vec::new(),
        };
        for probe in probes {
            let (hash, len, present) = probe?;
            if present {
                r.present += 1;
                r.present_bytes += u64::from(len);
            } else {
                r.missing += 1;
                if r.missing_sample.len() < MISSING_SAMPLE {
                    r.missing_sample.push(hash);
                }
            }
        }
        Ok(r)
    }
}

/// The distinct `(address, length)` pairs of a manifest, in first-seen order.
///
/// Length is a function of the address (both come from the same bytes), so the
/// first sighting's length is the right one for every later repeat.
fn distinct_chunks(manifest: &Manifest) -> Vec<(Hash, u32)> {
    let mut seen = HashSet::with_capacity(manifest.chunks.len());
    manifest
        .chunks
        .iter()
        .filter(|c| seen.insert(c.hash))
        .map(|c| (c.hash, c.len))
        .collect()
}

/// The manifest-only half of [`Fs::file_layout`] — everything that needs no I/O.
fn summarize(manifest: &Manifest, manifest_hash: Option<Hash>, size: u64) -> FileLayout {
    let distinct = distinct_chunks(manifest);
    let bounds = histogram_bounds();
    let mut histogram: Vec<(u32, u64)> = bounds.iter().map(|b| (*b, 0)).collect();
    for c in &manifest.chunks {
        // A chunk longer than the last bound cannot happen (the chunker caps at
        // `MAX_CHUNK`), but a corrupt manifest is untrusted input like any other,
        // so it lands in the top bucket rather than off the end of the slice.
        let idx = bounds
            .iter()
            .position(|b| c.len <= *b)
            .unwrap_or(bounds.len() - 1);
        histogram[idx].1 += 1;
    }

    let mut lens: Vec<u32> = manifest.chunks.iter().map(|c| c.len).collect();
    lens.sort_unstable();

    FileLayout {
        size,
        manifest: manifest_hash,
        chunks: manifest.chunks.len() as u64,
        distinct_chunks: distinct.len() as u64,
        distinct_bytes: distinct.iter().map(|(_, len)| u64::from(*len)).sum(),
        smallest: lens.first().copied(),
        largest: lens.last().copied(),
        median: lens.get(lens.len() / 2).copied(),
        histogram,
        residency: None,
        chunker: (MIN_CHUNK, AVG_CHUNK, MAX_CHUNK),
    }
}

// ── bench ───────────────────────────────────────────────────────────────────

/// A tuning knob's value **as configured**, for a report to echo back.
///
/// `None` means the environment variable is unset and the engine's own default
/// applies. Deliberately not resolved to that default here: the engine's
/// accessors are module-private and cache their value at first use, so a copy of
/// the number would be a second source of truth that goes on printing the old
/// default the day the real one changes — in a report whose entire purpose is to
/// be trusted. Reading the variable, on the other hand, cannot drift: edition
/// 2024 makes `set_var` `unsafe` precisely because mutating a process's own
/// environment mid-run is not sound, so what is read here is what the engine
/// cached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tunable {
    /// The environment variable that sets it.
    pub var: &'static str,
    /// Its parsed value, or `None` when unset (or set to something unusable, which
    /// the engine also ignores).
    pub value: Option<usize>,
}

impl Tunable {
    fn read(var: &'static str) -> Self {
        Tunable {
            var,
            value: std::env::var(var)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|n| *n > 0),
        }
    }
}

/// A seed that differs from run to run.
///
/// **The default has to be fresh, not fixed.** A fixed seed regenerates the same
/// bodies every time, so every run after the first dedups against the store and
/// posts a write "throughput" that is really the speed of deciding not to upload —
/// the single easiest way for this feature to produce a confident, enormous, wrong
/// number. Fresh bytes make the write path do the work each time.
///
/// The cost is that a run is only reproducible if the seed is pinned explicitly,
/// which is why [`BenchOpts::seed`] exists and why a report echoes it. Pinning it
/// deliberately (to compare two builds against identical bytes) reintroduces the
/// dedup on the second run — measure that against a fresh directory, or expect the
/// write figure to be about dedup.
fn fresh_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x0116_5E1F)
}

/// What [`Fs::bench`] should do. See [`BenchOpts::new`] for the defaults and why
/// they are what they are.
#[derive(Clone, Debug)]
pub struct BenchOpts {
    /// Absolute workspace directory to write the sample files into. Created if
    /// absent.
    pub dir: String,
    /// How many files to write and read back.
    pub files: usize,
    /// Bytes per file.
    pub file_size: u64,
    /// Seed for the generated bodies. Defaults to a fresh value per run; pin it to
    /// reproduce one, and read [`fresh_seed`] first.
    pub seed: u64,
    /// Leave the sample files in place instead of deleting them.
    pub keep: bool,
    /// Proceed even though `dir` already holds entries. See [`Fs::bench`].
    pub force: bool,
}

impl BenchOpts {
    /// 8 files of 8 MiB under `/.origofs-bench`.
    ///
    /// Sized to be over the interesting thresholds and under anyone's patience:
    /// 8 MiB is ~128 chunks at `AVG_CHUNK`, enough that per-chunk round trips
    /// dominate the per-file ones (which is the regime real workloads are in and
    /// the regime the concurrency knobs act on), and 64 MiB total finishes in
    /// seconds even on a slow link. Both are meant to be raised for a real
    /// measurement.
    pub fn new() -> Self {
        BenchOpts {
            dir: "/.origofs-bench".into(),
            files: 8,
            file_size: 8 << 20,
            seed: fresh_seed(),
            keep: false,
            force: false,
        }
    }
}

impl Default for BenchOpts {
    fn default() -> Self {
        Self::new()
    }
}

/// One measured phase of a [`BenchReport`].
#[derive(Clone, Debug, Default)]
pub struct BenchStage {
    /// Operations timed — one per file.
    pub ops: usize,
    /// Bytes they moved.
    pub bytes: u64,
    /// Summed time **inside** the engine call, not wall time across the phase.
    ///
    /// The two differ by the body generation between writes, which is this
    /// process's own CPU cost and has nothing to do with the store being
    /// measured; charging it to the store would understate a fast one.
    pub elapsed: Duration,
    /// Per-operation latencies, sorted ascending.
    pub latencies: Vec<Duration>,
}

impl BenchStage {
    /// Throughput in bytes per second, or `0.0` if nothing was measured.
    pub fn bytes_per_sec(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        self.bytes as f64 / secs
    }

    /// The `q`-quantile latency by nearest rank (`q` in `0.0..=1.0`).
    ///
    /// Nearest rank rather than interpolation because these samples are counted in
    /// files: at the default 8, interpolating between two of them invents a
    /// precision the sample size does not have. Read any quantile here next to
    /// [`ops`](Self::ops).
    pub fn quantile(&self, q: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let n = self.latencies.len();
        let rank = ((q.clamp(0.0, 1.0) * n as f64).ceil() as usize).clamp(1, n);
        self.latencies[rank - 1]
    }

    /// Mean per-operation latency.
    pub fn mean(&self) -> Duration {
        if self.ops == 0 {
            return Duration::ZERO;
        }
        self.elapsed / self.ops as u32
    }

    fn record(&mut self, bytes: u64, took: Duration) {
        self.ops += 1;
        self.bytes += bytes;
        self.elapsed += took;
        self.latencies.push(took);
    }

    fn finish(mut self) -> Self {
        self.latencies.sort_unstable();
        self
    }
}

/// The result of [`Fs::bench`] — an end-to-end number for *this* workspace,
/// together with the settings it was produced under.
#[derive(Clone, Debug)]
pub struct BenchReport {
    /// The options the run used, echoed so a report is self-describing.
    pub opts: BenchOpts,
    /// `files x file_size`.
    pub total_bytes: u64,
    /// The chunker parameters in effect: `(min, avg, max)`.
    pub chunker: (u32, u32, u32),
    /// `ORIGOFS_UPLOAD_CONCURRENCY`, as configured. See [`Tunable`].
    pub upload_concurrency: Tunable,
    /// `ORIGOFS_FETCH_CONCURRENCY`, as configured. See [`Tunable`].
    pub fetch_concurrency: Tunable,
    /// Chunk references the run actually produced, across all files — the number
    /// of store round trips a read of the sample costs, and the thing the
    /// concurrency knobs divide.
    pub chunks: u64,
    /// Distinct chunk addresses among them.
    ///
    /// Should equal [`chunks`](Self::chunks): each file is generated from its own
    /// derived seed precisely so the run cannot dedup against itself. If it is
    /// lower, the write figure counts uploads that were skipped and is
    /// **overstated** — which is why this is reported rather than assumed.
    pub distinct_chunks: u64,
    /// Writing the files.
    pub write: BenchStage,
    /// Reading them back, first pass.
    pub read: BenchStage,
    /// Reading them back again.
    ///
    /// Not labelled "warm" against a "cold" first pass, because nothing here can
    /// make a pass cold: the bytes were just written, so a cache tier already has
    /// them (writes are write-through) and so does the OS page cache on a local
    /// store. The honest claim is the narrow one — this pass ran second, against
    /// whatever the first one left warm.
    pub reread: BenchStage,
    /// Whether the sample files were left in place.
    pub kept: bool,
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Write, read, and re-read `opts.files` generated files, and report
    /// throughput and latency for each phase (issue #118).
    ///
    /// This is the measurement that cannot be borrowed from someone else's
    /// hardware: the Criterion benches in `benches/engine.rs` pin the engine's own
    /// cost against a local directory, and everything interesting about a real
    /// deployment — bucket latency, whether packing is on, what the concurrency
    /// windows are set to — is outside that. Running it before and after a change
    /// to the write path is the point.
    ///
    /// # It writes to the workspace
    ///
    /// Unavoidably: a benchmark against a store you do not use is a benchmark of a
    /// store you do not use. So the destructive surface is kept as small and as
    /// visible as it can be — the run touches only files it created, named
    /// `bench-NNNN.bin` under `opts.dir`, and deletes exactly those afterwards
    /// unless [`keep`](BenchOpts::keep). It **refuses to start** when `opts.dir`
    /// already holds anything, unless [`force`](BenchOpts::force) is set; with
    /// `force` it still only ever writes and deletes its own names, so an existing
    /// `bench-0000.bin` is the one file it can clobber.
    ///
    /// Cleanup unlinks the files but cannot reclaim their bytes: content is
    /// immutable and the garbage collector's grace period exists precisely so that
    /// freshly written chunks are not swept. A later `gc` reclaims them.
    ///
    /// # What it measures, and what it does not
    ///
    /// Files are written **one at a time**, so the figure isolates the engine's own
    /// chunk-level concurrency — what `ORIGOFS_UPLOAD_CONCURRENCY` and
    /// `ORIGOFS_FETCH_CONCURRENCY` tune — rather than however much parallelism the
    /// caller happened to supply. Writes are **unattributed**
    /// ([`write`](Fs::write), not `write_as`), so no `edit_op` or blame-index
    /// update is on the clock; an attributed write costs a little more than this
    /// says.
    ///
    /// Each file's bytes are generated in memory and dropped before the next, so
    /// peak memory is one `file_size`, not `files x file_size`.
    pub async fn bench(&self, opts: &BenchOpts) -> Result<BenchReport> {
        if opts.files == 0 {
            return Err(OrigoFSError::InvalidArgument(
                "benchmark needs at least one file".into(),
            ));
        }
        if opts.file_size == 0 {
            return Err(OrigoFSError::InvalidArgument(
                "benchmark needs a non-zero file size".into(),
            ));
        }
        let dir = opts.dir.trim_end_matches('/');
        if dir.is_empty() {
            // The run removes its own directory afterwards, and the workspace root
            // is not something to hand a `rmdir` — so this is refused up front
            // rather than left to fail confusingly during cleanup.
            return Err(OrigoFSError::InvalidArgument(
                "benchmark needs a subdirectory to run in, not the workspace root (it \
                 removes the directory it created afterwards)"
                    .into(),
            ));
        }
        let paths: Vec<String> = (0..opts.files)
            .map(|i| format!("{dir}/bench-{i:04}.bin"))
            .collect();

        // Check before writing anything, so a refusal leaves the workspace exactly
        // as it was rather than half-populated.
        match self.ls(dir).await {
            Ok(entries) if !entries.is_empty() && !opts.force => {
                return Err(OrigoFSError::InvalidArgument(format!(
                    "{dir} already holds {} entr(y/ies); pick an empty directory or pass \
                     force to run there anyway (the benchmark only ever writes and deletes \
                     its own bench-NNNN.bin files)",
                    entries.len()
                )));
            }
            Ok(_) => {}
            // Anything else — not found, or a file where a directory should be —
            // is `mkdir_p`'s to diagnose, and it gives a better message than a
            // re-worded one here would.
            Err(_) => {}
        }
        self.mkdir_p(dir).await?;

        let outcome = self.bench_phases(opts, &paths).await;
        // Clean up whatever the run got through before failing, too: a benchmark
        // that leaves gigabytes behind when the store goes away mid-run is worse
        // than the error that stopped it.
        if !opts.keep {
            for path in &paths {
                let _ = self.remove(path).await;
            }
            let _ = self.rmdir(dir).await;
        }
        outcome
    }

    /// The three timed phases. Split out so [`bench`](Self::bench) can clean up
    /// around it on both the success and the failure path.
    async fn bench_phases(&self, opts: &BenchOpts, paths: &[String]) -> Result<BenchReport> {
        let mut write = BenchStage::default();
        let mut digests = Vec::with_capacity(paths.len());
        for (i, path) in paths.iter().enumerate() {
            // Each file gets its own derived seed. Identical bodies would dedup
            // against each other and every file after the first would cost one
            // manifest write, turning the headline throughput into a measurement
            // of the dedup check.
            let body = generated(
                opts.file_size,
                opts.seed ^ (i as u64).wrapping_mul(0x9E37_79B9),
            );
            digests.push(Hash::of(&body));
            let t = Instant::now();
            self.write(path, &body).await?;
            write.record(opts.file_size, t.elapsed());
        }
        // Seal buffered writes *before* the read phases and outside their timers: a
        // `PackStore` holds the tail of a write in memory until it is flushed, so
        // reading straight through would measure a partly-in-process store and, on
        // a crash, would have measured a durability the run never established.
        self.content.flush().await?;

        let read = self
            .bench_read_pass(paths, opts.file_size, &digests)
            .await?;
        let reread = self
            .bench_read_pass(paths, opts.file_size, &digests)
            .await?;

        // Count chunks from the manifests the run actually produced, rather than
        // predicting them from `AVG_CHUNK`: the prediction is what a reader would
        // do in their head, and the point of the report is to replace it.
        let (mut chunks, mut distinct) = (0u64, HashSet::new());
        for path in paths {
            let (manifest, _) = self.open_for_range(path).await?;
            let manifest = manifest.unwrap_or_default();
            chunks += manifest.chunks.len() as u64;
            distinct.extend(manifest.chunks.iter().map(|c| c.hash));
        }

        Ok(BenchReport {
            opts: opts.clone(),
            total_bytes: opts.file_size.saturating_mul(opts.files as u64),
            chunker: (MIN_CHUNK, AVG_CHUNK, MAX_CHUNK),
            upload_concurrency: Tunable::read("ORIGOFS_UPLOAD_CONCURRENCY"),
            fetch_concurrency: Tunable::read("ORIGOFS_FETCH_CONCURRENCY"),
            chunks,
            distinct_chunks: distinct.len() as u64,
            write: write.finish(),
            read,
            reread,
            kept: opts.keep,
        })
    }

    /// One read pass over the sample, verifying as it goes.
    ///
    /// The verification is off the clock but not optional: a read path that
    /// returned short or wrong bytes would otherwise post the best throughput in
    /// the report, and a benchmark that rewards a broken read is worse than none.
    async fn bench_read_pass(
        &self,
        paths: &[String],
        file_size: u64,
        digests: &[Hash],
    ) -> Result<BenchStage> {
        let mut stage = BenchStage::default();
        for (path, want) in paths.iter().zip(digests) {
            let t = Instant::now();
            let got = self.read(path).await?;
            stage.record(file_size, t.elapsed());
            if got.len() as u64 != file_size || Hash::of(&got) != *want {
                return Err(OrigoFSError::Corrupt(format!(
                    "benchmark read back {} bytes of {path} that do not match the {file_size} \
                     written — the read path is broken, so no timing from this run means \
                     anything",
                    got.len()
                )));
            }
        }
        Ok(stage.finish())
    }
}

/// `len` bytes of deterministic, effectively incompressible filler.
///
/// Incompressible on purpose. Zeroes (or any low-entropy pattern) give FastCDC a
/// rolling hash that almost never hits a cut point, so nearly every chunk lands at
/// `MAX_CHUNK` and the file produces a quarter of the objects a real one would —
/// which flatters exactly the per-chunk round-trip cost the benchmark exists to
/// expose. SplitMix64 rather than a real RNG so a `seed` reproduces a run
/// byte-for-byte, and because the generator must not be slow enough to show up
/// next to the store.
fn generated(len: u64, seed: u64) -> Vec<u8> {
    let mut out = vec![0u8; len as usize];
    let mut state = seed;
    for slot in out.chunks_mut(8) {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let bytes = z.to_le_bytes();
        slot.copy_from_slice(&bytes[..slot.len()]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_bounds_are_monotone_and_reach_the_chunker_ceiling() {
        let bounds = histogram_bounds();
        assert!(bounds.windows(2).all(|w| w[0] < w[1]), "{bounds:?}");
        assert_eq!(bounds.last(), Some(&MAX_CHUNK));
    }

    /// The filler has to be high-entropy or every measurement over it is a
    /// measurement of a file that chunks nothing like a real one — see
    /// [`generated`].
    #[test]
    fn generated_bodies_chunk_like_real_data() {
        let body = generated(4 << 20, 7);
        let cuts = crate::chunk::chunk_bounds(&body);
        assert!(
            cuts.len() > (body.len() / MAX_CHUNK as usize) * 2,
            "{} chunks over 4 MiB is MAX_CHUNK-dominated: the filler is compressible",
            cuts.len()
        );
    }

    #[test]
    fn generated_bodies_are_reproducible_and_seed_separated() {
        assert_eq!(generated(4096, 1), generated(4096, 1));
        assert_ne!(generated(4096, 1), generated(4096, 2));
    }

    #[test]
    fn quantiles_use_nearest_rank() {
        let mut stage = BenchStage::default();
        for ms in [40u64, 10, 30, 20] {
            stage.record(1, Duration::from_millis(ms));
        }
        let stage = stage.finish();
        assert_eq!(stage.quantile(0.0), Duration::from_millis(10));
        assert_eq!(stage.quantile(0.5), Duration::from_millis(20));
        assert_eq!(stage.quantile(1.0), Duration::from_millis(40));
        assert_eq!(stage.mean(), Duration::from_millis(25));
    }

    #[test]
    fn an_unset_tunable_is_reported_as_unset_rather_than_as_a_guess() {
        // The whole point of `Tunable`: no duplicated copy of the engine's default
        // that could go stale.
        let t = Tunable::read("ORIGOFS_DEFINITELY_NOT_SET_118");
        assert_eq!(t.value, None);
        assert_eq!(t.var, "ORIGOFS_DEFINITELY_NOT_SET_118");
    }
}
