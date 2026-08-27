//! `Fs::file_layout` and `Fs::bench` (issue #118) — the two things that answer
//! "why is this one file slow" and "what does this store actually do".
//!
//! These are *reporting* commands, so the failure mode they have to be protected
//! from is not a crash: it is a plausible-looking number that is wrong. Everything
//! here therefore pins a figure against something independently known — a
//! hand-built manifest whose chunk lengths the test chose, a file built out of two
//! copies of the same block, a store the test emptied on purpose — rather than
//! asserting that the report is merely self-consistent.

use origofs_core::chunk::{MAX_CHUNK, MIN_CHUNK};
use origofs_core::perf::{BenchOpts, histogram_bounds};
use origofs_core::{ContentStore, Fs, MemStore, OrigoFSError, SqliteMetadataStore};
use std::sync::Arc;

/// Deterministic, high-entropy bytes, so content-defined chunking finds real
/// boundaries rather than running to `MAX_CHUNK` every time.
fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

async fn mem_fs() -> (Fs<SqliteMetadataStore, Arc<MemStore>>, Arc<MemStore>) {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();
    (fs, store)
}

// ── info ────────────────────────────────────────────────────────────────────

/// The headline numbers must match the manifest the write actually produced —
/// chunk count, summed lengths, extremes, and the histogram — because every
/// downstream claim (`read fetches N objects`, "this file is MAX_CHUNK-dominated")
/// is read straight off them.
#[tokio::test]
async fn the_report_matches_the_manifest_the_write_produced() {
    let (fs, _store) = mem_fs().await;
    let data = pseudo_random(4 << 20, 0xC0FFEE);
    fs.write("/big.bin", &data).await.unwrap();

    let (manifest, _) = fs.open_for_range("/big.bin").await.unwrap();
    let manifest = manifest.unwrap();
    let info = fs.file_layout("/big.bin", true).await.unwrap();

    assert_eq!(info.size, data.len() as u64);
    assert_eq!(info.chunks, manifest.chunks.len() as u64);
    assert!(info.chunks > 1, "4 MiB must span many chunks");
    assert_eq!(
        info.manifest,
        fs.stat("/big.bin").await.unwrap().content,
        "the reported manifest address is the inode's content hash"
    );

    let lens: Vec<u32> = manifest.chunks.iter().map(|c| c.len).collect();
    assert_eq!(info.smallest, lens.iter().copied().min());
    assert_eq!(info.largest, lens.iter().copied().max());
    assert!(info.largest.unwrap() <= MAX_CHUNK);
    assert_eq!(
        info.histogram.iter().map(|(_, n)| n).sum::<u64>(),
        info.chunks,
        "the histogram counts every chunk reference exactly once"
    );
    assert_eq!(info.histogram.len(), histogram_bounds().len());
    assert_eq!(
        info.chunker,
        (MIN_CHUNK, origofs_core::AVG_CHUNK, MAX_CHUNK)
    );

    // No repetition in pseudo-random bytes, so self-dedup is exactly 1:1 and the
    // distinct bytes are the whole file.
    assert_eq!(info.distinct_chunks, info.chunks);
    assert_eq!(info.distinct_bytes, info.size);
    assert!((info.self_dedup() - 1.0).abs() < 1e-9);
}

/// Self-dedup has to be *measured*, not assumed to be 1. A file that is one block
/// repeated is the case the number exists for: every repeat resolves to a chunk
/// already in the manifest, so a read still fetches N objects but the store holds
/// far fewer than N distinct ones.
#[tokio::test]
async fn a_self_repeating_file_reports_fewer_distinct_chunks_than_refs() {
    let (fs, _store) = mem_fs().await;
    // Well over MAX_CHUNK so the block itself spans several chunks, repeated so
    // the chunker sees the identical byte sequence again at the same alignment.
    let block = pseudo_random(4 * MAX_CHUNK as usize, 7);
    let mut data = Vec::new();
    for _ in 0..8 {
        data.extend_from_slice(&block);
    }
    fs.write("/repeat.bin", &data).await.unwrap();

    let info = fs.file_layout("/repeat.bin", true).await.unwrap();
    assert_eq!(info.size, data.len() as u64);
    assert!(
        info.distinct_chunks < info.chunks,
        "8 copies of one block must share chunks: {} refs, {} distinct",
        info.chunks,
        info.distinct_chunks
    );
    assert!(
        info.distinct_bytes < info.size,
        "distinct bytes ({}) must be below the logical size ({})",
        info.distinct_bytes,
        info.size
    );
    assert!(
        info.self_dedup() > 1.5,
        "8x repetition should show up clearly, got {:.2}x",
        info.self_dedup()
    );
    // The residency probe counts *distinct* chunks, not references — otherwise a
    // repeating file would report more chunks present than it has.
    let r = info.residency.unwrap();
    assert_eq!(r.present, info.distinct_chunks);
    assert_eq!(r.present_bytes, info.distinct_bytes);
    assert_eq!(r.missing, 0);
}

/// Residency is the reason someone runs this on a file that will not read. Losing
/// the chunks must show up as missing, with addresses to go looking with — and
/// `--no-probe` must genuinely skip the probe rather than report a stale answer.
#[tokio::test]
async fn missing_chunks_are_reported_as_missing() {
    let (fs, store) = mem_fs().await;
    let data = pseudo_random(1 << 20, 99);
    fs.write("/gone.bin", &data).await.unwrap();

    let before = fs.file_layout("/gone.bin", true).await.unwrap();
    assert_eq!(before.residency.as_ref().unwrap().missing, 0);

    let (manifest, _) = fs.open_for_range("/gone.bin").await.unwrap();
    for c in &manifest.unwrap().chunks {
        store.delete(&c.hash).await.unwrap();
    }

    let after = fs.file_layout("/gone.bin", true).await.unwrap();
    let r = after.residency.unwrap();
    assert_eq!(r.present, 0);
    assert_eq!(r.missing, after.distinct_chunks);
    assert!(
        !r.missing_sample.is_empty(),
        "give the user something to grep"
    );
    assert!(
        r.missing_sample.len() <= r.missing as usize,
        "the sample is a subset of what is missing"
    );
    // The manifest itself is still readable, so the layout half of the report
    // survives the loss of the body — which is the whole reason it is worth
    // printing when a read is failing.
    assert_eq!(after.chunks, before.chunks);

    assert!(
        fs.file_layout("/gone.bin", false)
            .await
            .unwrap()
            .residency
            .is_none(),
        "--no-probe must report no residency, not a guessed one"
    );
}

/// An empty file has no body and therefore no manifest. The report must say that
/// rather than dividing by a zero chunk count.
#[tokio::test]
async fn an_empty_file_reports_no_manifest_and_no_chunks() {
    let (fs, _store) = mem_fs().await;
    fs.write("/empty", b"").await.unwrap();

    let info = fs.file_layout("/empty", true).await.unwrap();
    assert_eq!(info.size, 0);
    assert_eq!(info.chunks, 0);
    assert_eq!(info.distinct_chunks, 0);
    assert_eq!(info.smallest, None);
    assert_eq!(info.median, None);
    assert_eq!(info.mean(), None);
    assert_eq!(info.self_dedup(), 1.0, "no body is not infinite dedup");
}

/// `info` must fail exactly where a read would: the report is a diagnosis of the
/// read path, so a path the two disagree about is a bug in one of them.
#[tokio::test]
async fn info_refuses_what_a_read_refuses() {
    let (fs, _store) = mem_fs().await;
    fs.mkdir_p("/dir").await.unwrap();

    assert!(matches!(
        fs.file_layout("/nope", true).await,
        Err(OrigoFSError::NotFound(_))
    ));
    assert!(matches!(
        fs.file_layout("/dir", true).await,
        Err(OrigoFSError::IsADirectory(_))
    ));
}

// ── bench ───────────────────────────────────────────────────────────────────

/// Small enough to run in a unit test, large enough that each file still spans
/// several chunks — a single-chunk file would make the chunk-count assertions
/// vacuous.
fn small_bench() -> BenchOpts {
    BenchOpts {
        dir: "/bench".into(),
        files: 3,
        file_size: 4 * MAX_CHUNK as u64,
        ..BenchOpts::new()
    }
}

/// The smoke test, and everything it can honestly assert about the numbers: that
/// all three phases ran over every file, that they moved the bytes they claim, and
/// that the report's chunk count is the one the run actually produced rather than
/// a prediction from `AVG_CHUNK`.
#[tokio::test]
async fn bench_runs_and_reports_plausible_numbers() {
    let (fs, _store) = mem_fs().await;
    let opts = small_bench();
    let report = fs.bench(&opts).await.unwrap();

    let total = opts.file_size * opts.files as u64;
    assert_eq!(report.total_bytes, total);
    for stage in [&report.write, &report.read, &report.reread] {
        assert_eq!(stage.ops, opts.files);
        assert_eq!(stage.bytes, total);
        assert_eq!(stage.latencies.len(), opts.files);
        assert!(
            stage.latencies.windows(2).all(|w| w[0] <= w[1]),
            "latencies must be sorted, or every quantile is wrong"
        );
        assert!(stage.bytes_per_sec() > 0.0);
        assert!(stage.quantile(0.5) <= stage.quantile(1.0));
    }

    assert!(
        report.chunks >= opts.files as u64 * 2,
        "got {}",
        report.chunks
    );
    assert_eq!(
        report.distinct_chunks, report.chunks,
        "each file is seeded differently precisely so the run cannot dedup against \
         itself — if it does, the write throughput is inflated"
    );
    assert_eq!(
        report.chunker,
        (MIN_CHUNK, origofs_core::AVG_CHUNK, MAX_CHUNK)
    );
    assert!(!report.kept);
}

/// Cleanup is part of the contract: a benchmark that leaves its sample behind
/// silently grows the workspace every time someone measures anything.
#[tokio::test]
async fn bench_removes_its_own_files_unless_kept() {
    let (fs, _store) = mem_fs().await;
    fs.bench(&small_bench()).await.unwrap();
    assert!(
        matches!(fs.ls("/bench").await, Err(OrigoFSError::NotFound(_))),
        "the bench directory should be gone too"
    );

    let opts = BenchOpts {
        keep: true,
        ..small_bench()
    };
    let report = fs.bench(&opts).await.unwrap();
    assert!(report.kept);
    assert_eq!(fs.ls("/bench").await.unwrap().len(), opts.files);
}

/// The one destructive-surface guarantee: `bench` will not run in a directory
/// that already holds something unless it is told to, and a refusal writes
/// nothing.
#[tokio::test]
async fn bench_refuses_a_populated_directory_without_force() {
    let (fs, _store) = mem_fs().await;
    fs.mkdir_p("/bench").await.unwrap();
    fs.write("/bench/precious.txt", b"do not delete me")
        .await
        .unwrap();

    let opts = small_bench();
    let err = fs.bench(&opts).await.unwrap_err();
    assert!(
        matches!(err, OrigoFSError::InvalidArgument(ref m) if m.contains("force")),
        "the refusal must name the escape hatch, got: {err}"
    );
    assert_eq!(
        fs.ls("/bench").await.unwrap().len(),
        1,
        "a refusal must not have written a sample file first"
    );

    // With force it runs, and still only ever touches its own names.
    let forced = BenchOpts {
        force: true,
        ..opts
    };
    fs.bench(&forced).await.unwrap();
    assert_eq!(
        &fs.read("/bench/precious.txt").await.unwrap()[..],
        b"do not delete me",
        "force must not license deleting a file the benchmark did not create"
    );
}

/// Degenerate options are refused rather than reported as an instantaneous
/// zero-byte run, which would divide by zero on every derived figure.
#[tokio::test]
async fn bench_refuses_degenerate_options() {
    let (fs, _store) = mem_fs().await;
    for opts in [
        BenchOpts {
            files: 0,
            ..small_bench()
        },
        BenchOpts {
            file_size: 0,
            ..small_bench()
        },
    ] {
        assert!(matches!(
            fs.bench(&opts).await,
            Err(OrigoFSError::InvalidArgument(_))
        ));
    }
}

/// A run's own report is what someone pastes into an issue, so the settings it
/// echoes have to be the settings it ran under — including a pinned seed, which
/// is the only thing that makes two runs comparable.
#[tokio::test]
async fn bench_echoes_the_options_it_ran_under() {
    let (fs, _store) = mem_fs().await;
    let opts = BenchOpts {
        seed: 0xDEAD_BEEF,
        ..small_bench()
    };
    let report = fs.bench(&opts).await.unwrap();
    assert_eq!(report.opts.seed, 0xDEAD_BEEF);
    assert_eq!(report.opts.files, opts.files);
    assert_eq!(report.opts.file_size, opts.file_size);
    assert_eq!(report.opts.dir, opts.dir);
    // Unset in the test environment, and reported as unset rather than as a copy
    // of the engine's default that could drift away from it.
    assert_eq!(report.upload_concurrency.var, "ORIGOFS_UPLOAD_CONCURRENCY");
    assert_eq!(report.fetch_concurrency.var, "ORIGOFS_FETCH_CONCURRENCY");
}

/// A fresh default seed is what stops the second run on a store from measuring
/// deduplication and calling it write throughput.
#[tokio::test]
async fn consecutive_default_runs_write_genuinely_new_bytes() {
    let (fs, _store) = mem_fs().await;
    let first = fs.bench(&small_bench()).await.unwrap();
    let second = fs.bench(&small_bench()).await.unwrap();
    assert_ne!(
        first.opts.seed, second.opts.seed,
        "a fixed default seed would make the second run dedup entirely"
    );
    assert_eq!(second.distinct_chunks, second.chunks);
}
