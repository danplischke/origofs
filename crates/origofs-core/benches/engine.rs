//! Micro-benchmarks for the origofs hot paths: the content-defined-chunking write
//! path (FastCDC + BLAKE3 + manifest), whole-file reads, commit/tree building,
//! and the overhead encryption at rest adds (`docs/DESIGN.md` §7, M9 benchmarks).
//!
//! Everything runs over the in-memory store so the numbers reflect origofs's own CPU
//! cost (chunking, hashing, encoding), not disk or network.
//!
//! Run with `cargo bench -p origofs-core`.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use origofs_core::{ContentStore, EncryptedStore, Fs, MemStore, SqliteMetadataStore};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

/// A deterministic pseudo-random buffer (xorshift), so runs are comparable.
fn buffer(len: usize, seed: u64) -> Vec<u8> {
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

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn fresh_fs(rt: &tokio::runtime::Runtime) -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    rt.block_on(async {
        let fs = Fs::new(
            SqliteMetadataStore::open_in_memory().unwrap(),
            Arc::new(MemStore::new()),
        );
        fs.init().await.unwrap();
        fs
    })
}

const SIZES: &[usize] = &[64 * 1024, 1024 * 1024, 8 * 1024 * 1024];

fn bench_write(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("write");
    for &size in SIZES {
        let fs = fresh_fs(&rt);
        let data = buffer(size, size as u64);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| rt.block_on(async { fs.write("/f", black_box(data)).await.unwrap() }));
        });
    }
    group.finish();
}

fn bench_incremental_write(c: &mut Criterion) {
    // Re-write a large file with a one-byte change each time: FastCDC should keep
    // most chunks stable, so this measures the copy-on-write cost of a small edit.
    let rt = runtime();
    let size = 8 * 1024 * 1024;
    let fs = fresh_fs(&rt);
    let base = buffer(size, 42);
    rt.block_on(async { fs.write("/f", &base).await.unwrap() });

    let counter = Cell::new(0u8);
    let mut group = c.benchmark_group("incremental_write");
    group.throughput(Throughput::Bytes(size as u64));
    group.bench_function(BenchmarkId::from_parameter(size), |b| {
        b.iter(|| {
            let mut data = base.clone();
            counter.set(counter.get().wrapping_add(1));
            data[0] = counter.get(); // change only the first chunk
            rt.block_on(async { fs.write("/f", black_box(&data)).await.unwrap() });
        });
    });
    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("read");
    for &size in SIZES {
        let fs = fresh_fs(&rt);
        let data = buffer(size, size as u64);
        rt.block_on(async { fs.write("/f", &data).await.unwrap() });
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| rt.block_on(async { black_box(fs.read("/f").await.unwrap()) }));
        });
    }
    group.finish();
}

fn bench_commit(c: &mut Criterion) {
    // A repo with 200 small files; each iteration edits one file and commits,
    // measuring tree building + commit encoding over a realistic tree.
    let rt = runtime();
    let fs = fresh_fs(&rt);
    rt.block_on(async {
        fs.mkdir_p("/dir").await.unwrap();
        for i in 0..200 {
            fs.write(&format!("/dir/f{i}.txt"), format!("content {i}").as_bytes())
                .await
                .unwrap();
        }
        fs.commit("bench", "seed").await.unwrap();
    });

    let counter = Cell::new(0u64);
    c.bench_function("commit_small_change_over_200_files", |b| {
        b.iter(|| {
            let n = counter.get();
            counter.set(n + 1);
            rt.block_on(async {
                fs.write("/dir/f0.txt", format!("edit {n}").as_bytes())
                    .await
                    .unwrap();
                black_box(fs.commit("bench", &format!("c{n}")).await.unwrap())
            });
        });
    });
}

fn bench_encryption_overhead(c: &mut Criterion) {
    let rt = runtime();
    let size = 1024 * 1024;
    let data = buffer(size, 7);

    let plain = fresh_fs(&rt);
    let enc = rt.block_on(async {
        let backend: Arc<dyn ContentStore> = Arc::new(MemStore::new());
        let store = Arc::new(EncryptedStore::new(backend, [0x5a; 32]));
        let fs = Fs::new(SqliteMetadataStore::open_in_memory().unwrap(), store);
        fs.init().await.unwrap();
        fs
    });

    let mut group = c.benchmark_group("encryption_overhead");
    group.throughput(Throughput::Bytes(size as u64));
    group.bench_function("write_plain", |b| {
        b.iter(|| rt.block_on(async { plain.write("/f", black_box(&data)).await.unwrap() }));
    });
    group.bench_function("write_encrypted", |b| {
        b.iter(|| rt.block_on(async { enc.write("/f", black_box(&data)).await.unwrap() }));
    });
    rt.block_on(async {
        plain.write("/r", &data).await.unwrap();
        enc.write("/r", &data).await.unwrap();
    });
    group.bench_function("read_plain", |b| {
        b.iter(|| rt.block_on(async { black_box(plain.read("/r").await.unwrap()) }));
    });
    group.bench_function("read_encrypted", |b| {
        b.iter(|| rt.block_on(async { black_box(enc.read("/r").await.unwrap()) }));
    });
    group.finish();
}

fn config() -> Criterion {
    // Keep a full run to a couple of minutes while staying stable.
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(20)
}

criterion_group! {
    name = benches;
    config = config();
    targets = bench_write, bench_incremental_write, bench_read, bench_commit, bench_encryption_overhead
}
criterion_main!(benches);
