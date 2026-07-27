//! Streaming reads over the chunk manifest (large files never fully resident),
//! and the removal of the fixed 8 GiB file-size ceiling. See `engine.rs`
//! (`read_stream`/`read_stream_owned`/`read_to_writer`) and `vfs.rs`.

use futures::StreamExt;
use origofs_core::{Fs, MemStore, OrigoFSError, SqliteMetadataStore};
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let fs = Fs::new(
        SqliteMetadataStore::open_in_memory().unwrap(),
        Arc::new(MemStore::new()),
    );
    fs.init().await.unwrap();
    fs
}

/// Deterministic pseudo-random bytes (xorshift64) so the content-defined chunker
/// produces several natural boundaries — a real multi-chunk file to stream.
fn pseudo_random(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

async fn collect_stream(
    fs: &Fs<SqliteMetadataStore, Arc<MemStore>>,
    path: &str,
) -> (Vec<u8>, usize) {
    let mut stream = fs.read_stream(path).await.unwrap();
    let mut bytes = Vec::new();
    let mut chunks = 0usize;
    while let Some(item) = stream.next().await {
        bytes.extend_from_slice(&item.unwrap());
        chunks += 1;
    }
    (bytes, chunks)
}

// read_stream reassembles exactly what read() returns, and yields the body as
// several chunks (proving it streams rather than buffering one blob).
#[tokio::test]
async fn read_stream_matches_read_and_is_chunked() {
    let fs = fixture().await;
    let content = pseudo_random(1024 * 1024); // 1 MiB -> many CDC chunks
    fs.write("/big.bin", &content).await.unwrap();

    let whole = fs.read("/big.bin").await.unwrap();
    assert_eq!(&whole[..], &content[..], "buffered read is intact");

    let (streamed, chunk_count) = collect_stream(&fs, "/big.bin").await;
    assert_eq!(streamed, content, "streamed body equals the buffered read");
    assert!(
        chunk_count >= 2,
        "expected multiple chunks, got {chunk_count}"
    );
}

// The owned stream ('static, moveable into a task/response body) yields the same
// bytes as the borrowed one.
#[tokio::test]
async fn read_stream_owned_is_equivalent_and_static() {
    let fs = fixture().await;
    let content = pseudo_random(300 * 1024); // > MAX_CHUNK, so at least two chunks
    fs.write("/o.bin", &content).await.unwrap();

    // Move the stream out of the borrow entirely before consuming it.
    let stream = fs.read_stream_owned("/o.bin").await.unwrap();
    let collected: Vec<u8> = stream
        .map(|r| r.unwrap())
        .fold(Vec::new(), |mut acc, b| async move {
            acc.extend_from_slice(&b);
            acc
        })
        .await;
    assert_eq!(collected, content);
}

// read_to_writer pumps the whole body into an async sink without materializing
// it, returning the byte count.
#[tokio::test]
async fn read_to_writer_copies_the_whole_body() {
    let fs = fixture().await;
    let content = pseudo_random(512 * 1024);
    fs.write("/w.bin", &content).await.unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    let written = {
        let file = tokio::fs::File::create(&path).await.unwrap();
        fs.read_to_writer("/w.bin", file).await.unwrap()
    };
    assert_eq!(written, content.len() as u64);
    assert_eq!(tokio::fs::read(&path).await.unwrap(), content);
}

// An empty file streams as zero chunks (not one empty chunk).
#[tokio::test]
async fn empty_file_streams_nothing() {
    let fs = fixture().await;
    fs.write("/empty", b"").await.unwrap();
    let (bytes, chunks) = collect_stream(&fs, "/empty").await;
    assert!(bytes.is_empty() && chunks == 0);
}

// read_stream validates the path up front: a directory, a missing file, or a
// symlink errors before any bytes stream (so an HTTP surface can still 4xx).
#[tokio::test]
async fn read_stream_rejects_non_files() {
    let fs = fixture().await;
    fs.mkdir_p("/d").await.unwrap();
    assert!(matches!(
        fs.read_stream("/d").await.err(),
        Some(OrigoFSError::IsADirectory(_))
    ));
    assert!(matches!(
        fs.read_stream("/missing").await.err(),
        Some(OrigoFSError::NotFound(_))
    ));
}

// A small file is a single chunk and still round-trips through the stream.
#[tokio::test]
async fn tiny_file_single_chunk_roundtrips() {
    let fs = fixture().await;
    fs.write("/hi.txt", b"hello world\n").await.unwrap();
    let (bytes, chunks) = collect_stream(&fs, "/hi.txt").await;
    assert_eq!(bytes, b"hello world\n");
    assert_eq!(chunks, 1);
}
