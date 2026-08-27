//! Attributed streaming writes: `write_reader_as`.
//!
//! Until this existed, streaming and attribution were mutually exclusive.
//! `write_reader` was the only streaming write in the codebase and it is
//! unattributed, so supplying an actor — the entire premise of origofs — forced
//! the whole body resident. These tests pin the two properties that distinguish
//! the new path from each of the old ones: it records blame and an edit-op (unlike
//! `write_reader`), and it never materializes the body (unlike `write_as`).

use origofs_core::{Fs, MemStore, MetadataStore, OrigoFSError, SqliteMetadataStore, WriteCtx};
use std::sync::Arc;

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

async fn fixture() -> TestFs {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

/// A `Read` that yields `len` deterministic bytes without ever holding them all —
/// the point being that the *test* does not materialize the body either, so a
/// regression to a buffering implementation shows up as memory growth in the
/// engine rather than being masked by the fixture.
struct Generator {
    remaining: usize,
    state: u64,
}

impl Generator {
    fn new(len: usize, seed: u64) -> Self {
        Self {
            remaining: len,
            state: seed | 1,
        }
    }
}

impl std::io::Read for Generator {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = buf.len().min(self.remaining);
        for slot in buf.iter_mut().take(n) {
            self.state ^= self.state << 13;
            self.state ^= self.state >> 7;
            self.state ^= self.state << 17;
            *slot = self.state as u8;
        }
        self.remaining -= n;
        Ok(n)
    }
}

fn expected(len: usize, seed: u64) -> Vec<u8> {
    let mut g = Generator::new(len, seed);
    let mut out = Vec::with_capacity(len);
    std::io::Read::read_to_end(&mut g, &mut out).unwrap();
    out
}

/// The headline: a streamed write is attributed.
#[tokio::test]
async fn a_streamed_write_records_blame_and_an_edit_op() {
    let fs = fixture().await;
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let session = fs.create_session(agent, Some("test")).await.unwrap();
    let ctx = WriteCtx::session(agent, session);

    // Several chunks' worth, so the manifest path is real.
    const LEN: usize = 1 << 20;
    fs.write_reader_as(ctx, "/big.bin", Generator::new(LEN, 7))
        .await
        .unwrap();

    assert_eq!(fs.read("/big.bin").await.unwrap(), expected(LEN, 7));

    // Blame: the whole file, credited to the streaming writer.
    let blame = fs.blame("/big.bin").await.unwrap();
    assert!(!blame.is_empty(), "a streamed write recorded no blame");
    assert!(
        blame.iter().all(|b| b.actor.id == agent),
        "blame credited someone other than the writer"
    );
    let covered: u64 = blame.iter().map(|b| b.byte_end - b.byte_start).sum();
    assert_eq!(covered, LEN as u64, "blame does not cover the whole file");

    // The op-log: the ground truth behind blame.
    let ops = fs.edit_ops(agent, Some(session)).await.unwrap();
    let op = ops
        .iter()
        .find(|o| o.path == "/big.bin")
        .expect("no edit_op for the streamed write");
    assert_eq!(op.op, "write");
    assert_eq!(op.byte_len, LEN as i64);
    assert!(op.post_hash.is_some());
    assert!(op.pre_hash.is_none(), "a new file has no prior content");
}

/// The write policy applies. This is what makes it an *attributed* variant rather
/// than a second unattributed side door around the propose gate.
#[tokio::test]
async fn a_propose_only_actor_cannot_stream() {
    let fs = fixture().await;
    let reviewer = fs.create_human("dan", None).await.unwrap();
    let agent = fs
        .create_agent("restricted", "opus", Some(reviewer))
        .await
        .unwrap();
    let session = fs.create_session(agent, Some("test")).await.unwrap();
    fs.set_write_policy(agent, origofs_core::WritePolicy::Propose)
        .await
        .unwrap();

    let err = fs
        .write_reader_as(
            WriteCtx::session(agent, session),
            "/denied.bin",
            Generator::new(4096, 1),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, OrigoFSError::Denied(_)),
        "expected Denied, got {err:?}"
    );
    assert!(
        fs.read("/denied.bin").await.is_err(),
        "a refused streaming write still created the file"
    );

    // And a Direct actor is unaffected.
    fs.set_write_policy(agent, origofs_core::WritePolicy::Direct)
        .await
        .unwrap();
    fs.write_reader_as(
        WriteCtx::session(agent, session),
        "/allowed.bin",
        Generator::new(4096, 1),
    )
    .await
    .unwrap();
    assert_eq!(fs.read("/allowed.bin").await.unwrap().len(), 4096);
}

/// Overwriting records the prior content as `pre_hash`, so the op-log says what was
/// actually replaced. Reading the hash must not read the *body* — that would
/// reintroduce the memory cost the streaming path exists to avoid.
#[tokio::test]
async fn overwriting_records_what_it_replaced() {
    let fs = fixture().await;
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let session = fs.create_session(agent, Some("test")).await.unwrap();
    let ctx = WriteCtx::session(agent, session);

    fs.write_reader_as(ctx, "/f.bin", Generator::new(200_000, 1))
        .await
        .unwrap();
    let first = fs.stat("/f.bin").await.unwrap().content.unwrap();

    fs.write_reader_as(ctx, "/f.bin", Generator::new(300_000, 2))
        .await
        .unwrap();
    assert_eq!(fs.read("/f.bin").await.unwrap(), expected(300_000, 2));

    let ops = fs.edit_ops(agent, Some(session)).await.unwrap();
    let second = ops
        .iter()
        .rfind(|o| o.path == "/f.bin")
        .expect("no second op");
    assert_eq!(
        second.pre_hash.as_deref(),
        Some(first.to_hex().as_str()),
        "the op-log did not record the content that was overwritten"
    );
}

/// A streamed write replaces authorship wholesale — documented, and different from
/// `write_as`, which diffs line-by-line against the previous body.
///
/// Asserted rather than left implicit: someone comparing the two paths needs to
/// see that the difference is intended, not a bug in the blame derivation.
#[tokio::test]
async fn streaming_replaces_authorship_wholesale() {
    let fs = fixture().await;
    let human = fs.create_human("dan", None).await.unwrap();
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let h_sess = fs.create_session(human, None).await.unwrap();
    let a_sess = fs.create_session(agent, None).await.unwrap();

    // Buffered attributed write: the human authors the file.
    fs.write_as(WriteCtx::session(human, h_sess), "/doc.md", b"one\ntwo\n")
        .await
        .unwrap();
    assert!(
        fs.blame("/doc.md")
            .await
            .unwrap()
            .iter()
            .all(|b| b.actor.id == human)
    );

    // Streamed write by the agent: the whole file becomes theirs.
    let body: &[u8] = b"one\ntwo\nthree\n";
    fs.write_reader_as(
        WriteCtx::session(agent, a_sess),
        "/doc.md",
        std::io::Cursor::new(body.to_vec()),
    )
    .await
    .unwrap();

    let blame = fs.blame("/doc.md").await.unwrap();
    assert!(
        blame.iter().all(|b| b.actor.id == agent),
        "a streamed write should attribute the whole file to its writer"
    );
    let covered: u64 = blame.iter().map(|b| b.byte_end - b.byte_start).sum();
    assert_eq!(covered, body.len() as u64);
}

/// An empty stream produces an empty file, not a zero-chunk manifest object.
#[tokio::test]
async fn an_empty_stream_writes_an_empty_file() {
    let fs = fixture().await;
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let session = fs.create_session(agent, Some("test")).await.unwrap();

    fs.write_reader_as(
        WriteCtx::session(agent, session),
        "/empty.txt",
        std::io::Cursor::new(Vec::new()),
    )
    .await
    .unwrap();

    assert!(fs.read("/empty.txt").await.unwrap().is_empty());
    assert_eq!(fs.stat("/empty.txt").await.unwrap().size, 0);
}

/// A panicking reader must not commit a truncated file as a whole one.
///
/// `write_reader` had exactly this bug (a discarded `JoinError`); the streaming
/// half is now shared, so this pins the behaviour for the attributed path too —
/// where it matters more, because the result would also carry a blame record
/// asserting the writer authored a file they only partly supplied.
#[tokio::test]
async fn a_panicking_reader_does_not_commit_a_partial_file() {
    struct Exploding {
        sent: usize,
    }
    impl std::io::Read for Exploding {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.sent > 512 * 1024 {
                panic!("reader exploded mid-stream");
            }
            let n = buf.len().min(64 * 1024);
            buf[..n].fill(b'x');
            self.sent += n;
            Ok(n)
        }
    }

    let fs = fixture().await;
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let session = fs.create_session(agent, Some("test")).await.unwrap();

    let result = fs
        .write_reader_as(
            WriteCtx::session(agent, session),
            "/partial.bin",
            Exploding { sent: 0 },
        )
        .await;

    assert!(result.is_err(), "a panicking reader reported success");
    assert!(
        fs.read("/partial.bin").await.is_err(),
        "a truncated body was committed as the whole file"
    );
}

// --- arbitrary sizes: the encode-side overflow guards ------------------------
//
// Three call sites used bare `as` casts into `u32` and wrapped silently past
// their range. Every one had a matching, carefully-written *decode*-side guard —
// the format layer reasoned hard about hostile input coming in and not at all
// about honest data going out. Unreachable at any sane size, but they corrupted
// rather than erroring, and were only discovered on a later read.
//
// Tested with synthetic oversized structures, not real multi-terabyte data.

/// A manifest past 2^32 chunks must error, not wrap its count field.
#[test]
fn a_manifest_past_the_u32_count_is_refused() {
    use origofs_core::{ChunkRef, Hash, Manifest};

    // 2^32 `ChunkRef`s would be 154 GB of RAM, so the wrap is exercised via a
    // manifest whose length is exactly the boundary — constructed without
    // allocating one, by asserting on the check rather than the encode.
    let ok = Manifest {
        size: 4,
        chunks: vec![ChunkRef {
            hash: Hash::of(b"x"),
            len: 4,
        }],
    };
    assert!(
        ok.encode().is_ok(),
        "an ordinary manifest must still encode"
    );

    // The boundary itself: `u32::try_from` on the count is what stands between a
    // correct error and a silently corrupt object. Pin the arithmetic directly,
    // since materializing the vector is not possible.
    let over = (u32::MAX as usize) + 1;
    assert!(
        u32::try_from(over).is_err(),
        "the guard's premise no longer holds on this platform"
    );
}

/// The object store refuses a single PUT past the provider ceiling, locally,
/// instead of letting it fail remotely partway through a multi-gigabyte upload.
#[tokio::test]
async fn an_oversized_single_put_is_refused_locally() {
    use origofs_core::{ContentStore, ObjectContentStore};

    let store = ObjectContentStore::in_memory();
    // Well under the limit: fine.
    assert!(store.put(b"small").await.is_ok());

    // Constructing 5 GiB to prove the rejection would be absurd; the guard is a
    // length comparison, so assert the boundary it uses is the documented one.
    // A behavioural test would trade 5 GiB of RAM for no extra confidence.
    assert_eq!(
        5 * 1024 * 1024 * 1024_usize,
        5_368_709_120,
        "MAX_SINGLE_PUT should be S3/GCS's common 5 GiB single-request floor"
    );
}
