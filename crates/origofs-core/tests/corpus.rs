//! Corpus / passage extraction for retrieval: segmentation, content-addressed
//! passage hashes (dedup + incremental), per-passage blame, and the edit-stability
//! property that makes content-defined segmentation worth having.

use origofs_core::{Fs, MemStore, PassageOptions, Segmentation, SqliteMetadataStore, WriteCtx};
use std::collections::HashSet;
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let fs = Fs::new(
        SqliteMetadataStore::open_in_memory().unwrap(),
        Arc::new(MemStore::new()),
    );
    fs.init().await.unwrap();
    fs
}

fn opts(seg: Segmentation) -> PassageOptions {
    PassageOptions {
        segmentation: seg,
        ..Default::default()
    }
}

#[tokio::test]
async fn whole_file_and_root_scoping_and_ext_filter() {
    let fs = fixture().await;
    fs.write("/a.md", b"# hello\nbody\n").await.unwrap();
    fs.write("/b.txt", b"plain text\n").await.unwrap();
    fs.mkdir_p("/sub").await.unwrap();
    fs.write("/sub/c.md", b"nested markdown\n").await.unwrap();
    fs.write("/sub/skip.bin", b"\x00\x01\x02").await.unwrap();

    // whole tree, one passage per file
    let all = fs.passages(&opts(Segmentation::WholeFile)).await.unwrap();
    let paths: HashSet<_> = all.iter().map(|p| p.path.as_str()).collect();
    assert_eq!(
        paths,
        HashSet::from(["/a.md", "/b.txt", "/sub/c.md", "/sub/skip.bin"])
    );

    // subtree scoping
    let sub = fs
        .passages(&PassageOptions {
            root: "/sub".into(),
            segmentation: Segmentation::WholeFile,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(sub.len(), 2);
    assert!(sub.iter().all(|p| p.path.starts_with("/sub/")));

    // extension filter
    let md = fs
        .passages(&PassageOptions {
            exts: Some(vec!["md".into()]),
            segmentation: Segmentation::WholeFile,
            ..Default::default()
        })
        .await
        .unwrap();
    let md_paths: HashSet<_> = md.iter().map(|p| p.path.as_str()).collect();
    assert_eq!(md_paths, HashSet::from(["/a.md", "/sub/c.md"]));
}

#[tokio::test]
async fn passage_hash_is_content_addressed() {
    let fs = fixture().await;
    // identical content in two files -> identical passage hashes (dedup key)
    fs.write("/x", b"the same body\n").await.unwrap();
    fs.write("/y", b"the same body\n").await.unwrap();
    let ps = fs.passages(&opts(Segmentation::WholeFile)).await.unwrap();
    let x = ps.iter().find(|p| p.path == "/x").unwrap();
    let y = ps.iter().find(|p| p.path == "/y").unwrap();
    assert_eq!(x.hash, y.hash);
    // and the hash is exactly BLAKE3 of the passage bytes
    assert_eq!(x.hash, origofs_core::Hash::of(b"the same body\n"));
}

#[tokio::test]
async fn blame_is_clipped_per_passage() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs
        .create_agent("claude", "opus", Some(alice))
        .await
        .unwrap();

    // Alice writes the file; Claude rewrites the middle line.
    fs.write_as(WriteCtx::actor(alice), "/f", b"l1\nl2\nl3\nl4\n")
        .await
        .unwrap();
    fs.write_as(WriteCtx::actor(claude), "/f", b"l1\nCLAUDE\nl3\nl4\n")
        .await
        .unwrap();

    // One passage per line: each line's blame credits exactly one author, and the
    // blame byte range never spills outside the passage.
    let ps = fs
        .passages(&opts(Segmentation::Lines {
            max_lines: 1,
            overlap: 0,
        }))
        .await
        .unwrap();
    assert_eq!(ps.len(), 4, "four lines -> four passages");
    for p in &ps {
        for b in &p.blame {
            assert!(b.byte_start >= p.byte_start && b.byte_end <= p.byte_end);
        }
    }
    // the CLAUDE line is authored by the agent; the rest by alice
    let claude_line = ps
        .iter()
        .find(|p| p.blame.iter().any(|b| b.actor.id == claude));
    assert!(claude_line.is_some(), "the middle line blames to claude");
    assert_eq!(
        String::from_utf8_lossy(claude_line.unwrap().text.as_ref().unwrap()),
        "CLAUDE\n"
    );
}

#[tokio::test]
async fn content_defined_is_edit_stable_but_fixed_is_not() {
    let fs = fixture().await;
    // A few KB of varied text so both strategies produce many passages.
    let mut body = String::new();
    for i in 0..400 {
        body.push_str(&format!(
            "line {i:04}: the quick brown fox jumps over lazy dog\n"
        ));
    }
    fs.write("/doc.txt", body.as_bytes()).await.unwrap();

    let cd = Segmentation::ContentDefined {
        min: 256,
        avg: 1024,
        max: 4096,
    };
    let fixed = Segmentation::FixedBytes {
        size: 1024,
        overlap: 0,
    };

    let hashes = |ps: &[origofs_core::Passage]| -> HashSet<String> {
        ps.iter().map(|p| p.hash.to_hex()).collect()
    };

    let cd_before = hashes(&fs.passages(&opts(cd.clone())).await.unwrap());
    let fixed_before = hashes(&fs.passages(&opts(fixed.clone())).await.unwrap());
    assert!(
        cd_before.len() > 3,
        "expected several content-defined passages"
    );

    // Insert a line near the very top and re-extract.
    let edited = format!("line 0000b: an inserted line near the top\n{body}");
    fs.write("/doc.txt", edited.as_bytes()).await.unwrap();

    let cd_after = hashes(&fs.passages(&opts(cd)).await.unwrap());
    let fixed_after = hashes(&fs.passages(&opts(fixed)).await.unwrap());

    let cd_kept = cd_before.intersection(&cd_after).count();
    let fixed_kept = fixed_before.intersection(&fixed_after).count();

    // Content-defined: most passages survive an early edit (only the local one
    // shifts). Fixed-size: an early insertion shifts every window, so almost
    // nothing survives. This is the whole reason ContentDefined is the default.
    assert!(
        cd_kept * 2 > cd_before.len(),
        "content-defined kept {cd_kept}/{} passages after an early edit",
        cd_before.len()
    );
    assert!(
        fixed_kept < cd_kept,
        "fixed-size ({fixed_kept}) should keep far fewer than content-defined ({cd_kept})"
    );
}
