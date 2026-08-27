//! A packed workspace through the SDK: writes round-trip, `commit` seals the
//! open pack, the data dir holds far fewer objects than there are chunks, and
//! `repack` reclaims deleted space.

use origofs_sdk::Workspace;
use std::path::Path;

fn count_objects(dir: &Path) -> usize {
    let objects = dir.join("objects");
    let mut n = 0;
    if let Ok(shards) = std::fs::read_dir(&objects) {
        for shard in shards.flatten() {
            if let Ok(entries) = std::fs::read_dir(shard.path()) {
                n += entries
                    .flatten()
                    .filter(|e| !e.file_name().to_string_lossy().ends_with(".tmp"))
                    .count();
            }
        }
    }
    n
}

fn blob(len: usize, seed: u64) -> Vec<u8> {
    let mut x = if seed == 0 {
        0x9E37_79B9_7F4A_7C15
    } else {
        seed
    };
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

#[tokio::test]
async fn packed_workspace_roundtrips_and_batches() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let index = dir.path().join("index");
    let ws = Workspace::open_local_packed(dir.path().join("meta.db"), &data, &index)
        .await
        .unwrap();

    let big = blob(3 * 1024 * 1024, 7);
    ws.write("/big.bin", &big).await.unwrap();
    ws.write("/small.txt", b"hi\n").await.unwrap();
    ws.commit("packer", "snapshot").await.unwrap(); // seals the open pack

    // round-trips through the packed store
    assert_eq!(&ws.read("/big.bin").await.unwrap()[..], &big[..]);
    assert_eq!(&ws.read("/small.txt").await.unwrap()[..], b"hi\n");

    // the index holds many chunk entries; the data dir holds only a few packs
    let packs = count_objects(&data);
    let entries = count_objects(&index);
    assert!(entries > 8, "big file yielded many chunks: {entries}");
    assert!(packs < entries, "packs {packs} << index entries {entries}");
    // A handful of packs: the file's chunks + the commit, plus the small pack the
    // commit seals for its ref-mirror snapshot (coalesced later by `repack`).
    assert!(packs <= 4, "only a handful of pack objects: {packs}");
}

#[tokio::test]
async fn flush_and_repack_are_reachable() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local_packed(
        dir.path().join("meta.db"),
        dir.path().join("data"),
        dir.path().join("index"),
    )
    .await
    .unwrap();

    ws.write("/a.bin", &blob(64 * 1024, 1)).await.unwrap();
    ws.flush().await.unwrap(); // seal without committing
    assert_eq!(ws.read("/a.bin").await.unwrap().len(), 64 * 1024);

    // repack is a valid maintenance call on a packed workspace (nothing dead yet)
    let reclaimed = ws.repack().await.unwrap();
    assert_eq!(reclaimed, 0);
}
