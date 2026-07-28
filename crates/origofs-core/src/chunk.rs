//! Content-defined chunking (FastCDC) and the file **manifest** (`docs/DESIGN.md`
//! §4a).
//!
//! A file body is split into content-defined chunks; each chunk is stored in the
//! [`ContentStore`](crate::ContentStore) addressed by BLAKE3. The ordered list of
//! chunk hashes + lengths is a [`Manifest`] ("blob object"), itself stored as a
//! content-addressed object. This gives sub-file dedup (an edit rewrites only the
//! chunks it touches), cheap snapshots, and ranged reads that fetch only the
//! covering chunks.

use crate::error::{OrigoFSError, Result};
use crate::types::Hash;

/// Minimum chunk size (bytes). Files at or below this are a single chunk.
pub const MIN_CHUNK: u32 = 16 * 1024;
/// Target/average chunk size (bytes).
pub const AVG_CHUNK: u32 = 64 * 1024;
/// Maximum chunk size (bytes).
pub const MAX_CHUNK: u32 = 256 * 1024;

const MANIFEST_MAGIC: &[u8; 5] = b"ORGM\x01";
const HEADER_LEN: usize = 17; // magic(5) + size(8) + count(4)
const ENTRY_LEN: usize = 36; // hash(32) + len(4)

/// A reference to one content chunk within a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkRef {
    pub hash: Hash,
    pub len: u32,
}

/// The ordered list of chunks that make up a file body (a "blob object").
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Manifest {
    pub size: u64,
    pub chunks: Vec<ChunkRef>,
}

impl Manifest {
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Canonical serialization so identical content yields an identical manifest
    /// hash: `magic | size(LE u64) | count(LE u32) | (hash[32] | len(LE u32))*`.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.chunks.len() * ENTRY_LEN);
        out.extend_from_slice(MANIFEST_MAGIC);
        out.extend_from_slice(&self.size.to_le_bytes());
        out.extend_from_slice(&(self.chunks.len() as u32).to_le_bytes());
        for c in &self.chunks {
            out.extend_from_slice(c.hash.as_bytes());
            out.extend_from_slice(&c.len.to_le_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Manifest> {
        let bad = || OrigoFSError::Content("malformed manifest".to_string());
        if bytes.len() < HEADER_LEN || &bytes[0..5] != MANIFEST_MAGIC {
            return Err(bad());
        }
        let size = u64::from_le_bytes(bytes[5..13].try_into().map_err(|_| bad())?);
        let count = u32::from_le_bytes(bytes[13..17].try_into().map_err(|_| bad())?) as usize;
        if bytes.len() != HEADER_LEN + count * ENTRY_LEN {
            return Err(bad());
        }
        let mut chunks = Vec::with_capacity(count);
        let mut off = HEADER_LEN;
        for _ in 0..count {
            let mut h = [0u8; 32];
            h.copy_from_slice(&bytes[off..off + 32]);
            let len = u32::from_le_bytes(bytes[off + 32..off + 36].try_into().map_err(|_| bad())?);
            chunks.push(ChunkRef {
                hash: Hash::from_array(h),
                len,
            });
            off += ENTRY_LEN;
        }
        // Cross-check the declared size against the chunks. A manifest always has
        // `size == Σ chunk.len` (chunks cover the whole body), so a mismatch means
        // corruption or tampering — and rejecting it here stops a hostile `size`
        // (e.g. u64::MAX) from driving an OOM pre-allocation in `content_bytes`.
        let total: u64 = chunks.iter().map(|c| c.len as u64).sum();
        if total != size {
            return Err(OrigoFSError::Corrupt(format!(
                "manifest size {size} != sum of chunk lengths {total}"
            )));
        }
        Ok(Manifest { size, chunks })
    }
}

/// Split `data` into content-defined chunk boundaries `(offset, length)`.
pub fn chunk_bounds(data: &[u8]) -> Vec<(usize, usize)> {
    fastcdc::v2020::FastCDC::new(data, MIN_CHUNK, AVG_CHUNK, MAX_CHUNK)
        .map(|c| (c.offset, c.length))
        .collect()
}
