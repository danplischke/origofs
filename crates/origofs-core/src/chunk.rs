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
use crate::format;
use crate::types::Hash;

/// Minimum chunk size (bytes). Files at or below this are a single chunk.
pub const MIN_CHUNK: u32 = 16 * 1024;
/// Target/average chunk size (bytes).
pub const AVG_CHUNK: u32 = 64 * 1024;
/// Maximum chunk size (bytes).
pub const MAX_CHUNK: u32 = 256 * 1024;

const HEADER_LEN: usize = 17; // tag+version(5) + size(8) + count(4)
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

/// Upper bound on how much a reassembly buffer may reserve up front from a
/// manifest's *declared* size. See [`Manifest::capacity_hint`].
const MAX_REASSEMBLY_HINT: usize = 8 * 1024 * 1024;

impl Manifest {
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// How much to reserve before reassembling this manifest's body.
    ///
    /// **Never reserve `size` directly.** `decode` only checks that
    /// `size == Σ chunk.len`, which a crafted or corrupt manifest satisfies
    /// trivially: a 53-byte manifest declaring one chunk of `len = u32::MAX`
    /// yields a consistent 4 GiB, and a few megabytes of manifest reaches
    /// hundreds of terabytes. Reserving from that is an allocator abort driven by
    /// untrusted bytes — the same hostile-input boundary the decoders guard, one
    /// layer up.
    ///
    /// So the reservation is a *hint*, capped at [`MAX_REASSEMBLY_HINT`], and the
    /// buffer grows as real chunk bytes arrive. An honest large file pays a few
    /// reallocations; a dishonest one allocates 8 MiB and then fails on the first
    /// missing chunk.
    pub fn capacity_hint(&self) -> usize {
        self.chunks
            .iter()
            .fold(0usize, |a, c| a.saturating_add(c.len as usize))
            .min(MAX_REASSEMBLY_HINT)
    }

    /// Canonical serialization so identical content yields an identical manifest
    /// hash: `ORGM | version | size(LE u64) | count(LE u32) | (hash[32] | len(LE u32))*`.
    ///
    /// The bytes are the object's address, so this encoding is frozen for v1 —
    /// see the format-evolution rules in [`crate::format`].
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.chunks.len() * ENTRY_LEN);
        out.extend_from_slice(&format::MANIFEST.header());
        out.extend_from_slice(&self.size.to_le_bytes());
        out.extend_from_slice(&(self.chunks.len() as u32).to_le_bytes());
        for c in &self.chunks {
            out.extend_from_slice(c.hash.as_bytes());
            out.extend_from_slice(&c.len.to_le_bytes());
        }
        out
    }

    /// Decode a manifest, dispatching on its header's format version. An object
    /// written by a newer origofs yields [`OrigoFSError::UnsupportedVersion`]
    /// rather than a "malformed" error that reads like corruption.
    pub fn decode(bytes: &[u8]) -> Result<Manifest> {
        match format::MANIFEST.version_of(bytes)? {
            1 => Manifest::decode_v1(bytes),
            // Unreachable while `version_of` caps at `max_read_version`; this is
            // the arm a future version is added beside (never *instead of* v1).
            v => Err(format::MANIFEST.unsupported(v)),
        }
    }

    fn decode_v1(bytes: &[u8]) -> Result<Manifest> {
        let bad = || format::MANIFEST.malformed();
        if bytes.len() < HEADER_LEN {
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
