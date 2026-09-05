# File sizes and limits

There is **no hard file-size limit** in the engine. `Manifest.size` is a `u64`, the
inode `size` column is a `BIGINT`, and content objects are *chunks* (≤ 256 KiB) —
never whole files — so no object-store per-object limit is reachable from file
content.

What exists instead are practical ceilings, and one rule that decides which you
hit: **stream, or buffer.**

## Stream or buffer

| Path | Behaviour |
|---|---|
| `write_reader_as` / Python `write_path_as` | **streams**, attributed |
| `write_reader` / Python `write_path` | **streams**, unattributed |
| `read_stream`, `read_to_writer`, Python `read_to_path` | **streams** |
| `read_range`, `vfs_read`, Python `read_range` | **chunk-scoped** — fetches only the covering chunks |
| HTTP `GET /v1/files/*`, `fastapi` `GET /files/{path}` | **streams**, honours `Range` |
| `fastapi` `PUT /files/{path}` | streams past 8 MiB (`SPOOL_MAX`) |
| `write`, `write_as`, `write_or_propose`, Python `write`/`write_as` | whole body resident |
| `read`, Python `read` | whole body resident |
| HTTP `PUT /v1/files/*` | whole body resident, capped |
| MCP `origofs_write` / `origofs_read` | whole body, as a JSON string |
| FUSE / NFS `write` | **the written range**, plus a chunk either side |
| FUSE / NFS `truncate` | one chunk (shrink) or nothing (grow) |

A buffered write of an N-byte file holds roughly 2N (`write_as` also loads the
previous body to diff against it). From Python it was ~3N, because pyo3 copies the
`bytes` object into a Rust `Vec` — which is why `write_path_as` exists.

## Where it actually breaks, in order

| Threshold | What happens | Configurable |
|---|---|---|
| **64 MiB** | HTTP `PUT` → `413` naming the limit | `ApiOptions::max_body_bytes`, via `serve_with` |
| **16 MiB** | a co-editing WebSocket frame is rejected | `MAX_COEDIT_FRAME` |
| **256 MiB** | `git import` rejects an object | `ORIGOFS_GIT_MAX_OBJECT_BYTES` |
| **RAM** | any buffered path above | — |
| **~1 GB** | `blob_blame.runs` hits the SQLite/Postgres `TEXT` limit | — |
| **~5 GiB** | a single object exceeds the S3/GCS single-PUT ceiling → `TooLarge` | — |
| **64–256 TiB** | a manifest exceeds the format's `u32` chunk count → `TooLarge` | — |

The last three are guards, not silent failures. They used to be: `Manifest::encode`
and `PackLoc::encode` both cast to `u32` with a bare `as` and wrapped, corrupting
the object at write time and only surfacing it on the next read. Every one of them
had a careful *decode*-side guard — the format layer reasoned hard about hostile
input coming in and not at all about honest data going out.

## The manifest is the real ceiling

A manifest is `36 bytes × chunk count`, held whole in memory *and* stored as one
object:

| File | Chunks (64 KiB average) | Manifest |
|---|---|---|
| 1 GiB | 16 K | ~0.6 MB |
| 10 GiB | 164 K | ~5.9 MB |
| 100 GiB | 1.6 M | ~59 MB |
| 1 TiB | 16.8 M | ~604 MB |

About 0.055% of file size. Comfortable to ~100 GiB. Past ~1 TiB the manifest is
itself a multi-hundred-megabyte allocation and a single PUT of that size, and
around 9 TiB it exceeds the object store's single-request ceiling. Even the
streaming paths accumulate this — they stream the *body*, not the manifest.

## Media

Media is the workload that stresses all of this at once: large, incompressible, and
read by seeking rather than sequentially. Three things follow.

**It does not deduplicate.** Encoded media is already compressed, so content-defined
chunking finds no shared boundaries — two encodes of the same source share nothing,
and a re-encode shares nothing with the original. Dedup is why chunking is cheap for
text and near-useless for media, so budget storage at full size and plan to run
`gc()` rather than hoping dedup absorbs churn.

**One gigabyte becomes ~13,700 objects.** At the 64 KiB average chunk size, that is
the object count in your bucket per GiB of media. Uploads run concurrently
(`ORIGOFS_UPLOAD_CONCURRENCY`, default 16), which is what keeps this from being
~13,700 sequential round trips — about seven minutes per GiB at a 30 ms RTT before
that window existed. Raise it for a high-latency bucket. `packed` also helps here in
a way it does not for many small files: one large write batches into few large PUTs,
which is exactly the case packing is for.

**Serving works, and needs `Range`.** `GET /v1/files/*` (Rust) and
`GET /files/{path}` (`origofs.fastapi`) both send `Accept-Ranges: bytes`, a guessed
`Content-Type`, a `Content-Length`, and answer a single-range request with `206` /
`416`. A `<video>` element can seek, and a download can resume. Ranged responses
stream — a player asking `bytes=0-` gets the whole file without the server
materializing it.

Blame on media is file-level (a single span), because `diff_spans` only applies to
text. That is the right answer for a binary: a re-encode is a new file, not an edit.

## Guidance

**Write large files with `write_path_as`** (Python) or `write_reader_as` (Rust).
Attribution costs nothing extra: blame and the edit-op are recorded either way.

Note that a streamed write attributes the **whole file** to the writer, rather than
diffing line-by-line against the previous body the way `write_as` does — not having
that body resident is the point. A streamed write *is* a wholesale replacement, so
this is exactly right for a file being replaced, and lossy for one being edited.
Use `write_as` when the file fits in memory and its line-level provenance matters.

**Read large files with `read_range` or `read_to_path`.** `read` materializes the
whole body.

**Prefer fewer, larger writes on a metered object store.** `PackStore` batches
chunks *within* a write, not across writes — each write ends with a durability
barrier that seals the open pack. Ten thousand small files are ten thousand PUTs
whatever the pack target. Streaming one archive through `write_reader_as` beats
writing its members individually by far more than any pack tuning will.

**Rewriting large files through a mount is no longer quadratic, but still costs
more than the SDK.** `vfs_write` used to be a read-modify-write of the *entire*
file per `write(2)`, which made rewriting a 1 GiB file quadratic in allocation and
hashing. Since #111 it **splices** the written range into the existing manifest —
re-chunking only a window around it — so a write costs `O(bytes written)`, and
since #112 the FUSE mount buffers per file handle so the kernel's small requests
are coalesced before they reach the engine. Measured: quadrupling a file multiplies
the work by ~4.5x rather than ~16x, and a 4 KiB write into a 4 MiB file touches
~270 KiB rather than the whole 8 MiB of read-plus-rewrite.

**Holes cost a manifest entry, not their bytes.** A growing `truncate`, or a write
that starts past EOF, leaves a run of zeroes behind it. That run is emitted as zero
chunks rather than being materialized: because the store is content-addressed and
deduplicating, a hole of any size stores at most two distinct objects and holds one
chunk in memory. Growth used to route through the splice path as a one-byte write at
the new end, which allocated and hashed the entire gap first — growing an empty file
to 256 MiB took over a second, and an ordinary `truncate -s` for a sparse file failed
on the allocation.

The manifest is **not** sparse, though: a hole still costs one 36-byte entry per
256 KiB, so a 1 TiB hole is a ~150 MB manifest — about what a real 1 TiB body costs,
and held whole in memory per the manifest note above. Making a hole free there needs a
sentinel chunk kind, which is a change to a frozen on-disk format.

What remains: two writers at *different* offsets of one file still contend, because
both compare-and-set the same inode `manifest_hash` (retried up to 16 times); and
each spliced write still pays for the widened re-chunk window. The SDK and HTTP API
remain the better path for bulk data, but a mount is no longer the wrong tool for a
large file.

**Encryption and packing compose with a real cost.** `EncryptedStore::get_range`
must decrypt a whole object before slicing — AEAD authenticates the whole
ciphertext, so a partial decrypt cannot be authenticated. That is cheap for chunks
(≤ 256 KiB) and not cheap for a pack (4 MiB default) or a large manifest. A ranged
read on an encrypted packed store pays for it.

**Blame has its own ceiling.** `blob_blame.runs` is a `TEXT` column holding one run
per contiguous same-author span — roughly one per line for multi-author text — and
it is rewritten on every attributed write. A very large multi-author text file
approaches the ~1 GB column limit. This is the one place file size touches the
metadata database.

## Co-editing: a decoder that is not hardened against hostile input

Every limit above is a *guard* — a threshold origofs checks and refuses at. This
one is not, and it is the reason `coedit` is off by default and excluded from
`full`.

A malformed y-sync update reaches `from_utf8_unchecked` inside `yrs`
(`encoding/read.rs`, `updates/decoder.rs`) and is then iterated in
`block::utf16_len`. That is undefined behaviour: it aborts under the debug UB
checks and is silent in release. 51 bytes suffice, through the public
`CoeditDoc::load`, and `handle_sync` feeds it bytes from clients origofs
explicitly does not trust.

There is no threshold to raise and no option to set. 0.24/0.25/0.26 reproduce it
identically; 0.27.4 has the same two `unsafe` sites and does not build on stable.
The abort is non-unwinding, so `catch_unwind` is no help, and pre-validating the
bytes would mean reimplementing the decoder.

**The only mitigation is deployment-shaped:** the co-editing WebSocket must be
reachable only by clients you trust. `crates/origofs-core/tests/coedit_malformed_update.rs`
holds the reproducer (`#[ignore]`d — it takes the suite down rather than failing
it; run with `--ignored` when trying a candidate `yrs`), and
`fuzz_targets/coedit_state_decode.rs` drives the same path and is expected to
abort. See [SECURITY.md](../SECURITY.md).
