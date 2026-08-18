# What JuiceFS does that origofs should adopt

> Status: **open.** Taken against `adb6ec8` (2026-08-18). A comparison of
> [JuiceFS](https://github.com/juicedata/juicefs)'s architecture against what is
> actually in this tree, kept as a live list. Findings are ordered by what would
> change the system most, not by effort.

## Why compare against JuiceFS at all

JuiceFS is the closest thing to prior art origofs has: it makes the same core
bet — **split the metadata store from the content store, keep bytes in object
storage addressed by content, keep names and structure in a database** — and it
has run that bet in production for years. Where the two differ on purpose
(origofs adds versioning and per-actor attribution; JuiceFS adds nothing above
POSIX), there is nothing to learn. Where they differ *by omission*, JuiceFS has
usually already hit the wall origofs has not reached yet.

So this document is deliberately narrow. It is not "features JuiceFS has." It is
the subset where JuiceFS solved a problem origofs will have, in a way that fits
origofs's model.

What is explicitly **not** worth taking is in [§8](#8-deliberately-not-adopted),
and it matters as much as the rest — several of JuiceFS's choices are wrong for
origofs's workload, and the temptation to copy them wholesale is real.

---

## 1. Slice-based writes — the one structural change ([#111](https://github.com/danplischke/origofs/issues/111))

**JuiceFS.** A file is a sequence of 64 MiB *chunks*; each chunk holds a list of
*slices*; each slice is a run of 4 MiB *blocks* in object storage. A `pwrite`
writes only the new bytes as a **new slice** and appends it to that chunk's slice
list. Reads overlay the slice list newest-wins. A background **compaction** job
merges a fragmented slice list back into one clean slice once it gets long enough.

**origofs.** `Fs::vfs_write` (`crates/origofs-core/src/vfs.rs:161`) loops
`vfs_write_attempt` (`vfs.rs:177`), and each attempt:

1. reads the **entire** file body into memory (`read_body`),
2. patches the written range,
3. re-runs FastCDC over the **whole** buffer (`engine.rs:546`, `store_body`),
4. re-uploads every changed chunk,
5. compare-and-sets the inode's manifest hash against the one it read from.

`docs/LIMITS.md` already states the consequence plainly: *"Do not rewrite large
files through a FUSE/NFS mount. `vfs_write` is a read-modify-write of the entire
file per `write(2)` … Rewriting a 1 GiB file through a mount is quadratic in
allocation and hashing."* That is an honest limit, not a bug — but it is a limit
JuiceFS does not have, and it is the reason origofs's mounts are documented as
being "for browsing and editing human-scale files."

**Two things get fixed, not one.**

*Cost.* A `write(2)` becomes `O(bytes written)` instead of `O(file size)`. The
kernel issues 128 KiB pages, so today a sequential rewrite of an N-byte file
costs `O(N²/128 KiB)` in hashing and allocation.

*Concurrency.* The retry loop exists **because** of the whole-file rewrite. The
comment on `VFS_CAS_ATTEMPTS` (`vfs.rs:158`) says so directly: "two writes to
*different* offsets of one file would each rewrite the whole body, and the second
would erase the first." Sixteen bounded retries is the right answer given the
write path; it is not needed at all given a different one. Two writers appending
**disjoint** slices do not conflict, so the common multi-agent case stops being a
contended CAS and becomes two independent appends.

**How it fits origofs's model.** A slice list is another overlay layer, and the
project already runs on overlays — the working tree is an overlay on a commit
tree (`DESIGN.md` §3). Concretely:

- the inode keeps its `manifest_hash` as the **materialized base**;
- pending slices live beside it (metadata rows, or a small side object — they are
  bounded and short-lived either way);
- `vfs_read` resolves base-manifest-then-slices, newest-wins;
- **compaction** collapses slices back into a manifest — which is just
  `store_body` on the resolved bytes, machinery that already exists;
- `commit` forces compaction first, so nothing downstream (trees, merge, blame,
  gc) ever sees a slice. **This is the property that keeps the change contained:
  the object model does not learn about slices at all.**

The one genuinely new question is **attribution**. `write_as` diffs against the
previous body to compute per-range blame; a slice already *is* a per-range record
of "this actor wrote these bytes at this offset." That is arguably a better fit
than the diff, but it needs designing rather than assuming — see
[§10](#10-open-questions).

Take the *slice* idea. Do **not** take fixed-size blocks with it (see §8).

---

## 2. Per-handle write buffering ([#112](https://github.com/danplischke/origofs/issues/112))

**JuiceFS.** Buffers writes per file handle in a read/write buffer (`--buffer-size`),
coalesces the kernel's 128 KiB pages into whole-block uploads, and flushes on
`fsync`/`close`.

**origofs.** The FUSE surface implements no `open` and no `release` at all. The
full op list in `crates/origofs-sdk/src/fuse.rs` is: `lookup`, `getattr`,
`setattr`, `readlink`, `read`, `write`, `readdir`, `create`, `mkdir`, `unlink`,
`rmdir`, `rename`, `symlink`, `flush`, `fsync`. So there is no handle-scoped
state anywhere, and every 128 KiB page is a full trip through the engine: read
body, re-chunk, upload, CAS.

Worth doing **independently of §1**, and much cheaper: a per-handle dirty buffer
flushed at `flush`/`fsync` collapses the quadratic behaviour for the sequential
case (the overwhelmingly common one) without touching the object model. It is
also a prerequisite for making §1 pay off — slices reduce the cost of each write,
but coalescing reduces the *number* of writes.

Note the interaction with attribution: buffering means the actor context must be
captured at `open` and held for the handle's lifetime, which the mounts do not do
today (they have no actor context at all — a deliberate bypass, per `CLAUDE.md`).
Buffering does not make that worse, but it should not be assumed to make it
better either.

---

## 3. Reads: concurrency and readahead ([#113](https://github.com/danplischke/origofs/issues/113))

`vfs_read` (`vfs.rs:113`) walks the manifest and awaits `get_range` for each
covering chunk **strictly one at a time**. On an S3-backed workspace at 30 ms
RTT, a 1 MiB read spanning 16 chunks is ~16 serial round trips.

The write path already solved this — `store_body` uses
`.buffered(upload_concurrency())` (`engine.rs:546`, tunable via
`ORIGOFS_UPLOAD_CONCURRENCY`, default 16). The read path never got the same
treatment. Applying `buffered(...)` over the covering chunks is a small, local
change with no design questions attached.

JuiceFS additionally does **readahead**: on a detected sequential pattern it
prefetches the following blocks into the cache. That is worth having too, but it
is only meaningful once there is a cache worth prefetching into — see §4.

---

## 4. The cache tier is built and never wired up ([#114](https://github.com/danplischke/origofs/issues/114))

`TieredStore` (`content.rs:834`) is a complete, tested two-tier store. **No
`open_*` constructor in `origofs-sdk` uses it.** `open_s3`, `open_pg_s3`,
`open_gcs`, `open_pg_s3_packed` and the rest all build
`VerifyingStore(ObjectContentStore::…)` with no local tier, so in practice every
remote-backed workspace reads every chunk from the bucket, every time. The one
thing that would most improve remote performance is already written and simply
not reachable from the front door.

It also has gaps JuiceFS has closed, and they are the reason it cannot just be
switched on as-is:

| | JuiceFS | origofs `TieredStore` |
|---|---|---|
| Size bound | `--cache-size` | none — grows without limit |
| Eviction | LRU | none |
| Free-space floor | `--free-space-ratio` | none |
| Integrity of cached data | `--cache-checksum` | none (but see below) |
| Warm-up | `juicefs warmup` | `prefetch`, sequential loop (`content.rs:845`) |

The checksum row is the least urgent: wrapping the workspace in `VerifyingStore`
on the outside — which every `open_*` already does — re-hashes on read regardless
of which tier served the bytes, so a bit-rotted cache entry surfaces as `Corrupt`
rather than as authentic data. The gap is that it surfaces as a hard error
instead of a cache miss that refetches from the backend, which is what it should
be.

The work is: bound + evict, make `prefetch` concurrent, and wire a sensible
default cache directory into the remote `open_*` recipes.

---

## 5. Trash ([#115](https://github.com/danplischke/origofs/issues/115))

**JuiceFS.** Deleted files move to a `.trash` directory and are purged after
`trash-days`.

**origofs.** GC has a grace period (`DEFAULT_GC_GRACE_SECS = 600`, `gc.rs`) but
that protects *content objects from the sweep*, not files from users. A committed
file is recoverable from history; an **uncommitted** delete is gone.

This one matters more for origofs than it does for JuiceFS, and it is worth being
blunt about why: the users are agents. An agent that shells out to `rm -rf` on a
bad path is a routine failure mode, not an exotic one, and "you should have
committed first" is not an answer when the actor that failed to commit is the
same one that deleted the tree.

It also composes with what origofs already has and JuiceFS does not: a trash
entry carries **the actor and session that deleted it**, so restoring is an
attributed operation and the deletion itself is already in the op-log. Trash is
closer to origofs's existing grain than it is to JuiceFS's.

---

## 6. Directory quotas, recursive stats, and `statfs` ([#116](https://github.com/danplischke/origofs/issues/116))

**JuiceFS.** Maintains per-directory used-space and inode counts, enforces
capacity and inode quotas per directory, and answers `statfs` from them.

**origofs.** `MetadataStore::child_count` and nothing else. No recursive
directory stats, no `du`, no quota, and **no `statfs` anywhere in the tree** — so
`df` on a mount reports nothing meaningful, which real tooling does notice.

The quota half is the interesting one given `docs/MULTI_TENANCY.md`: a
per-directory capacity limit is the natural blast-radius control for a runaway
agent, and it is enforceable in the same place the `Propose` write policy is
enforced (`ensure_may_write`, `suggest.rs`) rather than per surface. The stats
half is a prerequisite for it, and pays for itself separately by making `statfs`
and `du` answerable.

---

## 7. Operational surface

### 7a. A portable metadata dump ([#117](https://github.com/danplischke/origofs/issues/117))

`MetadataStore::backup_to` has a default impl that **returns an error**
(`metadata.rs:51`): "this metadata backend has no built-in backup; use the
backend's own tooling (for Postgres: `pg_dump`)." Only SQLite implements it.

That is a defensible position for *backup* and a weak one for everything else,
because `CLAUDE.md` is explicit that the DB is the irreplaceable half: "blame, the
audit log, actors, and uncommitted edits live **only** in the DB — so the DB is
the thing to back up." `fsck --rebuild` reconstructs committed files, dirs,
symlinks and branches from the bucket alone, and none of the attribution.

JuiceFS's `dump`/`load` is an **engine-independent** serialization of the whole
metadata tree, plus a scheduled automatic backup into the object store. Adopting
the equivalent buys three things at once:

- a real backup story for the Postgres deployments, in origofs's own terms;
- a **SQLite → Postgres migration path**, which today does not exist. `resync`
  moves committed state and blame between workspaces on different backends
  (`resync.rs`), which is most of the way there — but not the audit log, not the
  working tree, and not tool-call history;
- a debugging artifact, which is what `dump` gets used for most in practice.

### 7b. `info`, `bench`, `stats` ([#118](https://github.com/danplischke/origofs/issues/118))

`juicefs info <file>` prints a file's chunk/slice/block layout. origofs has
Prometheus metrics behind the `metrics` feature (emit-only, no exporter linked)
and 44 CLI subcommands, and nothing that answers *"why is this one file slow."*

An `origofs info <path>` — manifest chunk count, chunk size distribution, which
chunks are cache-resident, dedup ratio against the store — is a few hours of work
against APIs that already exist, and it is the first thing anyone will want the
next time §1 or §4 is being measured rather than argued about. `origofs bench`
likewise: there are Criterion micro-benchmarks (`origofs-core/benches/engine.rs`)
but no end-to-end number a user can produce against *their* bucket.

---

## 8. Deliberately not adopted

**Fixed-size blocks.** JuiceFS's 4 MiB fixed blocks are right for its workload
and wrong here. Content-defined chunking (`MIN 16 KiB / AVG 64 KiB / MAX 256 KiB`,
`chunk.rs:16-20`) is what makes a one-line edit rewrite one chunk instead of the
file's whole tail, and text is origofs's actual workload. Take slices from §1;
leave the chunker alone.

The **real** version of this complaint is object *count*, and it is already
documented: 1 GiB of media becomes ~13,700 objects at the 64 KiB average
(`LIMITS.md`). The proportionate fix is to make the chunker's target size
**configurable per workspace**, so a media or large-binary workspace can run at
1–4 MiB while a source workspace keeps 64 KiB — not to change the default. Note
that `PackStore` already mitigates this, but only *within* a single write, so it
does nothing for many-small-files.

**Alternative metadata engines** (Redis, TiKV, FoundationDB). JuiceFS needs them
because its metadata is a hot POSIX index and nothing more. origofs's metadata
store also holds the attribution op-log, the audit log and the suggestion
workflow — all of which want real transactions across several tables. The
Postgres/SQLite split is the right call and adding a third engine would cost the
`MetaTxn` guarantees the write path is built on.

**`juicefs sync`.** An rsync-alike between object stores. `resync` already does
strictly more for origofs's case: it moves the commit closure *and* remaps
attribution identities across two workspaces that share neither backend
(`resync.rs`). Nothing to take.

**Hadoop SDK, CSI driver, S3 gateway.** Surface breadth, not architecture. The S3
gateway is the only one worth revisiting later, and only if agents turn out to
want an S3 API more than they want MCP.

**Client-side compression** — undecided rather than rejected. JuiceFS compresses
blocks (LZ4/ZSTD) before upload. origofs has `EncryptedStore` and nothing
compressing. It would slot in as another decorator, but two constraints bind:
compress-then-encrypt (the reverse accomplishes nothing), and the address must
stay the **plaintext** hash or convergent dedup breaks. Worth measuring before
building — the media case does not compress and the text case is already
deduplicated, which is most of the tree.

---

## 9. POSIX holes the mounts will hit ([#119](https://github.com/danplischke/origofs/issues/119))

Not a JuiceFS idea so much as a checklist JuiceFS has finished and origofs has
not. Each of these is currently absent rather than stubbed, so it fails
confusingly instead of cleanly:

| Missing | Evidence | Why it bites |
|---|---|---|
| **hardlinks** | no `link` op on `Fs`, none in `fuse.rs`. `nlink` is in the schema and `MetaTxn::adjust_nlink` exists and is only ever called with `-1` | `git` uses hardlinks; several editors do `rename`+`link` |
| **`statfs`** | no occurrence in the tree | `df` on a mount; some installers refuse to run without it |
| **xattrs** | only `sandbox.rs` touches them, for overlayfs whiteouts | macOS metadata, SELinux labels, `git`'s own use |
| **`flock`/`fcntl`** | no occurrence in the tree | origofs has path-level advisory `lock`s (LFS-style) but nothing wired to POSIX locking, so a lock-taking process gets no protection |
| **`fallocate`, `copy_file_range`** | absent | falls back to read+write loops, which on §1's current write path is the expensive case |

The `nlink` row is worth calling out: the column, the type, and the decrement path
all exist, so the schema already anticipates hardlinks and only the increment side
was never built.

---

## 10. Open questions

1. **Slices and blame.** A slice is `(actor, session, offset, len, content)` —
   which is nearly the `edit_op` record already. Does the slice list *become* the
   attribution record for uncommitted state, or does it stay independent and get
   diffed at compaction time the way `write_as` does today? The first is more
   elegant and changes the meaning of `revert_session` mid-flight; the second is
   safer and duplicates bookkeeping.
2. **Where slices live.** Metadata rows (transactional with the inode update,
   costs DB writes on a hot path) or a content-store side object (cheap to write,
   needs its own CAS discipline and its own gc root). The `CLAUDE.md` rule — never
   put large bytes in the metadata DB — does not settle it, because a slice's
   *bytes* go to the content store either way and only the slice *list* is at
   issue.
3. **Cache defaults.** Wiring a cache into `open_s3` and friends means picking a
   default directory and a default bound, i.e. writing to the user's disk without
   being asked. Opt-in via an explicit constructor is safer and gets used less.
4. **Quota enforcement point.** `ensure_may_write` is the natural home, but it
   currently gates *attributed* mutations only, and quota should presumably bind
   the unattributed internal paths too (checkout, merge materialization).

## Suggested order

1. **§3 concurrent chunk reads (#113)** and **§4 bound + wire the cache tier
   (#114)** — small, local, no design questions, and they make everything after
   this measurable.
2. **§7b `origofs info` (#118)** — so §1 gets measured rather than argued about.
3. **§2 per-handle write buffering (#112)** — large win, contained to the FUSE
   surface.
4. **§5 trash (#115)** — the highest value-per-line item on this list given who
   the users are.
5. **§1 slices + compaction (#111)** — the structural change. Wants §10's open
   questions answered first.
6. **§6 quotas (#116)** and **§7a portable dump (#117)** — both worth doing,
   neither blocking. **§9 POSIX holes (#119)** as they bite.
