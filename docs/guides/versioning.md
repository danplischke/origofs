# Versioning and branches

Versioning is **opt-in** and Git-shaped: a real commit DAG, branches, checkout,
log, status, three-way merge and locks — backed by origofs's content-addressed
store, so snapshots are incremental and identical trees are shared across commits
for free.

## Three modes

Set when a workspace is initialized (`VersioningMode`):

| Mode | What you get |
|---|---|
| `off` | Working tree and attribution only. No commit DAG. |
| `native` | origofs's own chunked commit DAG. **The default.** |
| `git` | The native DAG *plus* export/import of genuine git objects and the `origofs://` remote helper. |

`native` and `git` share one commit DAG and one merge engine; they differ only in
on-disk object encoding, so moving between them does not rewrite your history.

## The basics

```bash
origofs --workspace "$WS" commit -m "initial" --actor "$DAN"
origofs --workspace "$WS" log
origofs --workspace "$WS" status                           # changes since HEAD

origofs --workspace "$WS" branch feature                   # create at HEAD
origofs --workspace "$WS" branch                           # list branches
origofs --workspace "$WS" checkout feature

origofs --workspace "$WS" diff main feature                # changed-path list
origofs --workspace "$WS" diff main feature --path /x.rs   # one file's line diff
```

`--actor` is the identity a commit is checked against and attributed to.
`--author` is the free-form name recorded *inside* the commit object, the way git
records one — it defaults to `origofs` and is not an identity.

!!! info "Why a diff is cheap"

    Branch comparison works on content addresses, not file reads. Equal hashes
    mean an identical file — a 32-byte compare — so a diff only ever reads the
    paths that actually changed. The metadata trees *are* the index.

## Merging

```bash
origofs --workspace "$WS" merge feature -m "land the feature"
origofs --workspace "$WS" conflicts                        # what is unresolved
```

Merge is three-way with diff3-style conflict markers. Where a file cannot be
merged textually — a binary — take an exclusive lock on it instead of racing:

```bash
origofs --workspace "$WS" lock  /assets/logo.png --owner dan
origofs --workspace "$WS" locks
origofs --workspace "$WS" unlock /assets/logo.png --owner dan
```

!!! warning "Two unrelated things called a lock"

    These are LFS-style **path locks**: a workflow convention that says "I am
    editing this binary, don't". They have nothing to do with
    [POSIX advisory locks](mounts.md#posix-advisory-locks), which are the
    `fcntl` byte-range locks a program takes through a mount.

Commits and checkouts are deliberately blind to
[live co-editing sessions](teams.md#live-co-editing). A checkout rematerializes
files and swaps the CRDT sidecars that ride with the branch, so a document opened
on the old branch must be re-opened after switching — the checkpoint is where
that is caught, and it refuses rather than writing the old branch's content onto
the new one.

## Real-git interop

origofs stays BLAKE3-native internally, but its history projects to — and imports
from — genuine git objects, so you can keep using the `git` CLI and hosts like
GitHub.

```bash
# origofs history → a real git repo the `git` binary reads directly
origofs --workspace "$WS" git export ./repo --format sha256   # sha1 is the default
git -C ./repo log --oneline
git -C ./repo fsck --strict                                   # clean

# a real git repo → origofs history
origofs --workspace "$WS2" git import ./repo --branch main
```

With `git-remote-origofs` on your `PATH` (it installs alongside `origofs`), the
real `git` can clone, fetch and push a workspace over `origofs://` URLs with no
export step:

```bash
git clone origofs://"$WS" checkout
cd checkout && echo hi >> readme.md && git commit -am edit && git push origin main
```

Large files can be exported as git-LFS pointer blobs (`--lfs-threshold <bytes>`),
backed by origofs's own chunk store.

!!! note "What export leaves behind"

    `git export` skips `/.origofs` at the root of every commit tree, so a
    published repository carries none of origofs's own state — no CRDT sidecars,
    and none of the actor/session stamps inside them. **Import is deliberately
    asymmetric** and keeps a `.origofs` directory it finds: in an incoming
    repository that name is somebody else's content. The export filter exists to
    keep origofs's state out of what it publishes, not to reserve the name.

    Attribution does not survive the round trip either way. Git has no per-byte
    blame model to carry it in.

## Working offline, then rejoining

A solo workspace can reconcile with a shared one over a branch, merging any
divergence with the ordinary three-way merge engine:

```bash
origofs --workspace "$WS" resync --remote ../shared --branch main
origofs --workspace "$WS" resync --remote-config team.toml
```

Objects move both ways as needed and **per-line blame travels with them** — which
is the difference from a git push, and the reason this is a first-class operation
rather than an export followed by an import. The remote branch only advances by
compare-and-swap, and both working trees must be clean.

`--remote-config` takes the same TOML as `--config`, so the remote can be a
Postgres + object-store deployment rather than a directory.
