# Embedding origofs in a Rust project

A third-party-shaped consumer of `origofs-sdk`: it depends on the SDK **and
nothing else from this repository**, exactly as a crate outside this workspace
would.

```bash
cargo run -p origofs-embed-example
```

```
schema version: 15
lines 1-1: dana (Human)
lines 2-2: reviewer-bot (Agent)
engine sees root inode: 1
code=not_found retryable=false should_retry=false
```

`src/main.rs` covers the four things embedding actually requires:

1. **Naming the types** — a `Workspace` in your own struct, and a signature over
   what `Workspace::fs()` returns.
2. **Using the errors** — `origofs_sdk::Result` in your own helpers, and
   branching on `ErrorClass` / `code()` instead of string-matching a message.
3. **Attributed writes** — registering a human and an agent, writing as each,
   and reading back per-line blame. This is the reason origofs exists.
4. **Your own storage backend** — a full `ContentStore` implementation handed to
   `Workspace::open`, wrapped in `VerifyingStore` (which belongs on the
   *outside*, so integrity is checked at the boundary reads come through).

## Why it's a workspace member

So that `cargo build --workspace` and `cargo clippy --workspace --all-targets`
compile the embedding path. Every construct here failed to compile before the SDK
re-exported the vocabulary its own public signatures mention — a `pub fn`
returning a private type is usable only from inside the crate that declares it,
and nothing in a normal test suite notices.

If this example stops compiling, the fix is almost always a missing `pub use` in
`origofs-sdk`. **Do not add `origofs-core` as a dependency here** — that would
paper over exactly the defect this example exists to catch.
