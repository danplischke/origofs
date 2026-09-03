# CLI reference

Every command takes `--workspace <dir>` (default `.origofs`), the directory
holding `meta.db` and `cas/`. For a Postgres or object-store deployment, pass
`--config <file>` instead — see [Configuration](configuration.md).

```bash
origofs --help                 # the full list
origofs <command> --help       # a command's own flags, with the long explanation
```

`--help` (long) carries more than `-h` (summary) on most commands, and is the
authoritative source. This page is the map.

## Global options

| Option | Meaning |
|---|---|
| `--workspace <DIR>` | Workspace directory. Default `.origofs`. |
| `--config <FILE>` | Backend configuration TOML. Selects Postgres/S3/GCS instead of the local defaults. |
| `--log-format <text\|json>` | Format for tracing output, always written to **stderr**. Level comes from `ORIGOFS_LOG` or `RUST_LOG`, default `info`. |

Logs go to stderr so `origofs mcp` keeps stdout for its JSON-RPC transport.

## Files

| Command | Notes |
|---|---|
| `init` | Create the workspace. |
| `write <PATH>` | Bytes from stdin or `--from <file>`. `--actor` records blame. |
| `read <PATH>` | Bytes to stdout. `--actor` applies read enforcement. |
| `ls <PATH>` | List a directory. |
| `stat <PATH>` | Inode metadata. |
| `rm <PATH>` | Remove a file or empty directory. |
| `mv <FROM> <TO>` | Move or rename. |
| `mkdir <PATH>` | Create a directory and any missing parents. |
| `info <PATH>` | What a file costs to read: chunk count and size distribution, self-dedup, and whether the store still holds the chunks. |
| `bench` | Measure this workspace's own backends end to end. |

`info` and `bench` are the two that answer questions a benchmark suite cannot —
they depend on *your* bucket, *your* latency and *your* settings.

## Identity and attribution

| Command | Notes |
|---|---|
| `actor <NAME>` | Register an actor. `--agent` with `--model`, and `--controller` for the human who launched it. Prints the id. |
| `write-policy <ACTOR> <direct\|propose>` | Whether the actor's writes land or [queue for review](../guides/review.md). |
| `blame <PATH>` | Per-line authorship. |
| `revert-session` | Undo one actor's session. `--actor`, `--session`, `--by`, optional `--path-prefix`. |
| `require-attribution [on\|off]` | Make an unattributed mutation an error. Off by default. |
| `watch` | Tail the [change feed](../guides/attribution.md#the-change-feed). `--since <seq>`, `--follow`. |
| `presence` | Sessions currently active. |

Every attributed command falls back to **`ORIGOFS_ACTOR`** when `--actor` is
omitted, and opens its own session labelled `cli`.

## Versioning

| Command | Notes |
|---|---|
| `commit -m <MSG>` | Snapshot the working tree. `--actor` is the identity; `--author` is the free-form name recorded inside the object. |
| `log` | Commit history, HEAD, first-parent. |
| `status` | Working-tree changes relative to HEAD. |
| `diff <FROM> <TO>` | Changed paths, or one file's line diff with `--path`. |
| `branch [NAME]` | Create at HEAD, or list when no name is given. |
| `checkout <BRANCH>` | Switch the working tree. |
| `merge <BRANCH>` | Three-way merge into the current branch. |
| `conflicts` | Unresolved merge conflicts. |
| `resync` | Reconcile with a remote workspace. `--remote <DIR>` or `--remote-config <FILE>`, `--branch`. |
| `lock` / `unlock` / `locks` | LFS-style path locks for binaries. `--owner`, default `cli`. |
| `git export <DIR>` | `--branch`, `--format sha1\|sha256`, `--lfs-threshold <bytes>`. |
| `git import <DIR>` | `--branch`. |

## Review queue

| Command | Notes |
|---|---|
| `suggest <PATH> --actor <A>` | Propose an edit. Bytes from stdin or `--from`; `--delete` proposes a removal; `--summary`, `--session`. |
| `suggestions` | List. Filter with `--status` and `--path`. |
| `suggestion-diff <ID>` | Unified diff, base → proposed. |
| `accept <ID> --actor <A>` | Land it, credited to the author. |
| `reject <ID>` | Discard it. |

## Access and capacity

| Command | Notes |
|---|---|
| `acl show` | Grants, plus both switches. `--actor` to narrow. |
| `acl check <ACTOR> <PATH>` | What that actor may actually do there, after prefix matching *and* the write-policy fallback. |
| `acl grant <ACTOR> <PREFIX> <PERMS>` | `read`, `write`, `propose`, `none`, or `read+write`. `--by` grants as an actor. |
| `acl revoke <ACTOR> <PREFIX>` | Remove the grant at exactly that prefix. |
| `acl default-deny [on\|off]` | Deny an actor with no matching grant. Off by default. |
| `acl enforce-reads [on\|off]` | Check `READ` on attributed reads. Off by default. |
| `du [PATH]` | Recursive usage. Logical bytes, an inode counted once. |
| `quota` | Show limits and use. `--bytes 10G\|off`, `--inodes N\|off`. |
| `trash list\|restore\|purge\|retention` | See [Operating a workspace](../guides/operating.md#undo-a-delete). |
| `posix-locks [on\|off]` | Cross-mount `fcntl` locks. `--path` lists holders. Off by default. |

## Serving

| Command | Notes |
|---|---|
| `serve` | [HTTP/JSON](http-api.md). `--addr`, `--auth-token`, `--gate-reads`, `--root`, `--cors-origin`, `--metrics`, and the request limits. Blocks. |
| `mcp` | [MCP over stdio](mcp.md). `--agent-name`, `--model`. |
| `mount <MOUNTPOINT>` | [FUSE](../guides/mounts.md). `--actor`. Blocks; needs root and `/dev/fuse`. |
| `nfs` | [NFSv3](../guides/mounts.md#nfs). `--addr`, `--actor`. Blocks. |
| `sandbox -- <CMD>` | Run over a copy-on-write view, then import. `--actor`, `--discard`, `--isolate`. |
| `overlay -- <CMD>` | Run in a live overlay, streaming changes in. `--actor`, `--sync-ms`, `--isolate`. |

`mount`, `nfs`, `sandbox` and `overlay` are Unix-only. On Windows they still
appear in `--help` and exit with an explanation, rather than disappearing.

## Maintenance

| Command | Notes |
|---|---|
| `gc` | Reclaim unreachable content. Safe alongside writers. |
| `repack` | Compact a packed content store. |
| `flush` | Seal buffered writes. |
| `backup <DEST>` | Snapshot the **metadata** store. Refuses to overwrite. |
| `dump [OUT]` / `load [IN]` | Engine-independent JSON Lines. `load` needs a pristine workspace. |
| `fsck` | Report what could be recovered from content. `--rebuild` to actually do it. |
| `migrate` | Apply pending schema migrations. |
| `schema-version` | This workspace's version, and the newest the binary knows. |

See [Backup and recovery](../guides/backup-and-recovery.md).

## Environment variables

| Variable | Effect |
|---|---|
| `ORIGOFS_ACTOR` | Default `--actor` for every attributed command. |
| `ORIGOFS_ENCRYPTION_KEY` | Opt a workspace into encryption at rest. Must match on every open. |
| `ORIGOFS_AUTH_TOKENS` | `serve` bearer mappings, keeping them out of `ps` and shell history. |
| `ORIGOFS_LOG` / `RUST_LOG` | Log level filter. Default `info`. |
| `ORIGOFS_METRICS` | `1` is equivalent to `serve --metrics`. |

The full list, including the tuning and Postgres variables, is in
[Configuration](configuration.md#environment-variables).
