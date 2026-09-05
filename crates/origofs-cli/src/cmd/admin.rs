//! Operating a workspace: storage reclamation, schema, backup, recovery, trash and the durable POSIX-lock table.

use crate::*;

pub async fn dump(ws: &Workspace, out: String) -> Result<()> {
    let n = if out == "-" {
        let stdout = std::io::stdout();
        ws.dump(std::io::BufWriter::new(stdout.lock())).await?
    } else {
        let f = std::fs::File::create(&out)?;
        let n = ws.dump(std::io::BufWriter::new(f)).await?;
        println!("dumped {n} records to {out}");
        n
    };
    let _ = n;

    Ok(())
}

pub async fn load(ws: &Workspace, input: String) -> Result<()> {
    let report = if input == "-" {
        let stdin = std::io::stdin();
        ws.load(std::io::BufReader::new(stdin.lock())).await?
    } else {
        let f = std::fs::File::open(&input)?;
        ws.load(std::io::BufReader::new(f)).await?
    };
    println!(
        "restored {} rows (dump taken at schema v{})",
        report.total_rows(),
        report.source_schema_version
    );
    for (table, n) in &report.tables {
        println!("  {table}: {n}");
    }
    // The single most likely way to be confused by a successful load: the
    // names and the blame are all here, and every read fails, because the
    // bytes were never in the dump. Say so at the moment it matters rather
    // than letting the user meet `content missing for hash ...` cold.
    println!(
        "note: this restored metadata only. File bytes live in the content \
             store, which a dump references by hash and does not carry — point \
             this workspace at the same content store, or reads will fail."
    );
    if !report.skipped_tables.is_empty() {
        // A dump from a newer build may carry tables this one does not
        // know. Skipping is deliberate (see `Fs::load`), but silence
        // would let a partial restore look complete.
        println!(
            "  skipped unknown tables: {}",
            report.skipped_tables.join(", ")
        );
    }

    Ok(())
}

pub async fn trash(ws: &Workspace, cmd: TrashCmd) -> Result<()> {
    run_trash(ws, cmd).await?;
    Ok(())
}

pub async fn posix_locks(
    ws: &Workspace,
    setting: Option<String>,
    path: Option<String>,
) -> Result<()> {
    if let Some(path) = path {
        let held = ws.posix_locks(&path).await?;
        if held.is_empty() {
            // Distinguishes "nothing holds this" from "not collecting",
            // the same way `trash list` has to.
            let on = ws.posix_locks_enabled().await?;
            println!(
                "no advisory locks on {path} (locking is {})",
                if on { "on" } else { "off" }
            );
        } else {
            for l in held {
                let end = if l.end == origofs_sdk::posixlock::LOCK_EOF {
                    "EOF".to_string()
                } else {
                    l.end.to_string()
                };
                println!(
                    "{}\t{}-{}\tpid {}\towner {}\tmount {}",
                    if l.exclusive { "WRITE" } else { "READ " },
                    l.start,
                    end,
                    l.pid,
                    l.owner,
                    l.holder
                );
            }
        }
    } else {
        match setting.as_deref() {
            None => {
                let on = ws.posix_locks_enabled().await?;
                println!("posix-locks is {}", if on { "on" } else { "off" });
            }
            Some(v) => {
                let on = match v {
                    "on" | "true" | "1" => true,
                    "off" | "false" | "0" => false,
                    other => {
                        return Err(origofs_sdk::OrigoFSError::InvalidArgument(format!(
                            "unknown setting {other:?} (expected `on` or `off`)"
                        ))
                        .into());
                    }
                };
                ws.set_posix_locks_enabled(on).await?;
                println!("posix-locks is {}", if on { "on" } else { "off" });
            }
        }
    }

    Ok(())
}

pub async fn gc(ws: &Workspace) -> Result<()> {
    let stats = ws.gc().await?;
    println!(
        "gc: kept {} object(s), deleted {} ({} bytes freed)",
        stats.reachable, stats.deleted, stats.bytes_freed
    );
    if stats.skipped_young > 0 {
        println!(
            "  {} unreferenced object(s) left for now: younger than the {}s grace \n  period, which is what keeps a collection safe alongside live writers",
            stats.skipped_young,
            origofs_sdk::DEFAULT_GC_GRACE_SECS
        );
    }
    if stats.skipped_undated > 0 {
        println!(
            "  warning: {} unreferenced object(s) could not be dated by this content \n  backend, so they were left alone — this store cannot be collected safely",
            stats.skipped_undated
        );
    }

    Ok(())
}

pub async fn repack(ws: &Workspace) -> Result<()> {
    let freed = ws.repack().await?;
    println!("repack: {freed} bytes reclaimed");

    Ok(())
}

pub async fn flush(ws: &Workspace) -> Result<()> {
    {
        ws.flush().await?;
        println!("flushed buffered writes to durable storage");
    }
    // `Migrate` and `SchemaVersion` are handled before the workspace is
    // opened — see `run_schema_cmd`. Opening is what migrates, so a report
    // produced from `ws` could only ever describe the state this process just
    // created.;
    Ok(())
}

pub async fn backup(ws: &Workspace, dest: PathBuf) -> Result<()> {
    let what = ws.backup_metadata(&dest).await?;
    println!("{what}");
    println!(
        "note: this is the metadata store only — content lives in the content store \n  and is already durable there. Blame, the audit log, actors, and uncommitted \n  edits exist ONLY in this file."
    );

    Ok(())
}

pub async fn fsck(ws: &Workspace, rebuild: bool) -> Result<()> {
    let report = if rebuild {
        ws.rebuild().await?
    } else {
        ws.scan().await?
    };
    let corrupt = if report.corrupt > 0 {
        format!(", {} corrupt", report.corrupt)
    } else {
        String::new()
    };
    println!(
        "fsck: scanned {} object(s){corrupt}, found {} commit(s)",
        report.objects_scanned, report.commits_found
    );
    // Only reachable on the dry run: `--rebuild` refuses outright when an
    // object it can't read would change what gets restored.
    if report.unsupported > 0 {
        let kinds = report
            .unsupported_kinds
            .iter()
            .map(|(kind, v)| format!("{kind} v{v}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "  WARNING: {} object(s) written by a NEWER origofs ({kinds}) — \
                 upgrade origofs before rebuilding, or history it can't read will be lost",
            report.unsupported
        );
    }
    if report.branches.is_empty() {
        println!("  no commits to recover (empty or non-versioned workspace)");
    } else {
        let src = if report.used_mirror {
            "ref mirror"
        } else {
            "inferred heads"
        };
        println!("  {} branch(es) via {src}:", report.branches.len());
        for (name, hex) in &report.branches {
            let tip = &hex[..hex.len().min(12)];
            let head = if report.checked_out.as_deref() == Some(name) {
                "  (HEAD)"
            } else {
                ""
            };
            println!("    {name}\t{tip}{head}");
        }
    }
    if rebuild {
        if let Some(branch) = &report.checked_out {
            println!(
                "  rebuilt working tree @ {branch}: {} dir(s), {} file(s), {} symlink(s)",
                report.dirs, report.files, report.symlinks
            );
        }
        println!("  note: blame/attribution is not recoverable (DB-only)");
    } else {
        println!("  (dry run — pass --rebuild to restore the metadata DB)");
    }

    Ok(())
}

pub async fn bench(
    ws: &Workspace,
    dir: String,
    files: usize,
    size: u64,
    seed: Option<u64>,
    keep: bool,
    force: bool,
) -> Result<()> {
    let mut opts = origofs_sdk::BenchOpts::new();
    opts.dir = dir;
    opts.files = files;
    opts.file_size = size;
    opts.seed = seed.unwrap_or(opts.seed);
    opts.keep = keep;
    opts.force = force;
    print_bench(&ws.bench(&opts).await?);

    Ok(())
}
