//! Small internal helpers.

/// Current wall-clock time in whole seconds since the Unix epoch.
pub(crate) fn now_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Run a synchronous, CPU- or IO-bound section without stalling the async runtime.
///
/// Some work in this crate genuinely blocks: rusqlite (the connection mutex,
/// `busy_timeout` waits, the WAL fsync a commit ends with) and content-defined
/// chunking plus per-chunk BLAKE3 hashing. Run bare on a multi-thread runtime,
/// each of those takes a worker thread out of service for its duration, so a
/// handful of concurrent callers can starve every other task in the process.
/// [`tokio::task::block_in_place`] hands the worker's queued tasks to another
/// worker first, so blocking costs a thread rather than a share of throughput.
///
/// `spawn_blocking` would be the other option, but it requires a `'static + Send`
/// closure, which would mean copying every borrowed argument across the boundary
/// on every call — for chunking that means copying the whole body. `block_in_place`
/// gives the same scheduler cooperation while letting these bodies keep borrowing.
///
/// It panics on a `current_thread` runtime, where there is no other worker to hand
/// work to, so the flavor is checked and the closure runs inline there — which is
/// exactly what a single-threaded runtime would do anyway. The check also covers
/// being called from a `spawn_blocking` task, from a plain thread with an entered
/// handle, and from no runtime at all.
pub(crate) fn blocking_section<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}
