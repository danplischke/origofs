//! Bounded retry for transient backend failures (`docs/DESIGN.md` §7).
//!
//! Postgres raises `40001` (serialization failure) and `40P01` (deadlock
//! detected) as a matter of course under concurrency, and SQLite raises
//! `SQLITE_BUSY` once its `busy_timeout` is exhausted. All three mean the same
//! thing: *this transaction did not happen; run it again*. They are not a report
//! about the caller's request, and surfacing them is how a normal multi-writer
//! workload turns into user-visible errors.
//!
//! [`OrigoFSError::retryable`] has classified these since M4, but nothing acted on
//! it outside the HTTP layer's status mapping — even `tests/concurrency.rs`
//! implemented its own retry loop, which is the tell that the engine owed one.
//!
//! # What may be wrapped
//!
//! **A whole logical operation, never a fragment of one.** Retrying is only sound
//! because a rolled-back `MetaTxn` leaves no metadata behind, so re-running the
//! operation from the top starts from the same state the first attempt saw.
//! Content writes that happened before the transaction are content-addressed and
//! idempotent, so they cost bytes on a retry and nothing else.
//!
//! Conversely, an operation that has *already* committed metadata must not be
//! retried, and a fragment of one must never be: re-running half a merge is how a
//! retry loop invents a torn state that no crash could have produced.
//!
//! # What is not retried
//!
//! Application-level conflicts. `Conflict` — a `cas_ref` that lost, a suggestion
//! whose base moved — is a real answer about the caller's request, and retrying it
//! would silently paper over the concurrent change the caller needs to see.

use crate::error::Result;
use std::time::Duration;

/// Attempts, including the first. Five attempts with the backoff below spans
/// roughly a second, which comfortably outlasts the lock waits these errors come
/// from without turning a genuinely wedged backend into a long stall.
const MAX_ATTEMPTS: u32 = 5;
/// Base backoff; doubles per attempt (2ms, 4ms, 8ms, 16ms).
const BASE_BACKOFF: Duration = Duration::from_millis(2);

/// Pseudo-jitter, so writers that collided once do not line up and collide again
/// on the same schedule. The clock's sub-millisecond digits are enough entropy for
/// spreading retries and save a `rand` dependency in the engine.
fn jitter(backoff: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    // Up to +50%.
    backoff + backoff.mul_f64(0.5 * (nanos % 1000) as f64 / 1000.0)
}

/// Run `op` until it succeeds, fails with something not worth retrying, or runs
/// out of attempts. `what` names the operation in the retry log line.
///
/// `op` is a closure returning a *fresh* future each call rather than a future to
/// poll again, because a retry has to re-run the operation from the top — see the
/// module docs for what that means for where this may be used.
pub(crate) async fn retrying<T, F, Fut>(what: &'static str, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut backoff = BASE_BACKOFF;
    for attempt in 1..=MAX_ATTEMPTS {
        match op().await {
            Err(e) if e.retryable() && attempt < MAX_ATTEMPTS => {
                tracing::warn!(
                    op = what,
                    attempt,
                    max_attempts = MAX_ATTEMPTS,
                    error = %e,
                    "backend asked to retry the transaction",
                );
                crate::metrics::record_retry(what);
                tokio::time::sleep(jitter(backoff)).await;
                backoff *= 2;
            }
            // Includes the final attempt's retryable error: the caller gets the
            // real backend error, still classified `retryable`, rather than a
            // wrapper that hides what happened.
            other => return other,
        }
    }
    unreachable!("the loop returns on the final attempt")
}
