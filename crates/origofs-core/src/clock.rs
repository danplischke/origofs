//! A clock seam so wall-clock time can be *injected*.
//!
//! This is the prerequisite for deterministic simulation testing (DST): a seeded
//! run must produce identical timestamps every time, and thus identical
//! commit/object hashes (a commit embeds its timestamp), so a single seed
//! reproduces an entire run exactly. Production uses [`SystemClock`]; the
//! simulation harness injects a deterministic clock.
//!
//! Only the *engine-layer* timestamps (commits, edit-ops, events, presence,
//! locks, sessions) flow through this seam — the metadata-store backends
//! (`SqliteMetadataStore`, `PostgresMetadataStore`) still read the wall clock for
//! their own internal bookkeeping, which is out of scope for a trait-seam
//! simulation of the engine.

/// A source of wall-clock time, in whole seconds since the Unix epoch.
pub trait Clock: Send + Sync {
    /// The current time, in whole seconds since the Unix epoch.
    fn now_secs(&self) -> i64;
}

/// The real wall clock — the production default.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_secs(&self) -> i64 {
        crate::util::now_secs()
    }
}
