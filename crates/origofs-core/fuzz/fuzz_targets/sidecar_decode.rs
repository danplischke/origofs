#![no_main]
//! Fuzz the **flat** co-edit sidecar framing (`ORGY | version | hash | state`,
//! plus the pre-versioning `1 | hash | state`).
//!
//! A sidecar is bytes read back out of the content store, so this is the same
//! kind of boundary as the four object decoders beside it — and unlike them it
//! is reached on a path that treats *unreadable* as *absent*, which is why the
//! framing carries a version at all (#140). Arbitrary bytes must never panic:
//! the failure this guards is a corrupt or truncated sidecar taking down the
//! room that opened it, rather than being reported.
//!
//! The one structural promise callers depend on is the split point — everything
//! after the 32-byte coherence hash is the ydoc state — so a successful parse
//! must hand back exactly 32 hash bytes. (#141/#142 follow-up)

use libfuzzer_sys::fuzz_target;
use origofs_core::fuzz_support::parse_flat_sidecar;

fuzz_target!(|data: &[u8]| {
    if let Ok(Some((hash, _state))) = parse_flat_sidecar(data) {
        assert_eq!(
            hash.len(),
            32,
            "a parsed sidecar must yield a 32-byte BLAKE3 coherence hash"
        );
    }
});
