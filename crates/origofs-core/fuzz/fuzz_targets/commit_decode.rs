#![no_main]
//! Fuzz the `commit` object decoder. Arbitrary bytes must never panic, abort,
//! or OOM — a crafted parent/field count must not force a huge pre-allocation.
//! Any successfully decoded commit must survive an encode→decode fixpoint.
//! (B3, #70)

use libfuzzer_sys::fuzz_target;
use origofs_core::Commit;

fuzz_target!(|data: &[u8]| {
    if let Ok(c) = Commit::decode(data) {
        let re = c.encode();
        let c2 = Commit::decode(&re).expect("re-encoded commit must decode");
        assert_eq!(c, c2, "commit encode→decode must be idempotent");
    }
});
