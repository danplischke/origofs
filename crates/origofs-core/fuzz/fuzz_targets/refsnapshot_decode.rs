#![no_main]
//! Fuzz the `RefSnapshot` decoder — the ref-table mirror written into the
//! content store for bare-bucket recovery, so hostile bytes here would poison
//! `fsck --rebuild`. Arbitrary input must never panic, abort, or OOM (a crafted
//! `count` must not force a huge pre-allocation), and any snapshot that decodes
//! must survive an encode→decode fixpoint. (B3, #70)

use libfuzzer_sys::fuzz_target;
use origofs_core::RefSnapshot;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = RefSnapshot::decode(data) {
        let re = s.encode();
        let s2 = RefSnapshot::decode(&re).expect("re-encoded ref snapshot must decode");
        assert_eq!(s, s2, "ref-snapshot encode→decode must be idempotent");
    }
});
