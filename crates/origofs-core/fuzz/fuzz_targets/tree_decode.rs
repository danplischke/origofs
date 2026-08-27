#![no_main]
//! Fuzz the directory-`tree` object decoder. Arbitrary bytes must never panic,
//! abort, or OOM — a crafted `count` must not force a huge pre-allocation. Any
//! successfully decoded tree must re-encode to bytes that decode back to the
//! same structure (idempotence): `Tree::decode` tolerates trailing bytes, so we
//! assert the encode→decode fixpoint rather than an exact byte round-trip.
//! (B3, #70)

use libfuzzer_sys::fuzz_target;
use origofs_core::Tree;

fuzz_target!(|data: &[u8]| {
    if let Ok(t) = Tree::decode(data) {
        // Anything that decoded came from bytes whose fields already fit the
        // format, so re-encoding it cannot overflow a length field.
        let re = t.encode().expect("a decoded tree must re-encode");
        let t2 = Tree::decode(&re).expect("re-encoded tree must decode");
        assert_eq!(t, t2, "tree encode→decode must be idempotent");
    }
});
