#![no_main]
//! Fuzz the chunk-manifest ("blob object") decoder. Arbitrary bytes must never
//! panic, abort, or OOM — a corrupt or hostile manifest has to surface as a
//! clean `Err`. This guards the anti-OOM cross-check in `Manifest::decode`: a
//! hostile `size`/`count` must not drive a multi-GB pre-allocation. (B3, #70)
//!
//! `Manifest::decode` enforces an exact length (`len == header + count*entry`),
//! so decoding is a bijection over the accepted set: anything that decodes must
//! re-encode to the identical bytes.

use libfuzzer_sys::fuzz_target;
use origofs_core::Manifest;

fuzz_target!(|data: &[u8]| {
    if let Ok(m) = Manifest::decode(data) {
        // `encode` is fallible since it grew an overflow guard on the chunk count.
        // Anything that *decoded* is inside that guard by construction — the count
        // came from a `u32` — so a failure here is itself a bug worth catching.
        let encoded = m
            .encode()
            .expect("a manifest that decoded must re-encode: encode rejected its own input");
        assert_eq!(
            encoded, data,
            "manifest decode∘encode must round-trip exactly"
        );
    }
});
