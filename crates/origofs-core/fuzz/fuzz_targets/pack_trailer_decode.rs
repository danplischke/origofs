#![no_main]
//! Fuzz the pack **trailer** decoder.
//!
//! The footer is `trailer_len(u32) ‖ ORGP ‖ version`, and the trailer it points
//! back into is a run of 36-byte `(hash, len)` records. `repack` is the only
//! caller, so a malformed pack is met on the maintenance path rather than a read
//! — which makes a panic here worse, not better: it takes down the operation that
//! reclaims space, on data the operator cannot inspect.
//!
//! Every input this accepts must have consistent offsets. The decoder computes
//! each record's offset by accumulating the previous lengths with a checked add,
//! so a trailer it accepts can never describe a chunk that starts before the one
//! in front of it.

use libfuzzer_sys::fuzz_target;
use origofs_core::pack::fuzz_entry::decode_trailer;

fuzz_target!(|data: &[u8]| {
    if let Ok(entries) = decode_trailer(data) {
        let mut expected: u64 = 0;
        for (_, offset, len) in entries {
            assert_eq!(
                u64::from(offset),
                expected,
                "an accepted trailer must describe contiguous, ascending offsets"
            );
            expected += u64::from(len);
        }
    }
});
