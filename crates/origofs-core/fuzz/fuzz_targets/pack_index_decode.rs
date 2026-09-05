#![no_main]
//! Fuzz the pack **index-entry** decoder.
//!
//! An index entry is `ORGI ‖ version ‖ pack(32) ‖ offset(4) ‖ len(4) ‖
//! addressing(1)`, read back out of an object store — the same
//! `&[u8] -> Result<_>` shape as the manifest/tree/commit decoders, and the one
//! self-describing format that had no target. Arbitrary bytes must surface as a
//! clean `Err`, never a panic: the decoder does window arithmetic
//! (`body[32..36]`, `body[36..40]`, `body[40]`) behind a length check, and
//! `docs/LIMITS.md` records that this exact family of code shipped a corruption
//! bug once already.

use libfuzzer_sys::fuzz_target;
use origofs_core::pack::fuzz_entry::decode_index_entry;

fuzz_target!(|data: &[u8]| {
    let _ = decode_index_entry(data);
});
