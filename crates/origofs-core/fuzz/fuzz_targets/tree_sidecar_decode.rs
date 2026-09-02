#![no_main]
//! Fuzz the **tree** co-edit sidecar framing
//! (`ORGX | version | root-len | root | hash | state`, plus the pre-versioning
//! `2 | …`).
//!
//! The richer of the two framings, and the one worth fuzzing most: it reads a
//! length byte out of the input and slices by it, then decodes that slice as
//! UTF-8. Every one of those steps is bounds-checked by construction — which is
//! a claim, and this is what tests the claim against inputs nobody thought of.
//!
//! It matters more than the flat shape because of what a *silent* failure costs
//! here: an unparseable tree sidecar opens an **empty** document, and a host that
//! does not check `resumed()` then checkpoints that emptiness over a real file.

use libfuzzer_sys::fuzz_target;
use origofs_core::fuzz_support::parse_tree_sidecar;

fuzz_target!(|data: &[u8]| {
    if let Ok(Some((root, hash, _state))) = parse_tree_sidecar(data) {
        assert_eq!(hash.len(), 32, "a parsed tree sidecar must yield a 32-byte hash");
        // The root name is length-prefixed by a single byte, so it can never
        // exceed 255 — `frame_tree_sidecar` refuses to write a longer one.
        assert!(
            root.len() <= 255,
            "root name {root:?} is longer than its length prefix can encode"
        );
    }
});
