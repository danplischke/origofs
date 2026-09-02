#![no_main]
//! Fuzz the **CRDT state** decode that a sidecar's payload feeds.
//!
//! This is the one that reaches third-party code: the bytes after a sidecar's
//! framing go to `yrs`'s `Update::decode_v1`. The framing above can be proved
//! total by reading it; this cannot, and a panic in it is not a decode error the
//! caller handles — it takes down the worker that opened the document.
//!
//! Both shapes are driven, because they build different `yrs` types from the same
//! opaque bytes: a `Y.Text` document and a `Y.XmlFragment` one.
//!
//! An `Err` is a perfectly good outcome — the point is that a malformed state is
//! *reported* rather than fatal.
//!
//! # Known finding — this target currently fails on purpose
//!
//! `yrs 0.23.5` (the pinned version) builds a `str` from unvalidated bytes while
//! decoding a malformed update and then iterates it in `block::utf16_len`, which
//! is undefined behaviour: it aborts under the debug UB checks and is silent in
//! release. A 51-byte input is enough. That is reachable from the y-sync
//! WebSocket, whose clients this codebase explicitly does not trust, so it is a
//! real exposure rather than a corrupt-file curiosity.
//!
//! **There is no version to upgrade to.** Checked, rather than assumed: the
//! reproducer aborts identically on 0.24.0, 0.25.0 and 0.26.0, and the two
//! `from_utf8_unchecked` sites behind it (`encoding/read.rs`,
//! `updates/decoder.rs`) are unchanged in 0.27.4, the latest release — which
//! additionally does not build on stable Rust at all, as it uses `if let` guards.
//! So expect this target to abort. That is it working, and it will keep working
//! until upstream validates those bytes.

use libfuzzer_sys::fuzz_target;
use origofs_core::{CoeditDoc, CoeditTreeDoc};

fuzz_target!(|data: &[u8]| {
    let _ = CoeditDoc::load(data);
    let _ = CoeditTreeDoc::load("content", data);
});
