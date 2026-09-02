//! A malformed y-sync update must come back as an error, never take the process
//! down (#144). Today it does not, and these tests are `#[ignore]`d because of it.
//!
//! `CoeditDoc::apply_update_as` is what `handle_sync` feeds bytes from a
//! co-editing WebSocket client, and `coedit.rs`'s own module docs set the trust
//! boundary: "The host is trusted; the clients editing through it are not." So
//! every byte string reaching the decoder is attacker-chosen, and the only
//! acceptable outcomes are `Ok` and `Err`.
//!
//! `yrs` has a third. It builds a `str` from unvalidated bytes while decoding
//! (`encoding/read.rs`, `updates/decoder.rs`) and then iterates it in
//! `block::utf16_len`, which is undefined behaviour: an abort under the debug UB
//! checks, silent in release. The 51-byte input below reaches it through the
//! public API.
//!
//! # Why these are ignored rather than fixed or deleted
//!
//! The abort is a **non-unwinding** panic, so `catch_unwind` cannot contain it and
//! a running test binary cannot survive it — left enabled, these would take the
//! whole suite down rather than fail it. Validating the bytes before yrs sees them
//! would mean reimplementing the decoder, since the length prefixes that say where
//! the strings are can only be found by decoding.
//!
//! Upgrading does not help, which was measured rather than assumed: the reproducer
//! aborts identically on 0.24.0, 0.25.0 and 0.26.0, and both `from_utf8_unchecked`
//! sites are unchanged in 0.27.4 — the latest release, which additionally does not
//! compile on stable Rust (it uses `if let` guards). So the pin stays at 0.23.5.
//!
//! **Run them when re-testing a candidate yrs**, which is the one thing that
//! changes this: `cargo test -p origofs-core --features coedit --test
//! coedit_malformed_update -- --ignored`. A clean pass means the class is closed
//! and the `#[ignore]`s come off in the same change that moves the pin.
#![cfg(feature = "coedit")]

use origofs_core::CoeditDoc;

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap())
        .collect()
}

#[test]
#[ignore = "aborts the test binary on yrs 0.23.5 (#144); run with --ignored when trying a new yrs"]
fn a_malformed_update_is_an_error_not_undefined_behaviour() {
    let bytes = unhex(
        "f57896ad4347334444c9c5120f84ab25d6931623ee3bb630718b6cfe6eb9c32627ae9275d7e99125bbe2602f46854dbabeed2d",
    );
    assert_eq!(bytes.len(), 51, "the recorded reproducer is 51 bytes");
    // Reaching the next line at all is the assertion: today this call aborts.
    let _ = CoeditDoc::load(&bytes);
}

/// The same guarantee over a spread of inputs, so a fix narrow to the one recorded
/// string does not read as the whole class being closed.
#[test]
#[ignore = "aborts the test binary on yrs 0.23.5 (#144); run with --ignored when trying a new yrs"]
fn arbitrary_bytes_never_take_the_process_down() {
    let mut state = 0x243f_6a88_85a3_08d3u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..2000 {
        let len = (next() % 96) as usize;
        let buf: Vec<u8> = (0..len).map(|_| (next() & 0xff) as u8).collect();
        let _ = CoeditDoc::load(&buf);
    }
}
