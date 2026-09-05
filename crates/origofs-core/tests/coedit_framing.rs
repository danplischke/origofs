//! A y-sync payload this server cannot act on is reported rather than dropped
//! (issue #162).
//!
//! `handle_sync` speaks the **y-websocket** envelope: an outer message tag wrapping
//! the y-sync payload. A client written against the y-sync protocol *directly*
//! sends a bare frame instead, and the first byte of a bare y-sync update is
//! `messageYjsUpdate` = 2 — which is `messageAuth` in the outer envelope. It
//! decodes cleanly, carries nothing this server acts on, and used to be dropped
//! without a word.
//!
//! The failure that produces is the expensive one: the socket connects, the
//! handshake completes, `sync_start` returns, awareness works, the peer count is
//! right, and the document simply never converges — with nothing anywhere to
//! attribute it to. "No error" reads as "my frames are fine, the problem is
//! elsewhere".
#![cfg(feature = "coedit")]

use origofs_core::{CoeditDoc, WriteCtx};
use yrs::encoding::write::Write as _;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};

fn ctx() -> WriteCtx {
    WriteCtx::session(1, 1)
}

/// The bytes a y-websocket client sends: `messageSync(0)`, then the y-sync payload.
fn enveloped(update: &[u8]) -> Vec<u8> {
    let mut e = EncoderV1::new();
    yrs::sync::Message::Sync(yrs::sync::SyncMessage::Update(update.to_vec())).encode(&mut e);
    e.to_vec()
}

/// The bytes a client written against y-sync directly sends: no envelope, so the
/// frame starts with `messageYjsUpdate` = 2.
fn bare(update: &[u8]) -> Vec<u8> {
    let mut e = EncoderV1::new();
    e.write_var(2u8);
    e.write_buf(update);
    e.to_vec()
}

/// An update carrying some text, produced by an ordinary Yjs replica.
fn some_update() -> Vec<u8> {
    let peer = CoeditDoc::new();
    peer.insert(ctx(), 0, "hello");
    peer.state_update()
}

#[tokio::test]
async fn a_bare_y_sync_frame_is_reported_instead_of_silently_ignored() {
    let update = some_update();
    let doc = CoeditDoc::new();
    let out = doc.handle_sync(ctx(), &bare(&update)).unwrap();

    // Still not an error, and still not applied — the contract has not changed.
    assert!(!out.content_changed);
    assert_eq!(doc.text(), "", "a bare frame carries no envelope to act on");
    // ...but the caller can now see that, and the byte says which tag arrived.
    assert_eq!(
        out.unhandled,
        vec![2],
        "the first byte of a bare y-sync update is `messageAuth` in the envelope"
    );
}

#[tokio::test]
async fn the_same_update_inside_the_envelope_lands() {
    let update = some_update();
    let doc = CoeditDoc::new();
    let out = doc.handle_sync(ctx(), &enveloped(&update)).unwrap();

    assert!(out.content_changed);
    assert_eq!(doc.text(), "hello");
    assert!(
        out.unhandled.is_empty(),
        "a well-framed payload reports nothing: {:?}",
        out.unhandled
    );
}

/// The tree shape shares `drive_sync`, so it reports identically. Pinned because
/// the two `handle_sync` methods are separate entry points and a future divergence
/// here would be silent in exactly the way this issue is about.
#[tokio::test]
async fn the_tree_shape_reports_the_same_way() {
    use origofs_core::CoeditTreeDoc;
    let update = some_update();
    let doc = CoeditTreeDoc::new("content");
    let out = doc.handle_sync(ctx(), &bare(&update)).unwrap();
    assert_eq!(out.unhandled, vec![2]);
    assert!(!out.content_changed);
}

/// A custom tag is reported as itself, not folded into a single "something was
/// ignored" flag — a host debugging a client needs the byte it actually sent.
#[tokio::test]
async fn a_custom_tag_is_reported_as_the_byte_that_arrived() {
    let mut e = EncoderV1::new();
    e.write_var(42u8);
    e.write_buf(b"whatever");
    let doc = CoeditDoc::new();
    let out = doc.handle_sync(ctx(), &e.to_vec()).unwrap();
    assert_eq!(out.unhandled, vec![42]);
}

/// Awareness is *handled* (relayed to the room), so it must not be counted —
/// every real Yjs client emits it constantly and a warning per heartbeat would
/// bury the signal this exists to give.
#[tokio::test]
async fn relayed_awareness_is_not_counted_as_unhandled() {
    let doc = CoeditDoc::new();
    let mut e = EncoderV1::new();
    yrs::sync::Message::AwarenessQuery.encode(&mut e);
    let out = doc.handle_sync(ctx(), &e.to_vec()).unwrap();
    assert!(!out.broadcast.is_empty(), "awareness is relayed");
    assert!(out.unhandled.is_empty(), "{:?}", out.unhandled);
}

/// A hostile frame must not make the report grow with it.
///
/// These bytes come off a socket this module explicitly does not trust, and one
/// frame may pack thousands of messages — the API's cap is 16 MiB, which holds
/// roughly eight million two-byte ones. Recording an entry per *message* would let
/// a client make the server allocate half the frame again and, worse, render all
/// of it into a single `warn!` line. The report is the *set* of tags, so it is
/// bounded by 256 whatever arrives.
#[tokio::test]
async fn a_frame_packed_with_unhandled_messages_reports_a_bounded_set() {
    const N: usize = 5_000;
    let mut e = EncoderV1::new();
    for _ in 0..N {
        // `Message::Custom` through `encode`, so the frame stays in sync: this is
        // about the size of the report, not about recovering from a malformed
        // stream (the test below covers that).
        yrs::sync::Message::Custom(42, b"payload".to_vec()).encode(&mut e);
    }
    let doc = CoeditDoc::new();
    let out = doc.handle_sync(ctx(), &e.to_vec()).unwrap();

    assert_eq!(
        out.unhandled,
        vec![42],
        "one entry per tag, not per message"
    );
    assert!(!out.content_changed);
}

/// A frame the reader **desynchronises** on is bounded the same way, and this is
/// the realistic shape of the bug #162 is about rather than a contrived one.
///
/// A bare y-sync stream decodes as `messageAuth`, which reads a permission byte
/// and — unless it is `PERMISSION_DENIED` — stops, leaving the rest of that
/// message in the buffer. The reader then resumes mid-payload and every following
/// byte is read as a fresh message tag. So a bare frame does not merely fail to
/// apply: it fragments into junk messages, one per surviving byte. That is exactly
/// the case where an entry-per-message report would track the frame's size.
#[tokio::test]
async fn a_desynchronised_frame_is_bounded_too() {
    let mut e = EncoderV1::new();
    for _ in 0..2_000 {
        e.write_var(2u8); // messageAuth — consumes less than was written
        e.write_buf(b"aaaaaaaa");
    }
    let doc = CoeditDoc::new();
    // Junk may or may not decode all the way through; either answer is fine, and
    // what must hold is that a report we *do* produce cannot grow with the input.
    if let Ok(out) = doc.handle_sync(ctx(), &e.to_vec()) {
        assert!(
            out.unhandled.len() <= 256,
            "a tag set cannot exceed the byte's range, got {}",
            out.unhandled.len()
        );
        let mut sorted = out.unhandled.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), out.unhandled.len(), "tags must be distinct");
        assert!(!out.content_changed);
    }
}
