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
