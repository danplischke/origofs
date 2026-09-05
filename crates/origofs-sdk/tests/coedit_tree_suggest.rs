//! Tree-shaped co-edit proposals (issues #75 §3.2, #92).
//!
//! The flat shape has had `suggest_coedit` since the CRDT review path landed:
//! a propose-only actor edits a throwaway replica and records the Yjs update, and
//! accepting merges rather than overwriting. The tree shape had no counterpart —
//! so on the document shape a rich-text editor actually uses, a propose-only
//! actor could not reach the review queue at all. Its only options were a byte
//! suggestion, whose base goes stale on every keystroke elsewhere in the file and
//! whose acceptance discards concurrent work, or nothing.
//!
//! The interesting half is acceptance. origofs does not own the document schema,
//! so it cannot turn an `XmlFragment` back into a file — the same reason
//! `checkpoint_coedit_tree` takes the host's bytes. So `accept_suggestion`
//! **refuses** a tree proposal and says what to call instead, rather than
//! applying a tree update to a flat `Y.Text` document and producing a file
//! nobody can read.
#![cfg(feature = "coedit")]

use origofs_sdk::{CoeditTreeDoc, OrigoFSError, Perms, TreeSpan, Workspace, WriteCtx};

const ROOT: &str = "content";

struct Fixture {
    ws: Workspace,
    owner: i64,
    agent: i64,
    _dir: tempfile::TempDir,
}

/// `/doc.md` is a live tree document owned by `owner`; `agent` may only propose.
async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let owner = ws.create_human("owner", None).await.unwrap();
    let agent = ws.create_agent("agent", "opus", Some(owner)).await.unwrap();
    ws.grant(owner, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    ws.grant(agent, "/", Perms::READ | Perms::PROPOSE, Some(owner))
        .await
        .unwrap();
    ws.set_acl_default_deny(true).await.unwrap();

    // Seed the document the way a host does: open, append, check point the bytes
    // it serialized. That is what leaves a sidecar for a proposer to resume from.
    let octx = WriteCtx::actor(owner);
    let doc = ws.open_coedit_tree(octx, "/doc.md", ROOT).await.unwrap();
    doc.append_text(octx, "p", "hello\n");
    ws.checkpoint_coedit_tree(octx, "/doc.md", &doc, b"hello\n", &[] as &[TreeSpan])
        .await
        .unwrap();
    ws.end_coedit("/doc.md").await.unwrap();

    Fixture {
        ws,
        owner,
        agent,
        _dir: dir,
    }
}

fn is_denied(e: &OrigoFSError) -> bool {
    matches!(e, OrigoFSError::Denied(_))
}

/// `CoeditTreeDoc` has no `Debug`, so `unwrap_err` is unavailable on a `Result`
/// carrying one. Assert on the error the tests actually care about instead.
fn denied_open(r: Result<origofs_sdk::CoeditTreeDoc, OrigoFSError>) {
    match r {
        Err(e) => assert!(is_denied(&e), "expected Denied, got {e:?}"),
        Ok(_) => panic!("expected the call to be refused"),
    }
}

#[tokio::test]
async fn a_propose_only_actor_can_propose_against_a_tree_document() {
    let f = fixture().await;
    let actx = WriteCtx::actor(f.agent);

    // The baseline this exists to fix: the write-shaped doors are all shut.
    denied_open(f.ws.open_coedit_tree(actx, "/doc.md", ROOT).await);
    denied_open(f.ws.load_coedit_tree_as(actx, "/doc.md", ROOT).await);

    // …and the propose-shaped one is open.
    let replica =
        f.ws.load_coedit_tree_to_propose(actx, "/doc.md", ROOT)
            .await
            .unwrap();
    assert!(
        replica.resumed(),
        "the proposer must see the document as it stands, not an empty one"
    );
    replica.append_text(actx, "p", "and a suggestion\n");

    let id =
        f.ws.suggest_coedit_tree(actx, "/doc.md", &replica, Some("add a line"), None)
            .await
            .unwrap();

    let s = f.ws.get_suggestion(id).await.unwrap().unwrap();
    assert_eq!(s.kind.as_str(), "crdt-tree");
    assert_eq!(s.actor_id, f.agent);
    assert_eq!(s.path, "/doc.md");

    // Nothing landed: a proposal is a proposal.
    assert_eq!(&f.ws.read("/doc.md").await.unwrap()[..], b"hello\n");
}

#[tokio::test]
async fn an_actor_with_neither_right_is_refused_the_proposal_path_too() {
    let f = fixture().await;
    let stranger = f.ws.create_agent("stranger", "opus", None).await.unwrap();
    let ctx = WriteCtx::actor(stranger);
    denied_open(f.ws.load_coedit_tree_to_propose(ctx, "/doc.md", ROOT).await);
}

#[tokio::test]
async fn the_review_diff_shows_the_effect_of_the_merge() {
    // A tree proposal's stored blobs are a state vector and an opaque update, so
    // neither addresses readable text. The diff is rendered from the *effect*:
    // the document's plain text now, versus with the proposal merged in.
    let f = fixture().await;
    let actx = WriteCtx::actor(f.agent);
    let replica =
        f.ws.load_coedit_tree_to_propose(actx, "/doc.md", ROOT)
            .await
            .unwrap();
    replica.append_text(actx, "p", "and a suggestion\n");
    let id =
        f.ws.suggest_coedit_tree(actx, "/doc.md", &replica, None, None)
            .await
            .unwrap();

    let patch = f.ws.suggestion_diff(id).await.unwrap();
    assert!(
        patch.contains("and a suggestion"),
        "the reviewer must see what is being proposed:\n{patch}"
    );
}

#[tokio::test]
async fn accept_suggestion_refuses_a_tree_proposal_and_says_what_to_call() {
    // The whole reason `CrdtTree` is a separate kind. Applying a tree update to a
    // flat `Y.Text` document — which one kind for both would have done — produces
    // a document nobody can read, so this refuses instead.
    let f = fixture().await;
    let actx = WriteCtx::actor(f.agent);
    let replica =
        f.ws.load_coedit_tree_to_propose(actx, "/doc.md", ROOT)
            .await
            .unwrap();
    replica.append_text(actx, "p", "proposed\n");
    let id =
        f.ws.suggest_coedit_tree(actx, "/doc.md", &replica, None, None)
            .await
            .unwrap();

    let e =
        f.ws.accept_suggestion(id, WriteCtx::actor(f.owner))
            .await
            .unwrap_err();
    assert!(
        e.to_string().contains("accept_coedit_tree_suggestion"),
        "the refusal has to name the call that works: {e}"
    );
    // Refused, not half-applied.
    assert_eq!(&f.ws.read("/doc.md").await.unwrap()[..], b"hello\n");
    assert_eq!(
        f.ws.get_suggestion(id)
            .await
            .unwrap()
            .unwrap()
            .status
            .as_str(),
        "pending"
    );
}

#[tokio::test]
async fn accepting_lands_the_host_bytes_attributed_to_the_author() {
    let f = fixture().await;
    let actx = WriteCtx::actor(f.agent);
    let replica =
        f.ws.load_coedit_tree_to_propose(actx, "/doc.md", ROOT)
            .await
            .unwrap();
    replica.append_text(actx, "p", "proposed\n");
    let id =
        f.ws.suggest_coedit_tree(actx, "/doc.md", &replica, None, None)
            .await
            .unwrap();

    // The reviewer's side: resume, merge, serialize (the host's job), land.
    let merged = f.ws.merge_coedit_tree_suggestion(id, ROOT).await.unwrap();
    assert!(merged.plain_text().contains("proposed"));
    let body = b"hello\nproposed\n";
    f.ws.accept_coedit_tree_suggestion(
        WriteCtx::actor(f.owner),
        id,
        &merged,
        body,
        &[] as &[TreeSpan],
    )
    .await
    .unwrap();

    assert_eq!(&f.ws.read("/doc.md").await.unwrap()[..], body);
    let s = f.ws.get_suggestion(id).await.unwrap().unwrap();
    assert_eq!(s.status.as_str(), "accepted");
    assert_eq!(s.resolved_by, Some(f.owner));

    // Attributed to the *author*: an acceptance credits whoever wrote the change,
    // not whoever waved it through. That is the rule the byte and flat-CRDT paths
    // already hold, and the one a separate accept call could most easily lose.
    let blame = f.ws.blame("/doc.md").await.unwrap();
    assert!(
        blame.iter().any(|r| r.actor.id == f.agent),
        "the agent's proposal must be blamed on the agent: {:?}",
        blame.iter().map(|r| r.actor.id).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_proposal_cannot_be_accepted_by_its_author() {
    // The review rule, restated on the path that had to re-implement it: this
    // accept does not go through `accept_suggestion`, so it would have been the
    // easy thing to leave out. Proposed by the *owner*, who does hold `WRITE`, so
    // the write gate cannot be what refuses it — only the author check can.
    let f = fixture().await;
    let octx = WriteCtx::actor(f.owner);
    let replica =
        f.ws.load_coedit_tree_to_propose(octx, "/doc.md", ROOT)
            .await
            .unwrap();
    replica.append_text(octx, "p", "self-approved\n");
    let id =
        f.ws.suggest_coedit_tree(octx, "/doc.md", &replica, None, None)
            .await
            .unwrap();

    let merged = f.ws.merge_coedit_tree_suggestion(id, ROOT).await.unwrap();
    let e =
        f.ws.accept_coedit_tree_suggestion(octx, id, &merged, b"whatever\n", &[] as &[TreeSpan])
            .await
            .unwrap_err();
    assert!(e.to_string().contains("different reviewer"), "{e}");
    assert_eq!(&f.ws.read("/doc.md").await.unwrap()[..], b"hello\n");
}

#[tokio::test]
async fn a_propose_only_reviewer_cannot_accept_either() {
    // The other half of the review gate (#78): a propose-only approver would be a
    // direct write wearing a review as a costume, and two such agents could
    // rubber-stamp each other into full write access.
    let f = fixture().await;
    let octx = WriteCtx::actor(f.owner);
    let replica =
        f.ws.load_coedit_tree_to_propose(octx, "/doc.md", ROOT)
            .await
            .unwrap();
    replica.append_text(octx, "p", "proposed\n");
    let id =
        f.ws.suggest_coedit_tree(octx, "/doc.md", &replica, None, None)
            .await
            .unwrap();

    let merged = f.ws.merge_coedit_tree_suggestion(id, ROOT).await.unwrap();
    let e =
        f.ws.accept_coedit_tree_suggestion(
            WriteCtx::actor(f.agent),
            id,
            &merged,
            b"rubber stamped\n",
            &[] as &[TreeSpan],
        )
        .await
        .unwrap_err();
    assert!(is_denied(&e), "expected Denied, got {e:?}");
    assert_eq!(&f.ws.read("/doc.md").await.unwrap()[..], b"hello\n");
}

#[tokio::test]
async fn an_already_resolved_proposal_is_not_accepted_twice() {
    let f = fixture().await;
    let actx = WriteCtx::actor(f.agent);
    let replica =
        f.ws.load_coedit_tree_to_propose(actx, "/doc.md", ROOT)
            .await
            .unwrap();
    replica.append_text(actx, "p", "proposed\n");
    let id =
        f.ws.suggest_coedit_tree(actx, "/doc.md", &replica, None, None)
            .await
            .unwrap();

    f.ws.reject_suggestion(id, WriteCtx::actor(f.owner))
        .await
        .unwrap();
    let merged = f.ws.merge_coedit_tree_suggestion(id, ROOT).await.unwrap();
    let e =
        f.ws.accept_coedit_tree_suggestion(
            WriteCtx::actor(f.owner),
            id,
            &merged,
            b"sneaked in\n",
            &[] as &[TreeSpan],
        )
        .await
        .unwrap_err();
    assert!(e.to_string().contains("already"), "{e}");
    // The check runs before the write, so the rejected body never lands.
    assert_eq!(&f.ws.read("/doc.md").await.unwrap()[..], b"hello\n");
}

#[tokio::test]
async fn the_two_kinds_do_not_accept_through_each_others_calls() {
    // The mirror of the refusal above: a flat proposal handed to the tree accept
    // is refused just as loudly, so neither kind can be laundered into the other.
    let f = fixture().await;
    let actx = WriteCtx::actor(f.agent);
    f.ws.write_as(WriteCtx::actor(f.owner), "/flat.md", b"flat\n")
        .await
        .unwrap();
    let id =
        f.ws.suggest(actx, "/flat.md", b"proposed flat\n", None, None)
            .await
            .unwrap();

    let doc = CoeditTreeDoc::new(ROOT);
    let e =
        f.ws.accept_coedit_tree_suggestion(
            WriteCtx::actor(f.owner),
            id,
            &doc,
            b"x\n",
            &[] as &[TreeSpan],
        )
        .await
        .unwrap_err();
    assert!(e.to_string().contains("accept_suggestion"), "{e}");
}

#[tokio::test]
async fn an_empty_update_proposes_nothing_and_is_refused() {
    let f = fixture().await;
    let actx = WriteCtx::actor(f.agent);
    let e =
        f.ws.suggest_coedit_tree_update(actx, "/doc.md", b"", b"", None, None)
            .await
            .unwrap_err();
    assert!(e.to_string().contains("proposes nothing"), "{e}");
}

#[tokio::test]
async fn the_sidecar_reports_the_root_a_document_lives_under() {
    // A reviewer has no schema, so it cannot know the root to resume under. The
    // document already records it, and that is why the suggestion row does not
    // have to: two proposals against one path must resume under the same root or
    // they are not proposals against the same document.
    let f = fixture().await;
    assert_eq!(
        f.ws.coedit_tree_root("/doc.md").await.unwrap().as_deref(),
        Some(ROOT)
    );
    assert_eq!(f.ws.coedit_tree_root("/nothing.md").await.unwrap(), None);
}

/// The flat shape had the same bug, and it made the CRDT review queue useless
/// for the only actors it exists for.
///
/// `apply_coedit_suggestion` checkpointed as the **author**, and
/// `checkpoint_coedit` re-checks `WRITE` at the path — so a propose-only actor's
/// `suggest_coedit` was recorded happily and then refused at acceptance, with a
/// `Denied` naming the *author* rather than the reviewer. A reviewer holding full
/// rights could not accept a proposal it had just read. The approver's right is
/// established by `accept_suggestion` before it ever gets there; checking again as
/// the author asks the wrong actor.
#[tokio::test]
async fn a_propose_only_actors_flat_crdt_proposal_can_actually_be_accepted() {
    let f = fixture().await;
    let octx = WriteCtx::actor(f.owner);
    let actx = WriteCtx::actor(f.agent);
    f.ws.write_as(octx, "/flat.md", b"hello\n").await.unwrap();

    let replica = f.ws.load_coedit_as(actx, "/flat.md").await.unwrap();
    replica.insert(actx, replica.text().len() as u32, "proposed\n");
    let id =
        f.ws.suggest_coedit(actx, "/flat.md", &replica, None, None)
            .await
            .unwrap();

    f.ws.accept_suggestion(id, octx).await.unwrap();

    let after = f.ws.read("/flat.md").await.unwrap();
    assert!(
        String::from_utf8_lossy(&after).contains("proposed"),
        "the merge must have landed: {:?}",
        String::from_utf8_lossy(&after)
    );
    let blame = f.ws.blame("/flat.md").await.unwrap();
    assert!(
        blame.iter().any(|r| r.actor.id == f.agent),
        "and be blamed on the proposer, not the approver"
    );
}
