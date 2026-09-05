//! Agent-suggestion review queue: an agent proposes edits that don't touch the
//! working tree until a human accepts them; accept applies (attributed to the
//! agent), reject discards, and a stale base is refused.

use origofs_sdk::{SuggestionStatus, Workspace, WriteCtx};

async fn setup() -> (Workspace, tempfile::TempDir, i64, WriteCtx, WriteCtx) {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let human = ws.create_human("dan", Some("dan@x")).await.unwrap();
    let agent = ws
        .create_agent("claude", "opus", Some(human))
        .await
        .unwrap();
    let sess = ws.create_session(agent, None).await.unwrap();
    let agent_ctx = WriteCtx::session(agent, sess);
    let human_ctx = WriteCtx::actor(human);
    (ws, dir, agent, agent_ctx, human_ctx)
}

#[tokio::test]
async fn suggest_does_not_touch_the_working_tree_until_accepted() {
    let (ws, _dir, agent, agent_ctx, human_ctx) = setup().await;
    ws.write("/notes.txt", b"line one\nline two\n")
        .await
        .unwrap();

    let id = ws
        .suggest(
            agent_ctx,
            "/notes.txt",
            b"line one\nline TWO\n",
            Some("fix caps"),
        )
        .await
        .unwrap();

    // working tree is untouched
    assert_eq!(
        &ws.read("/notes.txt").await.unwrap()[..],
        b"line one\nline two\n"
    );

    // it shows up as pending, attributed to the agent, with the summary
    let pending = ws
        .list_suggestions(Some(SuggestionStatus::Pending), None)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id);
    assert_eq!(pending[0].actor_id, agent);
    assert_eq!(pending[0].summary.as_deref(), Some("fix caps"));

    // the diff renders base -> proposed
    let patch = ws.suggestion_diff(id).await.unwrap();
    assert!(
        patch.contains("-line two") && patch.contains("+line TWO"),
        "{patch}"
    );

    // accept applies it, attributed to the agent that authored the content
    ws.accept_suggestion(id, human_ctx).await.unwrap();
    assert_eq!(
        &ws.read("/notes.txt").await.unwrap()[..],
        b"line one\nline TWO\n"
    );
    let blame = ws.blame("/notes.txt").await.unwrap();
    assert!(
        blame.iter().any(|r| r.actor.id == agent),
        "agent credited in blame"
    );

    // it's now accepted, stamped with the approver, and off the pending list
    let s = ws.get_suggestion(id).await.unwrap().unwrap();
    assert_eq!(s.status, SuggestionStatus::Accepted);
    assert_eq!(s.resolved_by, Some(human_ctx.actor));
    assert!(
        ws.list_suggestions(Some(SuggestionStatus::Pending), None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn reject_discards_without_applying() {
    let (ws, _dir, _agent, agent_ctx, human_ctx) = setup().await;
    ws.write("/a.txt", b"keep me\n").await.unwrap();

    let id = ws
        .suggest(agent_ctx, "/a.txt", b"clobbered\n", None)
        .await
        .unwrap();
    ws.reject_suggestion(id, human_ctx).await.unwrap();

    assert_eq!(&ws.read("/a.txt").await.unwrap()[..], b"keep me\n");
    assert_eq!(
        ws.get_suggestion(id).await.unwrap().unwrap().status,
        SuggestionStatus::Rejected
    );
    // resolving an already-resolved suggestion errors
    assert!(ws.accept_suggestion(id, human_ctx).await.is_err());
}

#[tokio::test]
async fn accept_refuses_a_stale_base() {
    let (ws, _dir, _agent, agent_ctx, human_ctx) = setup().await;
    ws.write("/x.txt", b"original\n").await.unwrap();

    let id = ws
        .suggest(agent_ctx, "/x.txt", b"proposed\n", None)
        .await
        .unwrap();
    // someone else changes the file after the suggestion was made
    ws.write("/x.txt", b"moved on\n").await.unwrap();

    let err = ws.accept_suggestion(id, human_ctx).await.unwrap_err();
    assert!(
        matches!(err, origofs_sdk::OrigoFSError::StaleBase(_)),
        "got {err:?}"
    );
    // The file keeps the newer content — a stale proposal is never applied.
    assert_eq!(&ws.read("/x.txt").await.unwrap()[..], b"moved on\n");
    // And the proposal is *retired*, not left Pending forever (issue #75 §3.2):
    // it is about a version of the file that no longer exists, and `Superseded`
    // is the state that says so. Re-diff and re-suggest to propose again.
    assert_eq!(
        ws.get_suggestion(id).await.unwrap().unwrap().status,
        SuggestionStatus::Superseded
    );
    assert!(
        ws.list_suggestions(Some(SuggestionStatus::Pending), None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn suggest_and_accept_a_deletion() {
    let (ws, _dir, _agent, agent_ctx, human_ctx) = setup().await;
    ws.write("/gone.txt", b"bye\n").await.unwrap();

    let id = ws
        .suggest_delete(agent_ctx, "/gone.txt", Some("remove"))
        .await
        .unwrap();
    assert!(
        ws.read("/gone.txt").await.is_ok(),
        "still there while pending"
    );

    ws.accept_suggestion(id, human_ctx).await.unwrap();
    assert!(ws.read("/gone.txt").await.is_err(), "removed on accept");
}

// SEC (security audit #6): a suggestion's author cannot approve their own
// suggestion — the review gate requires a different reviewer, so an agent can't
// rubber-stamp its own proposal into the working tree.
#[tokio::test]
async fn author_cannot_accept_their_own_suggestion() {
    let (ws, _dir, _agent, agent_ctx, _human_ctx) = setup().await;
    ws.write("/self.txt", b"base\n").await.unwrap();

    let id = ws
        .suggest(agent_ctx, "/self.txt", b"changed\n", None)
        .await
        .unwrap();

    // the proposing actor accepting its own suggestion is rejected...
    let err = ws.accept_suggestion(id, agent_ctx).await.unwrap_err();
    assert!(
        matches!(err, origofs_sdk::OrigoFSError::InvalidArgument(_)),
        "got {err:?}"
    );

    // ...the working tree is untouched and it stays pending for a real reviewer.
    assert_eq!(&ws.read("/self.txt").await.unwrap()[..], b"base\n");
    assert_eq!(
        ws.get_suggestion(id).await.unwrap().unwrap().status,
        SuggestionStatus::Pending
    );
}
