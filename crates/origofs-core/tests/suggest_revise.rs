//! Revising a proposal, rather than stacking a sibling beside it (issue #164).
//!
//! `write_or_propose` had no update-in-place, so an actor told "revise your
//! proposal" could only propose again — appending a **second** pending suggestion
//! against the same base. origofs resolved that correctly on **accept**: landing
//! either moves the file, which stales the other, and the sibling is retired
//! automatically. It resolved it wrongly on **reject**: the reviewer declines the
//! revision, and the abandoned earlier draft is still `pending`, still on a base
//! that matches the file, and still accepts cleanly — landing text the author had
//! already replaced and the reviewer never chose.
//!
//! `supersede_stale_byte_suggestions` cannot help, by construction: it retires
//! proposals whose *base* moved on, and an author revising a draft has changed no
//! bytes. "Abandoned by its own author" is a different relation, and nothing
//! expressed it.

use origofs_core::{Fs, MemStore, SqliteMetadataStore, SuggestionStatus, WriteCtx};
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let fs = Fs::new(
        SqliteMetadataStore::open_in_memory().unwrap(),
        Arc::new(MemStore::new()),
    );
    fs.init().await.unwrap();
    fs
}

async fn status(fs: &Fs<SqliteMetadataStore, Arc<MemStore>>, id: i64) -> SuggestionStatus {
    fs.get_suggestion(id).await.unwrap().unwrap().status
}

/// The reported failure, end to end: propose, revise, reject the revision — and
/// the first draft must not be sitting there ready to land.
#[tokio::test]
async fn rejecting_a_revision_does_not_leave_the_first_draft_accept_ready() {
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));
    fs.write_as(h, "/n.md", b"base\n").await.unwrap();

    let v1 = fs
        .suggest(a, "/n.md", b"v1 draft\n", None, None)
        .await
        .unwrap();
    let v2 = fs
        .suggest(a, "/n.md", b"v2\n", None, Some(v1))
        .await
        .unwrap();

    assert_eq!(status(&fs, v1).await, SuggestionStatus::Superseded);
    assert_eq!(status(&fs, v2).await, SuggestionStatus::Pending);

    fs.reject_suggestion(v2, h).await.unwrap();
    // The whole point: the reviewer said no to the proposal, and there is no
    // second one quietly waiting to be said yes to.
    assert_eq!(status(&fs, v1).await, SuggestionStatus::Superseded);
    assert!(
        fs.accept_suggestion(v1, h).await.is_err(),
        "an abandoned draft must not be acceptable"
    );
    assert_eq!(&fs.read("/n.md").await.unwrap()[..], b"base\n");
    assert!(
        fs.list_suggestions(Some(SuggestionStatus::Pending), Some("/n.md"))
            .await
            .unwrap()
            .is_empty()
    );
}

/// Without `replaces`, the stack is still there — and so is the bug. Pinned as the
/// negative control: it is what makes the test above about `replaces` rather than
/// about some unrelated change to the queue.
#[tokio::test]
async fn without_replaces_a_second_proposal_is_a_sibling() {
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));
    fs.write_as(h, "/n.md", b"base\n").await.unwrap();

    let v1 = fs.suggest(a, "/n.md", b"v1\n", None, None).await.unwrap();
    let v2 = fs.suggest(a, "/n.md", b"v2\n", None, None).await.unwrap();
    assert_eq!(status(&fs, v1).await, SuggestionStatus::Pending);
    assert_eq!(status(&fs, v2).await, SuggestionStatus::Pending);

    // And `supersede_stale_byte_suggestions` genuinely cannot see the difference:
    // both bases match the file, so by *its* measure neither is stale. That is not
    // a gap in it — it answers a different question, which is why #164 needed a
    // new one rather than a fix here.
    assert_eq!(
        fs.supersede_stale_byte_suggestions("/n.md", None)
            .await
            .unwrap(),
        0
    );
}

/// Two drafts a reviewer is meant to choose between is a real workflow, and origofs
/// cannot tell it from a revision — which is exactly why `replaces` is opt-in
/// rather than an automatic rule for a second proposal by the same actor.
#[tokio::test]
async fn siblings_stay_legal_when_nobody_asked_for_a_replacement() {
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));
    fs.write_as(h, "/n.md", b"base\n").await.unwrap();

    let alt1 = fs
        .suggest(a, "/n.md", b"option A\n", Some("A"), None)
        .await
        .unwrap();
    let alt2 = fs
        .suggest(a, "/n.md", b"option B\n", Some("B"), None)
        .await
        .unwrap();
    let pending = fs
        .list_suggestions(Some(SuggestionStatus::Pending), Some("/n.md"))
        .await
        .unwrap();
    assert_eq!(pending.len(), 2);

    // And the accept path's existing behaviour is untouched: landing one moves the
    // file, so the other goes stale and is retired.
    fs.accept_suggestion(alt2, h).await.unwrap();
    assert_eq!(status(&fs, alt1).await, SuggestionStatus::Superseded);
    assert_eq!(&fs.read("/n.md").await.unwrap()[..], b"option B\n");
}

/// A standalone withdrawal, for the draft that is retired with nothing taking its
/// place. Where a replacement is proposed in the same breath, `replaces` does both
/// halves at once and the two cannot come apart.
#[tokio::test]
async fn an_author_may_withdraw_its_own_draft() {
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));
    fs.set_write_policy(agent, origofs_core::WritePolicy::Propose)
        .await
        .unwrap();
    fs.write_as(h, "/n.md", b"base\n").await.unwrap();

    let id = fs
        .suggest(a, "/n.md", b"never mind\n", None, None)
        .await
        .unwrap();
    // A propose-only actor may retire *its own* draft: withdrawing is not review.
    fs.supersede_suggestion(id, a, Some("changed my mind"))
        .await
        .unwrap();
    assert_eq!(status(&fs, id).await, SuggestionStatus::Superseded);

    // ...and it is not a reject: the two states say different things to a reviewer
    // reading the queue back, which is the whole reason this is not just
    // `reject_suggestion` under another name.
    assert_ne!(status(&fs, id).await, SuggestionStatus::Rejected);
    assert!(
        fs.supersede_suggestion(id, a, None).await.is_err(),
        "retiring a resolved row must not silently succeed"
    );
}

/// Disposing of *somebody else's* proposal is a review action over its path, so it
/// takes the same check rejecting does. Without that, one propose-only agent could
/// quietly clear another's work out of the queue.
#[tokio::test]
async fn a_propose_only_actor_may_not_retire_someone_elses_draft() {
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let one = fs.create_agent("one", "m", Some(human)).await.unwrap();
    let two = fs.create_agent("two", "m", Some(human)).await.unwrap();
    for a in [one, two] {
        fs.set_write_policy(a, origofs_core::WritePolicy::Propose)
            .await
            .unwrap();
    }
    fs.write_as(WriteCtx::actor(human), "/n.md", b"base\n")
        .await
        .unwrap();

    let id = fs
        .suggest(WriteCtx::actor(one), "/n.md", b"mine\n", None, None)
        .await
        .unwrap();
    let r = fs
        .supersede_suggestion(id, WriteCtx::actor(two), None)
        .await;
    assert!(
        matches!(r, Err(origofs_core::OrigoFSError::Denied(_))),
        "{r:?}"
    );
    assert_eq!(status(&fs, id).await, SuggestionStatus::Pending);
    // A reviewer who *can* write there may, exactly as they may reject it.
    fs.supersede_suggestion(id, WriteCtx::actor(human), None)
        .await
        .unwrap();
    assert_eq!(status(&fs, id).await, SuggestionStatus::Superseded);
}

/// `replaces` naming a proposal on another path would silently retire unrelated
/// work, so it is refused rather than honoured — and nothing is proposed either,
/// because the propose runs after the replacement is settled.
#[tokio::test]
async fn replaces_must_name_a_pending_draft_on_the_same_path() {
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));
    fs.write_as(h, "/one.md", b"1\n").await.unwrap();
    fs.write_as(h, "/two.md", b"2\n").await.unwrap();

    let elsewhere = fs.suggest(a, "/two.md", b"x\n", None, None).await.unwrap();
    let r = fs
        .suggest(a, "/one.md", b"y\n", None, Some(elsewhere))
        .await;
    assert!(r.is_err(), "{r:?}");
    assert_eq!(status(&fs, elsewhere).await, SuggestionStatus::Pending);
    assert!(
        fs.list_suggestions(Some(SuggestionStatus::Pending), Some("/one.md"))
            .await
            .unwrap()
            .is_empty(),
        "a refused replacement must not leave a new proposal behind"
    );

    // A resolved row is likewise not replaceable, and again nothing is created.
    fs.reject_suggestion(elsewhere, h).await.unwrap();
    assert!(
        fs.suggest(a, "/two.md", b"z\n", None, Some(elsewhere))
            .await
            .is_err()
    );
    assert!(
        fs.list_suggestions(Some(SuggestionStatus::Pending), Some("/two.md"))
            .await
            .unwrap()
            .is_empty()
    );
}

/// The same relation through the policy gate a surface actually calls, including
/// a proposed **deletion** — which stacks the same way and resurrects the same way.
#[tokio::test]
async fn write_or_propose_and_remove_or_propose_carry_replaces() {
    use origofs_core::WriteOutcome;
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
    fs.set_write_policy(agent, origofs_core::WritePolicy::Propose)
        .await
        .unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));
    fs.write_as(h, "/n.md", b"base\n").await.unwrap();

    let WriteOutcome::Proposed(v1) = fs
        .write_or_propose(a, "/n.md", b"v1\n", None, None)
        .await
        .unwrap()
    else {
        panic!("a propose-only actor must propose");
    };
    // Revising the edit into a proposed *deletion* is still a revision of the same
    // intent about the same path.
    let WriteOutcome::Proposed(v2) = fs
        .remove_or_propose(a, "/n.md", None, Some(v1))
        .await
        .unwrap()
    else {
        panic!("a propose-only actor must propose");
    };
    assert_eq!(status(&fs, v1).await, SuggestionStatus::Superseded);
    assert_eq!(status(&fs, v2).await, SuggestionStatus::Pending);
}

/// The change feed names both rows, so a reviewer reading the trail can tell a
/// withdrawal from a rejection and see what took the draft's place.
#[tokio::test]
async fn the_trail_names_the_replacement() {
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));
    fs.write_as(h, "/n.md", b"base\n").await.unwrap();

    let v1 = fs.suggest(a, "/n.md", b"v1\n", None, None).await.unwrap();
    let v2 = fs
        .suggest(a, "/n.md", b"v2\n", None, Some(v1))
        .await
        .unwrap();

    let events = fs.events_since(0, 100).await.unwrap();
    let supersede = events
        .iter()
        .find(|e| e.kind == "supersede")
        .expect("the retirement must be on the feed");
    let detail = supersede.detail.clone().unwrap_or_default();
    assert!(detail.contains(&format!("#{v1}")), "{detail}");
    assert!(detail.contains(&format!("#{v2}")), "{detail}");
    let proposed = events.iter().rfind(|e| e.kind == "suggest").unwrap();
    assert!(
        proposed
            .detail
            .clone()
            .unwrap_or_default()
            .contains(&format!("replacing #{v1}")),
        "{proposed:?}"
    );
}

/// A CRDT proposal carries `replaces` too. Stacking is less dangerous there — a
/// CRDT proposal never goes stale, and applying an author's earlier state after
/// their later one merges a subset — but "the proposal I meant is no longer this
/// one" is the same relation on either shape, and a queue with three abandoned
/// drafts in it is still a queue nobody can read.
#[cfg(feature = "coedit")]
#[tokio::test]
async fn a_crdt_proposal_can_be_revised_too() {
    use origofs_core::CoeditDoc;
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));
    fs.write_as(h, "/n.md", b"base\n").await.unwrap();

    let draft = |text: &str| {
        let d = CoeditDoc::new();
        d.insert(a, 0, text);
        d
    };
    let v1 = fs
        .suggest_coedit(a, "/n.md", &draft("v1\n"), None, None)
        .await
        .unwrap();
    let v2 = fs
        .suggest_coedit(a, "/n.md", &draft("v2\n"), None, Some(v1))
        .await
        .unwrap();

    assert_eq!(status(&fs, v1).await, SuggestionStatus::Superseded);
    assert_eq!(status(&fs, v2).await, SuggestionStatus::Pending);
    // ...and a CRDT proposal on another path is refused as a replacement, exactly
    // as a byte one is: the check is on the relation, not on the kind.
    let elsewhere = fs
        .suggest_coedit(a, "/other.md", &draft("x\n"), None, None)
        .await
        .unwrap();
    assert!(
        fs.suggest_coedit(a, "/n.md", &draft("v3\n"), None, Some(elsewhere))
            .await
            .is_err()
    );
    assert_eq!(status(&fs, elsewhere).await, SuggestionStatus::Pending);
}

/// A settled row is a **conflict**, not a bad request (#164).
///
/// Every resolve path used to answer `InvalidArgument` — a `400` on HTTP and a
/// `ValueError` in Python — for a suggestion that was simply already accepted,
/// rejected or superseded. That says the *request* was malformed when it was
/// well-formed and merely out of date. `AlreadyResolved` is the third thing a
/// reviewing caller handles beside `StaleBase` and the raced-CAS `Conflict`, and
/// unlike either it is terminal.
#[tokio::test]
async fn a_settled_suggestion_reports_a_conflict_on_every_resolve_path() {
    use origofs_core::OrigoFSError;
    let fs = fixture().await;
    let human = fs.create_human("h", None).await.unwrap();
    let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));
    fs.write_as(h, "/n.md", b"base\n").await.unwrap();

    let id = fs.suggest(a, "/n.md", b"once\n", None, None).await.unwrap();
    fs.accept_suggestion(id, h).await.unwrap();

    for err in [
        fs.accept_suggestion(id, h).await.unwrap_err(),
        fs.reject_suggestion(id, h).await.unwrap_err(),
        fs.supersede_suggestion(id, a, None).await.unwrap_err(),
    ] {
        assert!(matches!(err, OrigoFSError::AlreadyResolved(_)), "{err:?}");
        assert_eq!(err.code(), "already_resolved");
        assert!(err.is_conflict(), "so every surface maps it to 409");
        assert!(!err.retryable(), "terminal: read the row, do not replay");
    }
}
