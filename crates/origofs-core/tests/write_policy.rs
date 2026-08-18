//! `WritePolicy::Propose` is a property of the **engine**, not a convention the
//! surfaces are trusted to remember (issue #78).
//!
//! Before this, `write_or_propose` was the only gated entry point, so a
//! propose-only actor could not overwrite a file — but could delete it, rename
//! it, and commit the result. The gate made the destructive path one call longer
//! rather than unavailable. These tests pin the whole surface of the fix:
//!
//! * every attributed mutation refuses a propose-only actor;
//! * a removal has a *propose* path (queue it) rather than only a refusal;
//! * an accepted deletion is attributed to whoever requested it;
//! * a propose-only actor cannot approve its way to write access;
//! * internal machinery — the unattributed engine ops — stays exempt, which is
//!   what keeps checkout, merge, and suggestion application working.

use origofs_core::{
    Fs, MemStore, MetadataStore, OrigoFSError, SqliteMetadataStore, SuggestionStatus, WriteCtx,
    WriteOutcome, WritePolicy,
};
use std::sync::Arc;

async fn fs() -> Fs<Arc<dyn MetadataStore>, Arc<MemStore>> {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

/// An agent restricted to proposing, and a human who may write directly.
async fn actors<M: MetadataStore>(fs: &Fs<M, Arc<MemStore>>) -> (WriteCtx, WriteCtx) {
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    fs.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    let human = fs
        .create_human("dan", Some("dan@example.com"))
        .await
        .unwrap();
    (WriteCtx::actor(agent), WriteCtx::actor(human))
}

fn assert_denied(e: OrigoFSError, what: &str) {
    assert!(
        matches!(e, OrigoFSError::Denied(_)),
        "{what} should be Denied for a propose-only actor, got {e:?}"
    );
    assert_eq!(e.code(), "denied");
}

#[tokio::test]
async fn propose_only_actor_is_refused_every_direct_mutation() {
    let fs = fs().await;
    let (agent, _human) = actors(&fs).await;
    fs.write("/a.txt", b"hello").await.unwrap();
    fs.mkdir_p("/d").await.unwrap();

    assert_denied(
        fs.remove_as(agent, "/a.txt").await.unwrap_err(),
        "remove_as",
    );
    assert_denied(
        fs.rename_as(agent, "/a.txt", "/b.txt").await.unwrap_err(),
        "rename_as",
    );
    assert_denied(fs.mkdir_as(agent, "/e").await.unwrap_err(), "mkdir_as");
    assert_denied(
        fs.symlink_as(agent, "/a.txt", "/link").await.unwrap_err(),
        "symlink_as",
    );
    // Metadata is a mutation too. `chmod 000` on a file an agent may not write is
    // the same denial of service one call further along — the shape of #78 that
    // this whole file exists to close. No propose-shaped equivalent exists for
    // either, so both refuse outright rather than queueing (like `symlink_as`).
    assert_denied(
        fs.chmod_as(agent, "/a.txt", 0o000).await.unwrap_err(),
        "chmod_as",
    );
    assert_denied(
        fs.chown_as(agent, "/a.txt", Some(0), None)
            .await
            .unwrap_err(),
        "chown_as",
    );
    assert_denied(
        fs.commit_as(agent, "claude", "nope").await.unwrap_err(),
        "commit_as",
    );

    // Nothing was destroyed by the refusals.
    assert_eq!(&fs.read("/a.txt").await.unwrap()[..], b"hello");
    assert!(fs.stat("/d").await.is_ok());
    assert!(fs.stat("/e").await.is_err());
}

#[tokio::test]
async fn a_direct_actor_performs_all_of_them() {
    let fs = fs().await;
    let (_agent, human) = actors(&fs).await;
    fs.write("/a.txt", b"hello").await.unwrap();

    fs.mkdir_as(human, "/d").await.unwrap();
    fs.symlink_as(human, "/a.txt", "/link").await.unwrap();
    fs.rename_as(human, "/a.txt", "/b.txt").await.unwrap();
    fs.commit_as(human, "dan", "initial").await.unwrap();
    fs.remove_as(human, "/b.txt").await.unwrap();

    assert!(fs.stat("/d").await.is_ok());
    assert!(fs.stat("/b.txt").await.is_err());
}

#[tokio::test]
async fn a_propose_only_removal_is_queued_not_refused() {
    let fs = fs().await;
    let (agent, human) = actors(&fs).await;
    fs.write("/doomed.txt", b"still here").await.unwrap();

    // The agent asks for a deletion: queued, and the file is untouched.
    let outcome = fs
        .remove_or_propose(agent, "/doomed.txt", Some("obsolete"))
        .await
        .unwrap();
    let id = match outcome {
        WriteOutcome::Proposed(id) => id,
        WriteOutcome::Wrote => panic!("a propose-only actor must not delete directly"),
    };
    assert_eq!(&fs.read("/doomed.txt").await.unwrap()[..], b"still here");

    // A trusted reviewer accepts it, and only then does the file go.
    fs.accept_suggestion(id, human).await.unwrap();
    assert!(
        fs.stat("/doomed.txt").await.is_err(),
        "accepting a deletion suggestion must remove the file"
    );
    let s = fs.get_suggestion(id).await.unwrap().unwrap();
    assert_eq!(s.status, SuggestionStatus::Accepted);
    assert_eq!(
        s.actor_id, agent.actor,
        "the deletion stays attributed to the actor that requested it"
    );
    assert_eq!(s.resolved_by, Some(human.actor));
}

#[tokio::test]
async fn a_direct_actors_removal_goes_straight_through() {
    let fs = fs().await;
    let (_agent, human) = actors(&fs).await;
    fs.write("/x.txt", b"bye").await.unwrap();

    let outcome = fs.remove_or_propose(human, "/x.txt", None).await.unwrap();
    assert!(matches!(outcome, WriteOutcome::Wrote));
    assert!(fs.stat("/x.txt").await.is_err());
}

#[tokio::test]
async fn a_propose_only_actor_cannot_approve_anything() {
    let fs = fs().await;
    let (agent, _human) = actors(&fs).await;
    // A second propose-only agent: without the approver check these two could
    // rubber-stamp each other into full write access.
    let other = fs.create_agent("gpt", "5", None).await.unwrap();
    fs.set_write_policy(other, WritePolicy::Propose)
        .await
        .unwrap();
    let other = WriteCtx::actor(other);

    fs.write("/shared.txt", b"base").await.unwrap();
    let id = fs
        .suggest(agent, "/shared.txt", b"rewritten", None)
        .await
        .unwrap();

    assert_denied(
        fs.accept_suggestion(id, other).await.unwrap_err(),
        "accept_suggestion by a propose-only approver",
    );
    assert_eq!(&fs.read("/shared.txt").await.unwrap()[..], b"base");
    assert_eq!(
        fs.get_suggestion(id).await.unwrap().unwrap().status,
        SuggestionStatus::Pending,
        "a refused approval must leave the suggestion pending, not resolved"
    );
}

#[tokio::test]
async fn namespace_mutations_are_attributed() {
    // The gate needed an actor on these ops; recording that actor also closes the
    // older gap that "who deleted this file" had no answer at all.
    let fs = fs().await;
    let (_agent, human) = actors(&fs).await;
    fs.write("/gone.txt", b"data").await.unwrap();

    fs.mkdir_as(human, "/dir").await.unwrap();
    fs.rename_as(human, "/gone.txt", "/moved.txt")
        .await
        .unwrap();
    fs.remove_as(human, "/moved.txt").await.unwrap();

    let ops = fs.edit_ops(human.actor, None).await.unwrap();
    let kinds: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    for expected in ["mkdir", "rename", "remove"] {
        assert!(
            kinds.contains(&expected),
            "{expected} should appear in the op-log, got {kinds:?}"
        );
    }
    let removal = ops.iter().find(|o| o.op == "remove").unwrap();
    assert_eq!(removal.path, "/moved.txt");
    assert!(
        removal.pre_hash.is_some(),
        "a removal records what was destroyed"
    );
    assert!(removal.post_hash.is_none());
}

#[tokio::test]
async fn internal_machinery_stays_exempt() {
    // The raw engine ops carry no actor and must keep working regardless of
    // policy — this is what checkout, merge materialization and
    // `apply_byte_suggestion` rely on. If gating ever leaks down to them, a
    // propose-only actor's accepted suggestion could never be applied.
    let fs = fs().await;
    let (agent, human) = actors(&fs).await;

    fs.write("/f.txt", b"one").await.unwrap();
    fs.mkdir_p("/sub").await.unwrap();
    // The unattributed metadata ops are exempt for the same reason as the rest:
    // checkout materializes a committed tree's modes without an actor to blame.
    fs.chmod("/f.txt", 0o600).await.unwrap();
    fs.chown("/f.txt", Some(1), Some(1)).await.unwrap();
    fs.rename("/f.txt", "/sub/f.txt").await.unwrap();
    fs.remove("/sub/f.txt").await.unwrap();
    assert!(fs.stat("/sub/f.txt").await.is_err());

    // And the accept path still lands a propose-only actor's *content* edit,
    // which internally writes as that very actor.
    fs.write("/g.txt", b"before").await.unwrap();
    let id = fs.suggest(agent, "/g.txt", b"after", None).await.unwrap();
    fs.accept_suggestion(id, human).await.unwrap();
    assert_eq!(&fs.read("/g.txt").await.unwrap()[..], b"after");
}

#[tokio::test]
async fn an_unknown_actor_defaults_to_direct() {
    // Matches the column default and `WritePolicy::from_i64`: an unrecognized
    // actor is not silently locked out. Identity is resolved server-side before
    // any of this runs, so this is a consistency guard, not a permission model.
    let fs = fs().await;
    fs.write("/a.txt", b"x").await.unwrap();
    let ghost = WriteCtx::actor(9_999);
    assert!(fs.mkdir_as(ghost, "/ok").await.is_ok());
}
