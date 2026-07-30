//! `WritePolicy::Propose` as a real boundary, not just a hint on `write`.
//!
//! The policy is documented as "direct writes are refused; edits must go through
//! the suggestion queue for review by a different actor before they land". It was
//! consulted in exactly one function (`write_or_propose`), so every other mutation
//! walked straight past it: a propose-only agent could delete any file, rename
//! anything, and create directories — while an operator who had set the policy
//! believed its changes were gated.

use origofs_core::{
    Fs, MemStore, OrigoFSError, SqliteMetadataStore, SuggestionStatus, WriteCtx, WriteOutcome,
    WritePolicy,
};
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let fs = Fs::new(
        SqliteMetadataStore::open_in_memory().unwrap(),
        Arc::new(MemStore::new()),
    );
    fs.init().await.unwrap();
    fs
}

/// An agent restricted to proposals, plus a human who can review.
async fn actors(fs: &Fs<SqliteMetadataStore, Arc<MemStore>>) -> (i64, i64) {
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    fs.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    let human = fs.create_human("dan", None).await.unwrap();
    (agent, human)
}

/// Deleting was the sharpest gap: more destructive than the edits the policy was
/// protecting against, and completely ungated.
#[tokio::test]
async fn propose_only_delete_is_queued_not_applied() {
    let fs = fixture().await;
    let (agent, human) = actors(&fs).await;
    fs.write("/notes.txt", b"important").await.unwrap();

    let outcome = fs
        .remove_or_propose(WriteCtx::actor(agent), "/notes.txt", None)
        .await
        .unwrap();
    let id = match outcome {
        WriteOutcome::Proposed(id) => id,
        other => panic!("a propose-only delete must be queued, got {other:?}"),
    };

    // The file is still there until someone reviews it.
    assert_eq!(&fs.read("/notes.txt").await.unwrap()[..], b"important");

    // A different actor accepting it is what actually deletes it.
    fs.accept_suggestion(id, WriteCtx::actor(human))
        .await
        .unwrap();
    assert!(fs.read("/notes.txt").await.is_err());

    // A direct actor still deletes directly.
    fs.write("/other.txt", b"x").await.unwrap();
    let outcome = fs
        .remove_or_propose(WriteCtx::actor(human), "/other.txt", None)
        .await
        .unwrap();
    assert_eq!(outcome, WriteOutcome::Wrote);
    assert!(fs.read("/other.txt").await.is_err());
}

/// Mutations with no suggestion form can only be honored by refusing them.
#[tokio::test]
async fn propose_only_rename_and_mkdir_are_refused() {
    let fs = fixture().await;
    let (agent, human) = actors(&fs).await;
    fs.write("/a.txt", b"x").await.unwrap();

    assert!(
        matches!(
            fs.rename_as(WriteCtx::actor(agent), "/a.txt", "/b.txt")
                .await,
            Err(OrigoFSError::PermissionDenied(_))
        ),
        "a propose-only actor must not be able to rename"
    );
    assert!(
        matches!(
            fs.mkdir_p_as(WriteCtx::actor(agent), "/newdir").await,
            Err(OrigoFSError::PermissionDenied(_))
        ),
        "a propose-only actor must not be able to create directories"
    );
    // Nothing happened.
    assert_eq!(&fs.read("/a.txt").await.unwrap()[..], b"x");
    assert!(fs.stat("/b.txt").await.is_err());
    assert!(fs.stat("/newdir").await.is_err());

    // A direct actor is unaffected.
    fs.rename_as(WriteCtx::actor(human), "/a.txt", "/b.txt")
        .await
        .unwrap();
    fs.mkdir_p_as(WriteCtx::actor(human), "/newdir")
        .await
        .unwrap();
    assert_eq!(&fs.read("/b.txt").await.unwrap()[..], b"x");
}

/// Proposing must not touch the working tree at all — including creating the
/// parent directories the edit would eventually need. Surfaces used to `mkdir_p`
/// before consulting the policy, so a "queued for review" edit had already
/// mutated the tree. The directories now appear when the edit lands.
#[tokio::test]
async fn proposing_a_nested_path_does_not_create_its_directories() {
    let fs = fixture().await;
    let (agent, human) = actors(&fs).await;

    let outcome = fs
        .write_or_propose(WriteCtx::actor(agent), "/deep/nested/f.txt", b"hi", None)
        .await
        .unwrap();
    let id = match outcome {
        WriteOutcome::Proposed(id) => id,
        other => panic!("expected a proposal, got {other:?}"),
    };
    assert!(
        fs.stat("/deep").await.is_err(),
        "a proposal must not create directories in the working tree"
    );

    // Accepting creates them, so the proposal is still applicable.
    fs.accept_suggestion(id, WriteCtx::actor(human))
        .await
        .unwrap();
    assert_eq!(&fs.read("/deep/nested/f.txt").await.unwrap()[..], b"hi");
    let s = fs.get_suggestion(id).await.unwrap().unwrap();
    assert_eq!(s.status, SuggestionStatus::Accepted);
}

/// An authorization control must fail closed. An unknown stored value is one a
/// newer binary wrote — resolving it to `Direct` would silently *grant* direct
/// writes to an actor someone had deliberately restricted.
#[test]
fn unknown_write_policy_decodes_to_the_restrictive_one() {
    assert_eq!(WritePolicy::from_i64(0), WritePolicy::Direct);
    assert_eq!(WritePolicy::from_i64(1), WritePolicy::Propose);
    for unknown in [2, 3, 99, -1, i64::MAX] {
        assert_eq!(
            WritePolicy::from_i64(unknown),
            WritePolicy::Propose,
            "unknown policy {unknown} must fail closed"
        );
    }
}
