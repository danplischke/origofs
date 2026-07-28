//! Attribution & blame: per-line human-vs-agent authorship, provenance, the
//! edit-op log, and reverting an agent's session.

use origofs_core::{ActorInit, ActorKind, Fs, MemStore, SqliteMetadataStore, WriteCtx};
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();
    fs
}

#[tokio::test]
async fn human_and_agent_blame_is_per_line() {
    let fs = fixture().await;
    let alice = fs
        .create_human("alice", Some("alice@example.com"))
        .await
        .unwrap();
    let claude = fs
        .create_agent("claude", "claude-opus-4-8", Some(alice))
        .await
        .unwrap();
    let s_alice = fs.create_session(alice, Some("editor")).await.unwrap();
    let s_claude = fs.create_session(claude, Some("mcp")).await.unwrap();

    // Alice writes the file.
    fs.write_as(WriteCtx::session(alice, s_alice), "/f", b"l1\nl2\nl3\n")
        .await
        .unwrap();
    let b = fs.blame("/f").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].actor.id, alice);
    assert_eq!(b[0].actor.kind, ActorKind::Human);
    assert_eq!((b[0].line_start, b[0].line_end), (1, 3));

    // Claude edits only line 2.
    fs.write_as(
        WriteCtx::session(claude, s_claude),
        "/f",
        b"l1\nCLAUDE\nl3\n",
    )
    .await
    .unwrap();
    let b = fs.blame("/f").await.unwrap();
    // three ranges: alice / claude / alice
    assert_eq!(b.len(), 3);
    assert_eq!(
        (b[0].actor.id, b[0].line_start, b[0].line_end),
        (alice, 1, 1)
    );
    assert_eq!(
        (b[1].actor.id, b[1].line_start, b[1].line_end),
        (claude, 2, 2)
    );
    assert_eq!(
        (b[2].actor.id, b[2].line_start, b[2].line_end),
        (alice, 3, 3)
    );
    assert_eq!(b[1].actor.kind, ActorKind::Agent);
    assert_eq!(b[1].actor.agent_model.as_deref(), Some("claude-opus-4-8"));

    // Provenance chain: the agent points at the human that launched it.
    let agent = fs.get_actor(claude).await.unwrap().unwrap();
    assert_eq!(agent.controller_actor_id, Some(alice));
}

#[tokio::test]
async fn edit_op_log_records_writes() {
    let fs = fixture().await;
    let claude = fs.create_agent("claude", "m", None).await.unwrap();
    let s = fs.create_session(claude, None).await.unwrap();
    fs.write_as(WriteCtx::session(claude, s), "/a", b"x")
        .await
        .unwrap();
    fs.write_as(WriteCtx::session(claude, s), "/b", b"y")
        .await
        .unwrap();

    let ops = fs.edit_ops(claude, Some(s)).await.unwrap();
    assert_eq!(ops.len(), 2);
    assert!(ops.iter().all(|o| o.actor_id == claude && o.op == "write"));
    let paths: Vec<&str> = ops.iter().map(|o| o.path.as_str()).collect();
    assert_eq!(paths, vec!["/a", "/b"]);
    // narrowing to a different (nonexistent) session yields nothing
    assert!(fs.edit_ops(claude, Some(s + 999)).await.unwrap().is_empty());
}

#[tokio::test]
async fn revert_session_removes_only_that_actors_lines() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_alice = fs.create_session(alice, None).await.unwrap();
    let s_claude = fs.create_session(claude, None).await.unwrap();

    fs.write_as(
        WriteCtx::session(alice, s_alice),
        "/doc",
        b"human-1\nhuman-2\n",
    )
    .await
    .unwrap();
    // Claude appends a line (keeps the human lines).
    fs.write_as(
        WriteCtx::session(claude, s_claude),
        "/doc",
        b"human-1\nhuman-2\nagent-line\n",
    )
    .await
    .unwrap();
    assert_eq!(
        fs.blame("/doc").await.unwrap().last().unwrap().actor.id,
        claude
    );

    // Revert everything the agent wrote in its session.
    let changed = fs.revert_session(claude, s_claude).await.unwrap();
    assert_eq!(changed, 1);
    assert_eq!(&fs.read("/doc").await.unwrap()[..], b"human-1\nhuman-2\n");
    let b = fs.blame("/doc").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].actor.id, alice);
}

#[tokio::test]
async fn binary_files_get_file_level_attribution() {
    let fs = fixture().await;
    let claude = fs.create_agent("claude", "m", None).await.unwrap();
    let s = fs.create_session(claude, None).await.unwrap();
    fs.write_as(
        WriteCtx::session(claude, s),
        "/b",
        &[0xff, 0xfe, 0x00, 0x01],
    )
    .await
    .unwrap();
    let b = fs.blame("/b").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].actor.id, claude);
}

// M9: blame is keyed by the content version an inode points at, so it survives a
// checkout that swaps the working tree between commits — the blame you see always
// matches the bytes you'd read, never a stale carry-over from another branch.
#[tokio::test]
async fn blame_survives_checkout() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_alice = fs.create_session(alice, None).await.unwrap();
    let s_claude = fs.create_session(claude, None).await.unwrap();

    // v1 on main: alice writes two lines, commit, then branch `dev` off v1.
    fs.write_as(WriteCtx::session(alice, s_alice), "/f", b"one\ntwo\n")
        .await
        .unwrap();
    fs.commit("alice", "v1").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    // v2 on main: claude appends a third line, commit.
    fs.write_as(
        WriteCtx::session(claude, s_claude),
        "/f",
        b"one\ntwo\nthree\n",
    )
    .await
    .unwrap();
    fs.commit("claude", "v2").await.unwrap();

    // main (v2): alice on 1-2, claude on 3.
    let b = fs.blame("/f").await.unwrap();
    assert_eq!(b.len(), 2);
    assert_eq!(
        (b[0].actor.id, b[0].line_start, b[0].line_end),
        (alice, 1, 2)
    );
    assert_eq!(
        (b[1].actor.id, b[1].line_start, b[1].line_end),
        (claude, 3, 3)
    );

    // Checkout dev (v1): the working tree is two lines again, and blame follows
    // the checked-out content — all alice, no stale `claude`/past-EOF run.
    fs.checkout("dev").await.unwrap();
    assert_eq!(&fs.read("/f").await.unwrap()[..], b"one\ntwo\n");
    let b = fs.blame("/f").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(
        (b[0].actor.id, b[0].line_start, b[0].line_end),
        (alice, 1, 2)
    );

    // Back to main (v2): blame is exactly what it was before we left.
    fs.checkout("main").await.unwrap();
    assert_eq!(&fs.read("/f").await.unwrap()[..], b"one\ntwo\nthree\n");
    let b = fs.blame("/f").await.unwrap();
    assert_eq!(b.len(), 2);
    assert_eq!(
        (b[0].actor.id, b[0].line_start, b[0].line_end),
        (alice, 1, 2)
    );
    assert_eq!(
        (b[1].actor.id, b[1].line_start, b[1].line_end),
        (claude, 3, 3)
    );
}

// H7: a plain (non-attributed) `write` replaces the content but records no
// authorship, so blame for the new version is simply absent — never the old
// version's runs stretched over content that no longer matches (the past-EOF /
// desync bug the per-inode model had).
#[tokio::test]
async fn unattributed_write_invalidates_blame() {
    let fs = fixture().await;
    let claude = fs.create_agent("claude", "m", None).await.unwrap();
    let s = fs.create_session(claude, None).await.unwrap();

    fs.write_as(WriteCtx::session(claude, s), "/f", b"a\nb\nc\n")
        .await
        .unwrap();
    let b = fs.blame("/f").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(
        (b[0].actor.id, b[0].line_start, b[0].line_end),
        (claude, 1, 3)
    );

    // Someone edits the file outside the attributed path, shrinking it.
    fs.write("/f", b"z\n").await.unwrap();
    assert_eq!(&fs.read("/f").await.unwrap()[..], b"z\n");
    let b = fs.blame("/f").await.unwrap();
    assert!(
        b.is_empty(),
        "unattributed write must leave no stale blame, got {b:?}"
    );

    // A later attributed write re-establishes blame for the current content.
    fs.write_as(WriteCtx::session(claude, s), "/f", b"z\nY\n")
        .await
        .unwrap();
    let b = fs.blame("/f").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(
        (b[0].actor.id, b[0].line_start, b[0].line_end),
        (claude, 1, 2)
    );
}

// M10: a pure re-indent is not an authorship change. The whitespace-normalized
// line still matches its deleted original, so the reformatter doesn't steal the
// blame for content they only shifted.
#[tokio::test]
async fn reindent_keeps_original_author() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_alice = fs.create_session(alice, None).await.unwrap();
    let s_claude = fs.create_session(claude, None).await.unwrap();

    fs.write_as(WriteCtx::session(alice, s_alice), "/f", b"foo\nbar\n")
        .await
        .unwrap();
    // Claude only re-indents the first line.
    fs.write_as(WriteCtx::session(claude, s_claude), "/f", b"    foo\nbar\n")
        .await
        .unwrap();

    let b = fs.blame("/f").await.unwrap();
    assert_eq!(b.len(), 1, "re-indent must not split authorship, got {b:?}");
    assert_eq!(
        (b[0].actor.id, b[0].line_start, b[0].line_end),
        (alice, 1, 2)
    );
}

// M10: a line that is *moved* carries its author to its new position, rather than
// being credited to whoever reordered the file.
#[tokio::test]
async fn moved_line_keeps_its_author() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_alice = fs.create_session(alice, None).await.unwrap();
    let s_claude = fs.create_session(claude, None).await.unwrap();

    // Alice writes two lines; claude appends a third.
    fs.write_as(WriteCtx::session(alice, s_alice), "/f", b"one\ntwo\n")
        .await
        .unwrap();
    fs.write_as(
        WriteCtx::session(claude, s_claude),
        "/f",
        b"one\ntwo\nthree\n",
    )
    .await
    .unwrap();

    // Alice reorders, hoisting claude's line to the top.
    fs.write_as(
        WriteCtx::session(alice, s_alice),
        "/f",
        b"three\none\ntwo\n",
    )
    .await
    .unwrap();

    let b = fs.blame("/f").await.unwrap();
    // `three` stays claude at its new home; `one`/`two` remain alice.
    assert_eq!(b.len(), 2, "got {b:?}");
    assert_eq!(
        (b[0].actor.id, b[0].line_start, b[0].line_end),
        (claude, 1, 1)
    );
    assert_eq!(
        (b[1].actor.id, b[1].line_start, b[1].line_end),
        (alice, 2, 3)
    );
}

// find_or_create_actor maps an external identity (auth_subject) to exactly one
// actor, idempotently — so an app can bind its own user id without a side table.
#[tokio::test]
async fn find_or_create_actor_is_idempotent_by_subject() {
    let fs = fixture().await;

    // Unknown subject resolves to nothing.
    assert!(fs.actor_by_subject("user_42").await.unwrap().is_none());

    // First call creates; a second with the same subject returns the same id.
    let a1 = fs.find_or_create_human("user_42", "Dan").await.unwrap();
    let a2 = fs
        .find_or_create_human("user_42", "Dan again")
        .await
        .unwrap();
    assert_eq!(a1, a2);

    // A different subject is a different actor.
    let b = fs.find_or_create_human("user_99", "Sam").await.unwrap();
    assert_ne!(a1, b);

    // The lookup now resolves and carries the identity.
    let found = fs.actor_by_subject("user_42").await.unwrap().unwrap();
    assert_eq!(found.id, a1);
    assert_eq!(found.auth_subject.as_deref(), Some("user_42"));
    assert_eq!(found.kind, ActorKind::Human);

    // Agents key on the subject the same way.
    let g1 = fs
        .find_or_create_agent("tok", "claude", "opus", Some(a1))
        .await
        .unwrap();
    let g2 = fs
        .find_or_create_agent("tok", "claude", "opus", Some(a1))
        .await
        .unwrap();
    assert_eq!(g1, g2);
    assert_ne!(g1, a1);

    // A plain create with a duplicate subject is refused by the unique index,
    // so identities can't silently fork.
    assert!(
        fs.create_human("dupe", Some("user_42")).await.is_err(),
        "unique index must reject a second actor for an existing subject"
    );

    // find_or_create requires a subject to key on.
    assert!(
        fs.find_or_create_actor(ActorInit::human("x", None))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn actor_lookup_by_id_and_list() {
    let fs = fixture().await;
    let alice = fs.create_human("Alice", Some("alice@x")).await.unwrap();
    let claude = fs
        .create_agent("claude", "claude-opus-4-8", Some(alice))
        .await
        .unwrap();

    // get_actor resolves a bare id (as carried by events/suggestions) to the record.
    let a = fs.get_actor(alice).await.unwrap().expect("alice exists");
    assert_eq!((a.id, a.kind), (alice, ActorKind::Human));
    assert_eq!(a.display_name, "Alice");
    assert!(fs.get_actor(9999).await.unwrap().is_none());

    // list_actors returns everyone, oldest first, with kind/model/controller.
    let all = fs.list_actors().await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].id, alice);
    assert_eq!((all[1].id, all[1].kind), (claude, ActorKind::Agent));
    assert_eq!(all[1].agent_model.as_deref(), Some("claude-opus-4-8"));
    assert_eq!(all[1].controller_actor_id, Some(alice));
}

#[tokio::test]
async fn suggestion_content_reads_base_and_proposed() {
    let fs = fixture().await;
    let dan = fs.create_human("Dan", None).await.unwrap();
    let claude = fs.create_agent("claude", "opus", None).await.unwrap();
    let sd = fs.create_session(dan, None).await.unwrap();
    let sc = fs.create_session(claude, None).await.unwrap();

    fs.write_as(WriteCtx::session(dan, sd), "/doc.md", b"one\ntwo\n")
        .await
        .unwrap();
    let id = fs
        .suggest(
            WriteCtx::session(claude, sc),
            "/doc.md",
            b"one\ntwo\nthree\n",
            Some("add a line"),
        )
        .await
        .unwrap();

    // The proposed content is readable straight from the store — no app-side stash.
    let c = fs.suggestion_content(id).await.unwrap();
    assert_eq!(c.base, "one\ntwo\n");
    assert_eq!(c.proposed.as_deref(), Some("one\ntwo\nthree\n"));

    // A proposed deletion has no proposed content.
    let del = fs
        .suggest_delete(WriteCtx::session(claude, sc), "/doc.md", Some("remove it"))
        .await
        .unwrap();
    let cd = fs.suggestion_content(del).await.unwrap();
    assert_eq!(cd.base, "one\ntwo\n");
    assert!(cd.proposed.is_none());
}
