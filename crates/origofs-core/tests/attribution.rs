//! Attribution & blame: per-line human-vs-agent authorship, provenance, the
//! edit-op log, and reverting an agent's session.

use origofs_core::{
    ActorInit, ActorKind, Fs, MemStore, OrigoFSError, SqliteMetadataStore, WriteCtx, WriteOutcome,
    WritePolicy,
};
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
    let changed = fs.revert_session(claude, s_claude, None).await.unwrap();
    assert_eq!(changed, vec!["/doc".to_string()]);
    assert_eq!(&fs.read("/doc").await.unwrap()[..], b"human-1\nhuman-2\n");
    let b = fs.blame("/doc").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].actor.id, alice);
}

// Issue #94: in a multi-tenant workspace an "undo this agent's work" button lives
// in *one* tenant's UI, but the session it reverts may have written anywhere. The
// scope bounds the blast radius to a subtree — and does it inside the call, so
// there is no window between reading the session's reach and acting on it.
#[tokio::test]
async fn a_path_prefix_bounds_the_revert_to_one_subtree() {
    let fs = fixture().await;
    let agent = fs.create_agent("claude", "m", None).await.unwrap();
    let s = fs.create_session(agent, None).await.unwrap();
    let ctx = WriteCtx::session(agent, s);

    // The same session writes in two tenants, and in a third whose name merely
    // *starts with* the first tenant's — the case a naive `starts_with` gets wrong.
    for p in [
        "/tenant-a/notes.txt",
        "/tenant-b/notes.txt",
        "/tenant-abc/notes.txt",
    ] {
        fs.mkdir_p(p.rsplit_once('/').unwrap().0).await.unwrap();
        fs.write_as(ctx, p, b"agent wrote this\n").await.unwrap();
    }

    let changed = fs
        .revert_session(agent, s, Some("/tenant-a"))
        .await
        .unwrap();
    assert_eq!(changed, vec!["/tenant-a/notes.txt".to_string()]);

    // Only the scoped tenant lost the agent's line.
    assert_eq!(&fs.read("/tenant-a/notes.txt").await.unwrap()[..], b"");
    assert_eq!(
        &fs.read("/tenant-b/notes.txt").await.unwrap()[..],
        b"agent wrote this\n"
    );
    // The sibling sharing a textual prefix is untouched — this is the assertion
    // the whole `PathScope` type exists for.
    assert_eq!(
        &fs.read("/tenant-abc/notes.txt").await.unwrap()[..],
        b"agent wrote this\n"
    );

    // A second, unscoped revert still reaches the rest.
    let changed = fs.revert_session(agent, s, None).await.unwrap();
    assert_eq!(
        changed,
        vec![
            "/tenant-b/notes.txt".to_string(),
            "/tenant-abc/notes.txt".to_string()
        ]
    );
}

#[tokio::test]
async fn a_trailing_slash_and_the_root_prefix_behave_sensibly() {
    let fs = fixture().await;
    let agent = fs.create_agent("claude", "m", None).await.unwrap();
    let s = fs.create_session(agent, None).await.unwrap();
    let ctx = WriteCtx::session(agent, s);

    fs.mkdir_p("/t").await.unwrap();
    fs.write_as(ctx, "/t/a.txt", b"x\n").await.unwrap();
    fs.write_as(ctx, "/top.txt", b"y\n").await.unwrap();

    // A trailing slash means the same thing as none.
    assert_eq!(
        fs.revert_session(agent, s, Some("/t/")).await.unwrap(),
        vec!["/t/a.txt".to_string()]
    );
    // `/t` must not have swallowed `/top.txt`.
    assert_eq!(&fs.read("/top.txt").await.unwrap()[..], b"y\n");

    // The root prefix covers everything, like passing None.
    assert_eq!(
        fs.revert_session(agent, s, Some("/")).await.unwrap(),
        vec!["/top.txt".to_string()]
    );

    // A relative prefix is a caller error, not a silent no-op.
    assert!(fs.revert_session(agent, s, Some("t")).await.is_err());
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

// write_as_blamed lets an authoritative caller (a CRDT/editor checkpoint) set
// authorship directly, so co-edited spans are credited to their true authors —
// not to whoever drove the write, which is all the diff heuristic could infer on
// a fresh file. This is the M8 CRDT -> blame bridge.
#[tokio::test]
async fn write_as_blamed_honors_explicit_authors() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs
        .create_agent("claude", "claude-opus-4-8", Some(alice))
        .await
        .unwrap();
    let s_alice = fs.create_session(alice, Some("editor")).await.unwrap();
    let s_claude = fs.create_session(claude, Some("mcp")).await.unwrap();

    // One checkpoint driven by the agent's session, but with lines authored by
    // alice / claude / alice — exactly what a co-edited CRDT snapshot produces. A
    // plain write_as here would credit all three lines to claude. Spans are byte
    // lengths: "alice-1\n"=8, "claude-1\n"=9, "alice-2\n"=8.
    fs.write_as_blamed(
        WriteCtx::session(claude, s_claude),
        "/doc",
        b"alice-1\nclaude-1\nalice-2\n",
        &[
            (alice, s_alice, 8),
            (claude, s_claude, 9),
            (alice, s_alice, 8),
        ],
    )
    .await
    .unwrap();

    let b = fs.blame("/doc").await.unwrap();
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
    assert_eq!(b[0].actor.kind, ActorKind::Human);
    assert_eq!(b[1].actor.kind, ActorKind::Agent);
    assert_eq!(b[1].session, Some(s_claude)); // session carried per span

    // The explicit map replaces prior authorship wholesale: re-checkpoint the same
    // bytes crediting everything to alice, and blame collapses to a single range.
    fs.write_as_blamed(
        WriteCtx::session(claude, s_claude),
        "/doc",
        b"alice-1\nclaude-1\nalice-2\n",
        &[(alice, s_alice, 25)],
    )
    .await
    .unwrap();
    let b = fs.blame("/doc").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(
        (b[0].actor.id, b[0].line_start, b[0].line_end),
        (alice, 1, 3)
    );
}

// The byte-range model's payoff: two authors on ONE line survive as two ranges,
// which the old per-line map could not represent. This is what a co-edited CRDT
// checkpoint of a single line produces.
#[tokio::test]
async fn write_as_blamed_preserves_sub_line_authorship() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_c = fs.create_session(claude, None).await.unwrap();

    // "hello world\n": alice authored "hello " (6 bytes), claude "world\n" (6).
    fs.write_as_blamed(
        WriteCtx::session(claude, s_c),
        "/doc",
        b"hello world\n",
        &[(alice, s_a, 6), (claude, s_c, 6)],
    )
    .await
    .unwrap();

    let b = fs.blame("/doc").await.unwrap();
    assert_eq!(b.len(), 2);
    // Both on line 1, distinct byte ranges — sub-line authorship survives.
    assert_eq!(
        (
            b[0].actor.id,
            b[0].line_start,
            b[0].line_end,
            b[0].byte_start,
            b[0].byte_end
        ),
        (alice, 1, 1, 0, 6)
    );
    assert_eq!(
        (
            b[1].actor.id,
            b[1].line_start,
            b[1].line_end,
            b[1].byte_start,
            b[1].byte_end
        ),
        (claude, 1, 1, 6, 12)
    );
}

// The spans must cover exactly the content's bytes, or the blame index would
// desync from it — reject rather than write a corrupt mapping.
#[tokio::test]
async fn write_as_blamed_rejects_spans_that_dont_cover_the_content() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let s = fs.create_session(alice, None).await.unwrap();

    // 8-byte content, spans cover only 4 -> rejected before anything is written.
    let err = fs
        .write_as_blamed(
            WriteCtx::session(alice, s),
            "/f",
            b"one\ntwo\n",
            &[(alice, s, 4)],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, OrigoFSError::InvalidArgument(_)),
        "got {err:?}"
    );

    // The file was never created (validation precedes the write).
    assert!(fs.blame("/f").await.is_err());
}

// The write policy is a bounded, actor-agnostic trust gate (§6): a `Direct` actor
// writes straight to the tree; a `Propose` actor's write is routed into the
// suggestion queue instead of landing, and only appears after a *different* actor
// accepts it. Nothing here keys on the actor's kind — a human or an agent is
// gated identically by its own policy.
#[tokio::test]
async fn write_policy_gates_direct_writes_into_suggestions() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap(); // reviewer, stays direct
    let ext = fs.create_human("ext", None).await.unwrap(); // an untrusted human contributor
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_e = fs.create_session(ext, None).await.unwrap();

    // Default is direct — both actors write straight through until reconfigured.
    assert_eq!(
        fs.get_actor(ext).await.unwrap().unwrap().write_policy,
        WritePolicy::Direct
    );

    // Bound `ext` to propose-only. (A human — the gate is the policy, not the kind.)
    fs.set_write_policy(ext, WritePolicy::Propose)
        .await
        .unwrap();
    assert_eq!(
        fs.get_actor(ext).await.unwrap().unwrap().write_policy,
        WritePolicy::Propose
    );

    // Alice (direct) lands her write immediately.
    let out = fs
        .write_or_propose(WriteCtx::session(alice, s_a), "/doc", b"from alice", None)
        .await
        .unwrap();
    assert_eq!(out, WriteOutcome::Wrote);
    assert_eq!(&fs.read("/doc").await.unwrap()[..], b"from alice");

    // `ext` cannot land a direct write — it becomes a pending suggestion, and the
    // file is untouched.
    let out = fs
        .write_or_propose(
            WriteCtx::session(ext, s_e),
            "/doc",
            b"from ext",
            Some("my edit"),
        )
        .await
        .unwrap();
    let sid = match out {
        WriteOutcome::Proposed(id) => id,
        WriteOutcome::Wrote => panic!("a propose-only actor must not write directly"),
    };
    assert_eq!(&fs.read("/doc").await.unwrap()[..], b"from alice"); // unchanged

    // A different actor (alice) reviews and accepts; only now does it land, credited
    // to its proposer (ext).
    fs.accept_suggestion(sid, WriteCtx::session(alice, s_a))
        .await
        .unwrap();
    assert_eq!(&fs.read("/doc").await.unwrap()[..], b"from ext");
    let blame = fs.blame("/doc").await.unwrap();
    assert!(blame.iter().all(|r| r.actor.id == ext));
}

// A9 (issue #70): `revert_session` must remove EXACTLY the lines an actor
// authored in a session and leave everyone else's intact — for arbitrary
// interleavings, not just the single hand-picked append covered above. This is a
// randomized model test: it builds a random interleaving of human and agent
// line-insertions (every line globally unique, so the line-diff attributes each
// insertion unambiguously) while tracking a ground-truth `is_agent` owner per
// line. After reverting the agent's session, both the file content and its blame
// must equal exactly the human-authored lines, in original order. A regression
// that over-reverts (drops a human line) or under-reverts (keeps an agent line)
// fails on some seed.
#[tokio::test]
async fn revert_session_removes_exactly_that_actors_lines_under_interleaving() {
    // xorshift64* — a tiny deterministic PRNG so failures reproduce by seed.
    fn rng_next(state: &mut u64) -> u64 {
        let mut x = *state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        *state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn render(lines: &[(String, bool)]) -> Vec<u8> {
        let mut s = String::new();
        for (t, _) in lines {
            s.push_str(t);
            s.push('\n');
        }
        s.into_bytes()
    }

    for seed in 0..64u64 {
        let fs = fixture().await;
        let human = fs.create_human("h", None).await.unwrap();
        let agent = fs.create_agent("a", "m", Some(human)).await.unwrap();
        let sh = fs.create_session(human, None).await.unwrap();
        let sa = fs.create_session(agent, None).await.unwrap();

        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut lines: Vec<(String, bool)> = Vec::new(); // (text, is_agent)
        let mut next = 0u64;

        // Seed with one human line so the file exists and is never empty.
        lines.push((format!("L{next}-human"), false));
        next += 1;
        fs.write_as(WriteCtx::session(human, sh), "/doc", &render(&lines))
            .await
            .unwrap();

        // A random number of insertions, each a full-file rewrite that splices in
        // one new unique line attributed to whoever "wrote" it.
        let ops = 5 + (rng_next(&mut state) % 12) as usize;
        for _ in 0..ops {
            let is_agent = rng_next(&mut state) & 1 == 0;
            let tag = if is_agent { "agent" } else { "human" };
            let text = format!("L{next}-{tag}");
            next += 1;
            let pos = (rng_next(&mut state) as usize) % (lines.len() + 1);
            lines.insert(pos, (text, is_agent));
            let ctx = if is_agent {
                WriteCtx::session(agent, sa)
            } else {
                WriteCtx::session(human, sh)
            };
            fs.write_as(ctx, "/doc", &render(&lines)).await.unwrap();
        }

        // Reverting the agent's session must strip exactly its lines.
        fs.revert_session(agent, sa, None).await.unwrap();

        // Oracle: only the human's lines survive, in their original order.
        let expected: Vec<(String, bool)> = lines
            .iter()
            .filter(|(_, is_agent)| !*is_agent)
            .cloned()
            .collect();
        assert_eq!(
            fs.read("/doc").await.unwrap()[..],
            render(&expected)[..],
            "seed {seed}: content after reverting the agent session must be exactly the human lines"
        );
        for run in fs.blame("/doc").await.unwrap() {
            assert_eq!(
                run.actor.id, human,
                "seed {seed}: a non-human line survived the agent-session revert"
            );
        }
    }
}
