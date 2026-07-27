"""find_or_create_actor: map your app's user id to an origofs actor, idempotently.

Build + run (from crates/origofs-py, in a venv):
    maturin develop
    python tests/test_actors.py       # or: pytest tests/
"""
import asyncio
import os
import tempfile

import origofs


def test_find_or_create_is_idempotent_by_subject():
    async def _exercise():
        d = tempfile.mkdtemp()
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )

        # unknown subject -> None
        assert await ws.actor_by_subject("user_42") is None

        # first call creates; second call with the same subject returns the SAME id
        a1 = await ws.find_or_create_human("user_42", "Dan")
        a2 = await ws.find_or_create_human("user_42", "Dan (renamed)")
        assert a1 == a2

        # a different subject is a different actor
        b = await ws.find_or_create_human("user_99", "Sam")
        assert b != a1

        # the lookup now resolves and carries the identity
        found = await ws.actor_by_subject("user_42")
        assert found is not None
        assert found["id"] == a1
        assert found["auth_subject"] == "user_42"
        assert found["kind"] == "human"

        # agents key on the subject the same way
        g1 = await ws.find_or_create_agent("agent_token_7", "claude", "opus", a1)
        g2 = await ws.find_or_create_agent("agent_token_7", "claude", "opus", a1)
        assert g1 == g2 and g1 != a1
        assert (await ws.actor_by_subject("agent_token_7"))["kind"] == "agent"

    asyncio.run(_exercise())


def test_actor_by_id_and_list_actors():
    async def _exercise():
        d = tempfile.mkdtemp()
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        dan = await ws.find_or_create_human("u_dan", "Dan")
        claude = await ws.find_or_create_agent("a_claude", "claude", "opus", dan)

        # resolve a bare actor_id (as carried by events/suggestions/presence)
        a = await ws.actor(dan)
        assert a is not None
        assert a["id"] == dan and a["kind"] == "human" and a["display_name"] == "Dan"
        assert await ws.actor(9999) is None

        # the whole directory, oldest first, no app-side table needed
        actors = await ws.list_actors()
        ids = [x["id"] for x in actors]
        assert ids == sorted(ids)  # oldest first
        by_id = {x["id"]: x for x in actors}
        assert dan in by_id and claude in by_id
        assert by_id[claude]["kind"] == "agent"
        assert by_id[claude]["agent_model"] == "opus"
        assert by_id[claude]["controller_actor_id"] == dan

    asyncio.run(_exercise())


def test_suggestion_content():
    async def _exercise():
        d = tempfile.mkdtemp()
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        dan = await ws.find_or_create_human("u_dan", "Dan")
        claude = await ws.find_or_create_agent("a_claude", "claude", "opus", dan)
        sc = await ws.create_session(claude)

        await ws.write_as(origofs.WriteCtx.actor(dan), "/doc.md", b"one\ntwo\n")
        sid = await ws.suggest(
            origofs.WriteCtx.session(claude, sc), "/doc.md", b"one\ntwo\nthree\n", "add a line"
        )

        # the proposed content reads straight from the store — no app-side stash
        c = await ws.suggestion_content(sid)
        assert c["base"] == "one\ntwo\n"
        assert c["proposed"] == "one\ntwo\nthree\n"

        # a proposed deletion has proposed == None
        did = await ws.suggest_delete(origofs.WriteCtx.session(claude, sc), "/doc.md", "remove it")
        cd = await ws.suggestion_content(did)
        assert cd["base"] == "one\ntwo\n" and cd["proposed"] is None

    asyncio.run(_exercise())


def _run_all():
    import inspect

    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and inspect.isfunction(fn):
            fn()
            print("ok  ", name)
    print("ALL OK")


if __name__ == "__main__":
    _run_all()
