"""Tests for the origofs document server (examples/web/server/app.py).

Run (after building the bindings — see requirements.txt):
    pip install fastapi httpx pytest
    pytest test_app.py            # or: python test_app.py

The end-to-end tests use a real origofs.Workspace (a temp dir opened by the app's
lifespan), so they need the compiled `origofs` extension. They prove the thing that
matters: a write made through the server is attributed, server-side, to the
actor the bearer token resolves to — and a client can't forge that.
"""
import origofs  # noqa: F401  (import guard: the app needs the compiled extension)
from fastapi.testclient import TestClient

from app import app


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_config_lists_demo_tokens():
    with TestClient(app) as c:
        cfg = c.get("/api/config").json()
        assert cfg["demo"] is True
        names = {t["token"] for t in cfg["tokens"]}
        assert {"tok-ada", "tok-grace", "tok-claude"} <= names


def test_actor_directory_is_complete_at_startup():
    # Every demo principal is onboarded on startup, so the id-only feeds
    # (events/suggestions) always resolve to a name.
    with TestClient(app) as c:
        actors = c.get("/api/actors").json()
        by_name = {a["display_name"]: a for a in actors}
        assert "Ada Lovelace" in by_name and by_name["Ada Lovelace"]["kind"] == "human"
        assert "claude" in by_name and by_name["claude"]["kind"] == "agent"


def test_unauthenticated_write_is_refused():
    with TestClient(app) as c:
        r = c.put("/fs/files/notes.md", content=b"sneaky")
        assert r.status_code == 401, r.text


def test_write_is_attributed_and_doc_load_carries_blame():
    with TestClient(app) as c:
        # /api/me resolves the token to an origofs actor server-side.
        me = c.get("/api/me", headers=_auth("tok-ada")).json()
        assert me["display_name"] == "Ada Lovelace" and me["kind"] == "human"

        body = b"# Notes\nfirst line by ada\nsecond line by ada\n"
        r = c.put("/fs/files/notes.md", content=body, headers=_auth("tok-ada"))
        assert r.status_code == 200, r.text

        doc = c.get("/api/doc/notes.md").json()
        assert doc["exists"] is True
        assert doc["text"] == body.decode()
        # blame covers every line, all credited to Ada (a human).
        assert doc["blame"], "attributed write must produce blame"
        assert all(r["actor"]["display_name"] == "Ada Lovelace" for r in doc["blame"])
        assert all(r["actor"]["kind"] == "human" for r in doc["blame"])
        covered = {ln for r in doc["blame"] for ln in range(r["line_start"], r["line_end"] + 1)}
        assert covered == {1, 2, 3}


def test_missing_doc_loads_as_empty_not_404():
    with TestClient(app) as c:
        doc = c.get("/api/doc/does/not/exist.md").json()
        assert doc == {"path": "/does/not/exist.md", "exists": False, "text": "", "blame": []}


def test_suggestion_flow_mixes_human_and_agent_blame():
    with TestClient(app) as c:
        v1 = b"# Notes\nwritten by ada\n"
        c.put("/fs/files/doc.md", content=v1, headers=_auth("tok-ada")).raise_for_status()

        # An agent *suggests* an edit — the working tree is untouched until accepted.
        v2 = b"# Notes\nwritten by ada\nappended by claude\n"
        r = c.post(
            "/fs/suggestions",
            params={"path": "/doc.md", "summary": "append a line"},
            content=v2,
            headers=_auth("tok-claude"),
        )
        r.raise_for_status()
        sid = r.json()["id"]
        assert c.get("/api/doc/doc.md").json()["text"] == v1.decode(), "not applied yet"

        pending = c.get("/fs/suggestions", params={"status": "pending"}).json()
        assert sid in [s["id"] for s in pending]
        assert "appended by claude" in c.get(f"/fs/suggestions/{sid}/diff").text

        # A reviewer accepts it — applied, credited to the agent (the author).
        c.post(f"/fs/suggestions/{sid}/accept", headers=_auth("tok-grace")).raise_for_status()
        doc = c.get("/api/doc/doc.md").json()
        assert doc["text"] == v2.decode()
        kinds = {r["actor"]["kind"] for r in doc["blame"]}
        assert kinds == {"human", "agent"}, f"blame should mix human + agent, got {kinds}"


def test_inline_review_detail_exposes_hunks():
    with TestClient(app) as c:
        c.put("/fs/files/r.md", content=b"line one\nline two\nline three\n",
              headers=_auth("tok-ada")).raise_for_status()
        r = c.post("/fs/suggestions", params={"path": "/r.md", "summary": "tweak"},
                   content=b"line one CHANGED\nline two\nline three\nline four added\n",
                   headers=_auth("tok-claude"))
        r.raise_for_status()
        sid = r.json()["id"]
        detail = c.get(f"/api/suggestion/{sid}").json()
        assert detail["actor_kind"] == "agent" and detail["actor_name"] == "claude"
        assert detail["hunks"] == 2, detail
        # segments: a replace (hunk 0) and an insert (hunk 1), plus equal context.
        changed = [s for s in detail["segments"] if s["hunk"] is not None]
        assert {s["hunk"] for s in changed} == {0, 1}


def test_inline_partial_keep_credits_the_agent_not_the_reviewer():
    with TestClient(app) as c:
        c.put("/fs/files/p.md", content=b"line one\nline two\nline three\n",
              headers=_auth("tok-ada")).raise_for_status()
        sid = c.post("/fs/suggestions", params={"path": "/p.md"},
                     content=b"line one CHANGED\nline two\nline three\nline four added\n",
                     headers=_auth("tok-claude")).json()["id"]

        # Grace reviews and keeps only hunk 0 (the change to line 1), discarding
        # the added line 4.
        r = c.post(f"/api/suggestion/{sid}/apply", json={"keep": [0]}, headers=_auth("tok-grace"))
        r.raise_for_status()
        assert r.json()["mode"] == "partial"

        doc = c.get("/api/doc/p.md").json()
        assert doc["text"] == "line one CHANGED\nline two\nline three\n", doc["text"]
        by_line = {}
        for rng in doc["blame"]:
            for ln in range(rng["line_start"], rng["line_end"] + 1):
                by_line[ln] = rng["actor"]
        # The kept change (line 1) is credited to the AGENT, not Grace the reviewer.
        assert by_line[1]["kind"] == "agent" and by_line[1]["display_name"] == "claude"
        assert by_line[2]["display_name"] == "Ada Lovelace"


def test_inline_keep_all_uses_native_accept():
    with TestClient(app) as c:
        c.put("/fs/files/k.md", content=b"alpha\nbeta\n", headers=_auth("tok-ada")).raise_for_status()
        sid = c.post("/fs/suggestions", params={"path": "/k.md"},
                     content=b"alpha\nbeta\ngamma by claude\n",
                     headers=_auth("tok-claude")).json()["id"]
        r = c.post(f"/api/suggestion/{sid}/apply", json={"keep": [0]}, headers=_auth("tok-grace"))
        r.raise_for_status()
        assert r.json()["mode"] == "accept"  # single hunk, all kept → native accept
        doc = c.get("/api/doc/k.md").json()
        assert "gamma by claude" in doc["text"]
        last = doc["blame"][-1]["actor"]
        assert last["kind"] == "agent" and last["display_name"] == "claude"


def test_commit_then_log_records_history():
    with TestClient(app) as c:
        c.put("/fs/files/h.md", content=b"one\n", headers=_auth("tok-ada")).raise_for_status()
        r = c.post("/fs/commit", json={"message": "first", "author": "ada"},
                   headers=_auth("tok-ada"))
        r.raise_for_status()
        log = c.get("/fs/log").json()
        assert log and log[0]["message"] == "first"
        assert isinstance(log[0]["hash"], str) and len(log[0]["hash"]) == 64


if __name__ == "__main__":
    import sys
    import traceback

    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("ok  ", name)
            except Exception:  # noqa: BLE001
                failures += 1
                print("FAIL", name)
                traceback.print_exc()
    print("ALL OK" if not failures else f"{failures} FAILED")
    sys.exit(1 if failures else 0)
