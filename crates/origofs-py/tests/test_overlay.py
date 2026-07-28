"""origofs.overlay: the argv it shells out to (running an actual kernel overlay
needs privileges, so we test the command construction, which is the wrapper's
whole job).

Run (from crates/origofs-py):
    python tests/test_overlay.py       # or: pytest tests/
"""
from origofs.overlay import overlay_command


def test_overlay_command_shape():
    argv = overlay_command("/srv/ws", 7, ["claude", "-p", "do it"], sync_ms=250)
    assert argv == [
        "origofs", "--workspace", "/srv/ws",
        "overlay", "--actor", "7", "--sync-ms", "250",
        "--", "claude", "-p", "do it",
    ]


def test_overlay_command_defaults_and_binary_override():
    argv = overlay_command("/w", 1, ["sh", "-c", "echo hi"], origofs_bin="/opt/origofs")
    assert argv[0] == "/opt/origofs"
    assert "--sync-ms" in argv and argv[argv.index("--sync-ms") + 1] == "500"  # default
    # the `--` separator precedes the agent command, so agent flags aren't parsed by origofs
    sep = argv.index("--")
    assert argv[sep + 1:] == ["sh", "-c", "echo hi"]


def test_overlay_command_rejects_empty():
    try:
        overlay_command("/w", 1, [])
        raise AssertionError("expected ValueError for empty command")
    except ValueError:
        pass


def _run_all():
    import inspect

    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and inspect.isfunction(fn):
            fn()
            print("ok  ", name)
    print("ALL OK")


if __name__ == "__main__":
    _run_all()
