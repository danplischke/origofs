"""Assert the workspace speaks one version, and that a release tag matches it.

Cargo cannot read a version out of a git tag. `[workspace.package].version` in
the root `Cargo.toml` is the single source of truth — every member inherits it
via `version.workspace = true`, and `crates/origofs-py/pyproject.toml` declares
`dynamic = ["version"]` so maturin stamps the wheel from that same value. A `v*`
tag is therefore never an *input* to the build; it is an *assertion about* the
committed source. This script is what makes a wrong assertion fail before any
wheel is built, instead of publishing an artifact whose version nobody can find
in git.

Two things drift, and both are checked:

  * **A member that hardcodes its own `version`** instead of inheriting. The
    workspace sat at `0.0.0` while origofs-core/-sdk/-cli each said `0.1.0`
    exactly this way, and origofs-py — the one crate that did inherit — shipped a
    wheel `pip` read as 0.0.0. That is the bug the comment on `Cargo.toml:6`
    records, and it is invisible to every test in the repo.
  * **An intra-workspace dependency requirement** (`origofs-sdk = { version =
    "0.0.3", path = "../origofs-sdk" }`) left behind by a bump. There are six of
    them against one authoritative version, so a hand-edited bump is a
    seven-place edit. Cargo does eventually reject the mismatch itself, but as a
    resolution error partway through a five-platform release matrix rather than
    as a sentence naming the file to fix.

Bump all seven together with `cargo set-version --workspace <version>`
(cargo-edit), which rewrites the dep requirements and `Cargo.lock` too — the
lockfile is tracked and ci.yml's `msrv` job runs `cargo check --locked`, so a
stale lockfile is its own failure.

This lives in `.github/scripts/` and is shared by two workflows rather than
inlined in either, for the same reason `wheel_smoke.py` is:

  * `ci.yml`      — the `rust` job, so drift is caught on the PR that causes it;
  * `release.yml` — the `guard` job, with the tag's version, so a mislabeled tag
    never reaches the build matrix.

Usage:
    check_versions.py              # workspace self-consistency only
    check_versions.py 0.1.0        # ...and require exactly that version

Deliberately dependency-free (stdlib + `cargo metadata`) so it runs on a bare
runner with no `pip install` step.
"""

import json
import subprocess
import sys

expected = sys.argv[1] if len(sys.argv) > 1 else None

meta = json.loads(
    subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
)

# `--no-deps` keeps this to the workspace's own manifests: no registry access, no
# lockfile resolution, so it stays fast enough to gate on and works offline.
members = {p["name"]: p["version"] for p in meta["packages"]}
errors = []

versions = sorted(set(members.values()))
if len(versions) > 1:
    listing = ", ".join(f"{n} {v}" for n, v in sorted(members.items()))
    errors.append(
        f"workspace members disagree on a version ({listing}); "
        "every member needs `version.workspace = true`"
    )

for pkg in meta["packages"]:
    for dep in pkg["dependencies"]:
        # Only intra-workspace path deps — a third-party `^1.0` requirement is
        # meant to be a range and says nothing about our version.
        if not dep.get("path") or dep["name"] not in members:
            continue
        want = members[dep["name"]]
        # cargo normalizes a bare `"0.0.3"` to `^0.0.3`; compare the operand.
        if dep["req"].lstrip("^=") != want:
            errors.append(
                f"{pkg['name']} requires {dep['name']} {dep['req']}, "
                f"but {dep['name']} is {want}"
            )

if expected is not None and len(versions) == 1 and versions[0] != expected:
    errors.append(
        f"tag says {expected}, but the workspace is {versions[0]}; "
        f"run `cargo set-version --workspace {expected}`, commit, then re-tag"
    )

# The same stale requirement shows up once per target section it appears in
# (origofs-py names origofs-sdk three times), and one line per fix reads better
# than three identical ones.
seen = set()
for e in errors:
    if e not in seen:
        seen.add(e)
        print(f"error: {e}", file=sys.stderr)

if errors:
    sys.exit(1)

print(f"workspace version {versions[0]} is consistent across {len(members)} crates")
