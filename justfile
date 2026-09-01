# origofs task runner — `just` or `just --list` for the recipes.
#
# Scope is deliberately narrow: cutting a release. Everything else (build, test,
# lint) is a plain cargo invocation documented in CLAUDE.md, and wrapping those
# would only add a second place for them to drift.
#
# The release recipes exist to make one specific mistake impossible. Cargo cannot
# read a git tag, so `[workspace.package].version` is the only source of truth and
# a `v*` tag is merely an assertion about it — an assertion that has been wrong in
# this repo before (v0.0.2 sits on a 0.1.0 manifest, v0.0.4 on a 0.0.3 one). The
# fix is not to type the tag more carefully; it is to never type it at all. So
# `just release` derives the tag from the manifest it just bumped, and the
# `guard` job in .github/workflows/release.yml re-checks the same thing for any
# tag pushed by hand.
#
# Pushing is a separate recipe on purpose: `just release` is entirely local and
# reversible, and `just release-push` is the step that publishes.

# List the recipes.
default:
    @just --list

# Assert the workspace agrees with itself, and with VERSION if given (same script CI runs).
check-versions version="":
    @python3 .github/scripts/check_versions.py {{version}}

# Bump every crate to VERSION, commit, and tag. Local only — nothing is pushed.
release version:
    #!/usr/bin/env bash
    set -euo pipefail
    want="{{version}}"

    if ! command -v cargo-set-version >/dev/null; then
        echo "error: cargo-edit is not installed — \`cargo install cargo-edit\`" >&2
        exit 1
    fi

    # release.yml refuses to publish a tag that is not contained in origin/main,
    # so tagging anywhere else produces a tag that is built and then rejected.
    if [ "$(git branch --show-current)" != "main" ]; then
        echo "error: not on main — releases are only cut from main" >&2
        exit 1
    fi
    if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "error: working tree is dirty; commit or stash first" >&2
        exit 1
    fi
    git fetch --quiet origin main
    if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
        echo "error: local main and origin/main have diverged; pull first" >&2
        exit 1
    fi
    if git rev-parse -q --verify "refs/tags/v${want}" >/dev/null; then
        echo "error: tag v${want} already exists" >&2
        exit 1
    fi

    # Rewrites [workspace.package].version, all six intra-workspace dependency
    # requirements, and Cargo.lock — the lockfile matters because ci.yml's `msrv`
    # job runs `cargo check --locked`.
    cargo set-version --workspace "$want"
    python3 .github/scripts/check_versions.py "$want"

    git commit --quiet --all --message "Release ${want}"

    # Read back from the manifest rather than reuse $want: the tag's job is to
    # name what the tree actually says, and this is the line that makes the two
    # incapable of disagreeing. The check above already proved there is exactly
    # one version, so popping the set is safe.
    tag="v$(cargo metadata --no-deps --format-version 1 |
        python3 -c 'import json,sys; v={p["version"] for p in json.load(sys.stdin)["packages"]}; print(v.pop())')"
    git tag -a "$tag" -m "Release ${tag#v}"

    echo
    git --no-pager show --stat --oneline "$tag" | head -n 20
    echo
    echo "Tagged ${tag} locally. Nothing has been pushed."
    echo "  publish:  just release-push"
    echo "  back out: git tag -d ${tag} && git reset --hard HEAD~1"

# Push the release commit and its tag. This is the step that publishes.
release-push:
    #!/usr/bin/env bash
    set -euo pipefail

    if [ "$(git branch --show-current)" != "main" ]; then
        echo "error: not on main" >&2
        exit 1
    fi
    tag="$(git describe --exact-match --tags HEAD 2>/dev/null || true)"
    if [ -z "$tag" ]; then
        echo "error: HEAD carries no tag — run \`just release VERSION\` first" >&2
        exit 1
    fi

    echo "Pushing ${tag} starts release.yml: five wheels, an sdist, a GitHub"
    echo "Release, and a PyPI upload once PUBLISH_TO_PYPI is set. A version"
    echo "published to PyPI cannot be reused, even after deletion."
    read -r -p "Push ${tag} to origin? [y/N] " reply
    if [ "$reply" != "y" ]; then
        echo "aborted"
        exit 1
    fi

    git push origin main
    git push origin "$tag"
