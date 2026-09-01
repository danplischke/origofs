# syntax=docker/dockerfile:1
#
# origofs — the `origofs` + `git-remote-origofs` binaries in a slim runtime image.
# Build:  docker build -t origofs .
# Run:    docker run --rm -p 8080:8080 -v $PWD/ws:/var/lib/origofs/ws origofs \
#           serve --addr 0.0.0.0:8080 --auth-token TOKEN=ACTOR_ID
# Or bring up the full Postgres + MinIO stack with docker-compose.yml.

# --- build stage: compile the CLI (workspace release build) ------------------
# Must be >= the workspace MSRV enforced by the `msrv` CI job (edition 2024 sets a
# 1.85 *language* floor, but the code uses let-chains, stabilized in 1.88). Keep
# this in step with .github/workflows/ci.yml's `msrv` job.
FROM rust:1.98-slim AS build
# The CLI enables origofs-sdk's `full` features, including FUSE — but `fuser`'s
# `libfuse`/`libfuse3` features are off by default (see release.yml), so on Linux
# it takes the pure-Rust mount path and links no C library at build time. The
# object-store TLS stack is rustls, so no OpenSSL dev headers are needed either.
WORKDIR /src
COPY . .
# --locked builds against the committed Cargo.lock. Installs both binaries
# (origofs, git-remote-origofs) into /out/bin.
RUN cargo install --path crates/origofs-cli --root /out --locked

# --- runtime stage -----------------------------------------------------------
FROM debian:bookworm-slim AS runtime
# ca-certificates: TLS to object storage. curl: the container HEALTHCHECK.
# fuse3: not a link-time dependency (fuser's pure-Rust path links nothing), but
# `origofs mount` execs the setuid `fusermount3` helper at runtime for an
# unprivileged mount, and that helper — not just libfuse3's shared library —
# is what the `fuse3` package provides.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl fuse3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/bin/origofs /usr/local/bin/origofs
COPY --from=build /out/bin/git-remote-origofs /usr/local/bin/git-remote-origofs

# Run unprivileged. The workspace dir holds local sidecars (e.g. the pack index);
# metadata lives in the DB and content in the object store, so it is disposable.
RUN useradd --system --uid 10001 --create-home --home-dir /var/lib/origofs origofs \
    && mkdir -p /var/lib/origofs/ws /etc/origofs \
    && chown -R origofs:origofs /var/lib/origofs
USER origofs
WORKDIR /var/lib/origofs
EXPOSE 8080
# Structured logs by default; quiet the Postgres driver's benign NOTICEs.
ENV ORIGOFS_LOG="info,tokio_postgres=warn"

ENTRYPOINT ["origofs"]
# No serve args by default: `serve` on a non-loopback address refuses to run
# without --auth-token (it never exposes an unauthenticated API), so the concrete
# invocation is supplied by `docker run …`/compose. Bare `docker run origofs`
# prints usage.
CMD ["--help"]
