# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.97.1
ARG GO_VERSION=1.25.12
ARG CELLD_COMMIT=unknown

FROM rust:${RUST_VERSION}-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=celld-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=celld-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=celld-target-${TARGETARCH},target=/src/target,sharing=locked \
    mkdir -p /out && \
    cargo build --release --locked -p celld && \
    install -m 755 target/release/celld /out/celld

# The release workflow builds this stage on every candidate push, so a
# break in the engine's own tests or lints stops a release before any
# artifact is drafted.
FROM build AS test
ARG TARGETARCH
RUN rustup component add clippy
RUN --mount=type=cache,id=celld-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=celld-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=celld-target-${TARGETARCH},target=/src/target,sharing=locked \
    cargo test --release --locked && \
    cargo clippy --release --all-targets --locked -- -D warnings

FROM golang:${GO_VERSION}-bookworm AS litestream-build
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates git && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY crates/celld/Cargo.toml /tmp/celld-Cargo.toml
RUN --mount=type=cache,id=celld-litestream-source,target=/var/cache/litestream,sharing=locked \
    LITESTREAM_SHA=$(sed -n \
      's/^litestream-commit = "\(.*\)"$/\1/p' /tmp/celld-Cargo.toml) && \
    test -n "${LITESTREAM_SHA}" && \
    if ! test -d /var/cache/litestream/.git; then \
      git -C /var/cache/litestream init && \
      git -C /var/cache/litestream remote add origin \
        https://github.com/benbjohnson/litestream.git; \
    fi && \
    if ! git -C /var/cache/litestream cat-file -e "${LITESTREAM_SHA}^{commit}"; then \
      git -C /var/cache/litestream fetch --depth=1 origin "${LITESTREAM_SHA}"; \
    fi && \
    git -C /var/cache/litestream archive "${LITESTREAM_SHA}" | tar -xf - -C /src
RUN --mount=type=cache,id=celld-litestream-go-build,target=/root/.cache/go-build,sharing=locked \
    --mount=type=cache,id=celld-litestream-go-mod,target=/go/pkg/mod,sharing=locked \
    LITESTREAM_VERSION=$(sed -n \
      's/^litestream-version = "\(.*\)"$/\1/p' /tmp/celld-Cargo.toml) && \
    test -n "${LITESTREAM_VERSION}" && \
    mkdir -p /out && \
    go build -trimpath \
      -ldflags "-X main.Version=${LITESTREAM_VERSION}" \
      -o /out/litestream \
      ./cmd/litestream && \
    test "$(/out/litestream version)" = "${LITESTREAM_VERSION}"

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
ARG CELLD_COMMIT
ARG CELLD_VERSION=unknown
LABEL org.opencontainers.image.title="celld" \
      org.opencontainers.image.revision="${CELLD_COMMIT}" \
      org.opencontainers.image.version="${CELLD_VERSION}"
COPY --from=build /out/celld /usr/local/bin/celld
COPY --from=litestream-build /out/litestream /usr/local/bin/litestream
ENV LITESTREAM_BIN=/usr/local/bin/litestream
ENTRYPOINT ["/usr/local/bin/celld"]
