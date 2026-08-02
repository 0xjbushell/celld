# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.97.1
ARG GO_VERSION=1.25.12
ARG ESBUILD_VERSION=0.28.1
ARG CELLD_COMMIT=unknown
ARG CELLD_VERSION=unknown

FROM rust:${RUST_VERSION}-bookworm AS celld-build
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=celld-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=celld-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=celld-target-${TARGETARCH},target=/src/target,sharing=locked \
    mkdir -p /out && \
    cargo build --release --locked && \
    install -m 755 target/release/celld /out/celld && \
    PACKAGE_VERSION=$(sed -n 's/^version = "\(.*\)"$/\1/p' crates/celld/Cargo.toml | head -1) && \
    test "$(/out/celld --version)" = "celld ${PACKAGE_VERSION}"

FROM celld-build AS test
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
RUN --mount=type=cache,id=litestream-source,target=/var/cache/litestream,sharing=locked \
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
RUN --mount=type=cache,id=litestream-go-build,target=/root/.cache/go-build,sharing=locked \
    --mount=type=cache,id=litestream-go-mod,target=/go/pkg/mod,sharing=locked \
    LITESTREAM_VERSION=$(sed -n \
      's/^litestream-version = "\(.*\)"$/\1/p' /tmp/celld-Cargo.toml) && \
    test -n "${LITESTREAM_VERSION}" && \
    mkdir -p /out && \
    go build -trimpath \
      -ldflags "-X main.Version=${LITESTREAM_VERSION}" \
      -o /out/litestream \
      ./cmd/litestream && \
    test "$(/out/litestream version)" = "${LITESTREAM_VERSION}"

FROM node:22-bookworm-slim AS esbuild
ARG ESBUILD_VERSION
ARG TARGETARCH
RUN npm install --global "esbuild@${ESBUILD_VERSION}" && \
    case "${TARGETARCH}" in \
      amd64) ESBUILD_ARCH=x64 ;; \
      arm64) ESBUILD_ARCH=arm64 ;; \
      *) echo "unsupported esbuild architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    mkdir -p /out && \
    install -m 755 \
      "/usr/local/lib/node_modules/esbuild/node_modules/@esbuild/linux-${ESBUILD_ARCH}/bin/esbuild" \
      /out/esbuild && \
    test "$(/out/esbuild --version)" = "${ESBUILD_VERSION}"

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
ARG CELLD_COMMIT
ARG CELLD_VERSION
ARG ESBUILD_VERSION
LABEL org.opencontainers.image.title="celld" \
      org.opencontainers.image.source="https://github.com/denoland/celld" \
      org.opencontainers.image.revision="${CELLD_COMMIT}" \
      org.opencontainers.image.version="${CELLD_VERSION}" \
      dev.celld.esbuild.version="${ESBUILD_VERSION}"
COPY --from=celld-build /out/celld /usr/local/bin/celld
COPY --from=litestream-build /out/litestream /usr/local/bin/litestream
COPY --from=esbuild /out/esbuild /usr/local/bin/esbuild
ENV LITESTREAM_BIN=/usr/local/bin/litestream \
    CELLD_ESBUILD=/usr/local/bin/esbuild
ENTRYPOINT ["/usr/local/bin/celld"]
