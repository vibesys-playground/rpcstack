FROM mongo@sha256:1f995ad6fdb93244a1addab1b58f934a0bc2f5643c38e02f5e9d7f0c7d227a7b AS mongo
FROM redis@sha256:46f15cf64fdef63e483dcda99e74e3d51c017b1175547b6bb874152ee2391aad AS redis
FROM golang:1.24-bookworm@sha256:1a6d4452c65dea36aac2e2d606b01b4a029ec90cc1ae53890540ce6173ea77ac AS go
FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
COPY --from=mongo /usr/bin/mongod /usr/local/bin/mongod
COPY --from=redis /usr/local/bin/redis-server /usr/local/bin/redis-server
COPY --from=go /usr/local/go /usr/local/go
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git build-essential pkg-config libssl-dev libcurl4 \
    libgssapi-krb5-2 liblzma5 python3 python3-venv util-linux protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo
ENV PATH=/opt/cargo/bin:/usr/local/go/bin:/usr/local/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain nightly-2026-01-07 \
    && chmod -R a+rwX /opt/cargo /opt/rustup
COPY --from=ghcr.io/astral-sh/uv:0.9.5@sha256:f459f6f73a8c4ef5d69f4e6fbbdb8af751d6fa40ec34b39a1ab469acd6e289b7 /uv /uvx /usr/local/bin/
