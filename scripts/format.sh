#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all
rustfmt --edition 2021 examples/tailclipper/queue.rs
