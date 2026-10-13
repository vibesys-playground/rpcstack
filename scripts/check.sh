#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/validate_boundary.py
cargo fmt --all -- --check
export RUSTFLAGS="${RUSTFLAGS:-} -D warnings"
cargo check --locked --workspace --all-targets --features tokio/full
for feature in sched_fifo sched_prio sched_custom tailclipper sched_mt sched_mt_multiqueue; do
  cargo check --locked -p rpcstack-echo --features "tonic/rpcstack,tokio/$feature,tokio/rt-multi-thread"
done
cargo check --locked -p paper-rpc --all-targets --features orf
