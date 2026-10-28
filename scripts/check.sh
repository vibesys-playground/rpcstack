#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/validate_boundary.py
cargo fmt --all -- --check
rustfmt --edition 2021 --check examples/tailclipper/queue.rs
export RUSTFLAGS="${RUSTFLAGS:-} -D warnings"
cargo check --locked --workspace --all-targets --features tokio/full
for feature in sched_fifo sched_prio sched_custom sched_mt sched_mt_multiqueue; do
  cargo check --locked -p rpcstack-echo --features "tonic/rpcstack,tokio/$feature,tokio/rt-multi-thread"
done
./examples/tailclipper/cargo.sh check --locked -p rpcstack-policy-composition --all-targets --features orf
./examples/tailclipper/cargo.sh check --locked -p masa --features sched_tailclipper
cargo check --locked -p masa --features sched_slo,abort_slo,signal_slack,est_mean_var,ac_pred,trace_queue_latency
cargo check --locked -p masa --features sched_fifo,ac_rajomon
