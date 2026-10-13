#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/validate_boundary.py
cargo test --locked -p rpcstack -p rpcstack-wire -p rpcstack-tonic -p rpcstack-sched -p multiqueue-prio-queue
cargo test --locked -p tonic --lib
cargo test --locked -p tonic-build
cargo test --locked -p hyper --lib --features full,rpcstack h2_priority_tests
cargo test --locked -p hyper --lib --features full h2_priority_tests
cargo test --locked -p tokio --test rpcstack_priority --features full,sched_prio
cargo test --locked -p rpcstack-echo
cargo test --locked -p rpcstack-echo --features priority
cargo test --locked -p rajomon -p tailclipper
cargo test --locked -p paper-rpc --features orf
cargo test --locked -p masa -p masa-core -p masa-policy -p masa-semantics
cargo test --locked -p masa-policy --features sched_slo,abort_slo,signal_slack,est_mean_var,ac_pred,trace_queue_latency
cargo test --locked -p masa-semantics --features sched_slo,abort_slo,signal_slack,est_mean_var,ac_pred
cargo test --locked -p masa-policy --features sched_fifo,ac_rajomon
