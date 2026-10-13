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
cargo test --locked -p rpcstack-call
cargo test --locked -p rpcstack-dagor
cargo test --locked -p rpcstack-breakwater
cargo test --locked -p rpcstack-hedged
cargo test --locked -p rpcstack-c3
