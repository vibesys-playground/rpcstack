#!/usr/bin/env bash
# Select this example's queue for one Cargo invocation; never edit framework files.
set -euo pipefail
example_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export RPCSTACK_QUEUE_SOURCE="$example_dir/queue.rs"
exec cargo "$@"
