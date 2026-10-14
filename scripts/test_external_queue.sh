#!/usr/bin/env bash
# Exercise source injection, same-target rebuilds, validation, and default recovery.
set -euo pipefail
cd "$(dirname "$0")/.."
queue_probe=$(mktemp -d)
trap 'rm -rf "$queue_probe"' EXIT
cat > "$queue_probe/queue.rs" <<'RS'
use crate::{RunQueue, SchedFlavor, TaskView};
pub struct Queue<T>(Vec<T>);
impl<T> RunQueue<T> for Queue<T> {
    const FLAVOR: SchedFlavor = SchedFlavor::Prio;
    fn with_capacity(cap: usize) -> Self { Self(Vec::with_capacity(cap)) }
    fn push(&mut self, item: T, _: &TaskView<'_>) { self.0.push(item); }
    fn pop(&mut self) -> Option<T> { self.0.pop() }
    fn len(&self) -> usize { self.0.len() }
    fn capacity(&self) -> usize { self.0.capacity() }
}
#[cfg(test)]
mod tests {
    use crate::{Meta, RunQueue, SelectedQueue, TaskView};
    #[test]
    fn application_queue_is_selected() {
        let mut q = SelectedQueue::with_capacity(0);
        let now = std::time::Instant::now();
        q.push(1u64, &TaskView::new(1, &Meta::new(1), now, now, 0));
        q.push(2u64, &TaskView::new(2, &Meta::new(2), now, now, 0));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.len(), 1);
    }
}
RS
run_probe() {
    RPCSTACK_QUEUE_SOURCE="$queue_probe/queue.rs" cargo test --locked -p rpcstack-sched \
        --features sched_custom --lib application_queue_is_selected > "$queue_probe/result" 2>&1
    cat "$queue_probe/result"
    # An old build which ignores the variable must not pass by running zero tests.
    grep -q '1 passed; 0 failed' "$queue_probe/result"
}
run_probe
python3 - "$queue_probe/queue.rs" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1])
s = p.read_text().replace('self.0.pop()', 'if self.0.is_empty() { None } else { Some(self.0.remove(0)) }')
p.write_text(s.replace('assert_eq!(q.pop(), Some(2))', 'assert_eq!(q.pop(), Some(1))'))
PY
run_probe
if RPCSTACK_QUEUE_SOURCE="$queue_probe/missing.rs" cargo check --locked -p rpcstack-sched --features sched_custom > "$queue_probe/error" 2>&1; then
    echo 'Missing external queue was silently accepted' >&2
    exit 1
fi
grep -q 'RPCSTACK_QUEUE_SOURCE must name an existing Rust source file' "$queue_probe/error"
# Cargo host/build dependencies can compile without sched_custom in the same build.
RPCSTACK_QUEUE_SOURCE="$queue_probe/queue.rs" cargo test --locked -p rpcstack-sched --lib > "$queue_probe/result" 2>&1
cat "$queue_probe/result"
if grep -q 'external::' "$queue_probe/result"; then
    echo 'External queue selected without sched_custom' >&2
    exit 1
fi
if RPCSTACK_QUEUE_SOURCE=relative.rs cargo check --locked -p rpcstack-sched --features sched_custom > "$queue_probe/error" 2>&1; then
    echo 'Relative external queue path was silently accepted' >&2
    exit 1
fi
grep -q 'RPCSTACK_QUEUE_SOURCE must be an absolute source path' "$queue_probe/error"
env -u RPCSTACK_QUEUE_SOURCE cargo test --locked -p rpcstack-sched --features sched_custom
