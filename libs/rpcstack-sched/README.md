# Scheduling interface

This dependency-free crate supplies `RunQueue<T>`, `TaskView`, task metadata,
generic FIFO/priority queues, and the editable `custom::Queue<T>` slot.
Paper-specific queue code belongs in the corresponding example folder.

## Application-owned queues

Enable `sched_custom` (or `tokio/sched_custom` in an application) and set
`RPCSTACK_QUEUE_SOURCE` to an **absolute path** to a Rust source file exporting
`pub type Queue<T> = ...` or `pub struct Queue<T>`, implementing `RunQueue<T>`.
The build script includes that module as `rpcstack_sched::external` and selects
its `Queue<T>`. The source can use `crate::{RunQueue, TaskView, Meta, ...}`;
relative submodules resolve next to the supplied source file. It has access to
this crate's dependencies only (currently the standard library).

This is explicit build-time source injection, not a runtime plugin loader or a
Cargo dependency on an example. The source is compiled as trusted Rust code.
Cargo tracks the environment variable and source changes. Changing the source or
unsetting the variable rebuilds the selection in the same target directory.
With `sched_custom`, a missing/relative/non-file path fails the build.
Without that feature, the variable is ignored so Cargo host/build dependencies
can compile with their own default features in the same invocation. One queue is selected for the
entire Cargo build; run different policies in separate invocations.

With no variable, `sched_custom` still selects the editable `src/custom.rs`.
Without `sched_custom`, `sched_prio` selects the priority heap and the default is
FIFO. Existing generic queue replay tests remain; external policies keep their
own ordering tests next to their source.

For a concrete example, see [TailClipper](../../examples/tailclipper/README.md).
Its wrapper supplies the absolute path without modifying framework files:

```sh
./examples/tailclipper/cargo.sh test -p rpcstack-sched --features sched_custom
```

Scheduling remains cooperative between future polls; no queue can forcibly
preempt a future inside `poll`.
