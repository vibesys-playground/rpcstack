# Atropos targeted RPC cancellation

Paper: [Mitigating Application Resource Overload with Targeted Task Cancellation,
SOSP 2025](https://yigonghu.github.io/paper/sosp25-atropos.pdf), §§3.4–3.6 and
Algorithm 1. Inspected [author code](https://github.com/OrderLab/Atropos/tree/f0ebf97f09cf860fa86a1f8487081ef421c72d9a).

Atropos studies application-resource overload, not a new RPC wire protocol. This
example applies its resource attribution and cancellation policy to live gRPC
handlers. The monitor detects an SLO miss with flat/falling windowed throughput,
filters explicitly cancellable tasks to a Pareto frontier, and chooses the largest
contention-weighted estimated future resource gain.

Integration:

1. Share a `Monitor` with handlers; register memory, synchronization or queue
   resources. Report windowed contention-induced delay and baseline execution
   duration using `resource_window` (paper normalization `D_r / T_exec`). Supply
   every resource's current window, including zero contention, before `observe`.
2. Register a `Task` for each handler. Only supply a `Cancellation` token when the
   handler can safely stop. Track progress as completed/total work, and report
   resource acquire/release events. Memory amounts are net held allocations;
   synchronization/queue amounts represent held locks/worker slots. Their holding
   interval begins at acquire, not at entry into the waiting queue. Each task has
   one holding interval per synchronization/queue resource; release it before
   reacquiring, and use separate resource names for independently acquired locks.
3. Gains follow the paper: held memory, or elapsed holding time, multiplied by
   remaining/completed work. Unknown/zero progress is ineligible rather than
   assigned a fabricated estimate. Fully completed tasks are also ineligible.
4. Feed measured throughput, latency and SLO to `observe`. It signals at most one
   eligible task per window. The application chooses its sampling cadence and
   how to aggregate latency; the live test measures a blocked lock wait.
5. Use the generic `rpcstack_call::cancellation::Cancellation::run` around a future
   whose owned resources can safely be dropped, or await `cancelled` and invoke an
   application-specific rollback path. Task drop removes monitoring state.

The framework extension is a sticky cooperative cancellation signal with owned
future cleanup. It cannot force CPU preemption, stop detached tasks, undo external
side effects, or automatically instrument arbitrary locks/databases. These require
application support. Instrumented async handler time is wall time, not CPU time.
The example does not reproduce Atropos's application patches, automatic source
instrumentation or all original application detectors. Cancellation never becomes
safe merely because a token is available. No performance reproduction is claimed.

`cargo test -p rpcstack-atropos -p rpcstack-call` checks the paper's numerical
multi-resource selection example, eligibility/progress, cleanup and sticky wakeups.
The real generated gRPC test cancels a monopolizing handler, releases its owned
lock and allows an unrelated queued request to finish.
