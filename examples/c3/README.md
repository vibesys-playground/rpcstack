# C3: adaptive replica selection and rate control

[NSDI 2015 paper](https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf)
and [author simulator](https://github.com/lalithsuresh/absim), inspected at
`348dac6afbc06532dda2d9da31808cfec5746d63`, especially `simulations/client.py`.

Implements the cubic replica score, pending-request compensation scaled by the
configured client count, EWMA service/queue/RTT feedback, per-replica token pacing,
CUBIC-style rate growth and multiplicative decrease with hysteresis. Unknown
replicas initially score zero. Eligible replicas are ranked, then scanned for a
rate token; selection waits if none can send. Optional `shadow_reads` reserves
read-only probes of other replicas to refresh otherwise stale observations. Like
the author simulator, probes bypass pacing but debit tokens. The caller owns and
awaits their attempts; no background work is detached. Use a probability such as
0.1 and never probe state-changing operations.

Server handlers run service work inside `rpcstack_call::Stage` and attach its
`StageSample` to the Response. `C3Module` emits service duration and queue length
on the wire. On the caller, `Selector::select` chooses an endpoint before constructing
its transport; `SelectedCall` transfers the reservation into `rpcstack_call::execute`.
Completion consumes feedback; dropping an unsent reservation refunds its token;
cancelling an in-flight attempt releases pending count without inventing feedback.
Wrap **selection and execution together** in the caller's overall deadline.
The transport test is a complete generated-client example of this sequence.

Adaptations: service time is the explicitly bounded application stage's elapsed
time, not Cassandra instrumentation or inferred CPU time. Queue length counts
waiters at that stage. Receive-rate EWMA uses elapsed interval-normalized counts;
it does not reproduce the simulator's integer-window boundary artifacts. Client
count and candidate eligibility are configured, not magically discovered. Failed
responses supply no successful service sample. The example does not implement
Cassandra quorum reads or claim the paper's throughput/latency improvements.

`cargo test -p rpcstack-c3` tests score arithmetic, pending compensation, pacing,
rate decrease, shadow accounting, and real two-replica feedback/selection with
cancellation cleanup. The reusable test fixture now supports measured stages.
