# The Tail at Scale: delayed hedged RPCs

[Dean and Barroso, CACM 2013](https://www.barroso.org/publications/TheTailAtScale.pdf)
describe sending an equivalent request to a second replica only after the first
exceeds a latency threshold, often the 95th percentile, and cancelling the loser.
This example implements that mechanism with lazy attempt factories, an explicit
read-only/idempotent contract, a shared cap on concurrent backups, and cancellation
by dropping both owned futures. The first response wins, including an error.
The paper's separate **tied request** protocol is not implemented.

`Hedger::call` surrounds two `rpcstack_call::execute` attempts using generated
rpcstack/Tonic clients. Each attempt runs its ordinary policy hooks and releases
its own ticket. Give both the same absolute deadline. If the backup budget is
exhausted, only the original proceeds. Clones share the budget. A bounded p95
estimator is available; use independent primary-only observations so cancelled
slow requests do not bias the threshold downward. Instantiate a hedger with the
current threshold; the example does not invent a workload classification scheme.

`cargo test -p rpcstack-hedged` exercises live gRPC replicas and deterministic
paused-time tests for no unnecessary backup, winning backup, local cancellation,
caller timeout, budget exhaustion, and percentile estimation. Cancellation is
local future ownership and HTTP/2 stream cancellation; it cannot roll back remote
side effects or force a non-cooperative handler to stop. The safety enum is a
caller assertion, not a proof of idempotence. No published performance result is
claimed and no author implementation was found linked from the paper.
