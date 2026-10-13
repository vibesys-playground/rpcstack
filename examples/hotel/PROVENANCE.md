# Provenance and benchmark limits

The hotel application, workload generator and experiment-runner lifecycle were
ported from `vic-lsh/masa` via the existing `vibesys-playground/masa` hotel example.
Source snapshot: `vibesys-playground/masa@91ae1fb87f7de111af3d096de3a486bd00926ac4`.
No Masa policy code, policy dependency or policy feature is included. Application
RPC stubs explicitly select rpcstack's empty, editable policy stack. Replica
balancing uses Tonic's stock channel. Timing metadata is workload observation only.

The workload generator cites the original DeathStarBench mixed workload source
in `apps/hotel/src/client/gen.rs`. This is a benchmark model, not a production
hotel application. In particular, its user service queries Mongo then returns a
seeded random admission decision (80% by default); it does not validate passwords.
The port preserves that behavior to avoid mixing business-logic optimization with
RPC optimization. A successful empty reservation response is therefore possible.
The semantic probe separately checks nonempty search results, real persisted
reservations, and capacity enforcement; passing is not a full application proof.

The process backend is added for ordinary VibeSys containers. It supports one
replica per service and loopback RPC with CPU affinity, not independent container
quotas or a distributed network. Report baseline/candidate changes within the same
backend and resource split. The host may be shared, so final results should use
interleaved repeated A/B measurements and disclose variability.
