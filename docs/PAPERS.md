# RPC policy implementations

Policies are optional example crates. They are not dependencies of the framework.
They exercise mechanisms; passing their tests does not reproduce a paper's performance.

## Rajomon and TailClipper

Sources: [Rajomon, NSDI 2025](https://www.usenix.org/system/files/nsdi25-xing.pdf)
and [TailClipper, SoCC 2024](https://doi.org/10.1145/3698038.3698554).

`examples/policies/rajomon` contains `PaperRajomonModule`: per-interface local plus
maximum-downstream pricing, unchanged token holdings across hops, root token spending,
Poisson refill, and probabilistic price feedback on success and errors. The original
historical `RajomonModule` is retained as an explicitly different alternative; use the
paper module for this study. Their wire section names collide, so do not compose both.

`examples/policies/tailclipper` propagates immutable global arrival times and sets
opaque task priorities. The queue maintains the C oldest runnable tasks, refilling
vacancies, and cycles among them. `yield_slice` immediately requeues a runnable future.
**This is cooperative TailClipper, not the paper's ghOSt CPU-preemptive scheduler.**
No safe hook can forcibly interrupt arbitrary Rust code inside `Future::poll`.
Dynamic C and multicore ORF are not implemented. Arrival timestamps assume comparable
host clocks. The crate READMEs describe parameters and limitations.

Run `cargo test -p rajomon -p tailclipper`, then
`cargo test -p paper-rpc --features orf` or `cargo run -p paper-rpc --features orf`.
The two-hop transport test checks unchanged tokens/timestamps, accumulated response
prices, overload feedback, and subsequent local root rejection. A runtime test checks
that the oldest runnable window shares cooperative slices before younger tasks.

These examples were ported from the earlier implementations at
https://github.com/vic-lsh/masa/pull/104, with framework dependencies redirected here.
The queue correction is a mechanism fix: vacancies must admit waiting runnable tasks
without waiting for an entire old round-robin batch to drain.

## Earlier policy studies

| Paper | Implementation and inspected code | Implemented scope | Fidelity limit |
| --- | --- | --- | --- |
| [DAGOR, SoCC 2018](https://www.cs.columbia.edu/~junfeng/papers/dagor-socc18.pdf) | [Example](../examples/policies/dagor); paper Algorithm 1 (unofficial gRPC ports were not treated as authoritative) | Business/user admission thresholds, overload windows, collaborative rejection feedback and propagation | Tokio first-poll queue signal; configured replica/cache scope; demand-driven windows |
| [Breakwater, OSDI 2020](https://inhocho89.github.io/papers/osdi20overload.pdf) | [Example](../examples/policies/breakwater); [author code](https://github.com/shenango/caladan/tree/main/breakwater) | Credit growth/decrease, demand/speculative grants, zero-credit control stream, delay AQM, revocation, deadlines and disconnect-safe accounting | Sampled runnable delay rather than Shenango packet/thread oldest-age sum; millisecond timer and adapted distribution |
| [C3, NSDI 2015](https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf) | [Example](../examples/policies/c3); [author simulator](https://github.com/lalithsuresh/absim) | Replica score, pending compensation, service/queue feedback, pacing, cubic rate control and shadow reads | Explicit service-stage measurement, configured client count; no Cassandra quorum system |
| [The Tail at Scale, CACM 2013](https://www.barroso.org/publications/TheTailAtScale.pdf) | [Example](../examples/policies/hedged); [author publication page](https://research.google/pubs/the-tail-at-scale/) (no linked author code found) | Delayed hedged requests, bounded backup concurrency, replay-safety contract, p95 estimator, owned losing-attempt cancellation | Implements hedging, not tied requests or guaranteed remote rollback |
| [Aequitas, SIGCOMM 2022](https://yiwenzhang92.github.io/assets/docs/aequitas-sigcomm22.pdf) | [Example](../examples/policies/aequitas); [author simulator](https://github.com/SymbioticLab/Aequitas) | Literal admission/downgrade controller, typed QoS/RNL adapter boundary, measured-completion replay | **Incomplete network deployment:** stock Tonic lacks packet WFQ, per-RPC QoS and final-ACK RNL; production adapter still required |

Example READMEs record source revisions, equations/algorithm sections, adaptations,
integration instructions and focused tests. These are policy implementations and
functional tests, not reproductions of published throughput or latency figures.
No new hotel-reservation, train-ticket or social-network experiment was run here.

[Masa](../examples/masa) is an additional optional composition reference, porting
the actual budget/guard/estimation/admission modules and semantic tests. It is not
counted as one of the five additional papers, and the framework does not depend on it.

## Review and test the full corpus

The original corpus was merged to `main` through PR #11. Individual
PRs preserve the implementation history:

- [#1 Rajomon + cooperative TailClipper](https://github.com/vibesys-playground/rpcstack/pull/1), based on main.
- [#2 generic async calls/resources](https://github.com/vibesys-playground/rpcstack/pull/2), based on main.
- [#3 DAGOR](https://github.com/vibesys-playground/rpcstack/pull/3) → [#4 Breakwater](https://github.com/vibesys-playground/rpcstack/pull/4) → [#5 hedging](https://github.com/vibesys-playground/rpcstack/pull/5) → [#6 C3](https://github.com/vibesys-playground/rpcstack/pull/6) → [#7 Aequitas](https://github.com/vibesys-playground/rpcstack/pull/7) stack on #2.
- [#8 Breakwater disconnect fix](https://github.com/vibesys-playground/rpcstack/pull/8) follows #7 and should be reviewed with #4.
- [#9 Masa example](https://github.com/vibesys-playground/rpcstack/pull/9) stacks on #1.
- [#10 external-consumer compatibility](https://github.com/vibesys-playground/rpcstack/pull/10) follows #8 and prevents a second incompatible Tokio.

The historical chain was for cumulative validation/review; DAGOR, Breakwater, C3, hedging and
Aequitas do not depend on each other's algorithms. Framework extensions, test
fixtures and Cargo manifests are shared. These PRs are all merged.

```sh
./scripts/test.sh
./scripts/check.sh
cargo test -p rpcstack-policy-composition
cargo run -p paper-rpc --features orf
```

The composition test exercises DAGOR, Breakwater and C3 together through generated
gRPC stubs, independent request/response wire sections, nested async tickets,
local rejection and recovery. The external-workspace test deliberately starts
without this repository's lockfile and checks that only the vendored Tokio resolves.
See [the VibeSys implementation guide](VIBESYS.md) for extension-point selection
and the substrate capabilities that these tests do not establish.

## Additional RPC mechanisms (second study)

- [Protego, NSDI 2023](https://www.usenix.org/system/files/nsdi23-cho-inho.pdf):
  [implementation](../examples/policies/protego), marginal-throughput credit
  control and budgeted synchronization queues with live RPC rejection/recovery.
  Uses measured handler intervals and the existing gRPC credit channel; see its
  README for the distinctions from Shenango and the paper's credit RTT sensor.
- [Perséphone/DARC, SOSP 2021](https://marioskogias.github.io/docs/persephone.pdf):
  [implementation](../examples/policies/persephone), typed reservations, short-to-long
  cycle stealing, spillway service, and runtime profiling. Enforces asynchronous
  handler-slot scheduling, not dedicated CPU-core isolation or kernel bypass.
- [R2P2, USENIX ATC 2019](https://www.usenix.org/system/files/atc19-kogias-r2p2_0.pdf):
  [implementation](../examples/policies/r2p2), centralized JBSQ(n) RPC dispatch
  with bounded backend queues and completion accounting. Uses gRPC forwarding;
  does not implement R2P2's UDP packet format, direct replies, or P4 offload.
- [RIFL, SOSP 2015](https://web.stanford.edu/~ouster/cgi-bin/papers/rifl.pdf):
  [implementation](../examples/policies/rifl), durable RPC identities/results,
  transactional effects, retry rendezvous, acknowledgments and lease GC. Includes
  process-exit tests before/after commit and lost-response gRPC retry. Uses a
  single SQLite shard; does not reproduce RAMCloud replication or multi-shard 2PC.
- [Atropos, SOSP 2025](https://yigonghu.github.io/paper/sosp25-atropos.pdf):
  [implementation](../examples/policies/atropos), resource attribution, estimated
  future gain, Pareto/weighted selection and targeted cancellation of RPC handlers.
  This is an application-overload paper adapted to RPC execution, not an RPC wire
  protocol. Explicit sensors/progress and safe cooperative cancellation are required;
  the original application patches and automatic instrumentation are not reproduced.
