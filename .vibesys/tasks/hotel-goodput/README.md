# VibeSys + rpcstack HotelReservation

This task asks VibeSys agents to implement an RPC policy in a policy-neutral
rpcstack application. Start a run from a clean copy of this repository:

```sh
vibesys --project /path/to/rpcstack --task hotel-goodput \
  --config /path/to/agent.toml --backend cpu --profiler none \
  --headless --max-rounds 4
```

The task-owned Dockerfile provides Rust, Go, Python/uv, MongoDB, Redis and taskset.
Current VibeSys uses an ordinary sandbox container: no Sysbox, nested Docker or
host socket is needed. Allow at least twelve logical CPUs and enough memory/disk
for the Rust build. A shared machine can affect latency; validate winners with
interleaved A/B runs before publishing percentage improvements.

`benchmark/rpcstack_hotel.py` is the Python evaluator shim. It invokes the ported
hotel experiment runner's process backend and uses the official VibeSys Go
Evaluator SDK via the small `benchmark/reporter` helper. The SDK is pinned to the
same VibeSys source snapshot used to author the task; Go checksums are committed.
There is no ServiceBench integration.

The baseline is an empty `Stack = ()`, not Masa. The agents may add RPC modules
and change the custom queue; they may not change workload, scoring, application
logic, resource allocation or compiler configuration. SHA-1 Git blob hashes in
`protected_files.txt` pin every protected baseline file independently of Git
history. Regenerate the manifest only when authoring a new benchmark baseline,
with the relevant files staged; never regenerate it during an optimization run.

Accuracy includes policy/scheduler tests, semantic RPC/persistence/capacity checks,
and light-load success/SLO floors. Benchmark has the same semantic probe and a
fixed five-rate sweep. Every recorded request is offered work, including rejection,
timeout and late success. Goodput counts only `/None` with latency <= SLO.
`/ClientMiss` is the load generator's marker for an OK response that missed SLO;
it is excluded from goodput but is not an RPC error. A missing API CSV, empty API,
or fewer than 90% of the requested arrivals fails evaluation.

Raw CSVs, effective configs, service logs and semantic-check output are under
`examples/hotel/exp/hotel/out/vibesys_<accuracy|benchmark>/0/rpcstack/`.
`OBJECTIVE.md` specifies the fixed workload and constraints. The application is a
benchmark model with probabilistic user admission, not production authentication;
see `examples/hotel/PROVENANCE.md` for limits.
