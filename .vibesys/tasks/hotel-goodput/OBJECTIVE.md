# Hotel Reservation custom RPC optimization

Raise `mean_goodput_rps`: requests that finish successfully within their
end-to-end SLO, averaged over a fixed open-loop Poisson load sweep.

Implement a custom RPC policy using rpcstack. The baseline `Stack = ()` has
no policy. You may design any mechanism supported by the framework: admission,
concurrency control, scheduling, cancellation, propagation, retries, or combinations.
Do not assume a particular policy family is best; use measured evidence.

Editable implementation slots:
- `examples/hotel/apps/rpc-policy/src/` (add modules and compose `Stack`).
- `libs/rpcstack-sched/src/custom.rs` and `replay_tests.rs` (pluggable task queue).

Everything else is frozen, including application code, service resource allocation,
load generation, scoring, and the evaluator task. Do not change dependencies,
compiler flags, tests outside the slots, or use external processes/files to affect
measurement. Do not fabricate responses, bypass business logic, special-case the
correctness probe, or alter application/database state except through ordinary RPCs.

Every generated server/client stub selects `rpc_policy::Hooks`. The root request
carries `x-workload-start-us`, `x-workload-slo-us`, `x-workload-id`, and
`x-workload-api`; these are observations, not an installed deadline policy. Use
rpcstack's typed context and lifecycle hooks for new policy state. Read the framework
examples and API documentation. Scheduling takes effect between future polls.

The workload is a Rust port of HotelReservation's benchmark model: Search and
Reservation, real gRPC fanout and MongoDB/Redis operations. Its user service uses a
probabilistic 80% admission model, not production authentication. Preserve it.
Services run as separate processes inside the VibeSys sandbox, on eight CPUs;
the generator uses four other CPUs. This is a single-host proof of concept.

Accuracy runs scope checks, scheduler/policy tests, semantic checks (including
persisted reservations and capacity), and 300 requests/s with <=1% RPC errors and
>=97% within SLO overall and per API. Benchmark uses 600/1200/1600/2000/2400 requests/s,
10 seconds warmup plus 20 seconds measured per point. Search SLO is 200 ms and
Reservation SLO is 100 ms. Errors, rejection and timeout never count as goodput.
The first benchmark point must retain >=97% in-SLO success overall and per API.

Run the task-owned shim in `benchmark/rpcstack_hotel.py` with `accuracy` or
`benchmark`, `--project-root "$PWD"`, and optionally `--vs-output /tmp/result.jsonl`.
The shim invokes the existing experiment runner directly; no ServiceBench is used.
