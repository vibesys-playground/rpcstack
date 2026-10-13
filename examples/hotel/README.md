# HotelReservation RPC policy experiment

A scoped example using rpcstack, its Tonic integration, and the custom Tokio
queue. `apps/rpc-policy/src/lib.rs` is the agent's policy slot; it starts with an
empty module stack. Hotel business logic and the open-loop Rust load generator
are ported from the pre-existing hotel experiment in vic-lsh/masa, without its
policy crate or default policy. See `PROVENANCE.md` for sources and limitations.

The nested Cargo workspace keeps application dependencies out of rpcstack itself.
The Python experiment runner owns deployment, workload execution and CSV artifacts.
VibeSys integration lives at the repository root under
`.vibesys/tasks/hotel-goodput/`; its Python shim directly invokes this runner and
reports measurements using the official VibeSys evaluator SDK.

## Run

Install the toolchain in the root `rust-toolchain.toml`, uv, Go, and protoc. The
process backend additionally needs MongoDB 7.0, Redis 7.2 and taskset on PATH, plus
at least twelve logical CPUs. The task Dockerfile provisions these tools for VibeSys.

Copy a task-owned `benchmark/experiments/<kind>` directory to
`exp/hotel/in/<name>`, and write `rpcstack` to its `policies` file. Then:

```sh
uv run python -m exp_runner run hotel <name> --deployment process --rm-data
```

The runner writes per-request CSVs, service logs, effective configuration and CPU
allocation to `exp/hotel/out/<name>/0/rpcstack`. The process backend runs seven
hotel services, four Mongo instances and three Redis instances. It uses fresh
stores, waits for readiness, verifies response semantics and persisted room
capacity, and tears down its own child processes.

Process measurements use eight shared CPUs for services/stores and four separate
CPUs for the generator. They are not interchangeable with Compose results. The
Compose backend remains available via `--deployment docker`; VibeSys uses process
mode to avoid requiring a nested Docker daemon or host Docker socket.
