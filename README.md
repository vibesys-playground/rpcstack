# rpcstack

A standalone framework for composing custom RPC policies in Rust. Modules control
request admission, root and child calls, per-poll behavior, response feedback, and
wire metadata. Applications choose their module stack explicitly.

## Components

| Component | Purpose |
| --- | --- |
| `libs/rpcstack` | Module lifecycle, composition, typed state and decision points |
| `libs/rpcstack-wire` | Independent header-section codec and opaque scheduling header |
| `libs/rpcstack-tonic` | Runs a module stack through Tonic's request hooks |
| `libs/rpcstack-call` | Async admission/pacing tickets, owned attempts, deadlines and measured service stages |
| `libs/rpcstack-sched` | Task metadata, queue interface, built-in queues, custom queue slot |
| `libs/tonic`, `libs/hyper`, `libs/tokio` | Vendored runtime/transport support for these mechanisms |
| `libs/multiqueue-prio-queue` | Optional concurrent queue backend |
| `examples/echo` | Small two-hop demonstration and transport regression test |

Each paper implementation has its own folder directly under [`examples/`](examples/README.md);
the framework never depends on a paper example. See [paper implementations](docs/PAPERS.md) for scope and fidelity.
The optional [Masa composition example](examples/masa/README.md) ports the original
policy stack and semantic tests without adding a framework dependency.
The [VibeSys implementation guide](docs/VIBESYS.md) maps policy requirements to
extension points, test evidence and remaining substrate gaps. The original policy corpus is merged on `main`; additional paper examples
are documented in [docs/PAPERS.md](docs/PAPERS.md).
The framework retains generic FIFO and priority queues. Paper-specific queues live
with their examples; TailClipper supplies its queue through the generic external
queue build interface described in [the scheduler guide](libs/rpcstack-sched/README.md).

The vendored runtime crates are required: stock Tonic lacks these lifecycle hooks,
and stock Tokio lacks the task metadata and pluggable queue integration. Their
original license notices are retained.

## Run

Install Rust through rustup and Protocol Buffers' `protoc` (for example,
`apt-get install protobuf-compiler`). The toolchain is pinned in
`rust-toolchain.toml`.

```sh
cargo run -p rpcstack-echo
cargo run -p rpcstack-echo --features priority
./scripts/test.sh
./scripts/check.sh
```

The example sends a marker through a client, frontend, and leaf; exercises server
and local root rejection; checks recovery; and checks generated default no-op
clients. The `priority` feature enables the opaque scheduling header and priority
queue. It is optional; ordinary calls need no scheduling header.

## Write a module

Implement `rpcstack::Module`, compose modules with `rpcstack::policy_stack!`, and
use `rpcstack_tonic::PolicyHooks<YourStack>`. Configure generated clients/servers:

```rust,ignore
tonic_build::configure()
    .default_hooks_path("crate::YourHooks")
    .compile(&["proto/service.proto"], &["proto"])?;
```

Without this override, generated stubs use
`tonic::rpcstack::noop::NoopHooks`. Rebuild stubs when migrating to this repository:
the generic hook namespace is `tonic::rpcstack`.

`before_root_rpc` / `after_root_rpc` handle client-initiated calls.
`before_child_rpc` / `seal_child_rpc` / `after_child_rpc` handle downstream calls.
Modules also receive server construction, request construction, per-poll, and
finalization hooks. See the component READMEs and `examples/echo/src/main.rs`.

For scheduling, enable `tonic/rpcstack` and choose the appropriate Tokio queue
feature (`sched_prio`, `sched_custom`, or another supported queue). The optional
`x-rpcstack-scheduling` header is a decimal `u64`; missing or invalid values use
`rpcstack_sched::default_policy` spawn metadata. No policy-specific header fallback
exists. Queue meanings are defined by the queue, not the transport. Runtime hooks
are cooperative; they cannot preempt arbitrary Rust code inside a future's poll.

For a separate application workspace, use the vendored Tonic and code generator as
path dependencies, and add `[patch.crates-io]` entries for the vendored Tokio and
Hyper, as this workspace does. Patches in dependency workspaces do not propagate.
The vendored Hyper constrains `tokio-util` to the runtime-compatible 0.7.11;
when migrating an existing application, refresh its lockfile after applying
the patches (the fresh-workspace fixture uses `cargo generate-lockfile` semantics)
and verify `cargo tree -d` contains no second Tokio. CI also builds a fresh external workspace to
catch dependency resolution that the repository lockfile would otherwise hide.

## Validation

`test.sh` runs the framework, queue, hook adapter, code generator, HTTP/2 scheduling,
runtime-priority tests, and the example with and without priority scheduling.
`check.sh` validates dependency boundaries, formatting, and compilation with warnings
denied across the scheduling configurations. Upstream runtime test suites are
retained but are not claimed to be exhaustively validated by these scripts.

Source attribution and extraction details are in [SOURCE.md](SOURCE.md).

Server-local controllers can be injected with
`ServerContext::try_with_resources` and generated `with_server_context` constructors.
For asynchronous admission, pacing, or replica attempts, see
[`rpcstack-call`](libs/rpcstack-call/README.md). These mechanisms do not prescribe
any admission or routing policy.
