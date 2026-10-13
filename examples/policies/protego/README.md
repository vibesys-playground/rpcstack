# Protego: contention-aware RPC admission

Source: [Protego: Overload Control for Applications with Unpredictable Lock
Contention, NSDI 2023](https://www.usenix.org/system/files/nsdi23-cho-inho.pdf),
§3–4, Algorithm 1. [Author project](https://inhocho89.github.io/protego/).
This is a new Rust policy implementation from the paper, not copied Shenango code.

`Controller` implements signed marginal-throughput efficiency, drop cap,
additive increase/multiplicative decrease, fractional credit accounting and bounds.
`Protego::start` applies it every four measured intervals with one settling interval.
The existing Breakwater control stream bootstraps clients and distributes/revokes
credits; `CreditClient` plugs into `rpcstack_call::execute` and generated stubs.
Install `BreakwaterModule` and inject `BreakwaterServer(protego.credits.clone())`.
Do not start the Breakwater delay controller alongside Protego.

Wrap handler execution in `Protego::run`; pass its one `QueueBudget` to every
`QueueGate::acquire` at contended resources. ASQM checks the oldest waiter before
enqueueing and deducts actual queue time on acquisition. A timeout also prevents
an unexpectedly stalled holder from exceeding the remaining budget. RAII cleans
up waiters and permits on cancellation. Application mutations still need their
own rollback or transaction boundaries when an acquisition fails.

Framework addition: `rpcstack-call::queue::{QueueGate,QueueBudget}` exposes
measured, budgeted asynchronous resource acquisition. This was missing from the
single-stage measurement API. The fixture's `serve_handler` tests real handler
resources with generated transport and module hooks.

Adaptations: RTT is measured around handler execution (not from the paper's unique
credit issuance timestamp); the control stream is the repository's window-based
Breakwater adaptation, not wire-compatible with Shenango. Tokio timers have
millisecond granularity. Only explicitly instrumented queues count against the
budget; arbitrary OS locks and scheduler queues are not intercepted. These are
functional mechanisms, not a reproduction of the paper's microsecond results.

Validation: `cargo test -p rpcstack-protego -p rpcstack-call` checks slope signs,
drop-cap precedence, fractional decrease, minimum credits, real gRPC lock rejection,
credit reuse after failure, and cancellation-safe resource accounting.
