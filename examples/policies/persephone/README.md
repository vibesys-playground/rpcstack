# Perséphone / DARC RPC dispatch

Paper: [When Idling is Ideal: Optimizing Tail-Latency for Heavy-Tailed Datacenter
Workloads with Perséphone, SOSP 2021](https://marioskogias.github.io/docs/persephone.pdf).
Algorithm 1 is request dispatch; Algorithm 2 and Equation 1 determine reservations.
Inspected author implementation:
[`dispatch_su.cc`](https://github.com/maxdml/psp/blob/510833b934e9df916d2f9f5b243d7e530a353840/src/c++/libos/su/dispatch_su.cc).
This is a Rust reimplementation, not a source port.

`Scheduler` groups similar request types, assigns workers from service-time ×
occurrence demand, keeps a spillway, and dispatches shorter types first. Short
requests can steal longer-type workers; long requests cannot use idle short-type
reservations. FIFO order is retained within a type. Queued/granted registrations
are owned by an RAII lease, including the grant/cancellation handoff.

Use `TypedCall` with `rpcstack_call::execute` to attach the type to generated RPCs.
At the receiving handler, call `request_type` and `scheduler.run(kind, handler)`.
Undeclared types use the spillway. Share one scheduler across the relevant methods.
The transport test demonstrates a short RPC completing while a long RPC remains
queued and its reserved workers are occupied.

Scope: workers are asynchronous execution slots, **not pinned CPU cores**. Service
profiles therefore measure elapsed handler duration. Initial profiles are supplied;
completed-request windows refresh reservations after observed queue slowdown.
The original c-FCFS bootstrap, DPDK fast path, and physical CPU isolation are not
reproduced. `similarity` is a service-time ratio (1.5 means within 50%); the author
code expresses that delta as 0.5. This adapter always dedicates the final slot to
the spillway. Queue bounds are a deployment safeguard.

No framework change was needed beyond existing asynchronous call control and
explicit handler scheduling. `cargo test -p rpcstack-persephone` checks reservations,
stealing restrictions, cancellation cleanup, overflow groups and live gRPC dispatch.
