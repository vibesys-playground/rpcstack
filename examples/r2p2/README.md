# R2P2 JBSQ(n) RPC dispatch

Paper: [R2P2: Making RPCs first-class datacenter citizens, USENIX ATC 2019](https://www.usenix.org/system/files/atc19-kogias-r2p2_0.pdf),
§2–4. Inspected author router:
[`r2p2-router.c`](https://github.com/epfl-dcsl/r2p2/blob/38e1dfc1974655177129077bab553b8753d2bc2f/dpdk-apps/r2p2-router.c).

The paper introduces an RPC transport and request-level dispatch. This crate
implements its **join-bounded-shortest-queue policy over gRPC**, not the full
R2P2 UDP transport. Each destination denotes a backend worker (or deliberately
configured worker group). One shared `Dispatcher` owns queue counters across all
clients. It chooses the shortest eligible queue, limits outstanding requests to
`n`, and queues excess work centrally in arrival order. Tie-breaking rotates.

`Routing(Arc<Dispatcher>)` implements rpcstack's `CallPolicy`, yielding a `RoutedTicket`
with the selected destination. `Dispatcher::call` owns forwarding and completion
accounting; use it inside a router's generated RPC handler, forwarding with the
backend's generated client. The transport test runs a frontend and two backends.

A cancelled queued request releases its registration. Once forwarding starts,
an upstream disconnect or deadline leaves forwarding alive until the backend
responds. An uncertain transport error quarantines that backend instead of
pretending execution stopped. After verifying a backend drained or restarted,
`reset_drained_backend` advances its epoch; stale completions cannot decrement
new-epoch counts. These are conservative gRPC adaptations. A permanently stuck
forward requires operational recovery; the router does not retry potentially
non-idempotent work automatically.

The central FIFO, queue bound and completion feedback are executable here.
R2P2's packet protocol, fragmentation/reassembly, direct server-to-client replies,
network failure recovery, and P4 fast path are outside this gRPC policy adaptation.
No throughput or latency equivalence is claimed. Existing rpcstack async call
control was sufficient; no transport-core policy dependency was added.

`cargo test -p rpcstack-r2p2` covers queue selection/bounds, queued cancellation,
timeout retention, uncertain-error quarantine, epoch recovery, and live multi-backend
RPC dispatch.
