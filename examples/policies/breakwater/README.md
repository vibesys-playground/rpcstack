# Breakwater

[OSDI 2020 paper](https://inhocho89.github.io/papers/osdi20overload.pdf), §3 equations
1–4 and AQM; [author implementation](https://github.com/shenango/caladan/tree/main/breakwater),
inspected at `bdb4ddebc2198cd95991079ed20e44a5f210d6d3`.

Implements server credit-window control (additive growth proportional to client count,
proportional decrease bounded to half), demand-prioritized/speculative distribution,
revocation of unused grants, request demand/response credit piggybacking, client credit
wait/expiration, and server delay AQM. Absolute window grants carry generations so
out-of-order responses cannot undo newer revocations. Consumed windows are released
on completion or cancellation through request/attempt ownership.

A real, policy-free server-streaming gRPC control service supplies initial grants and
unsolicited updates. This is essential: zero-credit clients cannot bootstrap by sending
an application request. Inject the same `BreakwaterServer` controller into the application
service and `CreditService`, call `Controller::start` inside a Tokio runtime, connect a
`CreditClient`, and pass it to `rpcstack_call::execute` around the generated stub.
Application methods share that server's credit pool, as in the paper. Allocate unique
client IDs for active subscriptions; duplicates are rejected.

## Fidelity and remaining substrate gap

This is a Tonic/Tokio adaptation, not the Shenango implementation or a microsecond
performance reproduction. The built-in queue observation is a maximum of sampled first-poll
runnable delays; the paper sums oldest queue age across packet and thread stages. Call
`observe_queue` with an external complete queue observation when available. Tokio timers
cannot reliably reproduce a 10µs control tick; default RTT/tick is explicitly 1ms here.
`alpha`, `beta`, target, AQM threshold, and limits are configurable. Distribution batches
known demand before random-order speculation and may send more control updates than the
paper's piggyback-first dispatcher. A per-server maximum is an engineering safety bound.

The test validates real streaming bootstrap, a single-credit concurrent call, expiration
before transport, credit reuse, and normal gRPC execution. Unit tests cover controller
increase/decrease, revocation, used-credit conservation, and stale feedback. No claim
that the framework observes the NIC/kernel receive queue or prevents receive livelock.
Run `cargo test -p rpcstack-breakwater`.
