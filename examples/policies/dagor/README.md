# DAGOR

[Paper: SoCC 2018, §4 / Algorithm 1](https://www.cs.columbia.edu/~junfeng/papers/dagor-socc18.pdf).
[Comparison code (unofficial)](https://github.com/pennsail/dagor-grpc).

Implements lexicographic business/account priorities, hourly account cohorts, unchanged
priority forwarding through the call tree, queue-delay overload detection, an admitted
histogram with Algorithm 1's incoming-request denominator, and response-threshold
feedback for early downstream rejection (including error responses).

The 1s/2000-request observation window is evaluated on request arrival; idle periods
need no timer because there is no load to shed. Queue delay is the first runnable poll's
Tokio enqueue delay, not handler wall time or downstream latency. HTTP/2 parsing and
pre-decode transport work are outside this sample. Tests inject queue samples to drive
controller transitions deterministically and separately exercise real two-hop gRPC.

Attach `DagorCall` to root Request extensions. Reuse `PeerThresholds` across calls.
Inject `DagorServer` with `ServerContext::try_with_resources` to customize configuration
or observe the controller. State belongs to a constructed service, never a process-global
map. Peer feedback is keyed by service/method; deployments with heterogeneous replicas
should maintain separate peer caches per endpoint rather than aggregate them.

The paper uses admitted counters but total incoming N in Algorithm 1; this implementation
follows that literal pseudocode, including its potential to reopen admission after a
mostly rejected window. The account hash is an engineering choice, not paper-specified.
No WeChat-scale performance claims. Run `cargo test -p rpcstack-dagor`.
