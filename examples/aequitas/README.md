# Aequitas QoS admission controller and transport contract

[SIGCOMM 2022 paper](https://yiwenzhang92.github.io/assets/docs/aequitas-sigcomm22.pdf),
Algorithm 1; [author simulator](https://github.com/SymbioticLab/Aequitas), inspected
at `a09a4420a3cd897e518172416b0cacfb575ced27`.

Implements per-source/destination/QoS admission probabilities, direct downgrade to
the lowest QoS, size-normalized RPC network latency, time-gated additive increase,
and the paper's size-weighted per-completion decrement `p -= beta * size_mtu`.
That decrement produces aggregate multiplicative behavior; replacing it with
`p *= beta` on each completion would implement a different algorithm. Lowest QoS
is uncontrolled. A positive floor prevents permanent starvation. A Controller
represents one source host, with destination host identifiers as keys.

Use `Controller::execute` with an explicit transport adapter. Its callback receives
`QosAssignment` and must apply packet QoS and return `RpcNetworkLatency`. The
result retains the assignment in response extensions so the application can see
that the RPC was downgraded. Deadline/cancellation are owned by rpcstack-call.
The lower-level CallPolicy is available for composition, but ordinary generated
Tonic stubs alone do not satisfy this transport contract.

## What remains unsupported by the stock Tonic transport

This is an implemented controller and tested adapter boundary, **not a complete
Aequitas network deployment**. Aequitas requires weighted-fair packet queues and
RNL measured from first packet entering transport through final packet ACK,
including congestion-control delay. Stock HTTP/2 channels do not provide these
measurements, per-RPC packet QoS, or the paper's Swift/WFQ network configuration.
A metadata header, a Tokio task priority, or application RTT would not substitute.
A production adapter must provide the packet transport/instrumentation and network
configuration. The PR stays draft to make this unresolved substrate requirement
visible; no performance result is claimed.

`cargo test -p rpcstack-aequitas` checks literal controller traces, source/destination
and class isolation, downgrade reporting, typed adapter handoff, and deadline
cancellation without fabricated feedback. `cargo run -p rpcstack-aequitas --example
replay` reads measured completion records from stdin; it is an offline replay,
not a packet simulator. Columns (no header):

```
# completion_us,destination,requested_qos,run_qos,size_mtu,rnl_us
0,server-a,0,0,10,300
2000,server-a,0,0,1,10
2100,server-a,0,2,10,1000
```

For a packet-level reproduction use the linked author simulator with its documented
Swift, WFQ weights and topology settings; export its measured completions to this
format to inspect controller decisions. Replay's illustrative targets are 15/25µs
per MTU at p99, alpha=.01, beta=.001, floor=.001; all are configurable in the library.
