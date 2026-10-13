# RPC policy implementations

Policies are optional example crates. They are not dependencies of the framework.
They exercise mechanisms; passing their tests does not reproduce a paper's performance.

## Rajomon and TailClipper

Sources: [Rajomon, NSDI 2025](https://www.usenix.org/system/files/nsdi25-xing.pdf)
and [TailClipper, APSys 2024](https://doi.org/10.1145/3698038.3698554).

`examples/policies/rajomon` contains `PaperRajomonModule`: per-interface local plus
maximum-downstream pricing, unchanged token holdings across hops, root token spending,
Poisson refill, and probabilistic price feedback on success and errors. The original
historical `RajomonModule` is retained as an explicitly different alternative; use the
paper module for this study. Their wire section names collide, so do not compose both.

`examples/policies/tailclipper` propagates immutable global arrival times and sets
opaque task priorities. The queue maintains the C oldest runnable tasks, refilling
vacancies, and cycles among them. `yield_slice` immediately requeues a runnable future.
**This is cooperative TailClipper, not the paper's ghOSt CPU-preemptive scheduler.**
No safe hook can forcibly interrupt arbitrary Rust code inside `Future::poll`.
Dynamic C and multicore ORF are not implemented. Arrival timestamps assume comparable
host clocks. The crate READMEs describe parameters and limitations.

Run `cargo test -p rajomon -p tailclipper`, then
`cargo test -p paper-rpc --features orf` or `cargo run -p paper-rpc --features orf`.
The two-hop transport test checks unchanged tokens/timestamps, accumulated response
prices, overload feedback, and subsequent local root rejection. A runtime test checks
that the oldest runnable window shares cooperative slices before younger tasks.

These examples were ported from the earlier implementations at
https://github.com/vic-lsh/masa/pull/104, with framework dependencies redirected here.
The queue correction is a mechanism fix: vacancies must admit waiting runnable tasks
without waiting for an entire old round-robin batch to drain.
