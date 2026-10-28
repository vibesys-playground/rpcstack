# Examples

Every paper implementation has a named folder directly in this directory.
Start with its README for the source paper, supported behavior, limitations and
tests. See [the paper comparison](../docs/PAPERS.md) for fidelity details.

| Folder | Implementation |
| --- | --- |
| [aequitas](aequitas/) | Aequitas: QoS admission/downgrade controller |
| [atropos](atropos/) | Atropos: resource-aware admission |
| [breakwater](breakwater/) | Breakwater: credit-based overload control |
| [c3](c3/) | C3: adaptive replica selection |
| [dagor](dagor/) | DAGOR: collaborative admission control |
| [hedged](hedged/) | The Tail at Scale: hedged requests |
| [masa](masa/) | Masa: composed RPC optimization policies |
| [persephone](persephone/) | Persephone: service-time-aware dispatch |
| [protego](protego/) | Protego: marginal-throughput credits |
| [r2p2](r2p2/) | R2P2: centralized RPC dispatch |
| [rajomon](rajomon/) | Rajomon: distributed prices and tokens |
| [rifl](rifl/) | RIFL: durable RPC deduplication |
| [tailclipper](tailclipper/) | TailClipper: arrival propagation and cooperative oldest-first sharing |

`echo/` is the minimal framework demo, `testkit/` supplies shared test plumbing,
and `policy-composition/` tests combinations of otherwise independent policies.
TailClipper's queue lives in its example, alongside its RPC module and replay tests.
