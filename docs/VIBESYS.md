# Using rpcstack as a VibeSys policy tool

The paper examples show that rpcstack can express several classes of custom RPC
policy, once synchronous lifecycle hooks are supplemented with async attempt
control and explicit service resources. They do **not** establish that arbitrary
transport or scheduling policies can be implemented faithfully inside Tonic/Tokio.

## Example discovery and custom queues

Start at [the examples index](../examples/README.md). Each paper has a direct
`examples/<paper>/` folder with its implementation, tests and fidelity notes.
Shared composition fixtures live in `examples/policy-composition/`.

For a new run queue, keep its source with the application or paper example and
select it using `RPCSTACK_QUEUE_SOURCE` plus `tokio/sched_custom`; see the
[scheduler interface](../libs/rpcstack-sched/README.md). The editable framework
`custom.rs` slot remains available when no external source is specified.

## Choose the extension point from the required mechanism

| Required behavior | Extension point | Executed example |
| --- | --- | --- |
| Root-client admission and response feedback | `Module::before_root_rpc` / `after_root_rpc`, request extensions for shared caller state | Paper Rajomon, DAGOR |
| Per-service state shared with a controller/control service | `ServerContext::try_with_resources`, `ServerInit::provide`, generated `with_server_context` | Breakwater; resource-isolation test |
| Admission at request execution; poll observations | `new`, `before_poll`, `after_poll`, `finalize` | DAGOR, Breakwater, Masa |
| Propagating independent wire sections across hops | `WireIn` / `WireOut`, child pre/seal/post hooks | Rajomon + TailClipper; DAGOR two-hop test |
| Waiting for credits, pacing, deadline ownership | `rpcstack_call::CallPolicy` and owned `Ticket` | Breakwater, C3 |
| Choosing a replica before constructing a call | Select an endpoint, then wrap its generated stub with `execute` | C3 |
| Racing equivalent attempts | Own multiple `execute` futures; drop losing futures | Delayed hedging |
| Service queue and service-duration feedback | Explicit `rpcstack_call::Stage` around the measured work | C3 |
| Scheduling opaque task priorities | `x-rpcstack-scheduling`, vendored runtime's queue interface | Cooperative TailClipper, Masa |
| Synchronization queue age and cumulative waiting budgets | `rpcstack_call::queue::{QueueGate, QueueBudget}` | Protego |
| Typed handler reservations and asymmetric stealing | Explicit async handler scheduler and typed call tickets | Perséphone/DARC |
| Central bounded backend dispatch | Owned forwarding with remote completion accounting | R2P2 JBSQ(n) |
| Retry identity and durable result replay | Call tickets plus atomic handler transaction boundary | RIFL |
| Resource-aware safe cancellation | Explicit sensors/progress plus `rpcstack_call::cancellation::Cancellation` | Atropos |
| Network QoS and packet-level completion observations | Custom send adapter with explicit typed measurements | Aequitas controller; stock adapter still missing |

Synchronous hooks must not block to await credit, sleep, or make a control-plane
RPC. Put asynchronous admission in `prepare`, then send through the generated
stub so ordinary root/child hooks still execute. Give each attempt an absolute
deadline. Cover replica selection and all attempts with an overall deadline too.
Nested `execute` calls compose independent call policies; order determines which
waiting time a latency controller observes. `examples/policy-composition` tests
DAGOR + Breakwater + C3 on one live service, including local root rejection and
subsequent recovery with both tickets released.

## Implementation workflow

1. Translate the paper into observable inputs, state scope, decision timing,
   outputs and cancellation behavior. Link the primary paper and any inspected
   author-code revision. Separate an algorithm from its runtime assumptions.
2. Keep concrete policies in scoped example/application crates. Add a generic
   mechanism to `libs` only when an existing hook cannot implement the required
   timing or ownership. `scripts/validate_boundary.py` rejects framework path
   dependencies on examples.
3. Use `policy_stack![...]`; declare dependencies with `requires`. Choose unique
   wire section names. State scoped to one service, destination, client or attempt
   must not silently become a process-global controller.
4. Generate stubs with the vendored `tonic-build`. Select `PolicyHooks<Stack>`
   explicitly. Existing channels can use the generated `with_custom_context`
   constructor; injected service contexts use `with_server_context`. Default
   generated stubs remain no-op. For external workspaces, also patch Tokio and
   Hyper locally: Cargo patches do not propagate from a dependency workspace.
5. Use ticket ownership/Drop for cancellation cleanup. Completion hooks alone
   cannot release every reservation because cancellation has no response. Handle
   error metadata and rejected-before-send outcomes separately. Control channels
   needed for zero-credit bootstrap must remain usable without application credit.
6. Test literal controller traces and a real generated-client/server path. Include
   cancellation/deadline, overload feedback, server/client isolation and composition
   with another independent module. Do not equate a mocked timer or injected queue
   sample with a faithful runtime measurement.
7. Run `scripts/format.sh`, `scripts/test.sh`, `scripts/check.sh`. Add the policy to
   workspace membership, the explicit boundary allowlist and test script. These
   checks are CI requirements. Run mutually exclusive scheduler features separately.

## Proven limits and open work

- **CPU preemption:** `Future::poll` boundaries are cooperative. TailClipper's
  ghOSt preemption, dynamic C and multicore scheduling are not supplied here.
- **Queue sensors:** sampled Tokio runnable delay does not include kernel/NIC packet
  queue age. Breakwater accepts external observations, but a Shenango-equivalent
  sensor and timer/runtime remain to be built.
- **Packet QoS:** Aequitas needs weighted-fair network queues and final-ACK RNL.
  Its adapter contract is implemented; a production packet transport and network
  configuration are not. Task priority and metadata cannot provide those guarantees.
- **Cancellation:** dropping a local transport future releases local policy state;
  it does not prove remote work stopped or roll back an operation. Hedging requires
  verified replay safety. The Tail at Scale's tied-request protocol is separate.
- **Evaluation:** functional and transport tests pass; hotel-reservation, train-ticket
  and social-network load experiments and paper-scale performance reproductions
  have not been run for this corpus. Do not infer improvement percentages from it.
- **Coverage:** these tests focus on unary application RPCs. Breakwater's control
  stream is exercised, but arbitrary streaming application-policy semantics and
  combinations of all policies are not certified.

For VibeSys, this is a tested set of extension mechanisms and examples, not an
unrestricted promise. A proposed optimization should name its required sensor,
actuator and lifetime; an unavailable one becomes an explicit framework/transport
change instead of a superficially similar policy.

## Lessons from the second paper study

Protego needed an instrumented synchronization queue and a budget that follows
one request across several locks. Atropos needed a persistent cancellation signal
and an application-owned cleanup boundary. Those are generic additions to
`rpcstack-call`; their control algorithms remain optional examples.

Perséphone can reserve async handler slots, but CPU-core isolation still needs a
different executor. R2P2's bounded dispatch must account for actual backend
completion: dropping a caller cannot free capacity while backend work continues.
Its example quarantines uncertain backend outcomes until an explicit drain/reset.
RIFL requires effects and results to commit atomically in the same storage domain;
no hook can make unrelated external effects exactly-once. Its SQLite adapter tests
this boundary across process exits and retries. Atropos requires application
resource/progress measurements and safe cancellation, not just lifecycle hooks.

These distinctions are part of the implementation contract an agent must check
before selecting or composing a policy. The examples are tested mechanism adapters,
not claims of reproducing specialized runtimes or published benchmark results.
