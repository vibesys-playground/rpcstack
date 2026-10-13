# rpcstack-call

Generic async preparation and attempt lifetime management. A `CallPolicy` acquires
its own per-attempt `Ticket`, which can decorate the request and observe completion.
Use `Drop` on the ticket for cancellation cleanup. `execute` accepts a generated
RPC stub as its send closure, so rpcstack's existing root/child hooks still run.
An optional deadline covers preparation and transport together. No work is spawned
or detached, and no policy-specific routing, retry, or admission rule is imposed.

`Stage` supplies a bounded service queue and measurements for policies that need
queue depth and service-time feedback. Queue counts and permits are cancellation
safe. It measures a wrapped future's wall time, not CPU execution time.

Transport cancellation means dropping the future. This does not roll back remote
side effects or guarantee an already executing remote handler stops immediately.

`queue::QueueGate` instruments a FIFO synchronization queue. Pass the same mutable
`QueueBudget` through sequential acquisitions to account for cumulative waiting,
reject excessive oldest-wait delay and bound stalled waiting. Holding/service time
is not deducted from the queue budget. Protego demonstrates this boundary.

`cancellation::Cancellation` supplies a cloneable sticky cancellation signal.
`run` races an explicitly cancellation-safe future against that signal and drops
its owned resources on cancellation. Applications may instead await `cancelled`
and invoke their own rollback. Atropos demonstrates resource-aware selection;
the framework does not choose which calls to cancel or declare side effects safe.
