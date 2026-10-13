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
