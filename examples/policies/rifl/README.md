# RIFL durable RPC deduplication

Paper: [Implementing Linearizability at Large Scale and Low Latency, SOSP 2015](https://web.stanford.edu/~ouster/cgi-bin/papers/rifl.pdf),
§§3–4. Inspected author implementation:
[`UnackedRpcResults.cc`](https://github.com/PlatformLab/RAMCloud/blob/b1866af19124325a6dfd8cbc267e2e3ef1f965d1/src/UnackedRpcResults.cc).

This adapter implements RIFL's identity/result-tracking mechanism on a single
SQLite shard. `(client lease, sequence)` identifies an RPC. Effects and saved
response commit in the **same transaction**. Duplicate calls serialize and replay
the result. Request bytes and object identity also detect accidental identity reuse.
Acknowledgments advance only past completed RPCs; expired-client result collection
retains identity tombstones. A durable clock high-water mark prevents time rollback
from resurrecting collected clients.

Integration:

1. Open `Store`, initialize the application schema, and allocate an authoritative
   lease. Lease APIs take trusted server time in milliseconds, not client timestamps.
   Renew the lease before expiry; a fresh tracker must use a fresh lease ID.
2. `RequestTracker::begin` creates an `Rpc` implementing `CallPolicy`. Send with
   `rpcstack_call::execute` and generated stubs. Clone that `Rpc` for retries;
   `Rpc::retry` retries Unavailable outcomes with a bounded overall deadline.
3. In the handler, parse `Identity::from_request`, then call `Store::execute` with
   the canonical operation bytes and a transaction callback. Run this synchronous
   storage adapter on `spawn_blocking`, as the live test does.
4. Explicitly `acknowledge` only after accepting the result. Dropping/timing out
   an attempt does not acknowledge it. A timeout can mean an indeterminate outcome;
   preserve the identity rather than blindly issue a new operation.

No lifecycle-only completion hook can make an arbitrary external side effect
atomic with a result cache. All effects must use the provided SQL transaction;
external HTTP calls, files or another database are outside this contract. Existing
async tickets and the explicit handler transaction boundary suffice for this adapter.
Lease provision/renew/GC APIs are colocated with the shard; this is not a distributed
lease service. Migration must transfer a consistent SQLite snapshot containing
both application and RIFL tables (including WAL state when applicable).

This does not implement RAMCloud replication, multi-shard transaction coordination,
or transparent exactly-once semantics for arbitrary handlers. No performance
reproduction is claimed. Persistent result limits reject excess unacknowledged work.

`cargo test -p rpcstack-rifl` tests concurrent duplicate suppression, out-of-order
ack holes, identity misuse, rollback, expiry, lost replies over gRPC, reopening a
shard, and child-process exits immediately before and after durable commit.
