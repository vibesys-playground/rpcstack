# Working on rpcstack

This repository contains the RPC module framework and necessary runtime support.
Keep concrete application policies in separate repositories or explicitly scoped
examples. Put each paper implementation directly in `examples/<paper>/`, including
its policy-specific queues and tests. Shared cross-policy fixtures belong in
`examples/policy-composition/`. Do not add policy dependencies to the framework or transport.

- `rpcstack-wire` is a leaf codec.
- `rpcstack-sched` is a leaf queue abstraction and queue implementations.
- `rpcstack` owns lifecycle composition; `rpcstack-tonic` owns dispatch plumbing.
- Tonic's generic hook API is `tonic::rpcstack`.
- Hyper consumes only opaque scheduling metadata.
- Generated stubs default to no-op hooks; applications select a stack explicitly.

Run `./scripts/test.sh` and `./scripts/check.sh` before committing changes.
Use `./scripts/format.sh` to format. Include focused regression tests for behavior
changes. Do not claim forced preemption: scheduling operates between future polls.
