# Policy composition

Cross-policy integration fixtures live here. Individual paper implementations
live in their own sibling folders; this crate contains no paper controller.

- Default tests compose DAGOR, Breakwater and C3, and exercise the Masa stack.
- `arrival_price` composes TailClipper and Rajomon over two real gRPC hops.
  It preserves arrival times and token holdings, checks rejection-price feedback
  and root admission, and verifies the oldest-six cooperative runtime schedule.

From the repository root:

```sh
cargo test -p rpcstack-policy-composition
./examples/tailclipper/cargo.sh test -p rpcstack-policy-composition --features orf --test arrival_price
./examples/tailclipper/cargo.sh run -p rpcstack-policy-composition --features orf --example arrival-price
```

The opt-in `orf` feature enables custom scheduling and the transport scheduling
adapter. The wrapper selects TailClipper's example-owned queue. These are
correctness fixtures, not reproductions of paper performance measurements.
