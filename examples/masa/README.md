# Masa as an optional rpcstack composition example

Ports the Masa client facade, core policy types, policy modules and deterministic
semantic tests from [vic-lsh/masa](https://github.com/vic-lsh/masa), source commit
`d44f038e83f0f48cdd62f7a31be8b1c63beb58d5` (the earlier paper-policy worktree).
This is the original composed policy implementation, not a new policy named Masa.

The example demonstrates budget decisions, end-to-end deadline rejection, latency
estimation, predictive admission, slack signaling, queue telemetry and custom
module stacks. Source modules live in `masa-policy/src/module`; `masa_stack.rs`
selects the stack. No framework, runtime or transport crate depends on these
examples. Default workspace members remain the four generic framework crates.

Use `masa::DefaultHooks` explicitly in generated stubs and enable a scheduling
feature, e.g. `sched_slo,abort_slo,signal_slack,est_mean_var,ac_pred`. The inherited
DefaultHooks selection is tied to scheduling features; admission features also
require a scheduling feature at compile time. A separate application must patch Tokio and
Hyper as described in the repository README. `MASA_POLICY_PARAMS_PATH` remains
Masa's own example configuration; the generic framework does not read it.
The separately ported Rajomon crate uses `RPCSTACK_POLICY_PARAMS_PATH`.

Port changes:

- Generic hook/feature paths now use `tonic::rpcstack` and `hyper/rpcstack`.
- Root context helpers and child budget sealing emit the independent opaque
  scheduling header. Generic transport never decodes Masa's budget section.
- The removed Masa-specific Tonic channel is replaced by example-owned buffered
  round-robin dispatch over public `tonic::transport::Channel` APIs. Clones share
  dispatch order; eager connection/retry behavior is retained. It uses normal
  channel buffering per endpoint and is not a byte-for-byte old transport port.
- Policy tests use Masa's existing virtual clock, with explicit time advances,
  so scheduling delays on a loaded CI host cannot expire test deadlines or rotate
  observation windows. Production builds retain the real clock.
- Historical `ac_rajomon` remains the historical algorithm. Use the separate
  `PaperRajomonModule` example for the published Rajomon policy study.

Validation runs the core/policy/semantic suites, combined SLO/abort/estimation/
predictive-admission feature tests and the historical Rajomon adapter separately.
Tests also check root/child opaque priority propagation and shared round-robin
selection. This is a composition and semantics reference, not a reproduction of
Masa's benchmark deployments. Cooperative runtime hooks cannot forcibly preempt
arbitrary Rust code. The full source project's benchmark applications, deployment
scripts and experiments are intentionally not part of this example.
