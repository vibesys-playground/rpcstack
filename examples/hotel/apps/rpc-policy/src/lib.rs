//! Agent-owned RPC policy. The baseline has no admission, scheduling, retry,
//! deadline, or cancellation policy. Every generated client/server selects Hooks.
//! Implement rpcstack::Module and compose modules into Stack here.
pub type Stack = ();
pub type Hooks = rpcstack_tonic::PolicyHooks<Stack>;
