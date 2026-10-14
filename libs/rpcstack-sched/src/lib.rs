//! Scheduling building blocks for the RPC stack runtime.
//!
//! The runtime (a vendored Tokio) owns mechanisms only: it stores a [`Meta`]
//! per task, hands each runnable task to a [`RunQueue`], and asks the queue
//! which task to poll next. The framework supplies generic queues; applications
//! can supply their own queue without a runtime dependency on their policy:
//!
//! - [`RunQueue`], [`TaskView`], [`SchedFlavor`]: the interface (mechanism).
//! - [`Meta`]: the per-task scheduling metadata type.
//! - [`builtin`]: the built-in queues (FIFO, priority heap).
//!   They interpret `Meta` and so embody policy.
//! - [`custom`]: a copy of the priority-heap queue, meant to be edited to
//!   write a new queue.
//! - [`default_policy`]: which `Meta` a spawn gets when its caller gave none.
//!
//! The queue the runtime uses is chosen at compile time by Cargo features and
//! exposed as [`SelectedQueue`].
//!
//! This is a leaf crate with no external dependencies.

pub mod builtin;
pub mod custom;
pub mod default_policy;
mod meta;
mod queue;
#[cfg(feature = "lifecycle_trace")]
pub mod trace;

#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod replay_tests;

pub use meta::Meta;
pub use queue::{PollOutcome, RunQueue, SchedFlavor, TaskView};

// The build script declares an external module only when the application
// supplies RPCSTACK_QUEUE_SOURCE. No example path is embedded in the library.
include!(concat!(env!("OUT_DIR"), "/external_queue.rs"));

/// The run queue selected by Cargo features. `sched_custom` selects the
/// application-supplied `Queue<T>` when `RPCSTACK_QUEUE_SOURCE` is set, otherwise
/// the editable `custom::Queue`. With no custom selection, `sched_prio` selects
/// the priority heap; the default is FIFO.
#[cfg(all(feature = "sched_custom", rpcstack_external_queue))]
pub type SelectedQueue<T> = external::Queue<T>;
#[cfg(all(feature = "sched_custom", not(rpcstack_external_queue)))]
pub type SelectedQueue<T> = custom::Queue<T>;
#[cfg(all(feature = "sched_prio", not(feature = "sched_custom")))]
pub type SelectedQueue<T> = builtin::prio_heap::BinaryHeapQueue<T>;
#[cfg(not(any(feature = "sched_prio", feature = "sched_custom")))]
pub type SelectedQueue<T> = builtin::fifo::FifoQueue<T>;
