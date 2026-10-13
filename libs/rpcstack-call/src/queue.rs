//! Measured, cancellation-safe waiting at explicit application resources.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tonic::Status;

/// Remaining *queueing* allowance. Time spent executing does not consume it.
#[derive(Debug)]
pub struct QueueBudget {
    remaining: Duration,
}
impl QueueBudget {
    pub fn new(remaining: Duration) -> Self {
        Self { remaining }
    }
    pub fn remaining(&self) -> Duration {
        self.remaining
    }
}
#[derive(Debug)]
struct Waiters {
    next: u64,
    entries: BTreeMap<u64, Instant>,
}
/// A FIFO resource gate. RAII removes cancelled waiters and releases acquired slots.
#[derive(Debug)]
pub struct QueueGate {
    permits: Arc<Semaphore>,
    waiters: Mutex<Waiters>,
}
impl QueueGate {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            permits: Arc::new(Semaphore::new(capacity)),
            waiters: Mutex::new(Waiters {
                next: 0,
                entries: BTreeMap::new(),
            }),
        }
    }
    pub fn waiting(&self) -> usize {
        self.waiters.lock().unwrap().entries.len()
    }
    pub fn oldest_wait(&self) -> Duration {
        self.waiters
            .lock()
            .unwrap()
            .entries
            .values()
            .next()
            .map_or(Duration::ZERO, Instant::elapsed)
    }
    pub async fn acquire(&self, budget: &mut QueueBudget) -> Result<OwnedSemaphorePermit, Status> {
        let start = Instant::now();
        let id = {
            let mut waiters = self.waiters.lock().unwrap();
            if waiters
                .entries
                .values()
                .next()
                .is_some_and(|oldest| oldest.elapsed() > budget.remaining)
            {
                return Err(Status::resource_exhausted(
                    "resource queue exceeds remaining budget",
                ));
            }
            let id = waiters.next;
            waiters.next = id
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("waiter IDs exhausted"))?;
            waiters.entries.insert(id, start);
            id
        };
        struct Waiting<'a>(&'a QueueGate, u64);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.waiters.lock().unwrap().entries.remove(&self.1);
            }
        }
        let _waiting = Waiting(self, id);
        // Timeout bounds a suddenly stalled holder in addition to admission's
        // oldest-wait estimate. Dropping acquire_owned removes its FIFO waiter.
        let result =
            tokio::time::timeout(budget.remaining, self.permits.clone().acquire_owned()).await;
        budget.remaining = budget.remaining.saturating_sub(start.elapsed());
        result
            .map_err(|_| Status::resource_exhausted("queue budget exhausted"))?
            .map_err(|_| Status::unavailable("resource closed"))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn budget_is_shared_across_queues_and_cancelled_waiters_are_removed() {
        let gate = Arc::new(QueueGate::new(1));
        let mut budget = QueueBudget::new(Duration::from_millis(10));
        let permit = gate.acquire(&mut budget).await.unwrap();
        assert!(gate.acquire(&mut budget).await.is_err());
        assert_eq!(budget.remaining(), Duration::ZERO);
        assert_eq!(gate.waiting(), 0);
        drop(permit);
        let mut fresh = QueueBudget::new(Duration::from_millis(10));
        let _permit = gate.acquire(&mut fresh).await.unwrap();
        let other = gate.clone();
        let task = tokio::spawn(async move { other.acquire(&mut fresh).await });
        tokio::task::yield_now().await;
        assert_eq!(gate.waiting(), 1);
        task.abort();
        let _ = task.await;
        assert_eq!(gate.waiting(), 0);
    }
}
