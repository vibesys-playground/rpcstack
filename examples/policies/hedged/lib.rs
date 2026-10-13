//! Delayed hedged requests from Dean & Barroso, The Tail at Scale (CACM 2013).
//! This implements hedging, not the paper's separate cross-server tied requests.
use std::{collections::VecDeque, future::Future, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tonic::{Response, Status};

/// Explicit contract: the caller has verified replay safety for this operation.
#[derive(Clone, Copy, Debug)]
pub enum ReplaySafety {
    ReadOnly,
    Idempotent,
}
#[derive(Clone, Debug)]
pub struct Hedger {
    delay: Duration,
    extras: Arc<Semaphore>,
}
impl Hedger {
    pub fn new(delay: Duration, max_concurrent_backups: usize) -> Self {
        Self {
            delay,
            extras: Arc::new(Semaphore::new(max_concurrent_backups)),
        }
    }
    /// Closures should call `rpcstack_call::execute` around generated RPC stubs.
    /// The backup closure is invoked only after delay and if a backup permit exists.
    /// First response wins, including errors. No automatic retry or remote rollback.
    pub async fn call<T, P, B, PF, BF>(
        &self,
        _: ReplaySafety,
        primary: P,
        backup: B,
    ) -> Result<Response<T>, Status>
    where
        P: FnOnce() -> PF,
        B: FnOnce() -> BF,
        PF: Future<Output = Result<Response<T>, Status>>,
        BF: Future<Output = Result<Response<T>, Status>>,
    {
        let primary = primary();
        tokio::pin!(primary);
        tokio::select! {
            biased;
            response=&mut primary => return response,
            _=tokio::time::sleep(self.delay) => {}
        }
        let Ok(_permit) = self.extras.clone().try_acquire_owned() else {
            return primary.await;
        };
        let backup = backup();
        tokio::pin!(backup);
        tokio::select! { biased; response=&mut primary=>response, response=&mut backup=>response }
        // Both local futures and the permit are dropped, including on cancellation.
    }
    pub fn available_backup_permits(&self) -> usize {
        self.extras.available_permits()
    }
}

/// Optional per-request-class p95 estimator. Feed independent primary-only samples
/// to avoid the censoring bias of cancelled slow primaries. Until warm, use fallback.
#[derive(Debug)]
pub struct LatencyWindow {
    samples: VecDeque<Duration>,
    capacity: usize,
    minimum: usize,
    fallback: Duration,
}
impl LatencyWindow {
    pub fn new(capacity: usize, minimum: usize, fallback: Duration) -> Self {
        assert!(minimum > 0 && minimum <= capacity);
        Self {
            samples: VecDeque::new(),
            capacity,
            minimum,
            fallback,
        }
    }
    pub fn observe(&mut self, latency: Duration) {
        if self.samples.len() == self.capacity {
            self.samples.pop_front();
        }
        self.samples.push_back(latency);
    }
    pub fn p95(&self) -> Duration {
        if self.samples.len() < self.minimum {
            return self.fallback;
        }
        let mut sorted: Vec<_> = self.samples.iter().copied().collect();
        sorted.sort_unstable();
        sorted[(sorted.len() * 95).div_ceil(100) - 1]
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    #[tokio::test(start_paused = true)]
    async fn fast_primary_does_not_launch_backup() {
        let h = Hedger::new(Duration::from_millis(10), 1);
        assert_eq!(
            h.call(
                ReplaySafety::ReadOnly,
                || async { Ok(Response::new(1)) },
                || async {
                    panic!("backup launched");
                    #[allow(unreachable_code)]
                    Ok(Response::new(2))
                }
            )
            .await
            .unwrap()
            .into_inner(),
            1
        );
    }
    #[tokio::test(start_paused = true)]
    async fn backup_wins_and_cancels_primary_and_releases_budget() {
        let h = Hedger::new(Duration::from_millis(10), 1);
        let dropped = Arc::new(AtomicUsize::new(0));
        let count = dropped.clone();
        let value = h
            .call(
                ReplaySafety::ReadOnly,
                || async move {
                    let _guard = Guard(count);
                    std::future::pending::<Result<Response<i32>, Status>>().await
                },
                || async { Ok(Response::new(2)) },
            )
            .await
            .unwrap();
        assert_eq!(value.into_inner(), 2);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert_eq!(h.available_backup_permits(), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn budget_exhaustion_keeps_primary_and_error_is_a_response() {
        let h = Hedger::new(Duration::from_millis(1), 0);
        let result = h
            .call(
                ReplaySafety::Idempotent,
                || async {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Err::<Response<()>, _>(Status::unavailable("primary"))
                },
                || async {
                    panic!("budget exceeded");
                    #[allow(unreachable_code)]
                    Ok(Response::new(()))
                },
            )
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }
    #[test]
    fn estimator_is_bounded_and_uses_nearest_rank() {
        let mut w = LatencyWindow::new(20, 20, Duration::from_secs(1));
        assert_eq!(w.p95(), Duration::from_secs(1));
        for i in 1..=20 {
            w.observe(Duration::from_millis(i));
        }
        assert_eq!(w.p95(), Duration::from_millis(19));
        w.observe(Duration::from_millis(21));
        assert_eq!(w.p95(), Duration::from_millis(20));
    }
}
