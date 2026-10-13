//! Explicit cooperative cancellation for application-approved cancellation points.
use std::future::Future;
use tonic::Status;
/// Cloneable, sticky cancellation signal. No forced thread interruption.
#[derive(Clone, Debug)]
pub struct Cancellation(tokio::sync::watch::Sender<bool>);
impl Default for Cancellation {
    fn default() -> Self {
        Self::new()
    }
}
impl Cancellation {
    pub fn new() -> Self {
        Self(tokio::sync::watch::channel(false).0)
    }
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) {
        let mut rx = self.0.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
    /// Opt in only when dropping the future and its owned resources is safe.
    /// This cannot roll back external effects or stop detached/CPU-blocking work.
    pub async fn run<T>(&self, work: impl Future<Output = Result<T, Status>>) -> Result<T, Status> {
        tokio::select! {biased;
            _=self.cancelled()=>Err(Status::cancelled("policy requested cancellation")),
            result=work=>result,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn sticky_signal_wakes_multiple_waiters_and_drops_work() {
        let c = Cancellation::new();
        let a = c.clone();
        let b = c.clone();
        let x = tokio::spawn(async move { a.cancelled().await });
        let y = tokio::spawn(async move { b.cancelled().await });
        tokio::task::yield_now().await;
        c.cancel();
        x.await.unwrap();
        y.await.unwrap();
        assert_eq!(
            c.run(async { Ok(()) }).await.unwrap_err().code(),
            tonic::Code::Cancelled
        );
    }
    #[tokio::test]
    async fn waking_a_pending_handler_releases_its_owned_resources() {
        let token = Cancellation::new();
        let child = token.clone();
        let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = permits.clone().acquire_owned().await.unwrap();
        let task = tokio::spawn(async move {
            child
                .run(async move {
                    let _permit = permit;
                    std::future::pending::<Result<(), Status>>().await
                })
                .await
        });
        tokio::task::yield_now().await;
        token.cancel();
        assert_eq!(
            task.await.unwrap().unwrap_err().code(),
            tonic::Code::Cancelled
        );
        assert_eq!(permits.available_permits(), 1);
    }
}
