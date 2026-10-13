//! Async call control for policies that must wait, route, or race RPC attempts.
//! Generated stubs remain the send function, so normal module hooks still run.
use std::{future::Future, pin::Pin};
use tonic::{Request, Response, Status};

pub type Prepare<'a, T> = Pin<Box<dyn Future<Output = Result<T, Status>> + Send + 'a>>;

/// Explicit attempt identity. A policy may interpret the destination or ordinal;
/// the executor itself assigns neither priorities nor replica-selection rules.
#[derive(Clone, Debug)]
pub struct CallInfo {
    pub destination: String,
    pub ordinal: usize,
    pub deadline: Option<tokio::time::Instant>,
}

/// Per-attempt state. Implement `Drop` to release leases if a caller is cancelled.
/// `finish` runs only when `send` resolves; cancellation/deadline drops the ticket.
pub trait Ticket {
    fn decorate<T>(&mut self, _request: &mut Request<T>) -> Result<(), Status> {
        Ok(())
    }
    fn finish<T>(&mut self, _result: &Result<Response<T>, Status>) {}
}
impl Ticket for () {}

/// An async preparation step can wait for credit, a pacing timer, or a permit.
pub trait CallPolicy {
    type Ticket: Ticket;
    fn prepare<'a>(&'a self, info: &'a CallInfo) -> Prepare<'a, Self::Ticket>;
}

/// Deadline includes preparation and transport. No detached tasks are spawned:
/// dropping this future drops preparation, transport and the acquired ticket.
/// The supplied send function must not detach work if cancellation is required.
pub async fn execute<P, Req, Ret, F, Fut>(
    policy: &P,
    info: &CallInfo,
    mut request: Request<Req>,
    send: F,
) -> Result<Response<Ret>, Status>
where
    P: CallPolicy,
    F: FnOnce(Request<Req>) -> Fut,
    Fut: Future<Output = Result<Response<Ret>, Status>>,
{
    let call = async {
        let mut ticket = policy.prepare(info).await?;
        ticket.decorate(&mut request)?;
        let result = send(request).await;
        ticket.finish(&result);
        result
    };
    match info.deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, call)
            .await
            .map_err(|_| Status::deadline_exceeded("RPC attempt deadline"))?,
        None => call.await,
    }
}

/// A bounded execution stage with queue and service measurements. The caller owns
/// the shared stage, so unrelated services never share permits implicitly.
#[derive(Debug)]
pub struct Stage {
    permits: std::sync::Arc<tokio::sync::Semaphore>,
    waiting: std::sync::atomic::AtomicUsize,
}
#[derive(Clone, Copy, Debug)]
pub struct StageSample {
    pub queued_for: std::time::Duration,
    pub serviced_for: std::time::Duration,
    pub waiting_after: usize,
}
impl Stage {
    pub fn new(concurrency: usize) -> Self {
        assert!(concurrency > 0, "stage concurrency must be positive");
        Self {
            permits: std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency)),
            waiting: 0.into(),
        }
    }
    pub fn waiting(&self) -> usize {
        self.waiting.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub async fn run<T>(&self, service: impl Future<Output = T>) -> (T, StageSample) {
        struct Waiting<'a>(&'a std::sync::atomic::AtomicUsize);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let start = tokio::time::Instant::now();
        self.waiting
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let waiting = Waiting(&self.waiting);
        let _permit = self.permits.acquire().await.expect("stage never closes");
        drop(waiting);
        let ready = tokio::time::Instant::now();
        let value = service.await;
        (
            value,
            StageSample {
                queued_for: ready.duration_since(start),
                serviced_for: ready.elapsed(),
                waiting_after: self.waiting(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct Gate(Arc<tokio::sync::Semaphore>, Arc<AtomicUsize>);
    struct Lease(tokio::sync::OwnedSemaphorePermit, Arc<AtomicUsize>);
    impl Ticket for Lease {
        fn finish<T>(&mut self, _: &Result<Response<T>, Status>) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    impl Drop for Lease {
        fn drop(&mut self) {
            let _ = self.0.num_permits();
        }
    }
    impl CallPolicy for Gate {
        type Ticket = Lease;
        fn prepare<'a>(&'a self, _: &'a CallInfo) -> Prepare<'a, Lease> {
            Box::pin(async {
                Ok(Lease(
                    self.0.clone().acquire_owned().await.unwrap(),
                    self.1.clone(),
                ))
            })
        }
    }
    #[tokio::test(start_paused = true)]
    async fn timeout_drops_lease_without_fabricating_completion() {
        let gate = Gate(
            Arc::new(tokio::sync::Semaphore::new(1)),
            Arc::new(AtomicUsize::new(0)),
        );
        let info = CallInfo {
            destination: "leaf".into(),
            ordinal: 0,
            deadline: Some(tokio::time::Instant::now() + std::time::Duration::from_millis(10)),
        };
        let result = execute(&gate, &info, Request::new(()), |_| {
            std::future::pending::<Result<Response<()>, Status>>()
        })
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
        assert_eq!(gate.0.available_permits(), 1);
        assert_eq!(gate.1.load(Ordering::Relaxed), 0);
        let no_deadline = CallInfo {
            deadline: None,
            ..info
        };
        execute(&gate, &no_deadline, Request::new(()), |_| async {
            Err::<Response<()>, _>(Status::unavailable("failure"))
        })
        .await
        .unwrap_err();
        assert_eq!(gate.1.load(Ordering::Relaxed), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_covers_waiting_before_send() {
        let gate = Gate(
            Arc::new(tokio::sync::Semaphore::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        let info = CallInfo {
            destination: "leaf".into(),
            ordinal: 0,
            deadline: Some(tokio::time::Instant::now() + std::time::Duration::from_millis(10)),
        };
        let result = execute(&gate, &info, Request::new(()), |_| async {
            panic!("must not send");
            #[allow(unreachable_code)]
            Ok(Response::new(()))
        })
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    }
    #[tokio::test(start_paused = true)]
    async fn cancelled_waiter_does_not_leak_stage_count() {
        let stage = Arc::new(Stage::new(1));
        let permit = stage.permits.acquire().await.unwrap();
        let cloned = stage.clone();
        let task = tokio::spawn(async move {
            cloned.run(async {}).await;
        });
        tokio::task::yield_now().await;
        assert_eq!(stage.waiting(), 1);
        task.abort();
        let _ = task.await;
        assert_eq!(stage.waiting(), 0);
        drop(permit);
        let (_, sample) = stage
            .run(async { tokio::time::sleep(std::time::Duration::from_millis(3)).await })
            .await;
        assert_eq!(sample.serviced_for, std::time::Duration::from_millis(3));
    }
}
