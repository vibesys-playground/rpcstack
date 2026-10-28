//! R2P2 JBSQ(n) request dispatcher, adapted to generated gRPC calls.
use rpcstack_call::{CallInfo, CallPolicy, Prepare, Ticket};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::{sync::oneshot, time::Instant};
use tonic::{Request, Response, Status};
struct Waiter {
    id: u64,
    ready: oneshot::Sender<usize>,
}
struct Backend {
    destination: String,
    outstanding: usize,
    healthy: bool,
    epoch: u64,
}
struct State {
    backends: Vec<Backend>,
    queue: VecDeque<Waiter>,
    assigned: BTreeMap<u64, (usize, u64)>,
    next: u64,
    tie: usize,
}
/// A single shared router owns all counters: create one per backend worker group,
/// not an independent instance in every client.
pub struct Dispatcher {
    bound: usize,
    max_pending: usize,
    state: Mutex<State>,
}
impl Dispatcher {
    pub fn new(destinations: Vec<String>, bound: usize, max_pending: usize) -> Arc<Self> {
        assert!(!destinations.is_empty() && bound > 0 && max_pending > 0);
        assert!(destinations.iter().all(|d| !d.is_empty()));
        let unique: std::collections::BTreeSet<_> = destinations.iter().collect();
        assert_eq!(unique.len(), destinations.len());
        Arc::new(Self {
            bound,
            max_pending,
            state: Mutex::new(State {
                backends: destinations
                    .into_iter()
                    .map(|destination| Backend {
                        destination,
                        outstanding: 0,
                        healthy: true,
                        epoch: 0,
                    })
                    .collect(),
                queue: VecDeque::new(),
                assigned: BTreeMap::new(),
                next: 0,
                tie: 0,
            }),
        })
    }
    pub fn outstanding(&self) -> Vec<usize> {
        self.state
            .lock()
            .unwrap()
            .backends
            .iter()
            .map(|b| b.outstanding)
            .collect()
    }
    pub fn queued(&self) -> usize {
        self.state.lock().unwrap().queue.len()
    }
    fn drain(&self, s: &mut State) {
        loop {
            while s.queue.front().is_some_and(|w| w.ready.is_closed()) {
                s.queue.pop_front();
            }
            if s.queue.is_empty() {
                break;
            }
            let n = s.backends.len();
            let selected = (0..n)
                .map(|i| (s.tie + i) % n)
                .filter(|&i| s.backends[i].healthy && s.backends[i].outstanding < self.bound)
                .min_by_key(|&i| s.backends[i].outstanding);
            let Some(index) = selected else { break };
            let waiter = s.queue.pop_front().unwrap();
            if waiter.ready.send(index).is_ok() {
                s.backends[index].outstanding += 1;
                s.assigned
                    .insert(waiter.id, (index, s.backends[index].epoch));
                s.tie = (index + 1) % n;
            }
        }
    }
    async fn reserve(self: &Arc<Self>, deadline: Option<Instant>) -> Result<Reservation, Status> {
        let (tx, rx) = oneshot::channel();
        let id = {
            let mut s = self.state.lock().unwrap();
            if s.queue.len() >= self.max_pending {
                return Err(Status::resource_exhausted("JBSQ router queue full"));
            }
            let id = s.next;
            s.next = id
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("RPC identity exhausted"))?;
            s.queue.push_back(Waiter { id, ready: tx });
            self.drain(&mut s);
            id
        };
        let reservation = Reservation {
            router: self.clone(),
            id,
            sent: false,
            completed: false,
        };
        if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, rx)
                .await
                .map_err(|_| Status::deadline_exceeded("JBSQ dispatch deadline"))?
                .map_err(|_| Status::unavailable("router stopped"))?;
        } else {
            rx.await
                .map_err(|_| Status::unavailable("router stopped"))?;
        }
        Ok(reservation)
    }
    /// Keep forwarding and its slot alive if the upstream caller disconnects.
    /// Unknown transport outcomes quarantine the backend until an operator confirms
    /// it drained/restarted. Releasing a slot early would violate JBSQ's bound.
    pub async fn call<Req, Ret, F, Fut>(
        self: &Arc<Self>,
        deadline: Option<Instant>,
        request: Request<Req>,
        send: F,
    ) -> Result<Response<Ret>, Status>
    where
        Req: Send + 'static,
        Ret: Send + 'static,
        F: FnOnce(String, Request<Req>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Response<Ret>, Status>> + Send + 'static,
    {
        let info = CallInfo {
            destination: "router".into(),
            ordinal: 0,
            deadline,
        };
        let policy = Routing(self.clone());
        let mut ticket = policy.prepare(&info).await?;
        let destination = ticket.destination();
        let mut request = request;
        ticket.decorate(&mut request)?;
        let work = tokio::spawn(async move {
            let result = send(destination, request).await;
            // A successful response confirms remote completion. Errors are
            // conservative: gRPC status alone cannot establish remote termination.
            ticket.finish(&result);
            result
        });
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, work)
                .await
                .map_err(|_| Status::deadline_exceeded("JBSQ response deadline"))?
                .map_err(|_| Status::internal("forwarding task failed"))?,
            None => work
                .await
                .map_err(|_| Status::internal("forwarding task failed"))?,
        }
    }
    /// Caller must ensure the old worker incarnation can no longer execute work.
    pub fn reset_drained_backend(&self, index: usize) -> Result<(), Status> {
        let mut s = self.state.lock().unwrap();
        let b = s
            .backends
            .get_mut(index)
            .ok_or_else(|| Status::invalid_argument("unknown worker"))?;
        b.epoch = b
            .epoch
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("worker epoch exhausted"))?;
        b.healthy = true;
        b.outstanding = 0;
        self.drain(&mut s);
        Ok(())
    }
}
struct Reservation {
    router: Arc<Dispatcher>,
    id: u64,
    sent: bool,
    completed: bool,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut s = self.router.state.lock().unwrap();
        s.queue.retain(|w| w.id != self.id);
        if let Some((index, epoch)) = s.assigned.remove(&self.id) {
            let b = &mut s.backends[index];
            if b.epoch == epoch {
                if self.sent && !self.completed {
                    b.healthy = false;
                } else {
                    b.outstanding -= 1;
                }
            }
        }
        self.router.drain(&mut s);
    }
}
/// Async call-policy ticket exposes the selected route and owns its accounting.
pub struct RoutedTicket {
    reservation: Reservation,
}
impl RoutedTicket {
    pub fn destination(&self) -> String {
        let s = self.reservation.router.state.lock().unwrap();
        let (index, _) = s.assigned[&self.reservation.id];
        s.backends[index].destination.clone()
    }
}
impl Ticket for RoutedTicket {
    fn decorate<T>(&mut self, _: &mut Request<T>) -> Result<(), Status> {
        self.reservation.sent = true;
        Ok(())
    }
    fn finish<T>(&mut self, result: &Result<Response<T>, Status>) {
        self.reservation.completed = result.is_ok();
    }
}
#[derive(Clone)]
pub struct Routing(pub Arc<Dispatcher>);
impl CallPolicy for Routing {
    type Ticket = RoutedTicket;
    fn prepare<'a>(&'a self, info: &'a CallInfo) -> Prepare<'a, Self::Ticket> {
        Box::pin(async move {
            Ok(RoutedTicket {
                reservation: self.0.reserve(info.deadline).await?,
            })
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[tokio::test(start_paused = true)]
    async fn shortest_queue_bound_and_cancelled_waiters() {
        let d = Dispatcher::new(vec!["a".into(), "b".into()], 2, 10);
        let a = d.reserve(None).await.unwrap();
        let b = d.reserve(None).await.unwrap();
        assert_eq!(d.outstanding(), vec![1, 1]);
        let c = d.reserve(None).await.unwrap();
        let e = d.reserve(None).await.unwrap();
        assert_eq!(d.outstanding(), vec![2, 2]);
        assert!(d
            .reserve(Some(Instant::now() + Duration::from_millis(10)))
            .await
            .is_err());
        assert_eq!(d.queued(), 0);
        drop((a, b, c, e));
        assert_eq!(d.outstanding(), vec![0, 0]);
    }
    #[tokio::test(start_paused = true)]
    async fn upstream_timeout_keeps_backend_slot_until_completion() {
        let d = Dispatcher::new(vec!["a".into()], 1, 10);
        let result = d
            .call(
                Some(Instant::now() + Duration::from_millis(5)),
                Request::new(()),
                |_, _| async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(Response::new(()))
                },
            )
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
        assert_eq!(d.outstanding(), vec![1]);
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert_eq!(d.outstanding(), vec![0]);
    }
    #[tokio::test(start_paused = true)]
    async fn uncertain_failure_quarantines_and_old_completions_do_not_corrupt_new_epoch() {
        let d = Dispatcher::new(vec!["a".into()], 1, 10);
        let result = d
            .call(None, Request::new(()), |_, _| async {
                Err::<Response<()>, _>(Status::unavailable("lost response"))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(d.outstanding(), vec![1]);
        assert!(d
            .reserve(Some(Instant::now() + Duration::from_millis(1)))
            .await
            .is_err());
        d.reset_drained_backend(0).unwrap();
        let old = d.reserve(None).await.unwrap();
        d.reset_drained_backend(0).unwrap();
        let new = d.reserve(None).await.unwrap();
        drop(old);
        assert_eq!(d.outstanding(), vec![1]);
        drop(new);
        assert_eq!(d.outstanding(), vec![0]);
    }
}
