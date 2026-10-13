//! Perséphone's DARC policy (SOSP 2021): non-work-conserving typed dispatch.
use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};
use tonic::Status;
#[derive(Clone, Debug)]
pub struct Profile {
    pub service: Duration,
    pub occurrence: f64,
}
#[derive(Clone, Debug)]
pub struct Config {
    pub workers: usize,
    pub similarity: f64,
    pub max_pending: usize,
    pub profile_window: u64,
    pub slowdown_trigger: f64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            workers: 4,
            similarity: 1.5,
            max_pending: 4096,
            profile_window: 64,
            slowdown_trigger: 10.0,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reservation {
    pub reserved: Vec<usize>,
    pub stealable: Vec<usize>,
}
/// Algorithms 1–2. The last worker is a spillway shared by overflow/unknown types.
pub fn reservations(config: &Config, profiles: &[Profile]) -> Vec<Reservation> {
    let mut order: Vec<_> = (0..profiles.len()).collect();
    order.sort_by_key(|&i| profiles[i].service);
    let total: f64 = profiles
        .iter()
        .map(|p| p.service.as_secs_f64() * p.occurrence)
        .sum();
    let mut result = vec![
        Reservation {
            reserved: vec![],
            stealable: vec![]
        };
        profiles.len()
    ];
    let mut cursor: usize = 0;
    let mut at = 0;
    while at < order.len() {
        let first = at;
        let minimum = profiles[order[at]].service.as_secs_f64();
        at += 1;
        while at < order.len()
            && profiles[order[at]].service.as_secs_f64() <= minimum * config.similarity
        {
            at += 1;
        }
        let demand: f64 = order[first..at]
            .iter()
            .map(|&i| profiles[i].service.as_secs_f64() * profiles[i].occurrence)
            .sum();
        let count = ((demand / total * config.workers as f64).round() as usize).max(1);
        let end = cursor.saturating_add(count).min(config.workers - 1);
        let reserved = if end > cursor {
            (cursor..end).collect()
        } else {
            vec![config.workers - 1]
        };
        let stealable = if end > cursor {
            (end..config.workers).collect()
        } else {
            vec![]
        };
        for &i in &order[first..at] {
            result[i] = Reservation {
                reserved: reserved.clone(),
                stealable: stealable.clone(),
            };
        }
        cursor = end;
    }
    result
}
struct Waiting {
    id: u64,
    entered: Instant,
    ready: oneshot::Sender<()>,
}
struct Active {
    id: u64,
    kind: usize,
    started: Instant,
}
struct State {
    profiles: Vec<Profile>,
    reservations: Vec<Reservation>,
    queues: Vec<VecDeque<Waiting>>,
    workers: Vec<Option<Active>>,
    next: u64,
    samples: Vec<(u64, Duration)>,
    completed: u64,
    slow: bool,
}
pub struct Scheduler {
    config: Config,
    state: Mutex<State>,
}
impl Scheduler {
    pub fn new(config: Config, profiles: Vec<Profile>) -> Arc<Self> {
        assert!(config.workers >= 2 && config.max_pending > 0 && config.profile_window > 0);
        assert!(
            config.similarity.is_finite()
                && config.similarity >= 1.0
                && config.slowdown_trigger.is_finite()
                && config.slowdown_trigger > 0.0
        );
        assert!(
            !profiles.is_empty()
                && profiles.iter().all(|p| !p.service.is_zero()
                    && p.occurrence.is_finite()
                    && p.occurrence > 0.0)
        );
        let r = reservations(&config, &profiles);
        let n = profiles.len();
        Arc::new(Self {
            state: Mutex::new(State {
                profiles,
                reservations: r,
                queues: (0..=n).map(|_| VecDeque::new()).collect(),
                workers: (0..config.workers).map(|_| None).collect(),
                next: 0,
                samples: vec![(0, Duration::ZERO); n],
                completed: 0,
                slow: false,
            }),
            config,
        })
    }
    pub fn profiles(&self) -> Vec<Profile> {
        self.state.lock().unwrap().profiles.clone()
    }
    pub fn plan(&self) -> Vec<Reservation> {
        self.state.lock().unwrap().reservations.clone()
    }
    pub fn queued(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .queues
            .iter()
            .map(VecDeque::len)
            .sum()
    }
    pub fn active(&self) -> usize {
        self.state.lock().unwrap().workers.iter().flatten().count()
    }
    fn dispatch(&self, s: &mut State) {
        let n = s.profiles.len();
        let mut order: Vec<_> = (0..n).collect();
        order.sort_by_key(|&i| s.profiles[i].service);
        order.push(n);
        for kind in order {
            while !s.queues[kind].is_empty() {
                let allowed = if kind == n {
                    vec![self.config.workers - 1]
                } else {
                    s.reservations[kind]
                        .reserved
                        .iter()
                        .chain(&s.reservations[kind].stealable)
                        .copied()
                        .collect()
                };
                let Some(worker) = allowed.into_iter().find(|&w| s.workers[w].is_none()) else {
                    break;
                };
                let waiter = s.queues[kind].pop_front().unwrap();
                if waiter.ready.is_closed() {
                    continue;
                }
                if kind < n
                    && waiter.entered.elapsed().as_secs_f64()
                        > self.config.slowdown_trigger * s.profiles[kind].service.as_secs_f64()
                {
                    s.slow = true;
                }
                s.workers[worker] = Some(Active {
                    id: waiter.id,
                    kind,
                    started: Instant::now(),
                });
                if waiter.ready.send(()).is_err() {
                    s.workers[worker] = None;
                }
            }
        }
    }
    /// `kind=None` (or an unregistered type) uses only the spillway worker.
    pub async fn acquire(self: &Arc<Self>, kind: Option<usize>) -> Result<Lease, Status> {
        let (tx, rx) = oneshot::channel();
        let id = {
            let mut s = self.state.lock().unwrap();
            if s.queues.iter().map(VecDeque::len).sum::<usize>() >= self.config.max_pending {
                return Err(Status::resource_exhausted("DARC queue full"));
            }
            let kind = kind
                .filter(|&k| k < s.profiles.len())
                .unwrap_or(s.profiles.len());
            let id = s.next;
            s.next = id
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("DARC identities exhausted"))?;
            s.queues[kind].push_back(Waiting {
                id,
                entered: Instant::now(),
                ready: tx,
            });
            self.dispatch(&mut s);
            id
        };
        let lease = Lease {
            scheduler: self.clone(),
            id,
            completed: false,
        };
        rx.await
            .map_err(|_| Status::unavailable("DARC dispatch closed"))?;
        Ok(lease)
    }
    pub async fn run<T>(
        self: &Arc<Self>,
        kind: Option<usize>,
        future: impl Future<Output = T>,
    ) -> Result<T, Status> {
        let mut lease = self.acquire(kind).await?;
        let result = future.await;
        lease.completed = true;
        Ok(result)
    }
    fn release(&self, id: u64, completed: bool) {
        let mut s = self.state.lock().unwrap();
        for q in &mut s.queues {
            q.retain(|w| w.id != id);
        }
        if let Some(worker) = s
            .workers
            .iter()
            .position(|w| w.as_ref().is_some_and(|a| a.id == id))
        {
            let a = s.workers[worker].take().unwrap();
            if completed && a.kind < s.profiles.len() {
                s.samples[a.kind].0 += 1;
                s.samples[a.kind].1 += a.started.elapsed();
                s.completed += 1;
                if s.completed >= self.config.profile_window {
                    let mut updated = s.profiles.clone();
                    for (i, (count, time)) in s.samples.iter().enumerate() {
                        if *count > 0 {
                            updated[i] = Profile {
                                service: time.div_f64(*count as f64).max(Duration::from_nanos(1)),
                                occurrence: *count as f64 / s.completed as f64,
                            };
                        }
                    }
                    // Trigger profile changes on queue slowdown, as in §3. Preserve
                    // configured profiles for types absent in a sampling window.
                    if s.slow {
                        s.reservations = reservations(&self.config, &updated);
                        s.profiles = updated;
                    }
                    s.completed = 0;
                    s.samples.fill((0, Duration::ZERO));
                    s.slow = false;
                }
            }
        }
        self.dispatch(&mut s);
    }
}
/// Owns the worker reservation, including the handoff from queue to execution.
pub struct Lease {
    scheduler: Arc<Scheduler>,
    id: u64,
    completed: bool,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.scheduler.release(self.id, self.completed);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn scheduler() -> Arc<Scheduler> {
        Scheduler::new(
            Config {
                workers: 3,
                ..Config::default()
            },
            vec![
                Profile {
                    service: Duration::from_millis(1),
                    occurrence: 0.9,
                },
                Profile {
                    service: Duration::from_millis(100),
                    occurrence: 0.1,
                },
            ],
        )
    }
    #[tokio::test(start_paused = true)]
    async fn long_requests_cannot_borrow_short_reservations_and_cancel_cleans_handoff() {
        let s = scheduler();
        let long = s.acquire(Some(1)).await.unwrap();
        let spill = s.acquire(None).await.unwrap();
        let another = s.clone();
        let queued = tokio::spawn(async move { another.acquire(Some(1)).await });
        tokio::task::yield_now().await;
        assert_eq!(s.queued(), 1);
        let short = s.acquire(Some(0)).await.unwrap();
        assert_eq!(s.active(), 3);
        queued.abort();
        let _ = queued.await;
        assert_eq!(s.queued(), 0);
        drop(short);
        drop(spill);
        drop(long);
        assert_eq!(s.active(), 0);
        // A grant racing with cancellation must not leak its reserved worker.
        let a = s.acquire(Some(0)).await.unwrap();
        drop(a);
        assert_eq!(s.active(), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn slowdown_updates_profiles_and_granted_cancellation_releases_worker() {
        use std::task::Poll;
        let s = Scheduler::new(
            Config {
                workers: 2,
                profile_window: 1,
                ..Config::default()
            },
            vec![Profile {
                service: Duration::from_millis(1),
                occurrence: 1.0,
            }],
        );
        let short = s.acquire(Some(0)).await.unwrap();
        let spill = s.acquire(None).await.unwrap();
        let another = s.clone();
        let task = tokio::spawn(async move {
            another
                .run(Some(0), async {
                    tokio::time::sleep(Duration::from_millis(30)).await
                })
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(20)).await;
        drop(short);
        task.await.unwrap().unwrap();
        assert_eq!(s.profiles()[0].service, Duration::from_millis(30));
        let short = s.acquire(Some(0)).await.unwrap();
        let mut waiting = Box::pin(s.acquire(Some(0)));
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(short);
        assert_eq!(s.active(), 2);
        drop(waiting);
        assert_eq!(s.active(), 1);
        drop(spill);
        assert_eq!(s.active(), 0);
    }
    #[test]
    fn similar_types_share_and_many_types_get_spillway() {
        let c = Config {
            workers: 3,
            similarity: 1.5,
            ..Config::default()
        };
        let p = vec![
            Profile {
                service: Duration::from_micros(10),
                occurrence: 0.5,
            },
            Profile {
                service: Duration::from_micros(12),
                occurrence: 0.5,
            },
        ];
        let r = reservations(&c, &p);
        assert_eq!(r[0], r[1]);
        assert!(r[0].stealable.contains(&2));
        let p: Vec<_> = (0..10)
            .map(|i| Profile {
                service: Duration::from_micros(1 << i),
                occurrence: 0.1,
            })
            .collect();
        assert!(reservations(&c, &p).iter().all(|r| !r.reserved.is_empty()));
    }
}

/// RPC-layer classifier tag; applications decide which methods/payloads share a type.
#[derive(Clone, Copy)]
pub struct TypedCall(pub usize);
impl rpcstack_call::Ticket for TypedCall {
    fn decorate<T>(&mut self, request: &mut tonic::Request<T>) -> Result<(), Status> {
        request.metadata_mut().insert(
            "darc-type",
            self.0
                .to_string()
                .parse()
                .map_err(|_| Status::invalid_argument("invalid request type"))?,
        );
        Ok(())
    }
}
impl rpcstack_call::CallPolicy for TypedCall {
    type Ticket = Self;
    fn prepare<'a>(&'a self, _: &'a rpcstack_call::CallInfo) -> rpcstack_call::Prepare<'a, Self> {
        Box::pin(async move { Ok(*self) })
    }
}
pub fn request_type<T>(request: &tonic::Request<T>) -> Result<Option<usize>, Status> {
    request
        .metadata()
        .get("darc-type")
        .map(|s| {
            s.to_str()
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| Status::invalid_argument("invalid DARC type"))
        })
        .transpose()
}
