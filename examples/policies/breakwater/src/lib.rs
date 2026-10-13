//! Breakwater's credit controller, demand speculation and delay AQM (OSDI 2020).
use rpcstack::{
    Extensions, MissingDependency, Module, ModuleServer, Outcome, ServerInit, WireIn, WireOut,
};
use rpcstack_call::{CallInfo, CallPolicy, Prepare, Ticket};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tonic::{CowGrpcMethod, Request, Response, Status};
pub mod pb {
    tonic::include_proto!("credit");
}

#[derive(Clone, Debug)]
pub struct Config {
    pub target: Duration,
    pub aqm: Duration,
    pub rtt: Duration,
    pub alpha: f64,
    pub beta: f64,
    pub initial: u64,
    pub maximum: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            target: Duration::from_micros(80),
            aqm: Duration::from_micros(160),
            rtt: Duration::from_millis(1),
            alpha: 0.001,
            beta: 0.02,
            initial: 8,
            maximum: 65536,
        }
    }
}
#[derive(Debug)]
struct Peer {
    connected: bool,
    incarnation: u64,
    demand: u64,
    inflight: u64,
    window: u64,
    generation: u64,
    updates: tokio::sync::watch::Sender<pb::Grant>,
}
#[derive(Debug)]
struct State {
    total: u64,
    next_incarnation: u64,
    peers: BTreeMap<u64, Peer>,
}
#[derive(Debug)]
pub struct Controller {
    config: Config,
    state: Mutex<State>,
    queue_us: AtomicU64,
    started: AtomicBool,
}
impl Controller {
    pub fn new(config: Config) -> Arc<Self> {
        assert!(!config.target.is_zero() && !config.rtt.is_zero() && config.aqm >= config.target);
        assert!(
            config.alpha.is_finite()
                && config.alpha >= 0.0
                && config.beta.is_finite()
                && config.beta >= 0.0
        );
        assert!(config.initial > 0 && config.initial <= config.maximum);
        Arc::new(Self {
            state: Mutex::new(State {
                total: config.initial,
                next_incarnation: 1,
                peers: BTreeMap::new(),
            }),
            config,
            queue_us: AtomicU64::new(0),
            started: AtomicBool::new(false),
        })
    }
    /// Reuse the credit distribution/control channel with an external controller.
    /// Do not call `start`/`tick` when another policy owns the pool size.
    /// Shrinking preserves credits occupied by executing calls until completion.
    pub fn set_total(&self, total: u64) {
        let mut state = self.state.lock().unwrap();
        state.total = total.clamp(1, self.config.maximum);
        Self::allocate(&mut state, None);
    }
    pub fn total(&self) -> u64 {
        self.state.lock().unwrap().total
    }
    pub fn observe_queue(&self, delay: Duration) {
        self.queue_us.fetch_max(
            delay.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
    pub fn tick(&self) {
        let queue = self.queue_us.swap(0, Ordering::Relaxed) as f64;
        let mut s = self.state.lock().unwrap();
        let n = s.peers.values().filter(|p| p.connected).count() as f64;
        let target = self.config.target.as_secs_f64() * 1e6;
        s.total = if queue < target {
            s.total
                .saturating_add((self.config.alpha * n).max(1.0) as u64)
                .min(self.config.maximum)
        } else {
            ((s.total as f64) * (1.0 - self.config.beta * (queue - target) / target).max(0.5))
                as u64
        }
        .max(1);
        Self::allocate(&mut s, None);
    }
    fn allocate(s: &mut State, preferred: Option<u64>) {
        let used: u64 = s.peers.values().map(|p| p.inflight).sum();
        let issued: u64 = s.peers.values().map(|p| p.window).sum();
        let extra = (s.total.saturating_sub(issued) / (s.peers.len().max(1) as u64)).max(1);
        let mut remaining = s.total.saturating_sub(used);
        // Give demand priority, then speculative credits. Rotate order to avoid
        // persistent bias; prefer a responding peer for piggyback delivery.
        let mut ids: Vec<_> = s
            .peers
            .iter()
            .filter(|(_, p)| p.connected)
            .map(|(id, _)| *id)
            .collect();
        use rand::seq::SliceRandom;
        ids.shuffle(&mut rand::thread_rng());
        if let Some(id) = preferred {
            if let Some(i) = ids.iter().position(|x| *x == id) {
                ids.swap(0, i);
            }
        }
        let old: BTreeMap<_, _> = s.peers.iter().map(|(id, p)| (*id, p.window)).collect();
        for p in s.peers.values_mut() {
            p.window = p.inflight;
        }
        for speculative in [false, true] {
            for id in &ids {
                let p = s.peers.get_mut(id).unwrap();
                let want = if speculative { extra } else { p.demand };
                let grant = want.min(remaining);
                p.window = p.window.saturating_add(grant);
                remaining -= grant;
            }
        }
        for (id, p) in &mut s.peers {
            if old[id] != p.window {
                p.generation = p.generation.saturating_add(1);
                p.updates.send_replace(pb::Grant {
                    incarnation: p.incarnation,
                    generation: p.generation,
                    window: p.window,
                });
            }
        }
    }
    fn register(self: &Arc<Self>, id: u64) -> Result<Subscription, Status> {
        let mut s = self.state.lock().unwrap();
        if s.peers.contains_key(&id) {
            return Err(Status::already_exists("client already connected"));
        }
        let incarnation = s.next_incarnation;
        s.next_incarnation = incarnation
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("subscription identities exhausted"))?;
        let (tx, rx) = tokio::sync::watch::channel(pb::Grant {
            incarnation,
            generation: 0,
            window: 0,
        });
        s.peers.insert(
            id,
            Peer {
                incarnation,
                connected: true,
                demand: 0,
                inflight: 0,
                window: 0,
                generation: 0,
                updates: tx,
            },
        );
        Self::allocate(&mut s, None);
        Ok(Subscription {
            controller: self.clone(),
            id,
            receiver: rx,
        })
    }
    fn enter(&self, id: u64, incarnation: u64, demand: u64) -> Result<(), Status> {
        let mut s = self.state.lock().unwrap();
        let p = s
            .peers
            .get_mut(&id)
            .ok_or_else(|| Status::failed_precondition("open credit subscription first"))?;
        if !p.connected || p.incarnation != incarnation {
            return Err(Status::failed_precondition("stale credit subscription"));
        }
        p.demand = demand;
        if p.inflight >= p.window {
            return Err(Status::resource_exhausted("no server credit"));
        }
        p.inflight += 1;
        Ok(())
    }
    fn finish(&self, id: u64, incarnation: u64) -> Option<pb::Grant> {
        let mut s = self.state.lock().unwrap();
        let p = s.peers.get_mut(&id)?;
        if p.incarnation != incarnation {
            return None;
        }
        p.inflight = p.inflight.saturating_sub(1);
        if !p.connected && p.inflight == 0 {
            s.peers.remove(&id);
            Self::allocate(&mut s, None);
            return None;
        }
        Self::allocate(&mut s, Some(id));
        let p = s.peers.get(&id)?;
        Some(pb::Grant {
            incarnation: p.incarnation,
            generation: p.generation,
            window: p.window,
        })
    }
    pub fn start(self: &Arc<Self>) {
        if self.started.swap(true, Ordering::Relaxed) {
            return;
        }
        let weak = Arc::downgrade(self);
        let period = self.config.rtt;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                let Some(c) = weak.upgrade() else { break };
                c.tick();
            }
        });
    }
}
struct Subscription {
    controller: Arc<Controller>,
    id: u64,
    receiver: tokio::sync::watch::Receiver<pb::Grant>,
}
impl Drop for Subscription {
    fn drop(&mut self) {
        let mut s = self.controller.state.lock().unwrap();
        if let Some(p) = s.peers.get_mut(&self.id) {
            p.connected = false;
            if p.inflight == 0 {
                s.peers.remove(&self.id);
            }
        }
        Controller::allocate(&mut s, None);
    }
}
/// Control traffic has no application-policy hooks: it must work with zero credit.
#[derive(Clone)]
pub struct CreditService(pub Arc<Controller>);
#[tonic::async_trait]
impl pb::credits_server::Credits for CreditService {
    type SubscribeStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<pb::Grant, Status>> + Send>>;
    async fn subscribe(
        &self,
        request: Request<pb::Hello>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut subscription = self.0.register(request.into_inner().client)?;
        let stream = async_stream::try_stream! {
            let initial=subscription.receiver.borrow_and_update().clone();yield initial;
            while subscription.receiver.changed().await.is_ok(){let grant=subscription.receiver.borrow_and_update().clone();yield grant;}
        };
        Ok(Response::new(Box::pin(stream)))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CreditWire {
    Request {
        client: u64,
        incarnation: u64,
        demand: u64,
    },
    Grant {
        generation: u64,
        window: u64,
        incarnation: u64,
    },
}
#[derive(Clone, Debug)]
pub struct BreakwaterServer(pub Arc<Controller>);
impl ModuleServer for BreakwaterServer {
    fn new(init: &mut ServerInit) -> Result<Self, MissingDependency> {
        init.require::<Self>()
    }
}
#[derive(Debug)]
pub struct BreakwaterModule {
    server: Arc<Controller>,
    client: Result<(u64, u64, u64), Status>,
    entered: AtomicBool,
    first: AtomicBool,
}
impl Drop for BreakwaterModule {
    fn drop(&mut self) {
        if self.entered.swap(false, Ordering::Relaxed) {
            if let Ok((id, incarnation, _)) = self.client {
                self.server.finish(id, incarnation);
            }
        }
    }
}
impl Module for BreakwaterModule {
    type Server = BreakwaterServer;
    type Wire = CreditWire;
    const NAME: &'static str = "breakwater";
    fn new(
        _: &CowGrpcMethod,
        server: &Self::Server,
        wire: &WireIn<'_>,
        _: &mut Extensions,
    ) -> Self {
        let client = match wire.get::<Self>() {
            Ok(Some(CreditWire::Request {
                client,
                incarnation,
                demand,
            })) => Ok((client, incarnation, demand)),
            _ => Err(Status::invalid_argument("missing credit client identity")),
        };
        Self {
            server: server.0.clone(),
            client,
            entered: AtomicBool::new(false),
            first: AtomicBool::new(true),
        }
    }
    fn before_poll<T>(&self, _: &mut Extensions) -> Result<(), Result<Response<T>, Status>> {
        let (id, incarnation, demand) = self.client.clone().map_err(Err)?;
        if self.first.swap(false, Ordering::Relaxed) {
            let delay = tokio::task::obtain_task_queue_latency();
            self.server.observe_queue(delay);
            if delay >= self.server.config.aqm {
                return Err(Err(Status::resource_exhausted("Breakwater delay AQM")));
            }
            self.server.enter(id, incarnation, demand).map_err(Err)?;
            self.entered.store(true, Ordering::Relaxed);
        }
        Ok(())
    }
    fn finalize<T>(
        &self,
        _: &mut Result<Response<T>, Status>,
        _: Outcome<'_>,
        wire: &mut WireOut,
        _: &Extensions,
    ) {
        if self.entered.swap(false, Ordering::Relaxed) {
            if let Ok((id, incarnation, _)) = self.client {
                if let Some(g) = self.server.finish(id, incarnation) {
                    wire.put::<Self>(&CreditWire::Grant {
                        incarnation: g.incarnation,
                        generation: g.generation,
                        window: g.window,
                    })
                    .expect("scalar credit wire");
                }
            }
        }
    }
}
#[derive(Debug)]
struct ClientState {
    grant: Mutex<pb::Grant>,
    inflight: AtomicUsize,
    waiting: AtomicUsize,
    notify: tokio::sync::Notify,
    closed: AtomicBool,
}
impl ClientState {
    fn update(&self, g: pb::Grant) {
        let mut current = self.grant.lock().unwrap();
        if (current.incarnation == 0 || current.incarnation == g.incarnation)
            && g.generation >= current.generation
        {
            *current = g;
            self.notify.notify_waiters();
        }
    }
}
#[derive(Clone)]
pub struct CreditClient {
    pub id: u64,
    state: Arc<ClientState>,
    worker: Arc<Worker>,
}
struct Worker(tokio::task::JoinHandle<()>);
impl Drop for Worker {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl CreditClient {
    pub async fn connect(endpoint: String, id: u64) -> Result<Self, Status> {
        let mut client = pb::credits_client::CreditsClient::connect(endpoint)
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let mut stream = client
            .subscribe(pb::Hello { client: id })
            .await?
            .into_inner();
        let state = Arc::new(ClientState {
            grant: Mutex::new(pb::Grant {
                incarnation: 0,
                generation: 0,
                window: 0,
            }),
            inflight: 0.into(),
            waiting: 0.into(),
            notify: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&state);
        let worker = tokio::spawn(async move {
            while let Ok(Some(g)) = stream.message().await {
                let Some(s) = weak.upgrade() else { return };
                s.update(g);
            }
            if let Some(s) = weak.upgrade() {
                s.closed.store(true, Ordering::Relaxed);
                s.notify.notify_waiters();
            }
        });
        Ok(Self {
            id,
            state,
            worker: Arc::new(Worker(worker)),
        })
    }
    pub fn inflight(&self) -> usize {
        self.state.inflight.load(Ordering::Relaxed)
    }
}
pub struct CreditTicket {
    client: CreditClient,
}
impl Drop for CreditTicket {
    fn drop(&mut self) {
        self.client.state.inflight.fetch_sub(1, Ordering::Relaxed);
        self.client.state.notify.notify_waiters();
    }
}
impl Ticket for CreditTicket {
    fn decorate<T>(&mut self, request: &mut Request<T>) -> Result<(), Status> {
        let mut wire = WireOut::from_metadata(request.metadata())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        wire.put::<BreakwaterModule>(&CreditWire::Request {
            client: self.client.id,
            incarnation: self.client.state.grant.lock().unwrap().incarnation,
            demand: self.client.state.waiting.load(Ordering::Relaxed) as u64,
        })
        .map_err(|e| Status::internal(e.to_string()))?;
        wire.install(request.metadata_mut());
        Ok(())
    }
    fn finish<T>(&mut self, result: &Result<Response<T>, Status>) {
        let md = match result {
            Ok(r) => r.metadata(),
            Err(s) => s.metadata(),
        };
        if let Ok(wire) = WireIn::from_metadata(md) {
            if let Ok(Some(CreditWire::Grant {
                generation,
                window,
                incarnation,
            })) = wire.get::<BreakwaterModule>()
            {
                self.client.state.update(pb::Grant {
                    generation,
                    window,
                    incarnation,
                });
            }
        }
    }
}
impl CallPolicy for CreditClient {
    type Ticket = CreditTicket;
    fn prepare<'a>(&'a self, _: &'a CallInfo) -> Prepare<'a, CreditTicket> {
        Box::pin(async move {
            struct Waiting<'a>(&'a AtomicUsize);
            impl Drop for Waiting<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::Relaxed);
                }
            }
            self.state.waiting.fetch_add(1, Ordering::Relaxed);
            let _waiting = Waiting(&self.state.waiting);
            let _keep_worker = &self.worker;
            loop {
                let notified = self.state.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.state.closed.load(Ordering::Relaxed) {
                    return Err(Status::unavailable("credit stream closed"));
                }
                let window = self.state.grant.lock().unwrap().window;
                if self
                    .state
                    .inflight
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                        ((used as u64) < window).then_some(used + 1)
                    })
                    .is_ok()
                {
                    return Ok(CreditTicket {
                        client: self.clone(),
                    });
                }
                notified.await;
            }
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pool_growth_halving_and_revocation() {
        let c = Controller::new(Config {
            initial: 100,
            beta: 1.0,
            alpha: 1.0,
            ..Config::default()
        });
        let a = c.register(1).unwrap();
        let _b = c.register(2).unwrap();
        c.tick();
        assert_eq!(c.total(), 102);
        c.observe_queue(Duration::from_secs(1));
        c.tick();
        assert_eq!(c.total(), 51);
        let state = c.state.lock().unwrap();
        assert!(state.peers.values().map(|p| p.window).sum::<u64>() <= 51);
        drop(state);
        drop(a);
        assert_eq!(c.state.lock().unwrap().peers.len(), 1);
    }
    #[test]
    fn consumed_credit_is_not_revoked_and_stale_updates_ignored() {
        let c = Controller::new(Config {
            initial: 1,
            ..Config::default()
        });
        let _subscription = c.register(1).unwrap();
        assert!(c.enter(1, 1, 0).is_ok());
        assert!(c.enter(1, 1, 0).is_err());
        c.observe_queue(Duration::from_secs(1));
        c.tick();
        assert_eq!(c.state.lock().unwrap().peers[&1].inflight, 1);
        c.finish(1, 1);
        assert!(c.enter(1, 1, 0).is_ok());
        let state = ClientState {
            grant: Mutex::new(pb::Grant {
                incarnation: 0,
                generation: 2,
                window: 1,
            }),
            inflight: 0.into(),
            waiting: 0.into(),
            notify: tokio::sync::Notify::new(),
            closed: false.into(),
        };
        state.update(pb::Grant {
            incarnation: 0,
            generation: 1,
            window: 100,
        });
        assert_eq!(state.grant.lock().unwrap().window, 1);
    }
}

#[cfg(test)]
mod disconnect_tests {
    use super::*;
    #[test]
    fn disconnected_inflight_calls_hold_credit_and_old_incarnations_cannot_reenter() {
        let c = Controller::new(Config {
            initial: 1,
            maximum: 1,
            ..Config::default()
        });
        let a = c.register(1).unwrap();
        assert!(c.enter(1, 1, 0).is_ok());
        drop(a);
        let _b = c.register(2).unwrap();
        {
            let state = c.state.lock().unwrap();
            assert_eq!(state.peers[&1].window, 1);
            assert_eq!(state.peers[&2].window, 0);
        }
        assert!(c.register(1).is_err());
        assert!(c.enter(1, 1, 0).is_err());
        c.finish(1, 1);
        assert_eq!(c.state.lock().unwrap().peers[&2].window, 1);
        let _a = c.register(1).unwrap();
        assert!(c.enter(1, 1, 0).is_err());
        assert!(c.finish(1, 1).is_none());
        assert_eq!(c.state.lock().unwrap().peers[&1].incarnation, 3);
    }
}
