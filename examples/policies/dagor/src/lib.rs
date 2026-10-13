//! DAGOR (SoCC 2018), §4 and Algorithm 1, as an RPC module.
use rpcstack::{
    ChildOutcome, ChildState, Extensions, MissingDependency, Module, ModuleServer, Outcome,
    ServerInit, WireIn, WireOut,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tonic::{CowGrpcMethod, Request, Response, Status};

const LEVELS: usize = 128 * 128;
/// Lexicographic business/user priority, encoded as 1..=16384; zero rejects all.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Priority(u16);
impl Priority {
    pub fn new(business: u8, user: u8) -> Result<Self, Status> {
        if !(1..=128).contains(&business) || !(1..=128).contains(&user) {
            return Err(Status::invalid_argument("priorities must be 1..=128"));
        }
        Ok(Self((business as u16 - 1) * 128 + user as u16))
    }
    /// Hourly deterministic account hashing. This mixing function is an engineering
    /// choice; the paper specifies rotation/cohort semantics, not a hash function.
    pub fn for_account(business: u8, account: u64, hour: u64) -> Result<Self, Status> {
        let mut hash = account ^ hour.wrapping_mul(0x9e3779b97f4a7c15);
        hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d049bb133111eb);
        Self::new(business, ((hash ^ (hash >> 31)) % 128 + 1) as u8)
    }
    pub fn level(self) -> u16 {
        self.0
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DagorWire {
    Request(Priority),
    Feedback(u16),
}
#[derive(Clone, Debug)]
pub struct Config {
    pub queue_threshold: Duration,
    pub window: Duration,
    pub max_requests: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            queue_threshold: Duration::from_millis(20),
            window: Duration::from_secs(1),
            max_requests: 2000,
        }
    }
}
#[derive(Debug)]
pub struct Controller {
    config: Config,
    threshold: u16,
    incoming: u64,
    admitted: Vec<u64>,
    queue_ns: u128,
    samples: u64,
    since: Instant,
}
impl Controller {
    pub fn new(config: Config) -> Self {
        assert!(!config.window.is_zero() && config.max_requests > 0);
        Self {
            config,
            threshold: LEVELS as u16,
            incoming: 0,
            admitted: vec![0; LEVELS],
            queue_ns: 0,
            samples: 0,
            since: Instant::now(),
        }
    }
    pub fn threshold(&self) -> u16 {
        self.threshold
    }
    /// First runnable poll only: downstream wait is not counted as ingress load.
    pub fn admit(&mut self, priority: Priority, queued: Duration, now: Instant) -> bool {
        if now.duration_since(self.since) >= self.config.window && self.incoming > 0 {
            self.adjust(now);
        }
        let accepted = priority.0 > 0 && priority.0 <= self.threshold;
        self.incoming = self.incoming.saturating_add(1);
        if accepted {
            self.admitted[priority.0 as usize - 1] += 1;
        }
        self.queue_ns = self.queue_ns.saturating_add(queued.as_nanos());
        self.samples += 1;
        if self.incoming >= self.config.max_requests {
            self.adjust(now);
        }
        accepted
    }
    pub fn adjust(&mut self, now: Instant) {
        if self.incoming == 0 {
            self.since = now;
            return;
        }
        let overloaded =
            self.queue_ns / self.samples.max(1) as u128 > self.config.queue_threshold.as_nanos();
        // Literal Algorithm 1: N is all incoming requests, histogram is admitted
        // requests. Do not silently replace N with the number admitted.
        let expected = self.incoming as u128 * if overloaded { 95 } else { 101 } / 100;
        let mut prefix = 0_u128;
        let mut threshold = 0;
        for (index, count) in self.admitted.iter().enumerate() {
            prefix += *count as u128;
            if prefix > expected {
                break;
            }
            threshold = index as u16 + 1;
        }
        self.threshold = threshold;
        self.incoming = 0;
        self.admitted.fill(0);
        self.queue_ns = 0;
        self.samples = 0;
        self.since = now;
    }
}
#[derive(Clone, Debug, Default)]
pub struct PeerThresholds(Arc<Mutex<HashMap<CowGrpcMethod, u16>>>);
impl PeerThresholds {
    fn allow(&self, method: &CowGrpcMethod, priority: Priority) -> bool {
        priority.0
            <= *self
                .0
                .lock()
                .unwrap()
                .get(method)
                .unwrap_or(&(LEVELS as u16))
    }
    fn learn(&self, method: &CowGrpcMethod, wire: &WireIn<'_>) -> Result<(), Status> {
        if let Some(DagorWire::Feedback(level)) = wire.get::<DagorModule>().map_err(wire_error)? {
            if level > LEVELS as u16 {
                return Err(Status::data_loss("invalid DAGOR threshold"));
            }
            self.0.lock().unwrap().insert(method.clone(), level);
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct DagorServer {
    pub controller: Arc<Mutex<Controller>>,
    pub peers: PeerThresholds,
}
impl DagorServer {
    pub fn new(config: Config) -> Self {
        Self {
            controller: Arc::new(Mutex::new(Controller::new(config))),
            peers: PeerThresholds::default(),
        }
    }
}
impl Default for DagorServer {
    fn default() -> Self {
        Self::new(Config::default())
    }
}
impl ModuleServer for DagorServer {
    fn new(init: &mut ServerInit) -> Result<Self, MissingDependency> {
        Ok(init.get::<Self>().unwrap_or_default())
    }
}
/// Attach to a root Request's extensions. Clone the peers handle across calls.
#[derive(Clone, Debug)]
pub struct DagorCall {
    pub priority: Priority,
    pub peers: PeerThresholds,
}
impl DagorCall {
    pub fn for_account(business: u8, account: u64, peers: PeerThresholds) -> Result<Self, Status> {
        let hour = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            / 3600;
        Ok(Self {
            priority: Priority::for_account(business, account, hour)?,
            peers,
        })
    }
}
#[derive(Debug)]
pub struct DagorModule {
    priority: Result<Priority, Status>,
    server: DagorServer,
    first: AtomicBool,
}
fn wire_error(e: rpcstack::WireError) -> Status {
    Status::invalid_argument(e.to_string())
}
fn put(wire: &mut WireOut, priority: Priority) -> Result<(), Status> {
    wire.put::<DagorModule>(&DagorWire::Request(priority))
        .map_err(wire_error)
}
impl Module for DagorModule {
    type Server = DagorServer;
    type Wire = DagorWire;
    const NAME: &'static str = "dagor";
    fn new(_: &CowGrpcMethod, server: &DagorServer, wire: &WireIn<'_>, _: &mut Extensions) -> Self {
        let priority = match wire.get::<Self>() {
            Ok(Some(DagorWire::Request(p))) if p.0 > 0 && p.0 <= LEVELS as u16 => Ok(p),
            _ => Err(Status::invalid_argument(
                "missing/invalid DAGOR request priority",
            )),
        };
        Self {
            priority,
            server: server.clone(),
            first: AtomicBool::new(true),
        }
    }
    fn before_root_rpc<T>(
        method: &CowGrpcMethod,
        request: &mut Request<T>,
        wire: &mut WireOut,
        state: &mut ChildState,
    ) -> Result<(), Status> {
        let call = request
            .extensions()
            .get::<DagorCall>()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("attach DagorCall to root request"))?;
        state.insert(call.clone());
        if !call.peers.allow(method, call.priority) {
            return Err(Status::resource_exhausted("DAGOR downstream threshold"));
        }
        put(wire, call.priority)
    }
    fn after_root_rpc<T>(
        method: &CowGrpcMethod,
        outcome: ChildOutcome<'_, T>,
        wire: &WireIn<'_>,
        state: &ChildState,
    ) -> Result<(), Status> {
        if outcome.sent().is_some() {
            if let Some(call) = state.get::<DagorCall>() {
                call.peers.learn(method, wire)?;
            }
        }
        Ok(())
    }
    fn before_poll<T>(&self, _: &mut Extensions) -> Result<(), Result<Response<T>, Status>> {
        let priority = self.priority.clone().map_err(Err)?;
        if self.first.swap(false, Ordering::Relaxed) {
            let queued = tokio::task::obtain_task_queue_latency();
            if !self
                .server
                .controller
                .lock()
                .unwrap()
                .admit(priority, queued, Instant::now())
            {
                return Err(Err(Status::resource_exhausted("DAGOR admission threshold")));
            }
        }
        Ok(())
    }
    fn before_child_rpc<T>(
        &self,
        method: &CowGrpcMethod,
        _: &mut ChildState,
        _: &mut Request<T>,
        wire: &mut WireOut,
        _: &mut Extensions,
    ) -> Result<(), Status> {
        let priority = self.priority.clone()?;
        if !self.server.peers.allow(method, priority) {
            return Err(Status::resource_exhausted("DAGOR downstream threshold"));
        }
        put(wire, priority)
    }
    fn after_child_rpc<T>(
        &self,
        method: &CowGrpcMethod,
        outcome: ChildOutcome<'_, T>,
        wire: &WireIn<'_>,
        _: &ChildState,
        _: &Extensions,
    ) -> Result<(), Status> {
        if outcome.sent().is_some() {
            self.server.peers.learn(method, wire)?;
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
        wire.put::<Self>(&DagorWire::Feedback(
            self.server.controller.lock().unwrap().threshold(),
        ))
        .expect("scalar wire");
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histogram_matches_algorithm_and_recovers() {
        let now = Instant::now();
        let mut c = Controller::new(Config {
            max_requests: 100,
            ..Config::default()
        });
        for i in 1..=100 {
            assert!(c.admit(Priority(i), Duration::from_millis(21), now));
        }
        assert_eq!(c.threshold(), 95);
        assert!(!c.admit(Priority(100), Duration::ZERO, now));
        c.adjust(now);
        assert_eq!(c.threshold(), LEVELS as u16);
    }
    #[test]
    fn cohorts_are_consistent_and_business_dominates() {
        let p = Priority::for_account(1, 42, 7).unwrap();
        assert_eq!(p, Priority::for_account(1, 42, 7).unwrap());
        assert!(p < Priority::new(2, 1).unwrap());
        assert!(Priority::new(0, 1).is_err());
        assert!((8..24).any(|hour| Priority::for_account(1, 42, hour).unwrap() != p));
    }
    #[test]
    fn rejection_feedback_is_method_scoped() {
        let peers = PeerThresholds::default();
        let mut out = WireOut::new();
        out.put::<DagorModule>(&DagorWire::Feedback(1)).unwrap();
        let mut md = tonic::metadata::MetadataMap::new();
        out.install(&mut md);
        let a = CowGrpcMethod::new("service", "a");
        let b = CowGrpcMethod::new("service", "b");
        peers
            .learn(&a, &WireIn::from_metadata(&md).unwrap())
            .unwrap();
        assert!(!peers.allow(&a, Priority(2)));
        assert!(peers.allow(&b, Priority(2)));
    }
}
