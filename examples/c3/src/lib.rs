//! C3 replica ranking and distributed rate control (NSDI 2015).
use rpcstack::{Extensions, Module, Outcome, WireIn, WireOut};
use rpcstack_call::{CallInfo, CallPolicy, Prepare, StageSample, Ticket};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tonic::{CowGrpcMethod, Request, Response, Status};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Feedback {
    pub service_ns: u64,
    pub queue_after: u64,
}
#[derive(Debug)]
pub struct C3Module;
impl Module for C3Module {
    type Server = ();
    type Wire = Feedback;
    const NAME: &'static str = "c3";
    const POLL_HOOKS: bool = false;
    fn new(_: &CowGrpcMethod, _: &(), _: &WireIn<'_>, _: &mut Extensions) -> Self {
        Self
    }
    fn finalize<T>(
        &self,
        response: &mut Result<Response<T>, Status>,
        _: Outcome<'_>,
        wire: &mut WireOut,
        _: &Extensions,
    ) {
        if let Ok(response) = response {
            if let Some(sample) = response.extensions().get::<StageSample>() {
                wire.put::<Self>(&Feedback {
                    service_ns: sample.serviced_for.as_nanos().min(u64::MAX as u128) as u64,
                    queue_after: sample.waiting_after as u64,
                })
                .expect("scalar feedback");
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct Config {
    pub clients: usize,
    pub rate_interval: Duration,
    pub initial_rate: f64,
    pub max_tokens: f64,
    pub cubic_c: f64,
    pub beta: f64,
    pub max_increase: f64,
    pub hysteresis: f64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            clients: 1,
            rate_interval: Duration::from_millis(10),
            initial_rate: 5.0,
            max_tokens: 10.0,
            cubic_c: 0.000004,
            beta: 0.2,
            max_increase: 10.0,
            hysteresis: 2.0,
        }
    }
}
#[derive(Clone, Debug)]
struct Replica {
    pending: usize,
    metrics: Option<(f64, f64, f64)>,
    rate: f64,
    tokens: f64,
    last_refill: tokio::time::Instant,
    received: u64,
    receive_rate: f64,
    receive_since: tokio::time::Instant,
    last_increase: tokio::time::Instant,
    last_decrease: tokio::time::Instant,
    last_max: f64,
}
impl Replica {
    fn score(&self, clients: usize) -> f64 {
        match self.metrics {
            None => 0.0,
            Some((rtt, service, queue)) => {
                (rtt - service).max(0.0)
                    + (1.0 + self.pending as f64 * clients as f64 + queue).powi(3) * service
            }
        }
    }
    fn refill(&mut self, now: tokio::time::Instant, c: &Config) {
        self.tokens = (self.tokens
            + self.rate * now.duration_since(self.last_refill).as_secs_f64()
                / c.rate_interval.as_secs_f64())
        .min(c.max_tokens);
        self.last_refill = now;
    }
    fn feedback(
        &mut self,
        rtt: Duration,
        feedback: Feedback,
        now: tokio::time::Instant,
        c: &Config,
    ) {
        self.refill(now, c);
        let sample = (
            rtt.as_secs_f64(),
            feedback.service_ns as f64 / 1e9,
            feedback.queue_after as f64,
        );
        self.metrics = Some(match self.metrics {
            None => sample,
            Some(old) => (
                0.9 * sample.0 + 0.1 * old.0,
                0.9 * sample.1 + 0.1 * old.1,
                0.9 * sample.2 + 0.1 * old.2,
            ),
        });
        self.received += 1;
        let intervals =
            now.duration_since(self.receive_since).as_secs_f64() / c.rate_interval.as_secs_f64();
        if intervals >= 1.0 {
            self.receive_rate = 0.9 * (self.received as f64 / intervals) + 0.1 * self.receive_rate;
            self.received = 0;
            self.receive_since = now;
        }
        if self.rate < self.receive_rate {
            let elapsed_ms = now.duration_since(self.last_decrease).as_secs_f64() * 1000.0;
            let cubic = c.cubic_c
                * (elapsed_ms - (self.last_max * c.beta / c.cubic_c).cbrt()).powi(3)
                + self.last_max;
            self.rate = cubic.min(self.rate + c.max_increase).max(0.0001);
            self.last_increase = now;
        } else if self.rate > self.receive_rate
            && now.duration_since(self.last_increase).as_secs_f64()
                > c.rate_interval.as_secs_f64() * c.hysteresis
        {
            self.last_max = self.rate;
            self.rate = (self.rate * c.beta).max(0.0001);
            self.last_decrease = now;
        }
    }
}
#[derive(Clone, Debug)]
pub struct Selector {
    config: Config,
    replicas: Arc<Mutex<BTreeMap<String, Replica>>>,
}
impl Selector {
    pub fn new(config: Config, endpoints: impl IntoIterator<Item = String>) -> Self {
        assert!(config.clients > 0 && !config.rate_interval.is_zero());
        for value in [
            config.initial_rate,
            config.max_tokens,
            config.cubic_c,
            config.max_increase,
            config.hysteresis,
        ] {
            assert!(value.is_finite() && value > 0.0);
        }
        assert!(config.max_tokens >= 1.0 && config.beta > 0.0 && config.beta < 1.0);
        let now = tokio::time::Instant::now();
        let replicas = endpoints
            .into_iter()
            .map(|e| {
                (
                    e,
                    Replica {
                        pending: 0,
                        metrics: None,
                        rate: config.initial_rate,
                        tokens: 0.0,
                        last_refill: now,
                        received: 0,
                        receive_rate: config.initial_rate * 2.0,
                        receive_since: now,
                        last_increase: now,
                        last_decrease: now,
                        last_max: config.initial_rate,
                    },
                )
            })
            .collect();
        Self {
            config,
            replicas: Arc::new(Mutex::new(replicas)),
        }
    }
    /// Await a token from the best-ranked eligible replica. Candidate lists must
    /// contain only replicas that can safely serve this particular request.
    pub async fn select(&self, candidates: &[String]) -> Result<Selection, Status> {
        if candidates.is_empty() {
            return Err(Status::invalid_argument("empty replica set"));
        }
        loop {
            let wait = {
                let mut peers = self.replicas.lock().unwrap();
                let now = tokio::time::Instant::now();
                for id in candidates {
                    if !peers.contains_key(id) {
                        return Err(Status::invalid_argument("unknown replica"));
                    }
                }
                let mut ranked = candidates.to_vec();
                use rand::seq::SliceRandom;
                ranked.shuffle(&mut rand::thread_rng());
                ranked.sort_by(|a, b| {
                    peers[a]
                        .score(self.config.clients)
                        .total_cmp(&peers[b].score(self.config.clients))
                });
                let mut minimum = f64::INFINITY;
                for id in ranked {
                    let p = peers.get_mut(&id).unwrap();
                    p.refill(now, &self.config);
                    if p.tokens >= 1.0 {
                        p.tokens -= 1.0;
                        p.pending += 1;
                        return Ok(Selection {
                            selector: self.clone(),
                            endpoint: id,
                            started: None,
                            finished: false,
                        });
                    }
                    minimum = minimum
                        .min((1.0 - p.tokens) * self.config.rate_interval.as_secs_f64() / p.rate);
                }
                Duration::from_secs_f64(minimum.max(0.000001))
            };
            tokio::time::sleep(wait).await;
        }
    }
    /// Optional shadow READS from the author implementation: with this probability,
    /// reserve one attempt on every other eligible replica. Caller must execute
    /// and await these reservations (e.g. join with foreground), discard their
    /// values, and ensure the operation is read-only. These bypass pacing but
    /// debit tokens, so subsequent foreground traffic pays for the extra load.
    pub fn shadow_reads(
        &self,
        primary: &str,
        candidates: &[String],
        probability: f64,
    ) -> Result<Vec<Selection>, Status> {
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            return Err(Status::invalid_argument("invalid shadow probability"));
        }
        let mut peers = self.replicas.lock().unwrap();
        let ids: std::collections::BTreeSet<_> = candidates
            .iter()
            .filter(|id| id.as_str() != primary)
            .collect();
        if !peers.contains_key(primary) || ids.iter().any(|id| !peers.contains_key(id.as_str())) {
            return Err(Status::invalid_argument("unknown replica"));
        }
        if rand::random::<f64>() >= probability {
            return Ok(Vec::new());
        }
        let now = tokio::time::Instant::now();
        Ok(ids
            .into_iter()
            .map(|id| {
                let p = peers.get_mut(id).unwrap();
                p.refill(now, &self.config);
                p.tokens -= 1.0;
                p.pending += 1;
                Selection {
                    selector: self.clone(),
                    endpoint: id.clone(),
                    started: None,
                    finished: false,
                }
            })
            .collect())
    }
    pub fn pending(&self, endpoint: &str) -> usize {
        self.replicas.lock().unwrap()[endpoint].pending
    }
    pub fn score(&self, endpoint: &str) -> f64 {
        self.replicas.lock().unwrap()[endpoint].score(self.config.clients)
    }
}
/// A reservation is acquired before choosing the transport. Moving it into
/// `SelectedCall` lets rpcstack-call manage its attempt and cancellation lifetime.
pub struct Selection {
    selector: Selector,
    endpoint: String,
    started: Option<tokio::time::Instant>,
    finished: bool,
}
impl Selection {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}
impl Drop for Selection {
    fn drop(&mut self) {
        let mut peers = self.selector.replicas.lock().unwrap();
        let p = peers.get_mut(&self.endpoint).unwrap();
        p.pending -= 1;
        if self.started.is_none() {
            p.tokens = (p.tokens + 1.0).min(self.selector.config.max_tokens);
        }
    }
}
impl Ticket for Selection {
    fn decorate<T>(&mut self, _: &mut Request<T>) -> Result<(), Status> {
        self.started = Some(tokio::time::Instant::now());
        Ok(())
    }
    fn finish<T>(&mut self, result: &Result<Response<T>, Status>) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let (Some(start), Ok(response)) = (self.started, result) {
            if let Ok(wire) = WireIn::from_metadata(response.metadata()) {
                if let Ok(Some(feedback)) = wire.get::<C3Module>() {
                    self.selector
                        .replicas
                        .lock()
                        .unwrap()
                        .get_mut(&self.endpoint)
                        .unwrap()
                        .feedback(
                            start.elapsed(),
                            feedback,
                            tokio::time::Instant::now(),
                            &self.selector.config,
                        );
                }
            }
        }
    }
}
pub struct SelectedCall(Mutex<Option<Selection>>);
impl SelectedCall {
    pub fn new(selection: Selection) -> Self {
        Self(Mutex::new(Some(selection)))
    }
}
impl CallPolicy for SelectedCall {
    type Ticket = Selection;
    fn prepare<'a>(&'a self, info: &'a CallInfo) -> Prepare<'a, Selection> {
        Box::pin(async move {
            let mut slot = self.0.lock().unwrap();
            if slot
                .as_ref()
                .is_some_and(|s| s.endpoint != info.destination)
            {
                return Err(Status::invalid_argument(
                    "selected replica differs from transport destination",
                ));
            }
            slot.take()
                .ok_or_else(|| Status::failed_precondition("selection already used"))
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn rank_compensates_for_pending_and_cancellation_releases() {
        let s = Selector::new(Config::default(), ["a".into(), "b".into()]);
        {
            let mut p = s.replicas.lock().unwrap();
            p.get_mut("a").unwrap().metrics = Some((0.002, 0.001, 0.0));
            p.get_mut("b").unwrap().metrics = Some((0.010, 0.005, 0.0));
        }
        let a = s.select(&["a".into(), "b".into()]).await.unwrap();
        assert_eq!(a.endpoint, "a");
        assert_eq!(s.pending("a"), 1);
        assert!((s.score("a") - 0.009).abs() < 1e-9);
        drop(a);
        assert_eq!(s.pending("a"), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn no_replica_is_sent_without_a_rate_token() {
        let s = Selector::new(Config::default(), ["a".into()]);
        let start = tokio::time::Instant::now();
        let a = s.select(&["a".into()]).await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(2));
        drop(a);
        assert!(s.select(&[]).await.is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn cubic_feedback_decreases_after_hysteresis() {
        let s = Selector::new(Config::default(), ["a".into()]);
        tokio::time::advance(Duration::from_millis(50)).await;
        let mut peers = s.replicas.lock().unwrap();
        let p = peers.get_mut("a").unwrap();
        p.receive_rate = 0.0;
        p.feedback(
            Duration::from_millis(10),
            Feedback {
                service_ns: 1_000_000,
                queue_after: 3,
            },
            tokio::time::Instant::now(),
            &s.config,
        );
        assert_eq!(p.rate, 1.0);
        assert!(p.metrics.is_some());
    }
}

#[cfg(test)]
mod shadow_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn shadow_reads_refresh_alternates_and_account_for_load() {
        let s = Selector::new(Config::default(), ["a".into(), "b".into()]);
        let reads = s
            .shadow_reads("a", &["a".into(), "b".into(), "b".into()], 1.0)
            .unwrap();
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].endpoint, "b");
        assert_eq!(s.pending("b"), 1);
        assert_eq!(s.replicas.lock().unwrap()["b"].tokens, -1.0);
        drop(reads);
        assert_eq!(s.pending("b"), 0);
        assert_eq!(s.replicas.lock().unwrap()["b"].tokens, 0.0);
        assert!(s.shadow_reads("a", &["b".into()], 0.0).unwrap().is_empty());
    }
}

#[cfg(test)]
mod rate_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn cubic_increase_is_capped_and_does_not_reprice_elapsed_tokens() {
        let s = Selector::new(Config::default(), ["a".into()]);
        tokio::time::advance(Duration::from_secs(1)).await;
        let mut peers = s.replicas.lock().unwrap();
        let p = peers.get_mut("a").unwrap();
        p.receive_rate = 1000.0;
        p.feedback(
            Duration::from_millis(1),
            Feedback {
                service_ns: 500_000,
                queue_after: 0,
            },
            tokio::time::Instant::now(),
            &s.config,
        );
        assert_eq!(p.rate, 15.0);
        assert_eq!(p.tokens, 10.0);
    }
}
