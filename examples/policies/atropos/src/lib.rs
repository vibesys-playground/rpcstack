//! Atropos (SOSP 2025): resource attribution and targeted cooperative cancellation.
use rpcstack_call::cancellation::Cancellation;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use tonic::Status;
#[derive(Clone, Copy, Debug)]
pub enum ResourceKind {
    Memory,
    Synchronization,
    Queue,
}
#[derive(Clone, Debug)]
struct Resource {
    kind: ResourceKind,
    contention: f64,
}
#[derive(Clone, Debug)]
struct Usage {
    amount: f64,
    since: Instant,
}
struct Record {
    cancel: Option<Cancellation>,
    progress: Option<f64>,
    usage: BTreeMap<String, Usage>,
    cancelling: bool,
}
struct State {
    next: u64,
    tasks: BTreeMap<u64, Record>,
    resources: BTreeMap<String, Resource>,
    previous_throughput: Option<f64>,
}
pub struct Monitor {
    state: Mutex<State>,
    flat_tolerance: f64,
}
#[derive(Clone, Debug)]
pub struct Candidate {
    pub id: u64,
    pub gains: Vec<f64>,
}
/// Algorithm 1: Pareto frontier, then contention-weighted scalarization.
pub fn select(weights: &[f64], candidates: &[Candidate]) -> Option<u64> {
    if !weights.iter().all(|v| v.is_finite() && *v >= 0.0) {
        return None;
    }
    let valid: Vec<_> = candidates
        .iter()
        .filter(|c| {
            c.gains.len() == weights.len() && c.gains.iter().all(|v| v.is_finite() && *v >= 0.0)
        })
        .collect();
    let mut best = None;
    let mut score = 0.0;
    for a in &valid {
        if valid.iter().any(|b| {
            b.id != a.id
                && b.gains.iter().zip(&a.gains).all(|(x, y)| x >= y)
                && b.gains.iter().zip(&a.gains).any(|(x, y)| x > y)
        }) {
            continue;
        }
        let value: f64 = a.gains.iter().zip(weights).map(|(g, w)| g * w).sum();
        if value > score && value.is_finite() {
            score = value;
            best = Some(a.id);
        }
    }
    best
}
impl Monitor {
    pub fn new(flat_tolerance: f64) -> Arc<Self> {
        assert!(flat_tolerance.is_finite() && (0.0..1.0).contains(&flat_tolerance));
        Arc::new(Self {
            flat_tolerance,
            state: Mutex::new(State {
                next: 0,
                tasks: BTreeMap::new(),
                resources: BTreeMap::new(),
                previous_throughput: None,
            }),
        })
    }
    pub fn add_resource(&self, name: &str, kind: ResourceKind) -> Result<(), Status> {
        let mut s = self.state.lock().unwrap();
        if s.resources.contains_key(name) {
            return Err(Status::already_exists("resource registered"));
        }
        s.resources.insert(
            name.into(),
            Resource {
                kind,
                contention: 0.0,
            },
        );
        Ok(())
    }
    /// Normalized window signal D_r/T_exec (§3.5), not raw cross-resource units.
    pub fn resource_window(
        &self,
        name: &str,
        contention_delay: Duration,
        baseline_execution: Duration,
    ) -> Result<(), Status> {
        if baseline_execution.is_zero() {
            return Err(Status::invalid_argument(
                "baseline execution must be positive",
            ));
        }
        let mut s = self.state.lock().unwrap();
        let r = s
            .resources
            .get_mut(name)
            .ok_or_else(|| Status::not_found("unknown resource"))?;
        r.contention = contention_delay.as_secs_f64() / baseline_execution.as_secs_f64();
        Ok(())
    }
    /// Only register a cancellation token when the application's handler supports
    /// safe cancellation/rollback. Noncancellable tasks remain observable.
    pub fn task(self: &Arc<Self>, cancel: Option<Cancellation>) -> Task {
        let mut s = self.state.lock().unwrap();
        let id = s.next;
        s.next = id.checked_add(1).expect("task IDs exhausted");
        s.tasks.insert(
            id,
            Record {
                cancel,
                progress: None,
                usage: BTreeMap::new(),
                cancelling: false,
            },
        );
        Task {
            monitor: self.clone(),
            id,
        }
    }
    pub fn active(&self) -> usize {
        self.state.lock().unwrap().tasks.len()
    }
    /// Feed windowed throughput and end-to-end latency. Resource cancellation only
    /// fires when SLO is missed, throughput is flat/falling, and attributed gain exists.
    pub fn observe(
        &self,
        throughput: f64,
        latency: Duration,
        slo: Duration,
    ) -> Result<Option<u64>, Status> {
        if !throughput.is_finite() || throughput < 0.0 || slo.is_zero() {
            return Err(Status::invalid_argument("invalid performance window"));
        }
        let selected = {
            let mut s = self.state.lock().unwrap();
            let previous = s.previous_throughput.replace(throughput);
            if latency <= slo
                || !previous.is_some_and(|p| throughput <= p * (1.0 + self.flat_tolerance))
            {
                return Ok(None);
            }
            let resources: Vec<_> = s.resources.iter().collect();
            let weights: Vec<_> = resources.iter().map(|(_, r)| r.contention).collect();
            let mut candidates = Vec::new();
            for (&id, t) in &s.tasks {
                if t.cancel.as_ref().is_none_or(|c| c.is_cancelled()) || t.cancelling {
                    continue;
                }
                // Unknown/zero progress is not fabricated as infinity. Instrument
                // progress before this task can be considered for cancellation.
                let Some(p) = t.progress.filter(|p| *p > 0.0 && *p < 1.0) else {
                    continue;
                };
                let gains = resources
                    .iter()
                    .map(|(name, r)| {
                        t.usage.get(*name).map_or(0.0, |u| {
                            let used = match r.kind {
                                ResourceKind::Memory => u.amount,
                                ResourceKind::Synchronization | ResourceKind::Queue => {
                                    u.since.elapsed().as_secs_f64() * u.amount
                                }
                            };
                            used * (1.0 - p) / p
                        })
                    })
                    .collect();
                candidates.push(Candidate { id, gains });
            }
            select(&weights, &candidates).map(|id| {
                let t = s.tasks.get_mut(&id).unwrap();
                t.cancelling = true;
                (id, t.cancel.clone().unwrap())
            })
        };
        // Invoke cancellation outside the state lock; cleanup may reenter Monitor.
        Ok(selected.map(|(id, c)| {
            c.cancel();
            id
        }))
    }
}
pub struct Task {
    monitor: Arc<Monitor>,
    id: u64,
}
impl Task {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn progress(&self, completed: u64, total: u64) -> Result<(), Status> {
        if total == 0 || completed > total {
            return Err(Status::invalid_argument("invalid task progress"));
        }
        self.monitor
            .state
            .lock()
            .unwrap()
            .tasks
            .get_mut(&self.id)
            .unwrap()
            .progress = Some(completed as f64 / total as f64);
        Ok(())
    }
    pub fn acquire(&self, resource: &str, amount: f64) -> Result<(), Status> {
        if !amount.is_finite() || amount <= 0.0 {
            return Err(Status::invalid_argument("resource amount must be positive"));
        }
        let mut s = self.monitor.state.lock().unwrap();
        let kind = s
            .resources
            .get(resource)
            .ok_or_else(|| Status::not_found("unknown resource"))?
            .kind;
        let usage = &mut s.tasks.get_mut(&self.id).unwrap().usage;
        if !matches!(kind, ResourceKind::Memory) && usage.contains_key(resource) {
            return Err(Status::failed_precondition(
                "release the previous holding interval before reacquiring",
            ));
        }
        let current = usage.get(resource).map_or(0.0, |u| u.amount);
        if !(current + amount).is_finite() {
            return Err(Status::invalid_argument("resource amount overflow"));
        }
        let u = usage.entry(resource.into()).or_insert(Usage {
            amount: 0.0,
            since: Instant::now(),
        });
        u.amount += amount;
        Ok(())
    }
    pub fn release(&self, resource: &str, amount: f64) -> Result<(), Status> {
        if !amount.is_finite() || amount <= 0.0 {
            return Err(Status::invalid_argument("invalid release"));
        }
        let mut s = self.monitor.state.lock().unwrap();
        let usage = &mut s.tasks.get_mut(&self.id).unwrap().usage;
        let u = usage
            .get_mut(resource)
            .ok_or_else(|| Status::failed_precondition("resource not held"))?;
        if amount > u.amount {
            return Err(Status::failed_precondition("release exceeds held resource"));
        }
        u.amount -= amount;
        if u.amount == 0.0 {
            usage.remove(resource);
        }
        Ok(())
    }
}
impl Drop for Task {
    fn drop(&mut self) {
        self.monitor.state.lock().unwrap().tasks.remove(&self.id);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn synchronization_gain_uses_holding_time_and_progress() {
        let m = Monitor::new(0.05);
        m.add_resource("lock", ResourceKind::Synchronization)
            .unwrap();
        m.resource_window("lock", Duration::from_secs(1), Duration::from_secs(1))
            .unwrap();
        let token = Cancellation::new();
        let task = m.task(Some(token.clone()));
        task.progress(4, 10).unwrap();
        task.acquire("lock", 1.0).unwrap();
        assert!(task.acquire("lock", 1.0).is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        let state = m.state.lock().unwrap();
        let usage = &state.tasks[&task.id()].usage["lock"];
        let gain = usage.since.elapsed().as_secs_f64() * usage.amount * (1.0 - 0.4) / 0.4;
        assert!((gain - 1.5).abs() < 1e-10);
        drop(state);
        let slo = Duration::from_millis(1);
        m.observe(100.0, slo * 2, slo).unwrap();
        assert_eq!(m.observe(100.0, slo * 2, slo).unwrap(), Some(task.id()));
        assert!(token.is_cancelled());
        task.release("lock", 1.0).unwrap();
        task.acquire("lock", 1.0).unwrap();
    }
    #[test]
    fn paper_example_and_pareto_frontier() {
        let cs = vec![
            Candidate {
                id: 1,
                gains: vec![3.0, 1.0],
            },
            Candidate {
                id: 2,
                gains: vec![2.0, 2.0],
            },
            Candidate {
                id: 3,
                gains: vec![1.0, 1.0],
            },
        ];
        assert_eq!(select(&[0.6, 0.4], &cs), Some(1));
        assert_eq!(select(&[0.1, 0.9], &cs), Some(2));
        assert_eq!(select(&[0.0, 0.0], &cs), None);
    }
    #[test]
    fn future_gain_excludes_finished_unknown_and_noncancellable_tasks() {
        let m = Monitor::new(0.05);
        m.add_resource("memory", ResourceKind::Memory).unwrap();
        m.resource_window("memory", Duration::from_secs(1), Duration::from_secs(1))
            .unwrap();
        let safe = m.task(Some(Cancellation::new()));
        safe.progress(1, 10).unwrap();
        safe.acquire("memory", 10.0).unwrap();
        let almost = m.task(Some(Cancellation::new()));
        almost.progress(9, 10).unwrap();
        almost.acquire("memory", 100.0).unwrap();
        let unsafe_task = m.task(None);
        unsafe_task.progress(1, 100).unwrap();
        unsafe_task.acquire("memory", 10000.0).unwrap();
        let unknown = m.task(Some(Cancellation::new()));
        unknown.acquire("memory", 10000.0).unwrap();
        let slo = Duration::from_secs(1);
        assert_eq!(m.observe(100.0, slo * 2, slo).unwrap(), None);
        assert_eq!(m.observe(100.0, slo * 2, slo).unwrap(), Some(safe.id()));
        assert_eq!(m.observe(100.0, slo * 2, slo).unwrap(), Some(almost.id()));
        assert_eq!(m.observe(100.0, slo * 2, slo).unwrap(), None);
        assert!(safe.progress(2, 1).is_err());
        assert!(safe.release("memory", 11.0).is_err());
        drop((safe, almost, unsafe_task, unknown));
        assert_eq!(m.active(), 0);
    }
}
