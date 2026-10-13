//! Aequitas (SIGCOMM 2022), Algorithm 1: RPC-level QoS admission/downgrade.
//! Network scheduling and RPC network-latency measurement are adapter obligations.
use rpcstack_call::{CallInfo, CallPolicy, Prepare, Ticket};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tonic::{Request, Response, Status};
#[derive(Clone, Debug)]
pub struct Class {
    pub target_per_mtu: Duration,
    pub percentile: f64,
}
#[derive(Clone, Debug)]
pub struct Config {
    pub classes: Vec<Class>,
    pub alpha: f64,
    pub beta_per_mtu: f64,
    pub floor: f64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QosAssignment {
    pub requested: usize,
    pub run: usize,
    pub size_mtu: u32,
}
impl QosAssignment {
    pub fn is_downgraded(self) -> bool {
        self.requested != self.run
    }
}
/// Supplied by a transport adapter, not inferred from application RPC wall time.
/// RNL runs from transport ingress of the first RPC packet until final packet ACK.
#[derive(Clone, Copy, Debug)]
pub struct RpcNetworkLatency(pub Duration);
#[derive(Clone, Debug)]
struct Admission {
    probability: f64,
    last_increase: tokio::time::Instant,
}
#[derive(Clone, Debug)]
pub struct Controller {
    config: Config,
    channels: Arc<Mutex<BTreeMap<String, Vec<Admission>>>>,
}
impl Controller {
    /// `classes` lists controlled QoS levels highest first. Lowest/best-effort is
    /// implicit at index classes.len(), and has no admission controller.
    pub fn new(config: Config) -> Self {
        assert!(!config.classes.is_empty());
        for c in &config.classes {
            assert!(
                !c.target_per_mtu.is_zero()
                    && c.percentile.is_finite()
                    && c.percentile > 0.0
                    && c.percentile < 100.0
            );
        }
        assert!(
            config.alpha.is_finite()
                && config.alpha > 0.0
                && config.beta_per_mtu.is_finite()
                && config.beta_per_mtu > 0.0
                && config.floor > 0.0
                && config.floor <= 1.0
        );
        Self {
            config,
            channels: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    pub fn probability(&self, destination: &str, class: usize) -> f64 {
        if class >= self.config.classes.len() {
            return 1.0;
        }
        self.channels
            .lock()
            .unwrap()
            .get(destination)
            .map(|v| v[class].probability)
            .unwrap_or(1.0)
    }
    pub fn assign(
        &self,
        destination: &str,
        requested: usize,
        size_mtu: u32,
        draw: f64,
    ) -> Result<QosAssignment, Status> {
        if requested > self.config.classes.len()
            || size_mtu == 0
            || !draw.is_finite()
            || !(0.0..=1.0).contains(&draw)
        {
            return Err(Status::invalid_argument("invalid QoS/size/random sample"));
        }
        let run = if draw <= self.probability(destination, requested) {
            requested
        } else {
            self.config.classes.len()
        };
        Ok(QosAssignment {
            requested,
            run,
            size_mtu,
        })
    }
    pub fn observe(
        &self,
        destination: &str,
        assignment: QosAssignment,
        latency: RpcNetworkLatency,
        now: tokio::time::Instant,
    ) -> Result<(), Status> {
        if assignment.size_mtu == 0
            || assignment.run > self.config.classes.len()
            || assignment.requested > self.config.classes.len()
            || (assignment.run != assignment.requested
                && assignment.run != self.config.classes.len())
        {
            return Err(Status::invalid_argument("invalid completed RPC assignment"));
        }
        let Some(class) = self.config.classes.get(assignment.run) else {
            return Ok(());
        };
        let mut map = self.channels.lock().unwrap();
        let channel = map.entry(destination.to_owned()).or_insert_with(|| {
            self.config
                .classes
                .iter()
                .map(|_| Admission {
                    probability: 1.0,
                    last_increase: now,
                })
                .collect()
        });
        let state = &mut channel[assignment.run];
        if latency.0.as_secs_f64() / f64::from(assignment.size_mtu)
            < class.target_per_mtu.as_secs_f64()
        {
            let window = class.target_per_mtu.as_secs_f64() * 100.0 / (100.0 - class.percentile);
            if now.duration_since(state.last_increase).as_secs_f64() > window {
                state.probability = (state.probability + self.config.alpha).min(1.0);
                state.last_increase = now;
            }
        } else {
            // Algorithm 1's per-completion size-weighted decrement, not p*=beta.
            state.probability = (state.probability
                - self.config.beta_per_mtu * f64::from(assignment.size_mtu))
            .max(self.config.floor);
        }
        Ok(())
    }
    /// Preferred entry point: the adapter must apply QoS and return measured RNL.
    /// One Controller represents one source host; destination keys identify hosts.
    pub async fn execute<Req, Ret, F, Fut>(
        &self,
        info: &CallInfo,
        requested: usize,
        size_mtu: u32,
        request: Request<Req>,
        send: F,
    ) -> Result<Response<Ret>, Status>
    where
        F: FnOnce(QosAssignment, Request<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<(Response<Ret>, RpcNetworkLatency), Status>>,
    {
        let call = self.call(requested, size_mtu);
        rpcstack_call::execute(&call, info, request, |request| send_with_qos(request, send)).await
    }
    pub fn call(&self, requested: usize, size_mtu: u32) -> QosCall {
        QosCall {
            controller: self.clone(),
            requested,
            size_mtu,
        }
    }
}
#[derive(Clone)]
pub struct QosCall {
    controller: Controller,
    requested: usize,
    size_mtu: u32,
}
pub struct QosTicket {
    controller: Controller,
    destination: String,
    assignment: QosAssignment,
}
impl CallPolicy for QosCall {
    type Ticket = QosTicket;
    fn prepare<'a>(&'a self, info: &'a CallInfo) -> Prepare<'a, QosTicket> {
        Box::pin(async move {
            let assignment = self.controller.assign(
                &info.destination,
                self.requested,
                self.size_mtu,
                rand::random(),
            )?;
            Ok(QosTicket {
                controller: self.controller.clone(),
                destination: info.destination.clone(),
                assignment,
            })
        })
    }
}
impl Ticket for QosTicket {
    fn decorate<T>(&mut self, request: &mut Request<T>) -> Result<(), Status> {
        request.extensions_mut().insert(self.assignment);
        Ok(())
    }
    fn finish<T>(&mut self, result: &Result<Response<T>, Status>) {
        if let Ok(response) = result {
            if let Some(latency) = response.extensions().get::<RpcNetworkLatency>() {
                let _ = self.controller.observe(
                    &self.destination,
                    self.assignment,
                    *latency,
                    tokio::time::Instant::now(),
                );
            }
        }
    }
}
/// Adapter boundary is explicit: ordinary Tonic neither implements packet WFQ nor
/// reports RNL. Require a transport to honor the assignment and return its sample.
/// The async closure may route through QoS-isolated channels or a custom transport.
pub async fn send_with_qos<Req, Ret, F, Fut>(
    request: Request<Req>,
    send: F,
) -> Result<Response<Ret>, Status>
where
    F: FnOnce(QosAssignment, Request<Req>) -> Fut,
    Fut: std::future::Future<Output = Result<(Response<Ret>, RpcNetworkLatency), Status>>,
{
    let assignment = *request
        .extensions()
        .get::<QosAssignment>()
        .ok_or_else(|| Status::failed_precondition("missing Aequitas assignment"))?;
    let (mut response, latency) = send(assignment, request).await?;
    response.extensions_mut().insert(latency);
    response.extensions_mut().insert(assignment);
    Ok(response)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn controller() -> Controller {
        Controller::new(Config {
            classes: vec![
                Class {
                    target_per_mtu: Duration::from_millis(1),
                    percentile: 99.0,
                },
                Class {
                    target_per_mtu: Duration::from_millis(2),
                    percentile: 99.0,
                },
            ],
            alpha: 0.1,
            beta_per_mtu: 0.01,
            floor: 0.01,
        })
    }
    #[tokio::test(start_paused = true)]
    async fn size_weighted_decrease_floor_and_direct_lowest_downgrade() {
        let c = controller();
        let a = c.assign("a", 0, 10, 0.0).unwrap();
        let now = tokio::time::Instant::now();
        c.observe("a", a, RpcNetworkLatency(Duration::from_millis(10)), now)
            .unwrap();
        assert!((c.probability("a", 0) - 0.9).abs() < 1e-9);
        assert_eq!(c.probability("b", 0), 1.0);
        assert_eq!(c.probability("a", 1), 1.0);
        let downgraded = c.assign("a", 0, 10, 0.99).unwrap();
        assert_eq!(downgraded.run, 2);
        c.observe("a", downgraded, RpcNetworkLatency(Duration::ZERO), now)
            .unwrap();
        assert_eq!(c.probability("a", 0), 0.9);
        for _ in 0..100 {
            c.observe("a", a, RpcNetworkLatency(Duration::from_secs(1)), now)
                .unwrap();
        }
        assert_eq!(c.probability("a", 0), 0.01);
    }
    #[tokio::test(start_paused = true)]
    async fn increase_is_clock_gated_not_completion_count_gated() {
        let c = controller();
        let a = c.assign("a", 0, 1, 0.0).unwrap();
        let start = tokio::time::Instant::now();
        c.observe("a", a, RpcNetworkLatency(Duration::from_millis(1)), start)
            .unwrap();
        for _ in 0..100 {
            c.observe("a", a, RpcNetworkLatency(Duration::ZERO), start)
                .unwrap();
        }
        assert_eq!(c.probability("a", 0), 0.99);
        tokio::time::advance(Duration::from_millis(101)).await;
        c.observe(
            "a",
            a,
            RpcNetworkLatency(Duration::ZERO),
            tokio::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(c.probability("a", 0), 1.0);
    }
    #[tokio::test(start_paused = true)]
    async fn executor_passes_qos_to_adapter_and_uses_network_sample() {
        let c = controller();
        let call = c.call(0, 1);
        let info = CallInfo {
            destination: "a".into(),
            ordinal: 0,
            deadline: None,
        };
        rpcstack_call::execute(&call, &info, Request::new(()), |request| {
            send_with_qos(request, |assignment, _| async move {
                assert_eq!(assignment.run, 0);
                Ok((
                    Response::new(()),
                    RpcNetworkLatency(Duration::from_millis(2)),
                ))
            })
        })
        .await
        .unwrap();
        assert_eq!(c.probability("a", 0), 0.99);
    }
}

#[cfg(test)]
mod adapter_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn adapter_contract_reports_downgrade_and_preserves_application_metadata() {
        let assignment = QosAssignment {
            requested: 0,
            run: 2,
            size_mtu: 3,
        };
        let mut req = Request::new(());
        req.extensions_mut().insert(assignment);
        req.metadata_mut()
            .insert("application", "value".parse().unwrap());
        let response = send_with_qos(req, |q, req| async move {
            assert!(q.is_downgraded());
            assert_eq!(req.metadata().get("application").unwrap(), "value");
            Ok((
                Response::new(7),
                RpcNetworkLatency(Duration::from_millis(2)),
            ))
        })
        .await
        .unwrap();
        assert_eq!(
            response.extensions().get::<QosAssignment>(),
            Some(&assignment)
        );
        let error = send_with_qos(Request::new(()), |_, _| async {
            Ok((Response::new(()), RpcNetworkLatency(Duration::ZERO)))
        })
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_cancels_adapter_without_fabricating_measurements() {
        let c = Controller::new(Config {
            classes: vec![Class {
                target_per_mtu: Duration::from_millis(1),
                percentile: 99.0,
            }],
            alpha: 0.1,
            beta_per_mtu: 0.01,
            floor: 0.01,
        });
        let info = CallInfo {
            destination: "a".into(),
            ordinal: 0,
            deadline: Some(tokio::time::Instant::now() + Duration::from_millis(5)),
        };
        let result = c
            .execute(&info, 0, 1, Request::new(()), |_, _| {
                std::future::pending::<Result<(Response<()>, RpcNetworkLatency), Status>>()
            })
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
        assert_eq!(c.probability("a", 0), 1.0);
    }
}
