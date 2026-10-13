//! Load-generator observations, independent of any RPC policy.
//! SLOs are scoring facts. Attaching them never rejects or schedules a request.
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tonic::Request;
#[derive(Debug, Clone)]
pub struct Context {
    api: String,
    id: u64,
    start: u64,
    slo: u64,
}
#[derive(Debug, Clone)]
pub struct RootContext(Context);
pub fn create_context(api: &str, slo: Duration) -> Context {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    Context {
        api: api.into(),
        id: NEXT.fetch_add(1, Ordering::Relaxed),
        start: crate::timing::time_now(),
        slo: slo.as_micros().try_into().expect("SLO overflow"),
    }
}
impl Context {
    pub fn api(&self) -> &str {
        &self.api
    }
    pub fn request_id(&self) -> u64 {
        self.id
    }
    pub fn slo(&self) -> u64 {
        self.slo
    }
    pub fn gateway_entry(&self) -> u64 {
        self.start
    }
    pub fn deadline(&self) -> u64 {
        self.start.saturating_add(self.slo)
    }
}
impl From<Context> for RootContext {
    fn from(c: Context) -> Self {
        Self(c)
    }
}
impl RootContext {
    pub fn into_context(self) -> Context {
        self.0
    }
    pub fn attach<T>(&self, mut request: Request<T>) -> Request<T> {
        for (name, value) in [
            ("x-workload-start-us", self.0.start),
            ("x-workload-slo-us", self.0.slo),
            ("x-workload-id", self.0.id),
        ] {
            request
                .metadata_mut()
                .insert(name, value.to_string().parse().unwrap());
        }
        request
            .metadata_mut()
            .insert("x-workload-api", self.0.api.parse().unwrap());
        request
    }
}
