//! Application-owned replica channel; uses stock Tonic balancing.
use std::task::{Context, Poll};
use tonic::{
    body::BoxBody,
    client::GrpcService,
    codegen::Service,
    http,
    transport::{Channel, Endpoint},
};
#[derive(Clone)]
pub struct LoadBalancedChannel {
    channel: Channel,
}
impl LoadBalancedChannel {
    pub async fn new(host: String, port: u16, replicas: u8) -> Self {
        assert!(replicas > 0);
        // Literal addresses are useful for process deployments; DNS deployments
        // retain the per-replica host names used by Compose.
        let literal = host.parse::<std::net::IpAddr>().is_ok();
        assert!(
            !literal || replicas == 1,
            "literal endpoint supports one replica"
        );
        let endpoints = (1..=replicas).map(|i| {
            let address = if literal {
                host.clone()
            } else {
                format!("{host}-{i}")
            };
            Endpoint::from_shared(format!("http://{address}:{port}")).unwrap()
        });
        Self {
            channel: Channel::balance_list(endpoints),
        }
    }
}
impl Service<http::Request<BoxBody>> for LoadBalancedChannel {
    type Response = <Channel as Service<http::Request<BoxBody>>>::Response;
    type Error = <Channel as Service<http::Request<BoxBody>>>::Error;
    type Future = <Channel as Service<http::Request<BoxBody>>>::Future;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        GrpcService::poll_ready(&mut self.channel, cx)
    }
    fn call(&mut self, r: http::Request<BoxBody>) -> Self::Future {
        Service::call(&mut self.channel, r)
    }
}
