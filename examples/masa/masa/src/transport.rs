//! Example-owned round-robin adapter over public Tonic channels.
use std::{
    marker::PhantomData,
    task::{Context, Poll},
};
use tonic::{
    body::BoxBody,
    codegen::Service,
    http,
    transport::{Channel, Endpoint},
};
use tower::buffer::Buffer;

struct RoundRobin<S, Req> {
    services: Vec<S>,
    next: usize,
    marker: PhantomData<fn(Req)>,
}
impl<S, Req> RoundRobin<S, Req> {
    fn new(services: Vec<S>) -> Self {
        assert!(!services.is_empty(), "at least one replica required");
        Self {
            services,
            next: 0,
            marker: PhantomData,
        }
    }
}
impl<S: Service<Req>, Req> Service<Req> for RoundRobin<S, Req> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.services[self.next].poll_ready(cx)
    }
    fn call(&mut self, req: Req) -> Self::Future {
        let result = self.services[self.next].call(req);
        self.next = (self.next + 1) % self.services.len();
        result
    }
}
type Buffered = Buffer<RoundRobin<Channel, http::Request<BoxBody>>, http::Request<BoxBody>>;
/// Fixed replica set, shared buffered round-robin dispatcher across clones.
#[derive(Clone)]
pub struct LoadBalancedChannel {
    channel: Buffered,
}
impl LoadBalancedChannel {
    async fn connect(endpoints: Vec<Endpoint>) -> Self {
        let mut channels = Vec::new();
        for endpoint in endpoints {
            loop {
                match endpoint.connect().await {
                    Ok(channel) => {
                        channels.push(channel);
                        break;
                    }
                    Err(error) => {
                        eprintln!("failed to connect to {}: {error}; retrying", endpoint.uri());
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    }
                }
            }
        }
        Self {
            channel: Buffer::new(RoundRobin::new(channels), 1024),
        }
    }
    pub async fn new_from(hostname_base: String, port: u16, replicas: u8, start: u8) -> Self {
        assert!(replicas > 0, "at least one replica required");
        let endpoints = (0..replicas)
            .map(|i| {
                Endpoint::from_shared(format!(
                    "http://{}-{}:{}",
                    hostname_base,
                    u16::from(start) + u16::from(i),
                    port
                ))
                .unwrap()
            })
            .collect();
        Self::connect(endpoints).await
    }
    pub async fn new(hostname_base: String, port: u16, replicas: u8) -> Self {
        Self::new_from(hostname_base, port, replicas, 1).await
    }
    /// One DNS endpoint; actual endpoint distribution is an infrastructure concern.
    pub async fn new_from_service_name(service_name: String, port: u16, _replicas: u8) -> Self {
        Self::connect(vec![Endpoint::from_shared(format!(
            "http://{service_name}:{port}"
        ))
        .unwrap()])
        .await
    }
}
impl Service<http::Request<BoxBody>> for LoadBalancedChannel {
    type Response = <Channel as Service<http::Request<BoxBody>>>::Response;
    type Error = tower::BoxError;
    type Future =
        tower::buffer::future::ResponseFuture<<Channel as Service<http::Request<BoxBody>>>::Future>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.channel.poll_ready(cx)
    }
    fn call(&mut self, request: http::Request<BoxBody>) -> Self::Future {
        self.channel.call(request)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Clone)]
    struct Echo(usize);
    impl Service<()> for Echo {
        type Response = usize;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<usize, Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: ()) -> Self::Future {
            std::future::ready(Ok(self.0))
        }
    }
    #[tokio::test]
    async fn clones_share_round_robin_dispatch() {
        use tower::ServiceExt;
        let mut a = Buffer::new(RoundRobin::new(vec![Echo(0), Echo(1)]), 8);
        let mut b = a.clone();
        assert_eq!(a.ready().await.unwrap().call(()).await.unwrap(), 0);
        assert_eq!(b.ready().await.unwrap().call(()).await.unwrap(), 1);
        assert_eq!(a.ready().await.unwrap().call(()).await.unwrap(), 0);
    }
}
