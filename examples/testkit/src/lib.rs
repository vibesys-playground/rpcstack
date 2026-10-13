//! Real gRPC transport fixture, shared only by optional policy examples/tests.
use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tonic::rpcstack::Hooks;
use tonic::{Request, Response, Status};
pub mod pb {
    tonic::include_proto!("probe");
}
pub type Client<S> =
    pb::probe_client::ProbeClient<tonic::transport::Channel, rpcstack_tonic::PolicyHooks<S>>;
struct Probe<H: Hooks> {
    downstream: Option<tonic::transport::Channel>,
    calls: Arc<AtomicUsize>,
    marker: PhantomData<H>,
}
#[tonic::async_trait]
impl<H: Hooks> pb::probe_server::Probe for Probe<H>
where
    H::ChildContext: Send,
{
    async fn call(&self, request: Request<pb::Input>) -> Result<Response<pb::Output>, Status> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_millis(request.get_ref().delay_ms)).await;
        if request.get_ref().fail {
            return Err(Status::unavailable("fixture failure"));
        }
        if let Some(channel) = self.downstream.clone() {
            let mut client = pb::probe_client::ProbeClient::<_, H>::with_custom_context(channel);
            return client.call(request.into_inner()).await;
        }
        let wire = request
            .metadata()
            .get("ctx")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        Ok(Response::new(pb::Output { wire }))
    }
}
pub struct Running {
    pub address: String,
    pub calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub async fn serve<S: rpcstack::ModuleStack>(
    context: rpcstack_tonic::ServerContext<S>,
    downstream: Option<tonic::transport::Channel>,
) -> Running {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let service = Probe::<rpcstack_tonic::PolicyHooks<S>> {
        downstream,
        calls: calls.clone(),
        marker: PhantomData,
    };
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::probe_server::ProbeServer::<
                _,
                rpcstack_tonic::PolicyHooks<S>,
            >::with_server_context(service, context))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    Running {
        address,
        calls,
        task,
    }
}
pub async fn connect<S: rpcstack::ModuleStack>(address: &str) -> Client<S> {
    Client::<S>::connect_with_custom_context(address.to_owned())
        .await
        .unwrap()
}
pub fn input() -> pb::Input {
    pb::Input {
        delay_ms: 0,
        fail: false,
    }
}
