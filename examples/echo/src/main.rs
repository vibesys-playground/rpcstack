use rpcstack::{ChildState, Extensions, Module, WireIn, WireOut};
use rpcstack_tonic::RequestExt;
use tonic::{CowGrpcMethod, Request, Response, Status};

pub type ExampleHooks = rpcstack_tonic::PolicyHooks<rpcstack::policy_stack![Marker]>;
pub mod pb {
    tonic::include_proto!("echo");
}
pub mod noop_pb {
    include!(concat!(env!("OUT_DIR"), "/noop/echo.rs"));
}

/// A toy wire-data module, not a scheduling or admission policy.
#[derive(Debug)]
pub struct Marker(u64);
impl Module for Marker {
    type Server = ();
    type Wire = u64;
    const NAME: &'static str = "marker";
    const POLL_HOOKS: bool = false;
    fn new(_: &CowGrpcMethod, _: &(), wire: &WireIn<'_>, _: &mut Extensions) -> Self {
        Self(wire.get::<Self>().unwrap().unwrap_or(0))
    }
    fn before_root_rpc<T>(
        _: &CowGrpcMethod,
        request: &mut Request<T>,
        wire: &mut WireOut,
        _: &mut ChildState,
    ) -> Result<(), Status> {
        if request.metadata().contains_key("reject") {
            return Err(Status::resource_exhausted("rejected before transport"));
        }
        wire.put::<Self>(&42)
            .map_err(|error| Status::internal(error.to_string()))?;
        request
            .metadata_mut()
            .insert(rpcstack_wire::SCHEDULING_HEADER, "42".parse().unwrap());
        Ok(())
    }
    fn before_child_rpc<T>(
        &self,
        _: &CowGrpcMethod,
        _: &mut ChildState,
        request: &mut Request<T>,
        wire: &mut WireOut,
        _: &mut Extensions,
    ) -> Result<(), Status> {
        wire.put::<Self>(&self.0)
            .map_err(|error| Status::internal(error.to_string()))?;
        request
            .metadata_mut()
            .insert(rpcstack_wire::SCHEDULING_HEADER, "42".parse().unwrap());
        Ok(())
    }
}
#[derive(Default)]
struct Echo {
    downstream: Option<pb::echo_client::EchoClient<tonic::transport::Channel>>,
}
#[tonic::async_trait]
impl pb::echo_server::Echo for Echo {
    async fn call(&self, request: Request<pb::Input>) -> Result<Response<pb::Output>, Status> {
        if request.get_ref().reject {
            return Err(Status::unavailable("server rejection"));
        }
        if let Some(mut downstream) = self.downstream.clone() {
            return downstream.call(pb::Input { reject: false }).await;
        }
        Ok(Response::new(pb::Output {
            value: request.get_wire::<Marker>().unwrap_or(0),
        }))
    }
}
async fn serve(service: Echo) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                pb::echo_server::EchoServer::<_, ExampleHooks>::with_server_context(
                    service,
                    rpcstack_tonic::ServerContext::try_with_resources("echo.Echo", |_| {}).unwrap(),
                ),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (address, handle)
}
async fn exercise() {
    let (leaf, leaf_task) = serve(Echo::default()).await;
    let downstream = pb::echo_client::EchoClient::connect(leaf).await.unwrap();
    let (front, front_task) = serve(Echo {
        downstream: Some(downstream),
    })
    .await;
    let mut client = pb::echo_client::EchoClient::connect(front.clone())
        .await
        .unwrap();
    assert_eq!(
        client
            .call(pb::Input { reject: false })
            .await
            .unwrap()
            .into_inner()
            .value,
        42
    );
    assert_eq!(
        client
            .call(pb::Input { reject: true })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    let mut rejected = Request::new(pb::Input { reject: false });
    rejected
        .metadata_mut()
        .insert("reject", "true".parse().unwrap());
    assert_eq!(
        client.call(rejected).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        client
            .call(pb::Input { reject: false })
            .await
            .unwrap()
            .into_inner()
            .value,
        42
    );
    // No scheduling header and no user-selected hooks must work as well.
    let mut noop = noop_pb::echo_client::EchoClient::connect(front)
        .await
        .unwrap();
    assert_eq!(
        noop.call(noop_pb::Input { reject: false })
            .await
            .unwrap()
            .into_inner()
            .value,
        0
    );
    front_task.abort();
    leaf_task.abort();
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    exercise().await;
    println!("Two-hop RPC, root rejection, recovery, and default hooks passed");
}
#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "current_thread")]
    async fn two_hop_rpc() {
        super::exercise().await;
    }
}
