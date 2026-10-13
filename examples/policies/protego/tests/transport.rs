use rpcstack_breakwater::{pb, BreakwaterModule, BreakwaterServer, CreditClient, CreditService};
use rpcstack_call::{execute, CallInfo};
use rpcstack_protego::*;
use rpcstack_testkit::{connect, input, serve_handler};
use rpcstack_tonic::ServerContext;
use std::{sync::Arc, time::Duration};
use tonic::{Request, Response};
type Stack = rpcstack::policy_stack![BreakwaterModule];
#[tokio::test]
async fn credits_control_real_rpc_and_contended_lock_sheds_without_leaks() {
    let policy = Protego::new(Config {
        initial: 2,
        maximum: 2,
        queue_budget: Duration::from_millis(20),
        ..Config::default()
    });
    let lock = Arc::new(QueueGate::new(1));
    let mut budget = QueueBudget::new(Duration::from_secs(1));
    let holder = lock.acquire(&mut budget).await.unwrap();
    let app = serve_handler::<Stack, _, _>(
        ServerContext::try_with_resources("probe.Probe", |init| {
            init.provide(BreakwaterServer(policy.credits.clone()))
        })
        .unwrap(),
        {
            let lock = lock.clone();
            let policy = policy.clone();
            move |_| {
                let lock = lock.clone();
                let policy = policy.clone();
                async move {
                    policy
                        .run(|mut budget| async move {
                            let _guard = lock.acquire(&mut budget).await?;
                            Ok(Response::new(rpcstack_testkit::pb::Output {
                                wire: "executed".into(),
                            }))
                        })
                        .await
                }
            }
        },
    )
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let service = CreditService(policy.credits.clone());
    let control = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::credits_server::CreditsServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let credits = CreditClient::connect(address, 42).await.unwrap();
    let info = CallInfo {
        destination: app.address.clone(),
        ordinal: 0,
        deadline: Some(tokio::time::Instant::now() + Duration::from_secs(3)),
    };
    let mut client = connect::<Stack>(&app.address).await;
    let error = execute(&credits, &info, Request::new(input()), |r| async move {
        client.call(r).await
    })
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(lock.waiting(), 0);
    assert_eq!(credits.inflight(), 0);
    drop(holder);
    let mut client = connect::<Stack>(&app.address).await;
    let response = execute(&credits, &info, Request::new(input()), |r| async move {
        client.call(r).await
    })
    .await
    .unwrap();
    assert_eq!(response.into_inner().wire, "executed");
    policy.credits.set_total(1);
    assert_eq!(policy.credits.total(), 1);
    policy.start();
    control.abort();
}
