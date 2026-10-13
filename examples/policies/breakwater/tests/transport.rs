use rpcstack_breakwater::*;
use rpcstack_call::{execute, CallInfo};
use rpcstack_testkit::{connect, input, serve};
use rpcstack_tonic::ServerContext;
use std::{sync::atomic::Ordering, time::Duration};
type Stack = rpcstack::policy_stack![BreakwaterModule];
#[tokio::test(flavor = "current_thread")]
async fn zero_credit_bootstrap_waiting_expiration_and_credit_reuse() {
    let controller = Controller::new(Config {
        initial: 1,
        maximum: 1,
        target: Duration::from_millis(500),
        aqm: Duration::from_secs(1),
        ..Config::default()
    });
    let app = serve::<Stack>(
        ServerContext::try_with_resources("probe.Probe", |init| {
            init.provide(BreakwaterServer(controller.clone()))
        })
        .unwrap(),
        None,
    )
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let credit_address = format!("http://{}", listener.local_addr().unwrap());
    let service = CreditService(controller.clone());
    let control_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::credits_server::CreditsServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let credits = CreditClient::connect(credit_address, 17).await.unwrap();
    let first_credit = credits.clone();
    let endpoint = app.address.clone();
    let first = tokio::spawn(async move {
        let mut client = connect::<Stack>(&endpoint).await;
        let info = CallInfo {
            destination: endpoint,
            ordinal: 0,
            deadline: Some(tokio::time::Instant::now() + Duration::from_secs(2)),
        };
        execute(
            &first_credit,
            &info,
            tonic::Request::new(rpcstack_testkit::pb::Input {
                delay_ms: 100,
                fail: false,
            }),
            |request| async move { client.call(request).await },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while credits.inflight() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut client = connect::<Stack>(&app.address).await;
    let info = CallInfo {
        destination: app.address.clone(),
        ordinal: 0,
        deadline: Some(tokio::time::Instant::now() + Duration::from_millis(10)),
    };
    let error = execute(
        &credits,
        &info,
        tonic::Request::new(input()),
        |request| async move { client.call(request).await },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    first.await.unwrap().unwrap();
    assert_eq!(credits.inflight(), 0);
    assert_eq!(app.calls.load(Ordering::Relaxed), 1);
    let mut client = connect::<Stack>(&app.address).await;
    let info = CallInfo {
        deadline: Some(tokio::time::Instant::now() + Duration::from_secs(2)),
        ..info
    };
    execute(
        &credits,
        &info,
        tonic::Request::new(input()),
        |request| async move { client.call(request).await },
    )
    .await
    .unwrap();
    assert_eq!(app.calls.load(Ordering::Relaxed), 2);
    control_task.abort();
}
