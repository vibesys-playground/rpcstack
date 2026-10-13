use rpcstack_call::{execute, CallInfo};
use rpcstack_persephone::*;
use rpcstack_testkit::{connect, input, serve_handler};
use rpcstack_tonic::ServerContext;
use std::time::Duration;
use tonic::{Request, Response};
#[tokio::test]
async fn grpc_long_backlog_does_not_consume_short_reserved_worker() {
    let s = Scheduler::new(
        Config {
            workers: 3,
            ..Config::default()
        },
        vec![
            Profile {
                service: Duration::from_millis(1),
                occurrence: 0.9,
            },
            Profile {
                service: Duration::from_millis(100),
                occurrence: 0.1,
            },
        ],
    );
    let held = s.acquire(Some(1)).await.unwrap();
    let spill = s.acquire(None).await.unwrap();
    let server = serve_handler::<(), _, _>(ServerContext::try_new("probe.Probe").unwrap(), {
        let s = s.clone();
        move |request| {
            let s = s.clone();
            async move {
                let kind = request_type(&request)?;
                s.run(kind, async {
                    Ok(Response::new(rpcstack_testkit::pb::Output {
                        wire: "served".into(),
                    }))
                })
                .await?
            }
        }
    })
    .await;
    let mut long_client = connect::<()>(&server.address).await;
    let info = CallInfo {
        destination: server.address.clone(),
        ordinal: 0,
        deadline: Some(tokio::time::Instant::now() + Duration::from_secs(3)),
    };
    let long_info = info.clone();
    let long = tokio::spawn(async move {
        execute(
            &TypedCall(1),
            &long_info,
            Request::new(input()),
            |r| async move { long_client.call(r).await },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while s.queued() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut short_client = connect::<()>(&server.address).await;
    let result = execute(
        &TypedCall(0),
        &info,
        Request::new(input()),
        |r| async move { short_client.call(r).await },
    )
    .await
    .unwrap();
    assert_eq!(result.into_inner().wire, "served");
    assert!(!long.is_finished());
    drop(held);
    long.await.unwrap().unwrap();
    drop(spill);
    assert_eq!(s.active(), 0);
    assert_eq!(s.queued(), 0);
}
