use rpcstack_r2p2::*;
use rpcstack_testkit::{connect, input, serve, serve_handler};
use rpcstack_tonic::ServerContext;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tonic::Request;
#[tokio::test]
async fn central_grpc_router_bounds_backends_and_drains_queued_requests() {
    let a = serve::<()>(ServerContext::try_new("probe.Probe").unwrap(), None).await;
    let b = serve::<()>(ServerContext::try_new("probe.Probe").unwrap(), None).await;
    let router = Dispatcher::new(vec![a.address.clone(), b.address.clone()], 1, 10);
    let dispatched = Arc::new(AtomicUsize::new(0));
    let front = serve_handler::<(), _, _>(ServerContext::try_new("probe.Probe").unwrap(), {
        let router = router.clone();
        let dispatched = dispatched.clone();
        move |r| {
            let router = router.clone();
            let dispatched = dispatched.clone();
            async move {
                router
                    .call(
                        Some(tokio::time::Instant::now() + Duration::from_secs(5)),
                        r,
                        move |destination, r| async move {
                            dispatched.fetch_add(1, Ordering::Relaxed);
                            let mut client = connect::<()>(&destination).await;
                            client.call(r).await
                        },
                    )
                    .await
            }
        }
    })
    .await;
    let mut requests = Vec::new();
    for _ in 0..3 {
        let mut client = connect::<()>(&front.address).await;
        requests.push(tokio::spawn(async move {
            client
                .call(Request::new(rpcstack_testkit::pb::Input {
                    delay_ms: 200,
                    fail: false,
                }))
                .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while router.queued() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(router.outstanding(), vec![1, 1]);
    assert_eq!(dispatched.load(Ordering::Relaxed), 2);
    for r in requests {
        r.await.unwrap().unwrap();
    }
    assert_eq!(router.outstanding(), vec![0, 0]);
    assert_eq!(router.queued(), 0);
    assert_eq!(
        a.calls.load(Ordering::Relaxed) + b.calls.load(Ordering::Relaxed),
        3
    );
    let mut client = connect::<()>(&front.address).await;
    client.call(input()).await.unwrap();
}
