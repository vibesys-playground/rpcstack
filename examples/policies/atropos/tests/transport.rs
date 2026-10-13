use rpcstack_atropos::*;
use rpcstack_call::{
    cancellation::Cancellation,
    queue::{QueueBudget, QueueGate},
};
use rpcstack_testkit::{connect, input, serve_handler};
use rpcstack_tonic::ServerContext;
use std::{sync::Arc, time::Duration};
use tonic::Response;
#[tokio::test]
async fn cancelling_monopolizing_rpc_releases_lock_and_preserves_waiter() {
    let monitor = Monitor::new(0.05);
    monitor
        .add_resource("lock", ResourceKind::Synchronization)
        .unwrap();
    let lock = Arc::new(QueueGate::new(1));
    let server = serve_handler::<(), _, _>(ServerContext::try_new("probe.Probe").unwrap(), {
        let monitor = monitor.clone();
        let lock = lock.clone();
        move |request| {
            let monitor = monitor.clone();
            let lock = lock.clone();
            async move {
                let delay = request.get_ref().delay_ms;
                let token = Cancellation::new();
                let task = monitor.task((delay > 0).then_some(token.clone()));
                let guard = lock
                    .acquire(&mut QueueBudget::new(Duration::from_secs(5)))
                    .await?;
                task.progress(1, 10)?;
                task.acquire("lock", 1.0)?;
                let result = token
                    .run(async move {
                        let _guard = guard;
                        if delay > 0 {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                        Ok(Response::new(rpcstack_testkit::pb::Output {
                            wire: "done".into(),
                        }))
                    })
                    .await;
                drop(task);
                result
            }
        }
    })
    .await;
    let mut long_client = connect::<()>(&server.address).await;
    let long = tokio::spawn(async move {
        long_client
            .call(rpcstack_testkit::pb::Input {
                delay_ms: 5000,
                fail: false,
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while monitor.active() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut victim_client = connect::<()>(&server.address).await;
    let victim = tokio::spawn(async move { victim_client.call(input()).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while lock.waiting() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let delay = lock.oldest_wait();
    let slo = Duration::from_millis(1);
    monitor.observe(100.0, Duration::ZERO, slo).unwrap();
    monitor
        .resource_window("lock", delay, Duration::from_millis(10))
        .unwrap();
    assert!(monitor.observe(100.0, delay, slo).unwrap().is_some());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), long)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .code(),
        tonic::Code::Cancelled
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), victim)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner()
            .wire,
        "done"
    );
    assert_eq!(monitor.active(), 0);
    assert_eq!(lock.waiting(), 0);
}
