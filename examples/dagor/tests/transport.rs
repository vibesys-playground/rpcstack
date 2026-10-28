use rpcstack_dagor::*;
use rpcstack_testkit::{connect, input, serve};
use rpcstack_tonic::ServerContext;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
type Stack = rpcstack::policy_stack![DagorModule];
#[tokio::test(flavor = "current_thread")]
async fn propagation_rejection_feedback_and_server_isolation() {
    let leaf_state = DagorServer::default();
    let leaf = serve::<Stack>(
        ServerContext::try_with_resources("probe.Probe", |init| init.provide(leaf_state.clone()))
            .unwrap(),
        None,
    )
    .await;
    let channel = tonic::transport::Endpoint::from_shared(leaf.address.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let front = serve::<Stack>(
        ServerContext::try_new("probe.Probe").unwrap(),
        Some(channel),
    )
    .await;
    let mut client = connect::<Stack>(&front.address).await;
    let peers = PeerThresholds::default();
    let priority = Priority::new(128, 128).unwrap();
    let make = || {
        let mut r = tonic::Request::new(input());
        r.extensions_mut().insert(DagorCall {
            priority,
            peers: peers.clone(),
        });
        r
    };
    let response = client.call(make()).await.unwrap();
    let mut md = tonic::metadata::MetadataMap::new();
    md.insert("ctx", response.get_ref().wire.parse().unwrap());
    assert!(
        matches!(rpcstack::WireIn::from_metadata(&md).unwrap().get::<DagorModule>().unwrap(),Some(DagorWire::Request(p)) if p==priority)
    );
    // Force an overloaded observation window deterministically; the call path still
    // uses real generated stubs and server/client rejection hooks.
    {
        let mut c = leaf_state.controller.lock().unwrap();
        for _ in 0..100 {
            c.admit(priority, Duration::from_secs(1), Instant::now());
        }
        c.adjust(Instant::now());
    }
    assert_eq!(
        client.call(make()).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    let leaf_calls = leaf.calls.load(Ordering::Relaxed);
    assert_eq!(
        client.call(make()).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(leaf.calls.load(Ordering::Relaxed), leaf_calls);
    // Frontend remains healthy; rejection is learned for its leaf, not all services.
    assert!(front.calls.load(Ordering::Relaxed) >= 3);
}
