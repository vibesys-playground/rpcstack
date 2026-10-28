use rpcstack_c3::{C3Module, Config, SelectedCall, Selector};
use rpcstack_call::{CallInfo, Stage};
use std::time::Duration;
use tonic::Request;
type Stack = rpcstack::policy_stack![C3Module];
#[tokio::test]
async fn replicas_report_stage_metrics_and_selection_owns_cancellation() {
    let a = rpcstack_testkit::serve_with_stage(
        rpcstack_tonic::ServerContext::<Stack>::try_new("a").unwrap(),
        None,
        Some(Stage::new(1)),
    )
    .await;
    let b = rpcstack_testkit::serve_with_stage(
        rpcstack_tonic::ServerContext::<Stack>::try_new("b").unwrap(),
        None,
        Some(Stage::new(1)),
    )
    .await;
    let selector = Selector::new(Config::default(), [a.address.clone(), b.address.clone()]);
    for (server, delay_ms) in [(&a, 40), (&b, 1)] {
        let mut client = rpcstack_testkit::connect::<Stack>(&server.address).await;
        let selection = selector
            .select(std::slice::from_ref(&server.address))
            .await
            .unwrap();
        let call = SelectedCall::new(selection);
        let info = CallInfo {
            destination: server.address.clone(),
            ordinal: 0,
            deadline: Some(tokio::time::Instant::now() + Duration::from_secs(2)),
        };
        let mut input = rpcstack_testkit::input();
        input.delay_ms = delay_ms;
        rpcstack_call::execute(&call, &info, Request::new(input), |req| client.call(req))
            .await
            .unwrap();
        assert_eq!(selector.pending(&server.address), 0);
        assert!(selector.score(&server.address) > 0.0);
    }
    assert!(selector.score(&a.address) > selector.score(&b.address));
    // Let both accumulate tokens: rank rather than pacing decides this choice.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let selection = selector
        .select(&[a.address.clone(), b.address.clone()])
        .await
        .unwrap();
    assert_eq!(selection.endpoint(), b.address);
    let info = CallInfo {
        destination: selection.endpoint().to_owned(),
        ordinal: 0,
        deadline: Some(tokio::time::Instant::now() + Duration::from_millis(5)),
    };
    let call = SelectedCall::new(selection);
    let result = rpcstack_call::execute(&call, &info, Request::new(()), |_| {
        std::future::pending::<Result<tonic::Response<()>, tonic::Status>>()
    })
    .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    assert_eq!(selector.pending(&b.address), 0);
}
