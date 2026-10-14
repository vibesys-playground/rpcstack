use rpcstack_call::{CallInfo, CallPolicy, Prepare};
use rpcstack_hedged::{Hedger, ReplaySafety};
use std::{sync::atomic::Ordering, time::Duration};
use tonic::Request;
struct Immediate;
impl CallPolicy for Immediate {
    type Ticket = ();
    fn prepare<'a>(&'a self, _: &'a CallInfo) -> Prepare<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
#[tokio::test]
async fn delayed_backup_uses_real_generated_stub_and_shared_deadline() {
    type Stack = ();
    let a = rpcstack_testkit::serve(
        rpcstack_tonic::ServerContext::<Stack>::try_new("primary").unwrap(),
        None,
    )
    .await;
    let b = rpcstack_testkit::serve(
        rpcstack_tonic::ServerContext::<Stack>::try_new("backup").unwrap(),
        None,
    )
    .await;
    let mut primary = rpcstack_testkit::connect::<Stack>(&a.address).await;
    let mut backup = rpcstack_testkit::connect::<Stack>(&b.address).await;
    let deadline = Some(tokio::time::Instant::now() + Duration::from_secs(2));
    let first = CallInfo {
        destination: a.address.clone(),
        ordinal: 0,
        deadline,
    };
    let second = CallInfo {
        destination: b.address.clone(),
        ordinal: 1,
        deadline,
    };
    let hedger = Hedger::new(Duration::from_millis(30), 1);
    let mut slow = rpcstack_testkit::input();
    slow.delay_ms = 1000;
    let start = tokio::time::Instant::now();
    hedger
        .call(
            ReplaySafety::ReadOnly,
            || {
                rpcstack_call::execute(&Immediate, &first, Request::new(slow), |req| {
                    primary.call(req)
                })
            },
            || {
                rpcstack_call::execute(
                    &Immediate,
                    &second,
                    Request::new(rpcstack_testkit::input()),
                    |req| backup.call(req),
                )
            },
        )
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_millis(800));
    assert_eq!(a.calls.load(Ordering::Relaxed), 1);
    assert_eq!(b.calls.load(Ordering::Relaxed), 1);
    assert_eq!(hedger.available_backup_permits(), 1);
}
#[tokio::test(start_paused = true)]
async fn caller_cancellation_releases_both_attempts_and_budget() {
    use std::sync::{atomic::AtomicUsize, Arc};
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let hedger = Hedger::new(Duration::from_millis(1), 1);
    let pending = || async {
        let _guard = Guard(dropped.clone());
        std::future::pending::<Result<tonic::Response<()>, tonic::Status>>().await
    };
    assert!(tokio::time::timeout(
        Duration::from_millis(10),
        hedger.call(ReplaySafety::ReadOnly, pending, pending)
    )
    .await
    .is_err());
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
    assert_eq!(hedger.available_backup_permits(), 1);
}
