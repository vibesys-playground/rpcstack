use rpcstack_call::{execute, CallInfo};
use rpcstack_rifl::*;
use rpcstack_testkit::{connect, input, serve_handler};
use rpcstack_tonic::ServerContext;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tonic::{Request, Response, Status};
#[tokio::test]
async fn lost_response_retry_after_reopen_replays_atomic_increment() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let store = Store::open(&path, 10).unwrap();
    store
        .initialize("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (0);")
        .unwrap();
    let client_id = store.allocate(0, 10000).unwrap();
    let lost = Arc::new(AtomicBool::new(true));
    let server = serve_handler::<(), _, _>(ServerContext::try_new("probe.Probe").unwrap(), {
        let store = store.clone();
        let lost = lost.clone();
        move |request| {
            let store = store.clone();
            let lost = lost.clone();
            async move {
                let id = Identity::from_request(&request)?;
                let bytes = tokio::task::spawn_blocking(move || {
                    store.execute(id, "counter", b"increment", 1, |tx| {
                        tx.execute("UPDATE counter SET value=value+1", [])
                            .map_err(|e| Status::internal(e.to_string()))?;
                        let n: i64 = tx
                            .query_row("SELECT value FROM counter", [], |r| r.get(0))
                            .map_err(|e| Status::internal(e.to_string()))?;
                        Ok(n.to_string().into_bytes())
                    })
                })
                .await
                .unwrap()?;
                if lost.swap(false, Ordering::Relaxed) {
                    return Err(Status::unavailable("response lost AFTER durable commit"));
                }
                Ok(Response::new(rpcstack_testkit::pb::Output {
                    wire: String::from_utf8(bytes).unwrap(),
                }))
            }
        }
    })
    .await;
    let tracker = RequestTracker::new(client_id, 10);
    let rpc = tracker.begin().unwrap();
    let info = CallInfo {
        destination: server.address.clone(),
        ordinal: 0,
        deadline: Some(tokio::time::Instant::now() + Duration::from_secs(3)),
    };
    let mut client = connect::<()>(&server.address).await;
    let first = execute(&rpc, &info, Request::new(input()), |r| async move {
        client.call(r).await
    })
    .await;
    assert_eq!(first.unwrap_err().code(), tonic::Code::Unavailable);
    assert_eq!(tracker.first_incomplete(), 1);
    drop(server);
    drop(store);
    // New server and connection after reopening the durable shard, with both its
    // application object and completion records. No in-memory cache survives.
    let store = Store::open(&path, 10).unwrap();
    let server = serve_handler::<(), _, _>(ServerContext::try_new("probe.Probe").unwrap(), {
        let store = store.clone();
        move |request| {
            let store = store.clone();
            async move {
                let id = Identity::from_request(&request)?;
                let bytes = tokio::task::spawn_blocking(move || {
                    store.execute(id, "counter", b"increment", 2, |_| {
                        panic!("retry reexecuted mutation")
                    })
                })
                .await
                .unwrap()?;
                Ok(Response::new(rpcstack_testkit::pb::Output {
                    wire: String::from_utf8(bytes).unwrap(),
                }))
            }
        }
    })
    .await;
    let channel = connect::<()>(&server.address).await;
    let result = rpc
        .retry(
            tokio::time::Instant::now() + Duration::from_secs(3),
            Duration::from_millis(1),
            |rpc| {
                let mut client = channel.clone();
                let info = info.clone();
                async move {
                    execute(&rpc, &info, Request::new(input()), |r| async move {
                        client.call(r).await
                    })
                    .await
                }
            },
        )
        .await
        .unwrap();
    assert_eq!(result.into_inner().wire, "1");
    rpc.acknowledge();
    assert_eq!(tracker.first_incomplete(), 2);
    assert_eq!(store.result_count().unwrap(), 1);
}
#[test]
fn concurrent_duplicate_requests_commit_once() {
    let store = Store::open(":memory:", 10).unwrap();
    let client = store.allocate(0, 100).unwrap();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut threads = Vec::new();
    for _ in 0..16 {
        let store = store.clone();
        let count = count.clone();
        threads.push(std::thread::spawn(move || {
            store
                .execute(
                    Identity {
                        client,
                        sequence: 1,
                        first_incomplete: 1,
                    },
                    "object",
                    b"operation",
                    1,
                    |_| {
                        count.fetch_add(1, Ordering::Relaxed);
                        Ok(b"done".to_vec())
                    },
                )
                .unwrap()
        }));
    }
    for t in threads {
        assert_eq!(t.join().unwrap(), b"done");
    }
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[test]
fn crash_child() {
    let Ok(path) = std::env::var("RPCSTACK_RIFL_CRASH_DB") else {
        return;
    };
    let mode = std::env::var("RPCSTACK_RIFL_CRASH_MODE").unwrap();
    let s = Store::open(path, 10).unwrap();
    s.execute(
        Identity {
            client: 1,
            sequence: 1,
            first_incomplete: 1,
        },
        "counter",
        b"inc",
        1,
        |tx| {
            tx.execute("UPDATE counter SET value=value+1", []).unwrap();
            if mode == "before" {
                std::process::exit(77);
            }
            Ok(b"1".to_vec())
        },
    )
    .unwrap();
    std::process::exit(77);
}
#[test]
fn process_exit_before_and_after_commit_preserves_atomicity() {
    for mode in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("crash.sqlite");
        let s = Store::open(&path, 10).unwrap();
        s.initialize("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0);")
            .unwrap();
        assert_eq!(s.allocate(0, 100).unwrap(), 1);
        drop(s);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child", "--nocapture"])
            .env("RPCSTACK_RIFL_CRASH_DB", &path)
            .env("RPCSTACK_RIFL_CRASH_MODE", mode)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(77));
        let s = Store::open(&path, 10).unwrap();
        let result = s
            .execute(
                Identity {
                    client: 1,
                    sequence: 1,
                    first_incomplete: 1,
                },
                "counter",
                b"inc",
                2,
                |tx| {
                    assert_eq!(mode, "before", "post-commit retry must replay");
                    let n: i64 = tx
                        .query_row("SELECT value FROM counter", [], |r| r.get(0))
                        .unwrap();
                    assert_eq!(n, 0, "pre-commit exit must roll back");
                    tx.execute("UPDATE counter SET value=value+1", []).unwrap();
                    Ok(b"1".to_vec())
                },
            )
            .unwrap();
        assert_eq!(result, b"1");
    }
}
