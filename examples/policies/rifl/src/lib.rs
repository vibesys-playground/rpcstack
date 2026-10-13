//! RIFL's RPC identity, durable result tracking, rendezvous, and lease/ack GC.
//! SQLite is the single-shard storage adapter; effects MUST use the same transaction.
use rpcstack_call::{CallInfo, CallPolicy, Prepare, Ticket};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
};
use tonic::{Request, Response, Status};
fn db_error(e: rusqlite::Error) -> Status {
    Status::internal(format!("RIFL storage: {e}"))
}
#[derive(Clone, Copy, Debug)]
pub struct Identity {
    pub client: i64,
    pub sequence: i64,
    pub first_incomplete: i64,
}
impl Identity {
    pub fn from_request<T>(request: &Request<T>) -> Result<Self, Status> {
        fn read<T>(r: &Request<T>, key: &'static str) -> Result<i64, Status> {
            r.metadata()
                .get(key)
                .and_then(|s| s.to_str().ok())
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| Status::invalid_argument("missing/invalid RIFL identity"))
        }
        let id = Self {
            client: read(request, "rifl-client")?,
            sequence: read(request, "rifl-sequence")?,
            first_incomplete: read(request, "rifl-first-incomplete")?,
        };
        id.validate()?;
        Ok(id)
    }
    fn validate(self) -> Result<(), Status> {
        if self.client <= 0
            || self.sequence <= 0
            || self.first_incomplete <= 0
            || self.first_incomplete > self.sequence
        {
            Err(Status::invalid_argument("invalid RIFL identity bounds"))
        } else {
            Ok(())
        }
    }
}
/// Authoritative lease server and result store colocated in one durable shard.
/// Moving/reopening this database moves completion records with the objects.
pub struct Store {
    connection: Mutex<Connection>,
    max_unacked: usize,
}
impl Store {
    pub fn open(path: impl AsRef<Path>, max_unacked: usize) -> Result<Arc<Self>, Status> {
        if max_unacked == 0 {
            return Err(Status::invalid_argument("max_unacked must be positive"));
        }
        let c = Connection::open(path).map_err(db_error)?;
        c.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(db_error)?;
        c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS rifl_clock(id INTEGER PRIMARY KEY CHECK(id=1),time INTEGER NOT NULL); INSERT OR IGNORE INTO rifl_clock VALUES(1,0);
            CREATE TABLE IF NOT EXISTS rifl_clients(id INTEGER PRIMARY KEY AUTOINCREMENT,expires INTEGER NOT NULL,first_incomplete INTEGER NOT NULL DEFAULT 1,expired INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS rifl_results(client INTEGER NOT NULL,seq INTEGER NOT NULL,object TEXT NOT NULL,request BLOB NOT NULL,response BLOB NOT NULL,PRIMARY KEY(client,seq));").map_err(db_error)?;
        Ok(Arc::new(Self {
            connection: Mutex::new(c),
            max_unacked,
        }))
    }
    fn clock(c: &Connection, now_ms: i64) -> Result<i64, Status> {
        c.execute(
            "UPDATE rifl_clock SET time=MAX(time,?) WHERE id=1",
            [now_ms],
        )
        .map_err(db_error)?;
        c.query_row("SELECT time FROM rifl_clock WHERE id=1", [], |r| r.get(0))
            .map_err(db_error)
    }
    /// Migration/schema setup only, before serving application traffic.
    pub fn initialize(&self, sql: &str) -> Result<(), Status> {
        self.connection
            .lock()
            .unwrap()
            .execute_batch(sql)
            .map_err(db_error)
    }
    pub fn allocate(&self, now_ms: i64, lease_ms: i64) -> Result<i64, Status> {
        if now_ms < 0 || lease_ms <= 0 {
            return Err(Status::invalid_argument("invalid lease time"));
        }
        let c = self.connection.lock().unwrap();
        let now_ms = Self::clock(&c, now_ms)?;
        let expires = now_ms
            .checked_add(lease_ms)
            .ok_or_else(|| Status::invalid_argument("lease overflow"))?;
        c.execute("INSERT INTO rifl_clients(expires) VALUES (?)", [expires])
            .map_err(db_error)?;
        Ok(c.last_insert_rowid())
    }
    pub fn renew(&self, client: i64, now_ms: i64, lease_ms: i64) -> Result<(), Status> {
        if now_ms < 0 || lease_ms <= 0 {
            return Err(Status::invalid_argument("invalid lease time"));
        }
        let c = self.connection.lock().unwrap();
        let now_ms = Self::clock(&c, now_ms)?;
        let expires = now_ms
            .checked_add(lease_ms)
            .ok_or_else(|| Status::invalid_argument("lease overflow"))?;
        let n=c.execute("UPDATE rifl_clients SET expires=MAX(expires,?) WHERE id=? AND expires>? AND expired=0",params![expires,client,now_ms]).map_err(db_error)?;
        if n == 0 {
            Err(Status::failed_precondition(
                "lease expired or unknown; outstanding RPC outcome is indeterminate",
            ))
        } else {
            Ok(())
        }
    }
    /// All SQL effects and the completion record commit together. On retry, return
    /// the saved bytes. A callback error rolls back both effects and result record.
    pub fn execute<F>(
        &self,
        id: Identity,
        object: &str,
        request: &[u8],
        now_ms: i64,
        operation: F,
    ) -> Result<Vec<u8>, Status>
    where
        F: FnOnce(&Transaction<'_>) -> Result<Vec<u8>, Status>,
    {
        id.validate()?;
        if now_ms < 0 || object.is_empty() {
            return Err(Status::invalid_argument("invalid time/object"));
        }
        let mut c = self.connection.lock().unwrap();
        let now_ms = Self::clock(&c, now_ms)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let lease: Option<(i64, i64)> = tx
            .query_row(
                "SELECT expires,first_incomplete FROM rifl_clients WHERE id=? AND expired=0",
                [id.client],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(db_error)?;
        let Some((expires, old_first)) = lease else {
            return Err(Status::failed_precondition("unknown lease"));
        };
        if expires <= now_ms {
            return Err(Status::failed_precondition(
                "expired lease; refusing possible reexecution",
            ));
        }
        let first = old_first.max(id.first_incomplete);
        if id.sequence < first {
            return Err(Status::failed_precondition("RPC already acknowledged"));
        }
        tx.execute(
            "UPDATE rifl_clients SET first_incomplete=? WHERE id=?",
            params![first, id.client],
        )
        .map_err(db_error)?;
        tx.execute(
            "DELETE FROM rifl_results WHERE client=? AND seq<?",
            params![id.client, first],
        )
        .map_err(db_error)?;
        let prior: Option<(String, Vec<u8>, Vec<u8>)> = tx
            .query_row(
                "SELECT object,request,response FROM rifl_results WHERE client=? AND seq=?",
                params![id.client, id.sequence],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(db_error)?;
        if let Some((old_object, old_request, response)) = prior {
            if old_object != object || old_request != request {
                return Err(Status::invalid_argument(
                    "RPC identity reused for a different operation",
                ));
            }
            tx.commit().map_err(db_error)?;
            return Ok(response);
        }
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM rifl_results WHERE client=?",
                [id.client],
                |r| r.get(0),
            )
            .map_err(db_error)?;
        if count >= self.max_unacked as i64 {
            return Err(Status::resource_exhausted(
                "acknowledge completed RPCs before issuing more",
            ));
        }
        let response = operation(&tx)?;
        tx.execute(
            "INSERT INTO rifl_results(client,seq,object,request,response) VALUES (?,?,?,?,?)",
            params![id.client, id.sequence, object, request, response],
        )
        .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(response)
    }
    /// Expired identities are retained as tombstones; IDs are never reused and
    /// delayed RPCs cannot resurrect an expired lease after result collection.
    pub fn collect_expired(&self, now_ms: i64) -> Result<usize, Status> {
        if now_ms < 0 {
            return Err(Status::invalid_argument("negative lease time"));
        }
        let mut c = self.connection.lock().unwrap();
        let now_ms = Self::clock(&c, now_ms)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        tx.execute(
            "UPDATE rifl_clients SET expired=1 WHERE expires<=?",
            [now_ms],
        )
        .map_err(db_error)?;
        let n=tx.execute("DELETE FROM rifl_results WHERE client IN (SELECT id FROM rifl_clients WHERE expired=1)",[]).map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(n)
    }
    pub fn result_count(&self) -> Result<usize, Status> {
        self.connection
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM rifl_results", [], |r| r.get(0))
            .map_err(db_error)
    }
}
struct TrackerState {
    next: i64,
    pending: BTreeSet<i64>,
}
#[derive(Clone)]
pub struct RequestTracker {
    client: i64,
    state: Arc<Mutex<TrackerState>>,
    limit: usize,
}
impl RequestTracker {
    /// A tracker is tied to a fresh authoritative lease, never recreate it for an
    /// old client ID after losing memory of that client's pending operations.
    pub fn new(client: i64, limit: usize) -> Self {
        assert!(client > 0 && limit > 0);
        Self {
            client,
            state: Arc::new(Mutex::new(TrackerState {
                next: 1,
                pending: BTreeSet::new(),
            })),
            limit,
        }
    }
    pub fn begin(&self) -> Result<Rpc, Status> {
        let mut s = self.state.lock().unwrap();
        if s.pending.len() >= self.limit {
            return Err(Status::resource_exhausted("too many unacknowledged RPCs"));
        }
        let sequence = s.next;
        s.next = sequence
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("sequence exhausted"))?;
        s.pending.insert(sequence);
        Ok(Rpc {
            tracker: self.clone(),
            sequence,
        })
    }
    pub fn first_incomplete(&self) -> i64 {
        let s = self.state.lock().unwrap();
        s.pending.first().copied().unwrap_or(s.next)
    }
}
/// Clone to retry the SAME operation. Dropping it does not acknowledge an outcome.
#[derive(Clone)]
pub struct Rpc {
    tracker: RequestTracker,
    sequence: i64,
}
impl Rpc {
    pub fn identity(&self) -> Identity {
        Identity {
            client: self.tracker.client,
            sequence: self.sequence,
            first_incomplete: self.tracker.first_incomplete(),
        }
    }
    /// Call only after the final result has been delivered/accepted by the caller.
    /// Retry transient transport failures with the same identity. A deadline error
    /// leaves the RPC unacknowledged: its outcome can still be indeterminate.
    pub async fn retry<T, F, Fut>(
        &self,
        deadline: tokio::time::Instant,
        backoff: std::time::Duration,
        mut attempt: F,
    ) -> Result<Response<T>, Status>
    where
        F: FnMut(Self) -> Fut,
        Fut: std::future::Future<Output = Result<Response<T>, Status>>,
    {
        if backoff.is_zero() {
            return Err(Status::invalid_argument("retry backoff must be positive"));
        }
        let work = async {
            loop {
                match attempt(self.clone()).await {
                    Err(s) if s.code() == tonic::Code::Unavailable => {
                        tokio::time::sleep(backoff).await
                    }
                    result => return result,
                }
            }
        };
        tokio::time::timeout_at(deadline, work).await.map_err(|_| {
            Status::deadline_exceeded("RIFL outcome indeterminate; retain identity for retry")
        })?
    }
    pub fn acknowledge(&self) {
        self.tracker
            .state
            .lock()
            .unwrap()
            .pending
            .remove(&self.sequence);
    }
}
impl Ticket for Rpc {
    fn decorate<T>(&mut self, request: &mut Request<T>) -> Result<(), Status> {
        let id = self.identity();
        id.validate()?;
        for (key, value) in [
            ("rifl-client", id.client),
            ("rifl-sequence", id.sequence),
            ("rifl-first-incomplete", id.first_incomplete),
        ] {
            request.metadata_mut().insert(
                key,
                value
                    .to_string()
                    .parse()
                    .map_err(|_| Status::internal("identity encoding"))?,
            );
        }
        Ok(())
    }
    // No automatic ack in finish: a caller may lose/cancel delivery after transport
    // completion. The application explicitly accepts the final response.
}
impl CallPolicy for Rpc {
    type Ticket = Self;
    fn prepare<'a>(&'a self, _: &'a CallInfo) -> Prepare<'a, Self> {
        Box::pin(async move { Ok(self.clone()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn retries_keep_identity_and_deadlines_do_not_acknowledge() {
        use std::{
            sync::atomic::{AtomicUsize, Ordering},
            time::Duration,
        };
        let tracker = RequestTracker::new(1, 10);
        let rpc = tracker.begin().unwrap();
        let attempts = AtomicUsize::new(0);
        let result = rpc
            .retry(
                tokio::time::Instant::now() + Duration::from_millis(10),
                Duration::from_millis(1),
                |attempt| {
                    assert_eq!(attempt.identity().sequence, 1);
                    let n = attempts.fetch_add(1, Ordering::Relaxed);
                    async move {
                        if n == 0 {
                            Err(Status::unavailable("transient"))
                        } else {
                            Ok(Response::new(42))
                        }
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(result.into_inner(), 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(tracker.first_incomplete(), 1);
        let error = rpc
            .retry(
                tokio::time::Instant::now() + Duration::from_millis(5),
                Duration::from_millis(1),
                |_| async { Err::<Response<()>, _>(Status::unavailable("still unavailable")) },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(tracker.first_incomplete(), 1);
        rpc.acknowledge();
        assert_eq!(tracker.first_incomplete(), 2);
    }
    #[test]
    fn out_of_order_completions_do_not_ack_holes() {
        let t = RequestTracker::new(1, 10);
        let a = t.begin().unwrap();
        let b = t.begin().unwrap();
        b.acknowledge();
        assert_eq!(t.first_incomplete(), 1);
        drop(a);
        assert_eq!(t.first_incomplete(), 1);
    }
    #[test]
    fn atomic_effect_result_replay_gc_and_expired_client() {
        let s = Store::open(":memory:", 10).unwrap();
        s.initialize("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (0);")
            .unwrap();
        let client = s.allocate(0, 100).unwrap();
        let id = Identity {
            client,
            sequence: 1,
            first_incomplete: 1,
        };
        let increment = |tx: &Transaction<'_>| {
            tx.execute("UPDATE counter SET value=value+1", [])
                .map_err(db_error)?;
            let n: i64 = tx
                .query_row("SELECT value FROM counter", [], |r| r.get(0))
                .map_err(db_error)?;
            Ok(n.to_le_bytes().to_vec())
        };
        assert_eq!(
            s.execute(id, "counter", b"inc", 1, increment).unwrap(),
            1_i64.to_le_bytes()
        );
        assert_eq!(
            s.execute(id, "counter", b"inc", 2, |_| panic!("duplicate executed"))
                .unwrap(),
            1_i64.to_le_bytes()
        );
        assert!(s.execute(id, "counter", b"other", 2, increment).is_err());
        let two = Identity {
            sequence: 2,
            first_incomplete: 2,
            ..id
        };
        assert!(s
            .execute(two, "counter", b"inc", 3, |tx| {
                increment(tx)?;
                Err(Status::aborted("rollback"))
            })
            .is_err());
        assert_eq!(
            s.execute(two, "counter", b"inc", 3, increment).unwrap(),
            2_i64.to_le_bytes()
        );
        assert_eq!(s.result_count().unwrap(), 1);
        assert!(s.execute(id, "counter", b"inc", 4, increment).is_err());
        assert_eq!(s.collect_expired(100).unwrap(), 1);
        assert!(s.renew(client, 100, 100).is_err());
        assert!(s.renew(client, 1, 100).is_err());
        assert!(s.execute(two, "counter", b"inc", 101, increment).is_err());
        assert!(s.allocate(100, 100).unwrap() > client);
    }
}
