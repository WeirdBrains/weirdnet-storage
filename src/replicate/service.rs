//! Replication over [weirdnet-service](https://crates.io/crates/weirdnet-service) sessions:
//! serve a node's log as a `Service` and follow it from a standby.
//!
//! Serve [`ReplicationService`] with weirdnet-service's `net::Server` on a weirdnet-net node
//! (Noise-encrypted streams), behind an `Authorize` hook: `RosterAuth` admits the user's own
//! linked devices, optionally only those the user granted a device role such as
//! `myapp.standby`. Every request asks for the [`PERMISSION`] access. On the standby, open a
//! `Session<Request, Reply>` to the primary and call [`follow`] on a timer.
use super::{DEFAULT_PAGE, ForkSearch, Progress, Rejoined, ReplError, Reply, Request, Standby, serve};
use rusqlite::Connection;
use std::sync::Mutex;
use weirdnet_service::{Access, Admitted, Service, ServiceError, Session};

/// The permission every replication request asks the `Authorize` hook for.
pub const PERMISSION: &str = "storage.replicate";

/// A node's log as a weirdnet-service `Service` (`Request` in, `Reply` out). It only reads,
/// except that a pull from a node that has seen a newer epoch raises this node's fence.
pub struct ReplicationService {
    conn: Mutex<Connection>,
    permission: String,
    resource: Option<String>,
}

impl ReplicationService {
    /// Serve the log of the database `conn` is open on. Give it its own connection to the
    /// primary's file (`open` with the same key): WAL lets it read while the primary writes.
    pub fn new(conn: Connection) -> Self {
        Self { conn: Mutex::new(conn), permission: PERMISSION.to_string(), resource: None }
    }

    /// Ask for a different permission.
    pub fn with_permission(mut self, permission: impl Into<String>) -> Self {
        self.permission = permission.into();
        self
    }

    /// Ask for the permission on a resource (a database name, say).
    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }
}

impl Service for ReplicationService {
    type Request = Request;
    type Response = Reply;

    fn access(&self, _request: &Request) -> Access {
        let a = Access::new(self.permission.clone());
        match &self.resource {
            Some(r) => a.on(r.clone()),
            None => a,
        }
    }

    async fn call(&self, _who: Admitted, request: Request) -> Result<Reply, ServiceError> {
        let conn = self.conn.lock().map_err(|_| ServiceError::failed("replication log lock poisoned"))?;
        serve(&conn, &request).map_err(|e| ServiceError::failed(e.to_string()))
    }
}

async fn ask(session: &Session<Request, Reply>, request: &Request) -> Result<Reply, ReplError> {
    session.call(request).await.map_err(|e| ReplError::Source(e.to_string()))
}

/// Pull and apply until the remote source has nothing newer. On [`ReplError::Diverged`] call
/// [`rejoin`].
pub async fn follow(standby: &mut Standby, session: &Session<Request, Reply>) -> Result<Progress, ReplError> {
    let mut total = 0;
    loop {
        let reply = ask(session, &standby.pull_request(DEFAULT_PAGE)?).await?;
        let p = standby.apply(reply)?;
        total += p.applied;
        if p.applied == 0 || p.behind() == 0 {
            return Ok(Progress { applied: total, ..p });
        }
    }
}

/// [`Standby::rejoin`] against a remote source.
pub async fn rejoin(standby: &mut Standby, session: &Session<Request, Reply>) -> Result<Rejoined, ReplError> {
    let theirs = match ask(session, &Request::Head).await? {
        Reply::Head(h) => h,
        other => return Err(ReplError::Source(format!("unexpected answer to head: {other:?}"))),
    };
    let mut search = ForkSearch::new(standby.connection(), &theirs)?;
    while let Some(seq) = search.next_probe() {
        let h = match ask(session, &Request::Hash { seq }).await? {
            Reply::Hash(h) => h,
            other => return Err(ReplError::Source(format!("unexpected answer to hash: {other:?}"))),
        };
        search.answer(standby.connection(), seq, h)?;
    }
    let fork = search.fork()?;
    standby.observe_epoch(theirs.epoch)?;
    let rewound = standby.rewind_to(fork)?;
    let progress = follow(standby, session).await?;
    Ok(Rejoined { fork, rewound, progress })
}
