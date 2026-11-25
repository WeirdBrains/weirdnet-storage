//! A standby follows a primary through weirdnet-service sessions (in-memory channels, as a
//! weirdnet-net stream would carry them), behind an `Authorize` hook.
use std::sync::Arc;
use weirdnet_service::{Access, Admitted, Authorize, Caller, Credentials, ServiceError, Session, SessionOptions, memory_pair, serve_session};
use weirdnet_storage::open;
use weirdnet_storage::replicate::service::{PERMISSION, ReplicationService, follow, rejoin};
use weirdnet_storage::replicate::{Manual, Primary, ReplError, Reply, Request, Standby};

/// Admits only the standby's peer, and only for replication (a stand-in for `RosterAuth` with a
/// device-role grant).
fn standby_only() -> Arc<dyn Authorize> {
    Arc::new(|caller: &Caller, access: &Access| -> Result<Admitted, ServiceError> {
        if caller.peer != "standby-peer" {
            return Err(ServiceError::refused("unknown-device"));
        }
        if !access.is_session() && access.permission != PERMISSION {
            return Err(ServiceError::refused("role"));
        }
        Ok(Admitted::new(caller.peer.clone()))
    })
}

async fn connect(path: &std::path::Path, peer: &str) -> Result<Session<Request, Reply>, ServiceError> {
    let service = Arc::new(ReplicationService::new(open(path, Some(&[1; 32]), "primary").unwrap()));
    let (client, server) = memory_pair(16);
    let peer = peer.to_string();
    tokio::spawn(async move {
        let _ = serve_session(service, standby_only(), server, peer, &SessionOptions::default()).await;
    });
    Session::start(client, Credentials::new()).await
}

fn insert(p: &mut Primary, id: i64) {
    p.write(|tx| -> Result<(), ReplError> {
        tx.execute("INSERT INTO t VALUES (?1, 'x')", [id])?;
        Ok(())
    })
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_follows_over_a_session_and_strangers_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("primary.db");
    let mut p = Primary::open(open(&path, Some(&[1; 32]), "primary").unwrap(), "a", Arc::new(Manual::granted(1))).unwrap();
    p.migrate(&["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);"], 1).unwrap();
    for i in 1..=300 {
        insert(&mut p, i);
    }

    let session = connect(&path, "standby-peer").await.unwrap();
    let mut s = Standby::open(open(&dir.path().join("standby.db"), Some(&[2; 32]), "standby").unwrap(), "b").unwrap();
    let progress = follow(&mut s, &session).await.unwrap();
    assert!(progress.applied > 256, "more than one page");
    assert_eq!(progress.behind(), 0);
    assert_eq!(s.head().unwrap(), p.head().unwrap());
    insert(&mut p, 301);
    assert_eq!(follow(&mut s, &session).await.unwrap().applied, 1);
    let n: i64 = s.connection().query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 301);

    // A rejoin with nothing to rewind is a plain catch-up.
    insert(&mut p, 302);
    let r = rejoin(&mut s, &session).await.unwrap();
    assert_eq!((r.rewound, r.progress.applied), (0, 1));

    // Someone else's device is refused.
    let stranger = match connect(&path, "stranger").await {
        Ok(session) => session.call(&Request::Head).await.map(|_| ()),
        Err(e) => Err(e),
    };
    assert!(matches!(stranger, Err(ServiceError::Refused { .. })), "{stranger:?}");
}
