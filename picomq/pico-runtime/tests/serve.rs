//! Booting a real process configuration: SQLite metadata log, object storage
//! from a bucket URI, admin + protocol listeners. Start the server, drive it
//! over HTTP, restart it.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use pico_frontend::Protocol;
use pico_runtime::{MetaBackend, ServerConfig};

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// A config with ephemeral ports, a SQLite log and storage under `dir`.
fn config(dir: &Path, protocol: Protocol, node_epoch: i64) -> ServerConfig {
    ServerConfig {
        node_epoch,
        addr: loopback(),
        admin_addr: Some(loopback()),
        protocol,
        meta_backend: MetaBackend::parse(&format!("sqlite:{}", dir.join("meta.db").display()))
            .unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!("2@file://{}", dir.join("wal").display())),
        engine: s3stream::Config {
            wal_upload_interval_ms: 200,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn pico_protocol_over_a_started_process() {
    let dir = tempfile::tempdir().unwrap();
    let server = pico_runtime::start(config(dir.path(), Protocol::Pico, 1))
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.local_addr());
    let admin = format!("http://{}", server.admin_addr().unwrap());

    let ready: serde_json::Value = http
        .get(format!("{admin}/ready"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ready["ready"], true, "registered against the SQLite log");

    let url = format!("{base}/streams/orders");
    let created = http
        .put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);

    let appended = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body("hello-from-pico")
        .send()
        .await
        .unwrap();
    assert_eq!(appended.status(), 200);
    assert_eq!(appended.headers()["Pico-Next-Seq"], "1");

    let read = http.get(format!("{url}?seq=0")).send().await.unwrap();
    assert_eq!(read.status(), 200);
    let records: serde_json::Value = read.json().await.unwrap();
    assert_eq!(records[0]["seq"], 0);
    assert_eq!(records[0]["body"], "hello-from-pico");

    server.shutdown().await;
}

#[tokio::test]
async fn ds_protocol_over_a_started_process() {
    let dir = tempfile::tempdir().unwrap();
    let server = pico_runtime::start(config(dir.path(), Protocol::Ds, 1))
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let url = format!("http://{}/streams/events", server.local_addr());

    let created = http
        .put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);

    // The Durable Streams protocol acks an append with 204 + the new offset.
    let appended = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body("hello-from-ds")
        .send()
        .await
        .unwrap();
    assert_eq!(appended.status(), 204);

    let read = http.get(format!("{url}?offset=-1")).send().await.unwrap();
    assert_eq!(read.status(), 200);
    assert_eq!(read.text().await.unwrap(), "hello-from-ds");

    server.shutdown().await;
}

/// The point of a SQL log plus object storage: state survives the process.
#[tokio::test]
async fn state_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let http = reqwest::Client::new();

    let first = pico_runtime::start(config(dir.path(), Protocol::Pico, 1))
        .await
        .unwrap();
    let url = format!("http://{}/streams/durable", first.local_addr());
    assert_eq!(
        http.put(&url)
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(
        http.post(&url)
            .header("Content-Type", "text/plain")
            .body("survives")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    // Let the periodic WAL upload land the record in object storage before the
    // process goes away.
    tokio::time::sleep(Duration::from_millis(500)).await;
    first.shutdown().await;

    // `nodeEpoch = System.currentTimeMillis()`).
    let second = pico_runtime::start(config(dir.path(), Protocol::Pico, 2))
        .await
        .unwrap();
    let url = format!("http://{}/streams/durable", second.local_addr());
    let head = http.head(&url).send().await.unwrap();
    assert_eq!(head.status(), 200, "stream metadata survived the restart");
    assert_eq!(head.headers()["Pico-Next-Seq"], "1");

    let read = http.get(format!("{url}?seq=0")).send().await.unwrap();
    assert_eq!(read.status(), 200);
    let records: serde_json::Value = read.json().await.unwrap();
    assert_eq!(records[0]["body"], "survives");

    second.shutdown().await;
}
