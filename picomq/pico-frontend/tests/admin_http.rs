//! Admin surface over a real socket: `/health` and `/ready`.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use pico_frontend::{serve, Protocol, ServeOptions};

#[tokio::test]
async fn health_and_ready() {
    let server = common::pico_server().await;
    let client = reqwest::Client::new();

    let health = client
        .get(format!("{}/health", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);
    assert_eq!(health.text().await.unwrap(), "ok");

    let ready = client
        .get(format!("{}/ready", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 200);
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["ready"], true);
    assert_eq!(body["registered"], true);
    assert_eq!(body["nodeId"], 1);
    assert!(
        body["appliedIndex"].as_u64().unwrap() > 0,
        "registration applied: {body}"
    );
}

#[tokio::test]
async fn ready_fails_while_draining() {
    let node = common::start_node().await;
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = serve(
        node,
        ServeOptions {
            protocol: Protocol::Pico,
            addr: loopback,
            admin_addr: Some(loopback),
            shutdown_drain: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let admin_url = format!("http://{}", server.admin_addr().unwrap());

    let draining = tokio::spawn(async move { server.shutdown().await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let ready = client
        .get(format!("{admin_url}/ready"))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 503, "draining node is not ready");
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["serving"], false);
    assert_eq!(body["registered"], true, "still registered while draining");

    // Liveness stays up: the process is healthy, just not accepting new work.
    let health = client
        .get(format!("{admin_url}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);

    draining.await.unwrap();
    assert!(
        client
            .get(format!("{admin_url}/ready"))
            .send()
            .await
            .is_err(),
        "listener closed after the drain window"
    );
}

#[tokio::test]
async fn admin_listener_can_be_disabled() {
    let node = common::start_node().await;
    let server = serve(
        node,
        ServeOptions {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            admin_addr: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(server.admin_addr().is_none());
    server.shutdown().await;
}
