//! Service-level end-to-end: a `PicoNode` on the metadata plane with
//! memory object storage.
//!
//! Covers create/append/read/close via services, with the atomic-batch and
//! trim tail. A third test repeats the core flow over the SQL-backed
//! sink (SQLite), and further tests cover long-poll waiters, producer
//! idempotency over the wire, delete, and ownership routing. Service-level
//! layer lands in a later phase.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pico_metadata::{CommandSink, LocalSink, ViewPublisher};
use pico_server::ownership::OwnershipService as _;
use pico_server::{AppendCommand, CreateCommand, ErrorKind, NodeConfig, OffsetToken, PicoNode};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};

async fn start_node(
    node_id: i32,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
) -> PicoNode {
    let object_storage: Arc<dyn ObjectStorageTrait> =
        Arc::new(MemoryObjectStorage::new((node_id * 2) as i16));
    let wal_storage: Arc<dyn ObjectStorageTrait> =
        Arc::new(MemoryObjectStorage::new((node_id * 2 + 1) as i16));
    PicoNode::start(
        NodeConfig {
            node_id,
            node_epoch: 1,
            http_address: format!("http://127.0.0.1:{}", 4000 + node_id),
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
    )
    .await
    .unwrap()
}

async fn local_node() -> PicoNode {
    let (sink, views) = LocalSink::new();
    start_node(1, Arc::new(sink), views).await
}

fn create(name: &str, content_type: &str) -> CreateCommand {
    CreateCommand {
        name: name.into(),
        content_type: content_type.into(),
        ttl_seconds: None,
        expires_at_ms: None,
        closed: false,
        initial_payload: Bytes::new(),
    }
}

fn append(name: &str, payloads: &[&[u8]], content_type: &str) -> AppendCommand {
    AppendCommand {
        name: name.into(),
        payloads: payloads.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        content_type: Some(content_type.into()),
        ..Default::default()
    }
}

/// (+ its `atomicBatchAppendReadAndTrim` tail).
#[tokio::test]
async fn create_append_read_close_via_services() {
    let node = local_node().await;
    let services = node.service();

    let created = services
        .create(create("/streams/demo", "text/plain"))
        .await
        .unwrap();
    assert!(created.created);

    let appended = services
        .append(append("/streams/demo", &[b"hello"], "text/plain"))
        .await
        .unwrap();
    assert!(appended.applied);
    assert!(!appended.closed);

    let batch = services
        .read("/streams/demo", OffsetToken::beginning(), 1024, 0)
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(&batch.records[0].payload[..], b"hello");
    assert!(batch.up_to_date);

    assert!(
        services
            .close("/streams/demo")
            .await
            .unwrap()
            .next_offset
            .record_offset()
            >= 1
    );
    assert!(
        services
            .head("/streams/demo")
            .await
            .unwrap()
            .unwrap()
            .closed
    );

    // ---- atomicBatchAppendReadAndTrim ----
    services
        .create(create("/streams/batch", "application/octet-stream"))
        .await
        .unwrap();
    let appended = services
        .append(AppendCommand {
            atomic: true,
            ..append(
                "/streams/batch",
                &[b"a", b"bb", b"ccc"],
                "application/octet-stream",
            )
        })
        .await
        .unwrap();
    assert!(appended.applied);
    assert_eq!(appended.next_offset.record_offset(), 3);

    let all = services
        .read("/streams/batch", OffsetToken::beginning(), 1024, 0)
        .await
        .unwrap();
    assert_eq!(all.records.len(), 3);
    assert_eq!(&all.records[0].payload[..], b"a");
    assert_eq!(&all.records[1].payload[..], b"bb");
    assert_eq!(&all.records[2].payload[..], b"ccc");
    assert_eq!(all.records[0].offset.record_offset(), 0);
    assert_eq!(all.records[1].offset.record_offset(), 1);
    assert_eq!(all.records[2].offset.record_offset(), 2);
    assert!(all.up_to_date);

    let tail = services
        .read("/streams/batch", OffsetToken::of_record_offset(1), 1024, 0)
        .await
        .unwrap();
    assert_eq!(tail.records.len(), 2);
    assert_eq!(&tail.records[0].payload[..], b"bb");

    let limited = services
        .read("/streams/batch", OffsetToken::beginning(), 1024, 2)
        .await
        .unwrap();
    assert_eq!(limited.records.len(), 2);
    assert_eq!(limited.next_offset.record_offset(), 2);
    assert!(!limited.up_to_date);

    let effective = services.trim("/streams/batch", 2).await.unwrap();
    assert!(effective <= 2, "effective trim {effective}");
    assert_eq!(
        services
            .head("/streams/batch")
            .await
            .unwrap()
            .unwrap()
            .start_offset
            .record_offset(),
        effective
    );

    let trimmed = services
        .read("/streams/batch", OffsetToken::of_record_offset(2), 1024, 0)
        .await
        .unwrap();
    assert_eq!(trimmed.records.len(), 1);
    assert_eq!(&trimmed.records[0].payload[..], b"ccc");

    node.close().await;
}

#[tokio::test]
async fn concurrent_producers_on_one_stream_pipeline() {
    let node = local_node().await;
    let services = node.service();
    services
        .create(create("/streams/hot", "application/octet-stream"))
        .await
        .unwrap();

    let mut tasks = tokio::task::JoinSet::new();
    for producer in 0..8u8 {
        let services = Arc::clone(&services);
        tasks.spawn(async move {
            for record in 0..16u8 {
                services
                    .append(append(
                        "/streams/hot",
                        &[&[producer, record]],
                        "application/octet-stream",
                    ))
                    .await
                    .unwrap();
            }
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.unwrap();
    }

    let all = services
        .read("/streams/hot", OffsetToken::beginning(), 1 << 20, 0)
        .await
        .unwrap();
    assert_eq!(all.records.len(), 128);
    assert!(all.up_to_date);
    for (i, record) in all.records.iter().enumerate() {
        assert_eq!(record.offset.record_offset(), i as u64, "offsets dense");
    }
    // Per-producer record order is preserved even though producers interleave.
    for producer in 0..8u8 {
        let seen: Vec<u8> = all
            .records
            .iter()
            .filter(|r| r.payload[0] == producer)
            .map(|r| r.payload[1])
            .collect();
        assert_eq!(
            seen,
            (0..16u8).collect::<Vec<_>>(),
            "producer {producer} order"
        );
    }

    node.close().await;
}

#[tokio::test]
async fn list_and_match_seq_via_services() {
    let node = local_node().await;
    let services = node.service();

    for name in ["/list/a", "/list/b", "/list/c", "/other/x"] {
        services.create(create(name, "text/plain")).await.unwrap();
    }

    // ---- listByPrefix ----
    let all = services.list("/list/", None, 0).await.unwrap();
    let names: Vec<&str> = all.streams.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["/list/a", "/list/b", "/list/c"]);
    assert!(!all.has_more);

    let page = services.list("/list/", None, 2).await.unwrap();
    let names: Vec<&str> = page.streams.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["/list/a", "/list/b"]);
    assert!(page.has_more);

    let rest = services.list("/list/", Some("/list/b"), 0).await.unwrap();
    let names: Vec<&str> = rest.streams.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["/list/c"]);
    assert!(!rest.has_more);

    // ---- matchSeq ----
    let at = |payload: &[u8], match_seq: u64| AppendCommand {
        match_seq: Some(match_seq),
        ..append("/list/a", &[payload], "text/plain")
    };
    let first = services.append(at(b"one", 0)).await.unwrap();
    assert!(first.applied);
    assert_eq!(first.next_offset.record_offset(), 1);

    let conflict = services.append(at(b"stale", 0)).await.unwrap_err();
    assert_eq!(conflict.kind, ErrorKind::MatchFailed);
    assert_eq!(conflict.next_offset.unwrap().record_offset(), 1);

    assert!(services.append(at(b"two", 1)).await.unwrap().applied);

    node.close().await;
}

/// The core flow on the SQL-backed sink: same service, durable log underneath.
#[tokio::test]
async fn core_flow_on_sql_sink() {
    use pico_sql::{MetaStore, SqlSink, SqlSinkConfig, SqliteStore};

    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn MetaStore> = Arc::new(
        SqliteStore::open(&dir.path().join("meta.db"))
            .await
            .unwrap(),
    );
    let (sink, views) = SqlSink::open(
        store,
        SqlSinkConfig {
            poll_interval: Duration::from_millis(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let node = start_node(1, Arc::new(sink), views).await;
    let services = node.service();

    services
        .create(create("/sql/demo", "application/json"))
        .await
        .unwrap();
    // JSON array body splits into one record per element.
    let appended = services
        .append(append(
            "/sql/demo",
            &[br#"[{"a":1},{"b":2}]"#],
            "application/json",
        ))
        .await
        .unwrap();
    assert_eq!(appended.next_offset.record_offset(), 2);

    let read = services
        .read("/sql/demo", OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert_eq!(read.records.len(), 2);
    assert_eq!(&read.records[0].payload[..], br#"{"a":1}"#);
    assert_eq!(&read.records[1].payload[..], br#"{"b":2}"#);

    assert!(services.delete("/sql/demo").await.unwrap());
    assert!(services.head("/sql/demo").await.unwrap().is_none());
    assert!(!services.delete("/sql/demo").await.unwrap());

    node.close().await;
}

/// Long-poll: a parked reader wakes when an append passes its offset, and
/// `wait_appended` short-circuits on already-readable data and closed streams.
#[tokio::test]
async fn wait_appended_long_poll() {
    let node = local_node().await;
    let services = node.service();

    services
        .create(create("/poll/a", "text/plain"))
        .await
        .unwrap();

    let waiter = services.clone();
    let parked = tokio::spawn(async move {
        waiter
            .wait_appended("/poll/a", OffsetToken::beginning(), Duration::from_secs(10))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!parked.is_finished());

    services
        .append(append("/poll/a", &[b"x"], "text/plain"))
        .await
        .unwrap();
    assert!(parked.await.unwrap().unwrap());

    // Already readable: immediate true.
    assert!(services
        .wait_appended(
            "/poll/a",
            OffsetToken::beginning(),
            Duration::from_millis(1)
        )
        .await
        .unwrap());
    // Unknown stream: false.
    assert!(!services
        .wait_appended(
            "/poll/none",
            OffsetToken::beginning(),
            Duration::from_millis(1)
        )
        .await
        .unwrap());
    // Closed stream: true.
    services.close("/poll/a").await.unwrap();
    assert!(services
        .wait_appended(
            "/poll/a",
            OffsetToken::of_record_offset(99),
            Duration::from_millis(1)
        )
        .await
        .unwrap());

    node.close().await;
}

/// Producer idempotency over the service: duplicate suppressed, stale epoch
/// fenced, gap rejected. And the closed-stream replay path.
#[tokio::test]
async fn producer_idempotency_and_closed_replay() {
    use pico_server::types::Producer;

    let node = local_node().await;
    let services = node.service();
    services
        .create(create("/prod/a", "text/plain"))
        .await
        .unwrap();

    let with_producer = |payload: &[u8], epoch: u64, seq: u64| AppendCommand {
        producer: Some(Producer::new("p1", epoch, seq).unwrap()),
        ..append("/prod/a", &[payload], "text/plain")
    };

    assert!(
        services
            .append(with_producer(b"one", 1, 0))
            .await
            .unwrap()
            .applied
    );
    // Duplicate (same seq): not applied, last seq echoed.
    let dup = services.append(with_producer(b"one", 1, 0)).await.unwrap();
    assert!(!dup.applied);
    assert_eq!(dup.producer_seq, Some(0));
    // Gap.
    let gap = services
        .append(with_producer(b"three", 1, 2))
        .await
        .unwrap_err();
    assert_eq!(gap.kind, ErrorKind::SequenceGap);
    assert_eq!((gap.expected_seq, gap.received_seq), (Some(1), Some(2)));
    // Stale epoch after a bump.
    assert!(
        services
            .append(with_producer(b"two", 2, 0))
            .await
            .unwrap()
            .applied
    );
    let stale = services
        .append(with_producer(b"nope", 1, 1))
        .await
        .unwrap_err();
    assert_eq!(stale.kind, ErrorKind::Fenced);
    assert_eq!(stale.producer_epoch, Some(2));

    // Producer-attributed close, then idempotent close replay.
    let close = AppendCommand {
        close_after: true,
        producer: Some(Producer::new("p1", 2, 1).unwrap()),
        ..append("/prod/a", &[b"last"], "text/plain")
    };
    assert!(services.append(close.clone()).await.unwrap().closed);
    let replay = services.append(close).await.unwrap();
    assert!(replay.closed);
    assert!(!replay.applied);
    // A different producer appending to the closed stream is rejected.
    let other = AppendCommand {
        producer: Some(Producer::new("p2", 0, 0).unwrap()),
        ..append("/prod/a", &[b"x"], "text/plain")
    };
    let err = services.append(other).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::Closed);
    assert!(err.closed);

    node.close().await;
}

/// Ownership routing: a stream opened by another registered node resolves to
/// a remote owner with that node's advertised address. Unknown names and
/// closed streams stay local. Driven end-to-end through two nodes sharing
/// one metadata plane.
#[tokio::test]
async fn ownership_routes_to_open_owner() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;

    // Unknown name: local (create may land here).
    let owner = node1.ownership().owner_of("/own/a").await.unwrap();
    assert!(owner.local);
    assert_eq!(owner.stream_id, None);

    node1
        .service()
        .create(create("/own/a", "text/plain"))
        .await
        .unwrap();
    assert!(node1.ownership().owner_of("/own/a").await.unwrap().local);
    let from_node2 = node2.ownership().owner_of("/own/a").await.unwrap();
    assert!(!from_node2.local);
    assert_eq!(from_node2.owner_node_id, Some(1));
    assert_eq!(
        from_node2.owner_advertised_address.as_deref(),
        Some("http://127.0.0.1:4001")
    );

    // Closing releases ownership: closed (not OPENED) streams are local anywhere.
    node1.close().await;
    let released = node2.ownership().owner_of("/own/a").await.unwrap();
    assert!(released.local);
    assert!(released.stream_id.is_some());

    node2.close().await;
}

/// Registry entries and data survive a node restart: the KV plane holds the
/// registry (the entry cache is a cache, not the source of truth) and the
/// shared object storage holds the data.
#[tokio::test]
async fn named_streams_survive_restart() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));

    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 1,
            ..Default::default()
        },
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await
    .unwrap();
    node.service()
        .create(create("/durable/a", "text/plain"))
        .await
        .unwrap();
    node.service()
        .append(append("/durable/a", &[b"kept"], "text/plain"))
        .await
        .unwrap();
    node.close().await;

    // Same metadata plane and storage, fresh node at a higher epoch.
    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 2,
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
    )
    .await
    .unwrap();

    let head = node.service().head("/durable/a").await.unwrap().unwrap();
    assert_eq!(head.next_offset.record_offset(), 1);
    let read = node
        .service()
        .read("/durable/a", OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert_eq!(read.records.len(), 1);
    assert_eq!(&read.records[0].payload[..], b"kept");

    node.close().await;
}
