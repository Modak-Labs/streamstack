//! Liveness and readiness over a node: the operational surface every
//! deployment needs, independent of which stream protocol is served.
//! [`serve`](crate::serve) binds this router on the admin address.
//!
//! Raft-shaped admin endpoints (`/admin/peers`, `/admin/transfer-leader`,
//! `/admin/snapshot(s)`, a `leaderKnown` readiness term) are deliberately
//! absent. They describe a consensus deployment, and this node's metadata
//! plane is a SQL-backed log (see `pico_sql`). The static dashboard is out of
//! scope.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use pico_server::PicoNode;
use serde_json::json;

/// What `/ready` reports on.
///
/// `#drainBeforeShutdown` clears. It lives here rather than on [`PicoNode`]
/// because it is a serving-lifecycle fact (this process still wants traffic),
/// not node state: an embedded node with no HTTP has no use for it.
#[derive(Clone)]
pub struct AdminState {
    node: Arc<PicoNode>,
    serving: Arc<AtomicBool>,
}

impl AdminState {
    pub fn new(node: Arc<PicoNode>) -> Self {
        Self {
            node,
            serving: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn stop_serving(&self) {
        self.serving.store(false, Ordering::Relaxed);
    }
}

/// The admin router: `/health` and `/ready`.
pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(state)
}

async fn health() -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// 200 once this node can serve, 503 otherwise,
/// with a diagnostic body of the same shape.
///
/// Registration is the readiness signal: `PicoNode::start` returns only after
/// its `RegisterNode` command applied and the engine recovered its WAL, so a
/// node present in the applied metadata state is a node that can serve.
/// shows an operator whether the metadata tailer is making progress.
async fn ready(State(state): State<AdminState>) -> Response {
    let node_id = state.node.config().node_id;
    let view = state.node.views().load();
    let registered = view.state.get_node_address(node_id).is_some();
    let serving = state.serving.load(Ordering::Relaxed);
    let ready = serving && registered;
    let body = json!({
        "ready": ready,
        "serving": serving,
        "registered": registered,
        "appliedIndex": view.applied_index,
        "nodeId": node_id,
    });
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body)).into_response()
}
