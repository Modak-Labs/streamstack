//! Stream ownership: who serves a named stream (the HTTP layer turns a remote
//! owner into a 307 redirect).
//!
//! [`MetadataOwnershipService`] reads the owner from the published metadata
//! view. The decision table:
//!
//! 1. name not registered → local (create lands here).

use std::sync::Arc;

use async_trait::async_trait;
use pico_metadata::ViewPublisher;
use s3stream::StreamState;

use crate::error::ServiceError;
use crate::service::S3StreamService;
use crate::types::{NodeMeta, Owner};

#[async_trait]
pub trait OwnershipService: Send + Sync {
    async fn owner_of(&self, name: &str) -> Result<Owner, ServiceError>;

    fn local_node(&self) -> NodeMeta;
}

pub struct MetadataOwnershipService {
    views: Arc<ViewPublisher>,
    node_id: i32,
    http_address: String,
    service: Arc<S3StreamService>,
}

impl MetadataOwnershipService {
    pub fn new(
        views: Arc<ViewPublisher>,
        node_id: i32,
        http_address: String,
        service: Arc<S3StreamService>,
    ) -> Self {
        Self {
            views,
            node_id,
            http_address,
            service,
        }
    }
}

#[async_trait]
impl OwnershipService for MetadataOwnershipService {
    async fn owner_of(&self, name: &str) -> Result<Owner, ServiceError> {
        let Some(stream_id) = self.service.lookup_stream_id(name).await? else {
            return Ok(Owner::local(None));
        };
        let view = self.views.load();
        let Some(stream) = view.state.get_stream(stream_id) else {
            return Ok(Owner::local(Some(stream_id)));
        };
        if stream.state != StreamState::Opened {
            return Ok(Owner::local(Some(stream_id)));
        }
        let owner_id = stream.node_id;
        if owner_id == self.node_id {
            return Ok(Owner::local(Some(stream_id)));
        }
        match view.state.get_node_address(owner_id) {
            Some(address) if !address.is_empty() => {
                Ok(Owner::remote(stream_id, owner_id, address.to_owned()))
            }
            _ => Ok(Owner::local(Some(stream_id))),
        }
    }

    fn local_node(&self) -> NodeMeta {
        NodeMeta {
            node_id: self.node_id,
            advertised_address: self.http_address.clone(),
        }
    }
}
