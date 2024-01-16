#![feature(async_fn_in_trait)]

use std::sync::Arc;
use std::time::Duration;
use crate::serialize::Serializable;
use atlas_common::error::*;
use thiserror::Error;
use atlas_common::channel::OneShotRx;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::node_id::NodeId;
use crate::reconfiguration_node::{NetworkInformationProvider, ReconfigurationNode};
use crate::protocol_node::ProtocolNetworkNode;

pub mod serialize;
pub mod message;
pub mod cpu_workers;
pub mod client_pooling;
pub mod config;
pub mod message_signing;
pub mod metric;
pub mod reconfiguration_node;
pub mod protocol_node;
pub mod conn_utils;

/// Actual node implementations
//pub mod tcpip;
//pub mod tcp_ip_simplex;
pub mod mio_tcp;

/// Trait for taking requests from the network node
/// We separate the various sources of requests in order to
/// allow for better handling of the requests
pub trait NodeIncomingRqHandler<T>: Send {

    /// Get the pending request count
    fn pending_rqs(&self) -> usize;

    /// Receive requests in a blocking fashion, until a request has been received, with a given possible timeout
    fn receive_requests(&self, timeout: Option<Duration>) -> Result<MaybeVec<T>>;

    /// Try to receive the requests without doing any blocking
    fn try_receive_requests(&self) -> Result<Option<MaybeVec<T>>>;
}

/// A trait defined that indicates how the connections are managed
/// Allows us to verify various things about our current connections as well
/// as establishing new ones.
pub trait NodeConnections {
    /// Are we currently connected to a given node?
    fn is_connected_to_node(&self, node: &NodeId) -> bool;

    /// How many nodes are we currently connected to in this node
    fn connected_nodes_count(&self) -> usize;

    /// Get the nodes we are connected to at this time
    fn connected_nodes(&self) -> Vec<NodeId>;

    /// Connect this node to another node.
    /// This will attempt to create various connections,
    /// depending on the configuration for concurrent connection count.
    /// Returns a vec with the results of each of the attempted connections
    fn connect_to_node(self: &Arc<Self>, node: NodeId) -> Vec<OneShotRx<Result<()>>>;

    /// Disconnect this node from another node
    async fn disconnect_from_node(&self, node: &NodeId) -> Result<()>;
}

pub trait NetworkNode {
    type ConnectionManager: NodeConnections;

    type NetworkInfoProvider: NetworkInformationProvider;

    /// Reports the id of this `Node`.
    fn id(&self) -> NodeId;

    /// Get a handle to the connection manager of this node.
    fn node_connections(&self) -> &Arc<Self::ConnectionManager>;

    fn network_info_provider(&self) -> &Arc<Self::NetworkInfoProvider>;
}

/// A full network node implementation
pub trait FullNetworkNode<NI, RM, PM, CM>:
    ProtocolNetworkNode<PM> + ReconfigurationNode<RM> + ProtocolNetworkNode<CM> + Send + Sync
    where
        NI: NetworkInformationProvider,
        RM: Serializable,
        PM: Serializable,
        CM: Serializable {
    /// The configuration type this node wants to accept
    type Config;

    /// Bootstrap the node
    async fn bootstrap(id: NodeId, network_info_provider: Arc<NI>, node_config: Self::Config) -> Result<Self>
        where Self: Sized;
}

#[derive(Error, Debug)]
pub enum NetworkSendError {
    #[error("Peer not found {0:?}")]
    PeerNotFound(NodeId)
}