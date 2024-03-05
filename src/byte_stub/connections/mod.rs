use std::sync::Arc;
use atlas_common::channel::OneShotRx;
use atlas_common::node_id::NodeId;
use atlas_common::error::*;

/// The byte level connection controller definitions
///
/// This defines the necessary methods to provide connections to other peers
pub trait NetworkConnectionController: Send + Sync {
    /// Check if we are connected to a given node
    fn has_connection(&self, node: &NodeId) -> bool;

    // Get the amount of nodes we are currently connected to
    fn currently_connected_node_count(&self) -> usize;

    /// Get the nodes that we are currently connected to
    fn currently_connected_nodes(&self) -> Vec<NodeId>;

    /// Connect to a given node
    fn connect_to_node(self: &Arc<Self>, node: NodeId) -> Result<Vec<OneShotRx<Result<()>>>>;

    // Destroy our connection to a given node
    fn disconnect_from_node(self: &Arc<Self>, node: &NodeId) -> Result<()>;
}

