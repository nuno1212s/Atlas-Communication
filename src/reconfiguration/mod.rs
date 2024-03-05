use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use getset::{CopyGetters, Getters};

use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, TryRecvError};
use atlas_common::channel;
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::error::*;
use atlas_common::node_id::{NodeId, NodeType};
use atlas_common::peer_addr::PeerAddr;

#[derive(Clone, Getters, CopyGetters, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeInfo {
    #[get_copy = "pub"]
    node_id: NodeId,
    #[get_copy = "pub"]
    node_type: NodeType,
    #[get = "pub"]
    public_key: PublicKey,
    #[get = "pub"]
    addr: PeerAddr,
}

/// Represents the network information that a node needs to know about other nodes
pub trait NetworkInformationProvider: Send + Sync {
    /// Get our own node info
    fn own_node_info(&self) -> &NodeInfo;

    /// Get our own key pair
    fn get_key_pair(&self) -> &Arc<KeyPair>;

    /// Get the node info for a given node
    fn get_node_info(&self, node: &NodeId) -> Option<NodeInfo>;
}

/// The reconfiguration network update trait, to send updates about newly discovered
/// nodes to the networking layer. This is because when a new node connects to us and
/// we don't know about it, it will be left in a pending state until we receive a
/// network update telling us about this new node. In this pending state we will
/// only receive reconfiguration messages from this node.
pub trait ReconfigurationNetworkUpdate {
    fn send_reconfiguration_update(&self, update: NetworkUpdateMessage) -> Result<()>;
}

#[derive(Clone)]
pub enum NetworkUpdateMessage {
    NodeConnectionPermitted(NodeId, NodeType, PublicKey)
}

#[derive(Getters, Clone)]
pub struct ReconfigurationMessageHandler {
    #[get = "pub"]
    update_channel_tx: ChannelSyncTx<NetworkUpdateMessage>,
    #[get = "pub"]
    update_channel_rx: ChannelSyncRx<NetworkUpdateMessage>,
}

impl ReconfigurationMessageHandler {
    pub fn initialize() -> Self {
        let (network_updates_tx, network_updates_rx) = channel::new_bounded_sync(100, Some("Reconfig update message"));

        ReconfigurationMessageHandler {
            update_channel_tx: network_updates_tx,
            update_channel_rx: network_updates_rx,
        }
    }
}

impl ReconfigurationMessageHandler {
    pub fn receive_network_update(&self) -> Result<NetworkUpdateMessage> {
        Ok(self.update_channel_rx.recv().unwrap())
    }

    pub fn try_receive_network_update(&self, timeout: Option<Duration>) -> Result<Option<NetworkUpdateMessage>> {
        if let Some(timeout) = timeout {
            match self.update_channel_rx.recv_timeout(timeout) {
                Ok(msg) => {
                    Ok(Some(msg))
                }
                Err(err) => {
                    match err {
                        TryRecvError::ChannelEmpty | TryRecvError::Timeout => {
                            Ok(None)
                        }
                        TryRecvError::ChannelDc => {
                            Err(anyhow!("Reconfig message channel has disconnected?"))
                        }
                    }
                }
            }
        } else {
            match self.update_channel_rx.try_recv() {
                Ok(msg) => {
                    Ok(Some(msg))
                }
                Err(err) => {
                    match err {
                        TryRecvError::ChannelEmpty | TryRecvError::Timeout => {
                            Ok(None)
                        }
                        TryRecvError::ChannelDc => {
                            Err(anyhow!("Reconfig message channel has disconnected?"))
                        }
                    }
                }
            }
        }
    }
}

impl NodeInfo {
    pub fn new(node_id: NodeId, node_type: NodeType, public_key: PublicKey, addr: PeerAddr) -> Self {
        NodeInfo {
            node_id,
            node_type,
            public_key,
            addr,
        }
    }
}

impl ReconfigurationNetworkUpdate for ReconfigurationMessageHandler {
    fn send_reconfiguration_update(&self, update: NetworkUpdateMessage) -> Result<()> {
        self.update_channel_tx.send(update)
    }
}
