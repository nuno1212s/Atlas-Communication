use log::error;

use atlas_common::crypto::signature::PublicKey;
use atlas_common::error::*;
use atlas_common::node_id::{NodeId, NodeType};

use crate::byte_stub::PeerConnectionManager;
use crate::reconfiguration::{NetworkUpdateMessage, ReconfigurationMessageHandler};
use crate::serialization::Serializable;

pub(super) trait PendingConnectionManagement {
    type Conn;

    fn has_pending_connection(&self, node: &NodeId) -> bool;

    fn get_connection_to_node(&self, node: &NodeId) -> Option<Self::Conn>;

    fn upgrade_connection(&self, node: &NodeId, node_type: NodeType, key: PublicKey) -> Result<()>;
}

pub(super) fn initialize_network_info_handle<PM>(reconfiguration_message_handler: ReconfigurationMessageHandler,
                                                 connection_handler: PM) where
    PM: PendingConnectionManagement {
    loop {
        match reconfiguration_message_handler.receive_network_update() {
            Ok(message) => {
                match message {
                    NetworkUpdateMessage::NodeConnectionPermitted(node, node_type, key) => {
                        if connection_handler.has_pending_connection(&node) {
                            if let Err(err) = connection_handler.upgrade_connection(&node, node_type, key) {
                                error!("Failed to upgrade connection to node {:?} of type {:?}. Error: {:?}", node, node_type, err);

                                continue;
                            }
                        } else {
                            error!("Received a connection permitted message for a node that is not pending connection. Node: {:?}", node);
                        }
                    }
                }
            }
            Err(err) => {
                error!("Failed to receive network update message. Error: {:?}", err);

                break;
            }
        }
    }
}