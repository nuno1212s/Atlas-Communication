use std::collections::BTreeMap;
use std::sync::Arc;

use thiserror::Error;

use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;

use crate::{NetworkNode, NodeIncomingRqHandler};
use crate::message::{SerializedMessage, StoredMessage, StoredSerializedProtocolMessage};
use crate::message_signing::NetworkMessageSignatureVerifier;
use crate::serialize::Serializable;

/// A Network node devoted to handling
pub trait ProtocolNetworkNode<M>: NetworkNode +  Send + Sync where M: Serializable {

    /// Incoming request handler for this node
    type IncomingRqHandler: NodeIncomingRqHandler<StoredMessage<M::Message>>;

    /// The signature verifier for this node
    type NetworkSignatureVerifier: NetworkMessageSignatureVerifier<M, Self::NetworkInfoProvider>;

    /// Get a reference to the incoming request handling
    fn node_incoming_rq_handling(&self) -> &Arc<Self::IncomingRqHandler>;

    /// Sends a message to a given target.
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the target or err if not. No other checks are made
    /// on the success of the message dispatch
    fn send(&self, message: M::Message, target: NodeId, flush: bool) -> Result<()>;

    /// Sends a signed message to a given target
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the target or err if not. No other checks are made
    /// on the success of the message dispatch
    fn send_signed(&self, message: M::Message, target: NodeId, flush: bool) -> Result<()>;

    /// Broadcast a message to all of the given targets
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the targets or err if not. No other checks are made
    /// on the success of the message dispatch
    fn broadcast(&self, message: M::Message, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>>;

    /// Broadcast a signed message for all of the given targets
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the targets or err if not. No other checks are made
    /// on the success of the message dispatch
    fn broadcast_signed(&self, message: M::Message, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>>;

    /// Serialize a message to a given target.
    /// Creates the serialized byte buffer along with the header, so we can send it later.
    fn serialize_digest_message(&self, message: M::Message) -> Result<(SerializedMessage<M::Message>, Digest)>;

    /// Broadcast the serialized messages provided.
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the targets or err if not. No other checks are made
    /// on the success of the message dispatch
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedProtocolMessage<M::Message>>) -> std::result::Result<(), Vec<NodeId>>;
}

#[derive(Error, Debug)]
pub enum SendProtocolMessage {
    #[error("There are no established connection to node {0:?}")]
    UnknownNodes(Vec<NodeId>)
}