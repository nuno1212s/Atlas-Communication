use std::sync::Mutex;

use getset::{CopyGetters, Getters};

use atlas_common::collections::HashMap;
use atlas_common::node_id::NodeId;

use crate::byte_stub::ByteNetworkStub;
use crate::byte_stub::incoming::PeerStubLookupTable;
use crate::byte_stub::outgoing::loopback::LoopbackOutgoingStub;
use crate::serialization::Serializable;

mod loopback;

/// Active outgoing connections
pub struct ActiveOutgoingConnections<CN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable
{
    connection_map: Mutex<HashMap<NodeId, PeerOutgoingStub<CN, R, O, S, A>>>,
}

/// The connection manager for outgoing connections
#[derive(Getters, CopyGetters)]
pub struct PeerOutgoingConnectionManager<CN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    #[get_copy = "pub(crate)"]
    own_id: NodeId,
    active_connections: ActiveOutgoingConnections<CN, R, O, S, A>,
    #[get = "pub(crate)"]
    loopback_stub: PeerOutgoingStub<CN, R, O, S, A>,
}

/// the outgoing stub possibilities.
pub enum PeerOutgoingConnection<CN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    OutgoingStub(CN),
    LoopbackStub(LoopbackOutgoingStub<R, O, S, A>),
}

/// The outgoing stub for a peer.
///
/// Used to send messages to a peer.
/// If we were just doing binary messages, this would just be an impl [ByteNetworkStub].
/// Since we also want to handle loopback efficiently (without going through the binary stack, which
/// requires deserializing the message without need since we already have the parsed message and the digest/signature).
#[derive(Getters, CopyGetters)]
pub struct PeerOutgoingStub<CN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    #[get_copy = "pub(crate)"]
    node: NodeId,
    #[get = "pub(crate)"]
    stub: PeerOutgoingConnection<CN, R, O, S, A>,
}

impl<CN, R, O, S, A> PeerOutgoingStub<CN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    pub fn initialize_loopback_stub(node: NodeId, peer_stub: PeerStubLookupTable<R, O, S, A>) -> Self {
        Self {
            node,
            stub: PeerOutgoingConnection::LoopbackStub(LoopbackOutgoingStub::init(peer_stub)),
        }
    }

    pub fn initialize_outgoing_stub(node: NodeId, stub: CN) -> Self {
        Self {
            node,
            stub: PeerOutgoingConnection::OutgoingStub(stub),
        }
    }
}

impl<CN, R, O, S, A> PeerOutgoingConnectionManager<CN, R, O, S, A>
where R: Serializable, O: Serializable,
      S: Serializable, A: Serializable {


    pub fn new(own_id: NodeId) -> Self {
        Self {
            own_id,
            active_connections: ActiveOutgoingConnections::new(),
            loopback_stub,
        }
    }





}