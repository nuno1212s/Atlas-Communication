use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::time::Duration;

use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use crate::byte_stub::{ModuleStubEndPoint, StubEndpoint};
use crate::lookup_table::MessageModule;

use crate::message::{SerializedMessage, StoredMessage, StoredSerializedMessage};
use crate::NetworkManagement;
use crate::reconfiguration_node::NetworkInformationProvider;
use crate::serialization::Serializable;

pub(crate) mod outgoing;
mod incoming;

/// The outgoing stub trait, valid for any type of messages
pub trait ModuleOutgoingStub<M> {
    /// Sends a message to a given target.
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the target or err if not. No other checks are made
    /// on the success of the message dispatch
    fn send(&self, message: M, target: NodeId, flush: bool) -> Result<()>;

    /// Sends a signed message to a given target
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the target or err if not. No other checks are made
    /// on the success of the message dispatch
    fn send_signed(&self, message: M, target: NodeId, flush: bool) -> Result<()>;

    /// Broadcast a message to all of the given targets
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the targets or err if not. No other checks are made
    /// on the success of the message dispatch
    fn broadcast(&self, message: M, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>>;

    /// Broadcast a signed message for all of the given targets
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the targets or err if not. No other checks are made
    /// on the success of the message dispatch
    fn broadcast_signed(&self, message: M, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>>;

    /// Serialize a message to a given target.
    /// Creates the serialized byte buffer along with the header, so we can send it later.
    fn serialize_digest_message(&self, message: M) -> Result<(SerializedMessage<M>, Digest)>;

    /// Broadcast the serialized messages provided.
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the targets or err if not. No other checks are made
    /// on the success of the message dispatch
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedMessage<M>>) -> std::result::Result<(), Vec<NodeId>>;
}

/// The trait that directs the ordering of operations
/// for an incoming stub.
///
/// These methods serve to receive messages from the network layer, for a specific module,
/// with the separations described in [MessageModule]
pub trait ModuleIncomingStub<M>: Send + Sync + Clone {
    /// Get the amount of pending requests contained in this stub
    fn pending_rqs(&self) -> usize;

    /// Receive a message from this stub.
    /// Blocks until a message is available
    fn receive_messages(&self) -> Result<StoredMessage<M>>;

    /// Try to receive messages. If no timeout is provided, then
    /// we immediately return whether a message is present or not (with the corresponding [Option] value).
    /// If a timeout is provided, we wait until the provided timeout
    fn try_receive_messages(&self, timeout: Option<Duration>) -> Result<Option<StoredMessage<M>>>;
}

/// The trait that directs the ordering of operations
/// for a batched incoming stub. This is similar to the [ModuleIncomingStub] trait, but
/// this one is used for modules that have a batched message handling (namely [ConnectedPeersGroup] )
pub trait BatchedModuleIncomingStub<M> {
    /// Receive a batch of messages from this stub.
    /// Blocks until a message is available
    fn receive_messages(&self) -> Result<Vec<StoredMessage<M>>>;

    /// Try to receive messages. If no timeout is provided, then
    /// we immediately return whether a message is present or not (with the corresponding [Option] value).
    /// If a timeout is provided, we wait until the provided timeout
    fn try_receive_messages(&self, timeout: Option<Duration>) -> Result<Option<Vec<StoredMessage<M>>>>;
}

/// A basic network stub
pub trait NetworkStub<T> where T: Serializable {
    /// The outgoing message handler.
    type Outgoing: ModuleOutgoingStub<T::Message>;

    fn outgoing_stub(&self) -> &Self::Outgoing;
}

/// An extended network stub, with the incoming request handling stubs as well
pub trait RegularNetworkStub<T>: NetworkStub<T> where T: Serializable {
    type Incoming: ModuleIncomingStub<T::Message>;

    fn incoming_stub(&self) -> &Self::Incoming;
}

/// The batched network stub, with the incoming request handling stubs as well
pub trait BatchedNetworkStub<T>: NetworkStub<T> where T: Serializable {
    type Incoming: BatchedModuleIncomingStub<T::Message>;

    fn incoming_stub(&self) -> &Self::Incoming;
}


/// In reality, this should all be macros, but I'm not into the macro scene
/// And I can't take the time to learn it atm
/// Reconfiguration stub
pub struct ReconfigurationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    network_management: NetworkManagement<NI, CN, BN, R, O, S, A>,
    stub_endpoint: StubEndpoint<R::Message>,
    _p: PhantomData<fn() -> L>,
}

impl<NI, CN, BN, R, O, S, A, L> ReconfigurationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    pub fn new(network_management: NetworkManagement<NI, CN, BN, R, O, S, A>) -> Self {
        let end_point = network_management.conn_manager().endpoints().get_endpoint_for_module(&MessageModule::Reconfiguration);

        let end_point = end_point.into_reconfig_endpoint();

        Self {
            network_management,
            stub_endpoint: end_point,
            _p: Default::default(),
        }
    }
}

impl<NI, CN, BN, R, O, S, A, L> Clone for ReconfigurationStub<NI, CN, BN, R, O, S, A, L>
    where A: Serializable, O: Serializable,
          R: Serializable, S: Serializable,
          BN: Clone {
    fn clone(&self) -> Self {
        Self {
            network_management: self.network_management.clone(),
            stub_endpoint: self.stub_endpoint.clone(),
            _p: Default::default(),
        }
    }
}

impl<NI, CN, BN, R, O, S, A, L> NetworkStub<R> for ReconfigurationStub<NI, CN, BN, R, O, S, A, L>
    where A: Serializable, O: Serializable,
          R: Serializable, S: Serializable,
          BN: Clone, NI: NetworkInformationProvider {
    type Outgoing = Self;

    fn outgoing_stub(&self) -> &Self::Outgoing {
        self
    }
}

/// Operation stub
///
///
pub struct OperationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    network_management: NetworkManagement<NI, CN, BN, R, O, S, A>,
    stub_endpoint: StubEndpoint<O::Message>,
    _p: PhantomData<fn() -> L>,
}

impl<NI, CN, BN, R, O, S, A, L> OperationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    pub fn new(network_management: NetworkManagement<NI, CN, BN, R, O, S, A>) -> Self {
        let end_point = network_management.conn_manager().endpoints().get_endpoint_for_module(&MessageModule::Protocol);

        let end_point = end_point.into_protocol_endpoint();

        Self {
            network_management,
            stub_endpoint: end_point,
            _p: Default::default(),
        }
    }
}

impl<NI, CN, BN, R, O, S, A, L> Clone for OperationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    fn clone(&self) -> Self {
        Self {
            network_management: self.network_management.clone(),
            stub_endpoint: self.stub_endpoint.clone(),
            _p: Default::default(),
        }
    }
}

impl<NI, CN, BN, R, O, S, A, L> NetworkStub<O> for OperationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone, NI: NetworkInformationProvider {
    type Outgoing = Self;

    fn outgoing_stub(&self) -> &Self::Outgoing {
        self
    }
}

/// State protocol stub
pub struct StateProtocolStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    network_management: NetworkManagement<NI, CN, BN, R, O, S, A>,
    stub_endpoint: StubEndpoint<S::Message>,
    _p: PhantomData<fn() -> L>,
}

impl<NI, CN, BN, R, O, S, A, L> StateProtocolStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    pub fn new(network_management: NetworkManagement<NI, CN, BN, R, O, S, A>) -> Self {
        let end_point = network_management.conn_manager().endpoints().get_endpoint_for_module(&MessageModule::StateProtocol);

        let end_point = end_point.into_state_protocol_endpoint();

        Self {
            network_management,
            stub_endpoint: end_point,
            _p: Default::default(),
        }
    }
}

//Clone impl
impl<NI, CN, BN, R, O, S, A, L> Clone for StateProtocolStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    fn clone(&self) -> Self {
        Self {
            network_management: self.network_management.clone(),
            stub_endpoint: self.stub_endpoint.clone(),
            _p: Default::default(),
        }
    }
}

impl<NI, CN, BN, R, O, S, A, L> NetworkStub<S> for StateProtocolStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone, NI: NetworkInformationProvider {
    type Outgoing = Self;

    fn outgoing_stub(&self) -> &Self::Outgoing {
        self
    }
}

/// Application stub
pub struct ApplicationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    network_management: NetworkManagement<NI, CN, BN, R, O, S, A>,
    stub_endpoint: StubEndpoint<A::Message>,
    _p: PhantomData<fn() -> L>,
}

impl<NI, CN, BN, R, O, S, A, L> ApplicationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    pub fn new(network_management: NetworkManagement<NI, CN, BN, R, O, S, A>) -> Self {
        let end_point = network_management.conn_manager().endpoints().get_endpoint_for_module(&MessageModule::Application);

        let end_point = end_point.into_application_endpoint();

        Self {
            network_management,
            stub_endpoint: end_point,
            _p: Default::default(),
        }
    }
}

impl<NI, CN, BN, R, O, S, A, L> Clone for ApplicationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    fn clone(&self) -> Self {
        Self {
            network_management: self.network_management.clone(),
            stub_endpoint: self.stub_endpoint.clone(),
            _p: Default::default(),
        }
    }
}

impl<NI, CN, BN, R, O, S, A, L> NetworkStub<A> for ApplicationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone, NI: NetworkInformationProvider {
    type Outgoing = Self;

    fn outgoing_stub(&self) -> &Self::Outgoing {
        self
    }
}