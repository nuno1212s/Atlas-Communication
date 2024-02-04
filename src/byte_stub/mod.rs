use std::sync::{Arc, Mutex};
use std::thread::Thread;
use std::time::Duration;

use anyhow::anyhow;
use enum_map::EnumMap;
use getset::{CopyGetters, Getters};
use strum::IntoEnumIterator;

use atlas_common::collections::HashMap;
use atlas_common::error::*;
use atlas_common::node_id::{NodeId, NodeType};
use atlas_common::prng::ThreadSafePrng;
use crate::byte_stub::connections::NetworkConnectionController;

use crate::byte_stub::incoming::{PeerIncomingConnection, PeerStubController, PeerStubLookupTable, pooled_stub, unpooled_stub};
use crate::byte_stub::outgoing::loopback::LoopbackOutgoingStub;
use crate::byte_stub::outgoing::PeerOutgoingConnection;
use crate::lookup_table::{EnumLookupTable, LookupTable, MessageModule};
use crate::message::{StoredMessage, WireMessage};
use crate::reconfiguration::{NetworkInformationProvider, ReconfigurationMessageHandler};
use crate::serialization::Serializable;
use crate::stub::{BatchedModuleIncomingStub, ModuleIncomingStub};

pub mod incoming;
pub(crate) mod outgoing;
pub mod connections;

pub(crate) const MODULES: usize = enum_map::enum_len::<MessageModule>();

/// The byte network controller.
///
/// Meant to store type info and provide access to multiple things at the byte networking level
pub trait ByteNetworkController: Send + Sync + Clone {
    ///  The configuration type
    type Config: Send + 'static;

    /// The connection controller type, used to instruct the byte network layer
    /// to query the network level connections
    type ConnectionController: NetworkConnectionController;

    /// Get the reference to this controller's connection controller
    fn connection_controller(&self) -> &Arc<Self::ConnectionController>;
}

/// The
/// The generics are meant for:
/// NI: NetworkInformationProvider
/// NSC: The Node stub controller
/// BS: The byte network stub, which is connection oriented
/// IS: The incoming stub, which is connection oriented
pub trait ByteNetworkControllerInit<NI, NSC, BS, IS>: ByteNetworkController {
    fn initialize_controller(reconf: ReconfigurationMessageHandler,
                             network_info: Arc<NI>,
                             config: Self::Config,
                             stub_controllers: NSC) -> Result<Self>
        where Self: Sized,
              NI: NetworkInformationProvider,
              BS: ByteNetworkStub,
              IS: NodeIncomingStub,
              NSC: NodeStubController<BS, IS>;
}

/// The network stub for byte messages (after they have gone through the serialization process)
/// This is connection oriented, meaning each Stub which implements this trait should only
/// be referring to a single connection to a single peer
pub trait ByteNetworkStub: Send + Sync + Clone {
    // Dispatch a message to the peer this stub is responsible for.
    fn dispatch_message(&self, message: WireMessage) -> Result<()>;
}

/// The stub controller, responsible for generating and managing stubs for connected peers.
///
/// See why we chose a model where we orient ourselves around the peer instead of the message
/// in [NodeIncomingStub]. It's basically revolving around maintaining context, reducing lookups on
/// connection maps and non static look up tables (which would have to be protected with locks since they
/// would have to be modified on the fly).
///
/// This is a trait (and not directly the [PeerConnectionManager] which implements this trait and its behaviours)
/// because we want to obscure the generic information found at this module's level (R, O, S, A) from the byte network
/// level, such that the byte network only has to deal with bytes and a small amount of generics.
pub trait NodeStubController<BS, IS>: Send + Sync + Clone {
    // Check if a given node has a stub registered.
    fn has_stub_for(&self, node: &NodeId) -> bool;

    /// Generate a stub for a given node connection.
    /// Accepts the node id, the type of the node and the output stub that we can use to send byte messages (already serialized)
    ///
    /// Returns an implementation of [NodeIncomingStub] that we can use to handle messages we have received from that node.
    /// By handle we mean deserialize, verify and pass on to the correct stub
    fn generate_stub_for(&self, node: NodeId, node_type: NodeType, byte_stub: BS) -> Result<IS>
        where BS: ByteNetworkStub, IS: NodeIncomingStub;

    // Get the stub that is responsible for handling messages from a given node.
    fn get_stub_for(&self, node: &NodeId) -> Option<IS> where IS: NodeIncomingStub;

    // Shutdown the active stubs for a given node (effectively closing the connection)
    fn shutdown_stubs_for(&self, node: &NodeId);
}

/// The stub that is responsible for handling messages from a given node.
/// This stub will be produced by [NodeStubController::get_or_generate_stub_for]
/// and will then be responsible for handling all messages we received from that given
/// node.
///
/// This is architectured in this way to facilitate connection oriented handling instead of message
/// oriented handling, which involves much more concurrency and therefore much less performance.
pub trait NodeIncomingStub: Send + Sync + Clone {
    /// Handle a binary message being received, sent from the peer this stub is responsible for.
    /// This should trigger all necessary actions to handle the message. (Deserialization, signature verification, etc.)
    fn handle_message<NI>(&self, network_info: &Arc<NI>, message: WireMessage) -> Result<()>
        where NI: NetworkInformationProvider + 'static;
}

/// The map of endpoints, connecting each MessageModule with the given
/// receiver for that module
pub struct PeerStubEndpoints<R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    stub_output_map: EnumMap<MessageModule, ModuleStubEndPoint<R, O, S, A>>,
}

/// The various possible receivers for a given message module
/// These have to be differentiated because of the implementations for Module Input, namely
/// [BatchedModuleIncomingStub] and [ModuleIncomingStub]
///
/// This is what is going to be used by the final message stub modules in order to receive the
/// processed messages
pub enum ModuleStubEndPoint<R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    Reconfiguration(StubEndpoint<R::Message>),
    Protocol(StubEndpoint<O::Message>),
    StateProtocol(StubEndpoint<S::Message>),
    Application(StubEndpoint<A::Message>),
}

/// The output stub for a given message module
///
#[derive(Clone)]
pub enum StubEndpoint<M> where M: Send {
    Unpooled(unpooled_stub::UnpooledStubRX<StoredMessage<M>>),
    Pooled(pooled_stub::PooledStubOutput<StoredMessage<M>>),
}

/// The active stubs, connecting to a given peer
#[derive(Getters)]
pub struct ActiveConnections<CN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    id: NodeId,
    #[get = "pub"]
    loopback: PeerConnection<CN, R, O, S, A, L>,
    connection_map: Mutex<HashMap<NodeId, PeerConnection<CN, R, O, S, A, L>>>,
}

/// The connection manager for a given peer
///
/// This struct implements the [NodeStubController] trait, which means it is responsible for
/// generating and managing stubs for connected peers. These stubs will then be used in the byte layer to
/// propagate messages upwards
#[derive(Getters)]
pub struct PeerConnection<CN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    #[get = "pub"]
    incoming_connection: PeerIncomingConnection<R, O, S, A, L>,
    #[get = "pub"]
    outgoing_connection: PeerOutgoingConnection<CN, R, O, S, A>,
}

/// The peer connection manager, responsible for generating and managing connections for a given peer
///
/// Handles all connections , outgoing and incoming
#[derive(Getters, CopyGetters)]
pub struct PeerConnectionManager<CN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    // The id of the node this connection manager is responsible for
    #[get_copy = "pub"]
    node_id: NodeId,
    // The look up table, common to all peers, based on the lookup table abstraction
    #[get = "pub"]
    lookup_table: L,
    #[get = "pub"]
    rng: Arc<ThreadSafePrng>,
    controller: Arc<PeerStubController<R, O, S, A>>,
    #[get = "pub"]
    endpoints: Arc<PeerStubEndpoints<R, O, S, A>>,
    connections: Arc<ActiveConnections<CN, R, O, S, A, L>>,
}

impl<CN, R, O, S, A, L> PeerConnectionManager<CN, R, O, S, A, L>
    where L: Send + Clone,
          CN: Clone,
          R: Serializable, O: Serializable,
          S: Serializable, A: Serializable, {
    pub fn initialize(id: NodeId, node_type: NodeType, lookup_table: L, rng: Arc<ThreadSafePrng>) -> Result<Self>
        where L: LookupTable<R, O, S, A> {
        let (stub_controller, endpoints) = PeerStubController::initialize_controller(id, node_type)?;

        let loopback = Self::initialize_loopback(&stub_controller, lookup_table.clone(), id)?;

        let active_conns = ActiveConnections::init(id, loopback);

        Ok(Self {
            node_id: id,
            lookup_table,
            rng,
            controller: Arc::new(stub_controller),
            endpoints: Arc::new(endpoints),
            connections: Arc::new(active_conns),
        })
    }

    fn initialize_loopback(controller: &PeerStubController<R, O, S, A>, l_table: L, node: NodeId) -> Result<PeerConnection<CN, R, O, S, A, L>>
        where L: LookupTable<R, O, S, A> {
        let table = Self::initialize_stub_lookup_table(controller, node)?;

        let loopback = PeerOutgoingConnection::LoopbackStub(LoopbackOutgoingStub::init(table.clone()));

        Ok(PeerConnection {
            incoming_connection: PeerIncomingConnection::initialize_incoming_conn(l_table, table),
            outgoing_connection: loopback,
        })
    }

    fn initialize_stub_lookup_table(controller: &PeerStubController<R, O, S, A>, node_id: NodeId) -> Result<PeerStubLookupTable<R, O, S, A>> {
        // Initialize all of the stubs required to create a new connection
        let mut enum_array = Vec::with_capacity(enum_map::enum_len::<MessageModule>());

        // Initialize all of the stubs required to create a new connection
        // TODO: In the future, maybe add an ability to specify which stubs we want to initialize or not
        for module in MessageModule::iter() {
            let controller = controller.get_stub_controller_for(&module);

            enum_array.push(controller.initialize_stud_for(node_id));
        }

        Ok(EnumMap::from_array(from_arr::<_, MODULES>(enum_array)?).into())
    }

    /// Initialize an incoming connection for a given node
    /// This will initialize all the stubs required to handle messages from that node
    pub fn initialize_incoming_connection_for(&self, node_id: NodeId) -> Result<PeerIncomingConnection<R, O, S, A, L>>
        where L: LookupTable<R, O, S, A> {
        let lookup_table = self.lookup_table.clone();

        Ok(PeerIncomingConnection::initialize_incoming_conn(lookup_table, Self::initialize_stub_lookup_table(&self.controller, node_id)?))
    }

    /// Initialize an outgoing connection for a given node, given a byte level stub to that
    /// node (which is connection oriented)
    pub fn initialize_outgoing_connection_for(&self, _node_id: NodeId, node_stub: CN) -> Result<PeerOutgoingConnection<CN, R, O, S, A>> {
        let connection = PeerOutgoingConnection::OutgoingStub(node_stub);

        Ok(connection)
    }

    /// Initialize a connection to a given peer
    pub fn initialize_connection(&self, node: NodeId, node_stub: CN)
                                 -> Result<PeerConnection<CN, R, O, S, A, L>> where L: LookupTable<R, O, S, A> {
        Ok(PeerConnection {
            incoming_connection: self.initialize_incoming_connection_for(node)?,
            outgoing_connection: self.initialize_outgoing_connection_for(node, node_stub)?,
        })
    }

    pub fn get_connection_to_node(&self, node: &NodeId) -> Option<PeerConnection<CN, R, O, S, A, L>> {
        self.connections.get_connection(node)
    }
}

impl<CN, R, O, S, A, L> NodeStubController<CN, PeerIncomingConnection<R, O, S, A, L>> for PeerConnectionManager<CN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          L: LookupTable<R, O, S, A>,
          CN: Send + Clone + Sync, {
    fn has_stub_for(&self, node: &NodeId) -> bool {
        self.connections.has_connection(node) || *node == self.node_id
    }

    fn generate_stub_for(&self, node: NodeId, node_type: NodeType, byte_stub: CN) -> Result<PeerIncomingConnection<R, O, S, A, L>>
        where CN: ByteNetworkStub {
        let connection = self.initialize_connection(node, byte_stub)?;

        let incoming_conn = connection.incoming_connection.clone();

        self.connections.add_connection(node, connection);

        Ok(incoming_conn)
    }

    fn get_stub_for(&self, node: &NodeId) -> Option<PeerIncomingConnection<R, O, S, A, L>> {
        self.connections.get_connection(node).map(|c| c.incoming_connection)
    }

    fn shutdown_stubs_for(&self, node: &NodeId) {
        self.connections.remove_connection(node);
    }
}


impl<CN, R, O, S, A, L> ActiveConnections<CN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable, CN: Clone, L: Clone {
    pub fn has_connection(&self, node: &NodeId) -> bool {
        self.connection_map.lock().unwrap().contains_key(node)
    }

    pub fn get_connection(&self, node: &NodeId) -> Option<PeerConnection<CN, R, O, S, A, L>> {
        if *node == self.id {
            return Some(self.loopback.clone());
        }

        self.connection_map.lock().unwrap().get(node).cloned()
    }

    pub fn add_connection(&self, node: NodeId, connection: PeerConnection<CN, R, O, S, A, L>) {
        self.connection_map.lock().unwrap().insert(node, connection);
    }

    pub fn remove_connection(&self, node: &NodeId) -> Option<PeerConnection<CN, R, O, S, A, L>> {
        self.connection_map.lock().unwrap().remove(node)
    }
}

impl<R, O, S, A> PeerStubEndpoints<R, O, S, A>
    where R: Serializable, O: Serializable, S: Serializable, A: Serializable {
    pub fn get_endpoint_for_module(&self, module: &MessageModule) -> &ModuleStubEndPoint<R, O, S, A> {
        &self.stub_output_map[module.clone()]
    }
}

impl<CN, R, O, S, A, L> Clone for PeerConnectionManager<CN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          L: Clone, CN: Clone {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            lookup_table: self.lookup_table.clone(),
            rng: self.rng.clone(),
            controller: self.controller.clone(),
            endpoints: self.endpoints.clone(),
            connections: self.connections.clone(),
        }
    }
}

pub(crate) fn from_arr<T, const N: usize>(v: Vec<T>) -> Result<[T; N]> {
    v.try_into()
        .map_err(|v: Vec<T>| anyhow!("Expected a Vec of length {} but it was {}", N, v.len()))
}

impl<CN, R, O, S, A, L> ActiveConnections<CN, R, O, S, A, L>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    fn init(id: NodeId, loopback: PeerConnection<CN, R, O, S, A, L>) -> Self {
        Self {
            id,
            loopback,
            connection_map: Mutex::new(Default::default()),
        }
    }
}

impl<R, O, S, A> ModuleStubEndPoint<R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    // Unwrapping into a reconfiguration endpoint
    pub fn into_reconfig_endpoint(self) -> StubEndpoint<R::Message> {
        match self {
            ModuleStubEndPoint::Reconfiguration(endpoint) => {
                endpoint
            }
            _ => unreachable!("Unpacked a wrong endpoint?")
        }
    }

    // Unwrapping into a protocol endpoint
    pub fn into_protocol_endpoint(self) -> StubEndpoint<O::Message> {
        match self {
            ModuleStubEndPoint::Protocol(endpoint) => {
                endpoint
            }
            _ => unreachable!("Unpacked a wrong endpoint?")
        }
    }

    // Unwrapping into a state protocol endpoint
    pub fn into_state_protocol_endpoint(self) -> StubEndpoint<S::Message> {
        match self {
            ModuleStubEndPoint::StateProtocol(endpoint) => {
                endpoint
            }
            _ => unreachable!("Unpacked a wrong endpoint?")
        }
    }

    // Unwrapping into an application endpoint
    pub fn into_application_endpoint(self) -> StubEndpoint<A::Message> {
        match self {
            ModuleStubEndPoint::Application(endpoint) => {
                endpoint
            }
            _ => unreachable!("Unpacked a wrong endpoint?")
        }
    }
}

impl<R, O, S, A> Clone for ModuleStubEndPoint<R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    fn clone(&self) -> Self {
        match self {
            ModuleStubEndPoint::Reconfiguration(endpoint) => {
                ModuleStubEndPoint::Reconfiguration(endpoint.clone())
            }
            ModuleStubEndPoint::Protocol(endpoint) => {
                ModuleStubEndPoint::Protocol(endpoint.clone())
            }
            ModuleStubEndPoint::StateProtocol(endpoint) => {
                ModuleStubEndPoint::StateProtocol(endpoint.clone())
            }
            ModuleStubEndPoint::Application(endpoint) => {
                ModuleStubEndPoint::Application(endpoint.clone())
            }
        }
    }
}

impl<M> ModuleIncomingStub<M> for StubEndpoint<M>
    where M: Send + Clone {
    fn pending_rqs(&self) -> usize {
        match self {
            StubEndpoint::Unpooled(unpooled_stub) => {
                unpooled_stub.pending_rqs()
            }
            _ => unreachable!()
        }
    }

    fn receive_messages(&self) -> Result<StoredMessage<M>> {
        match self {
            StubEndpoint::Unpooled(unpooled_stub) => {
                unpooled_stub.receive_messages()
            }
            _ => unreachable!()
        }
    }

    fn try_receive_messages(&self, timeout: Option<Duration>) -> Result<Option<StoredMessage<M>>> {
        match self {
            StubEndpoint::Unpooled(unpooled_stub) => {
                unpooled_stub.try_receive_messages(timeout)
            }
            _ => unreachable!()
        }
    }
}

impl<M> BatchedModuleIncomingStub<M> for StubEndpoint<M> where M: Send {
    fn receive_messages(&self) -> Result<Vec<StoredMessage<M>>> {
        match &self {
            StubEndpoint::Pooled(pooled) => {
                pooled.receive_messages()
            }
            _ => unreachable!()
        }
    }

    fn try_receive_messages(&self, timeout: Option<Duration>) -> Result<Option<Vec<StoredMessage<M>>>> {
        match &self {
            StubEndpoint::Pooled(pooled) => {
                pooled.try_receive_messages(timeout)
            }
            _ => unreachable!()
        }
    }
}

impl<CN, R, O, S, A, L> Clone for PeerConnection<CN, R, O, S, A, L>
    where R: Serializable, O: Serializable, S: Serializable, A: Serializable,
          L: Clone, CN: Clone
{
    fn clone(&self) -> Self {
        Self {
            incoming_connection: self.incoming_connection.clone(),
            outgoing_connection: self.outgoing_connection.clone(),
        }
    }
}