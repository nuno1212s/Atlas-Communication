#![feature(return_position_impl_trait_in_trait)]
#![feature(inherent_associated_types)]

use std::sync::Arc;

use getset::{CopyGetters, Getters};

use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::prng::ThreadSafePrng;

use crate::byte_stub::{ByteNetworkController, ByteNetworkControllerInit, ByteNetworkStub, PeerConnectionManager};
use crate::byte_stub::incoming::PeerIncomingConnection;
use crate::lookup_table::EnumLookupTable;
use crate::reconfiguration::{NetworkInformationProvider, ReconfigurationMessageHandler};
use crate::serialization::Serializable;
use crate::stub::{ApplicationStub, BatchedModuleIncomingStub, NetworkStub, OperationStub, ReconfigurationStub, RegularNetworkStub, StateProtocolStub};

pub mod config;
pub mod byte_stub;
pub mod stub;
pub mod message;
pub mod reconfiguration;
pub mod lookup_table;
pub mod serialization;
pub mod message_signing;
mod message_ingestion;
pub mod metric;
mod message_outgoing;


/// The struct that coordinates the entire network stack
/// We have all of the abstractions here, as we want to handle as many
/// possible combinations of implementations as possible
///
/// This module of Atlas is meant to translate our messages into their
/// byte level representation, so they can be safely sent through the
/// underlying network implementation, effectively making this completely
/// abstract on the network type it's running in
#[derive(CopyGetters, Getters)]
pub struct NetworkManagement<NI, CN, BN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {
    // The ID of our node
    #[get_copy = "pub(crate)"]
    id: NodeId,
    // Information about the topology of the network
    #[getset(get = "pub(crate)")]
    network_info: Arc<NI>,
    // The thread safe random number generator
    #[get = "pub(crate)"]
    rng: Arc<ThreadSafePrng>,
    // The controller for all the connections that are incoming into our node
    #[get = "pub(crate)"]
    conn_manager: PeerConnectionManager<CN, R, O, S, A, EnumLookupTable<R, O, S, A>>,
    // The byte level network controller
    #[get = "pub"]
    byte_network_controller: BN,
}

pub type NodeInputStub<R, O, S, A> = PeerIncomingConnection<R, O, S, A, EnumLookupTable<R, O, S, A>>;
pub type NodeStubController<CN, R, O, S, A> = PeerConnectionManager<CN, R, O, S, A, EnumLookupTable<R, O, S, A>>;

impl<NI, CN, BN, R, O, S, A> NetworkManagement<NI, CN, BN, R, O, S, A>
    where R: Serializable + 'static, O: Serializable + 'static,
          S: Serializable + 'static, A: Serializable + 'static,
          BN: Clone, CN: Clone
{
    type NetworkController = PeerConnectionManager<CN, R, O, S, A, EnumLookupTable<R, O, S, A>>;

    type InputStub = PeerIncomingConnection<R, O, S, A, EnumLookupTable<R, O, S, A>>;

    pub fn initialize(network_info: Arc<NI>, config: BN::Config) -> Result<(Arc<Self>, ReconfigurationMessageHandler)>
        where BN: ByteNetworkControllerInit<NI, PeerConnectionManager<CN, R, O, S, A, EnumLookupTable<R, O, S, A>>, CN, PeerIncomingConnection<R, O, S, A, EnumLookupTable<R, O, S, A>>>,
              NI: NetworkInformationProvider,
              CN: ByteNetworkStub {
        let reconf = ReconfigurationMessageHandler::initialize();

        let our_id = network_info.get_own_id();
        let our_type = network_info.get_own_node_type();

        let lookup_table = EnumLookupTable::default();

        let rng = Arc::new(ThreadSafePrng::new());

        let connection_controller = PeerConnectionManager::initialize(our_id, our_type, lookup_table, rng.clone())?;

        // Initialize the underlying byte level network controller
        let network_controller = BN::initialize_controller(reconf.clone(), network_info.clone(), config, connection_controller.clone())?;

        Ok((Arc::new(Self {
            id: our_id,
            network_info,
            rng,
            conn_manager: connection_controller,
            byte_network_controller: network_controller,
        }), reconf))
    }

    pub fn init_op_stub(&self) -> OperationStub<NI, CN, BN::ConnectionController, R, O, S, A>
        where BN: ByteNetworkController, {
        OperationStub::new(self, self.byte_network_controller().connection_controller().clone())
    }

    pub fn init_reconf_stub(&self) -> ReconfigurationStub<NI, CN, BN::ConnectionController, R, O, S, A>
        where BN: ByteNetworkController, {
        ReconfigurationStub::new(self, self.byte_network_controller().connection_controller().clone())
    }

    pub fn init_state_stub(&self) -> StateProtocolStub<NI, CN, BN::ConnectionController, R, O, S, A>
        where BN: ByteNetworkController, {
        StateProtocolStub::new(self, self.byte_network_controller().connection_controller().clone())
    }

    pub fn init_app_stub(&self) -> ApplicationStub<NI, CN, BN::ConnectionController, R, O, S, A>
        where BN: ByteNetworkController, {
        ApplicationStub::new(self, self.byte_network_controller().connection_controller().clone())
    }
}

impl<NI, CN, BN, R, O, S, A> Clone for NetworkManagement<NI, CN, BN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone, CN: Clone {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            network_info: self.network_info.clone(),
            rng: self.rng.clone(),
            conn_manager: self.conn_manager.clone(),
            byte_network_controller: self.byte_network_controller.clone(),
        }
    }
}