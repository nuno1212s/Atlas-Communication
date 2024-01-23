#![feature(return_position_impl_trait_in_trait)]

use std::sync::Arc;

use getset::{CopyGetters, Getters};

use atlas_common::node_id::NodeId;
use atlas_common::prng::ThreadSafePrng;

use crate::byte_stub::{ByteNetworkController, PeerConnectionManager};
use crate::lookup_table::EnumLookupTable;
use crate::reconfiguration_node::NetworkInformationProvider;
use crate::serialization::Serializable;
use crate::stub::{BatchedModuleIncomingStub, NetworkStub, RegularNetworkStub};

pub mod byte_stub;
pub mod stub;
pub mod message;
pub mod reconfiguration_node;
pub mod lookup_table;
mod message_signing;
mod serialization;
mod message_ingestion;
mod metrics;
mod message_outgoing;
mod config;


/// The struct that coordinates the entire network stack
/// We have all of the abstractions here, as we want to handle as many
/// possible combinations of implementations as possible
///
/// This module of Atlas is meant to translate our messages into their
/// byte level representation, so they can be safely sent through the
/// underlying network implementation, effectively making this completely
/// abstract on the network type it's running in
#[derive(CopyGetters, Getters, Clone)]
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

impl<NI, CN, BN, R, O, S, A> NetworkManagement<NI, CN, BN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          BN: Clone {

    pub fn initialize(network_info: Arc<NI>) -> Arc<Self>
        where BN: ByteNetworkController<NI>,
              NI: NetworkInformationProvider {
        let our_id = network_info.get_own_id();
        let our_type = network_info.get_own_node_type();

        let lookup_table = EnumLookupTable::default();

        let connection_controller = PeerConnectionManager::initialize(our_id, our_type, lookup_table);

        let rng = Arc::new(ThreadSafePrng::new());

        let network_controller = BN::initialize_controller(network_info.clone(), connection_controller.clone());

        Arc::new(Self {
            id: our_id,
            network_info,
            rng,
            conn_manager: connection_controller,
            byte_network_controller: network_controller,
        })
    }
}