use anyhow::{anyhow, Context};
use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, OneShotRx};
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::node_id::{NodeId, NodeType};
use atlas_common::peer_addr::PeerAddr;
use atlas_communication::byte_stub::connections::NetworkConnectionController;
use atlas_communication::byte_stub::incoming::PeerIncomingConnection;
use atlas_communication::byte_stub::{
    ByteNetworkController, ByteNetworkControllerInit, ByteNetworkStub, NodeIncomingStub,
    NodeStubController, PeerConnectionManager,
};
use atlas_communication::lookup_table::EnumLookupTable;
use atlas_communication::message::{Header, StoredMessage, WireMessage};
use atlas_communication::reconfiguration::{
    NetworkInformationProvider, ReconfigurationMessageHandler,
};
use atlas_communication::serialization::{InternalMessageVerifier, Serializable};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct NodeInfo<K> {
    id: NodeId,
    addr: PeerAddr,
    node_type: NodeType,
    key: K,
}

pub struct MockNetworkInfo {
    own_node: NodeInfo<Arc<KeyPair>>,
    other_nodes: BTreeMap<NodeId, NodeInfo<PublicKey>>,
}

impl NetworkInformationProvider for MockNetworkInfo {
    fn get_own_id(&self) -> NodeId {
        self.own_node.id
    }

    fn get_own_addr(&self) -> PeerAddr {
        self.own_node.addr.clone()
    }

    fn get_key_pair(&self) -> &Arc<KeyPair> {
        &self.own_node.key
    }

    fn get_own_node_type(&self) -> NodeType {
        self.own_node.node_type
    }

    fn get_node_type(&self, node: &NodeId) -> Option<NodeType> {
        self.other_nodes.get(node).map(|info| info.node_type)
    }

    fn get_public_key(&self, node: &NodeId) -> Option<PublicKey> {
        self.other_nodes.get(node).map(|info| info.key.clone())
    }

    fn get_addr_for_node(&self, node: &NodeId) -> Option<PeerAddr> {
        self.other_nodes.get(node).map(|info| info.addr.clone())
    }
}

struct MockNetworkInfoFactory {
    nodes: BTreeMap<NodeId, NodeInfo<Arc<KeyPair>>>,
}

impl MockNetworkInfoFactory {
    const PORT: u32 = 10000;

    fn initialize_for(node_count: usize) -> atlas_common::error::Result<Self> {
        let buf = [0; 32];
        let mut map = BTreeMap::default();

        for node_id in 0..node_count {
            let key = KeyPair::from_bytes(buf.as_slice())?;

            let info = NodeInfo {
                id: NodeId::from(node_id as u32),
                addr: PeerAddr::new(
                    format!("127.0.0.1:{}", Self::PORT + (node_id as u32)).parse()?,
                    String::from("localhost"),
                ),
                node_type: NodeType::Replica,
                key: Arc::new(key),
            };

            map.insert(info.id.clone(), info);
        }

        Ok(Self { nodes: map })
    }

    fn generate_network_info_for(
        &self,
        node_id: NodeId,
    ) -> atlas_common::error::Result<MockNetworkInfo> {
        let own_network_id = self
            .nodes
            .get(&node_id)
            .ok_or(anyhow!("Node not found"))?
            .clone();

        let other_nodes: BTreeMap<NodeId, NodeInfo<PublicKey>> = self
            .nodes
            .iter()
            .filter(|(id, _)| **id != node_id)
            .map(|(id, info)| {
                (
                    id.clone(),
                    NodeInfo {
                        id: info.id.clone(),
                        addr: info.addr.clone(),
                        node_type: info.node_type,
                        key: PublicKey::from(info.key.public_key()),
                    },
                )
            })
            .collect();

        Ok(MockNetworkInfo {
            own_node: own_network_id,
            other_nodes,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MockMessage;

struct MockProtocol;

impl Serializable for MockProtocol {
    type Message = MockMessage;
    type Verifier = MockVerifier;
}

struct MockVerifier;

impl InternalMessageVerifier<MockMessage> for MockVerifier {
    fn verify_message<NI>(
        _: &Arc<NI>,
        _: &Header,
        _: &MockMessage,
    ) -> atlas_common::error::Result<()>
    where
        NI: NetworkInformationProvider + 'static,
    {
        Ok(())
    }
}

#[derive(Clone)]
struct MockByteStub(ChannelSyncTx<WireMessage>);

impl ByteNetworkStub for MockByteStub {
    fn dispatch_message(&self, message: WireMessage) -> atlas_common::error::Result<()> {
        // When we dispatch a message, we send it to the other node

        self.0
            .send(message)
            .context("Failed to send message to another node")
    }
}

type LookupTable = EnumLookupTable<MockProtocol, MockProtocol, MockProtocol, MockProtocol>;
type PeerCNNMngmt = PeerConnectionManager<
    MockByteStub,
    MockProtocol,
    MockProtocol,
    MockProtocol,
    MockProtocol,
    LookupTable,
>;

type PeerInnCnn =
    PeerIncomingConnection<MockProtocol, MockProtocol, MockProtocol, MockProtocol, LookupTable>;

/// The byte level mock controller
/// handles faking the byte level network by utilizing channels
/// to actually handle all communication that was meant to be sent over the wire.
///
#[derive(Clone)]
struct MockByteController {
    connection_controller: Arc<MockByteConnectionController>,
}

impl ByteNetworkController for MockByteController {
    type Config = MockByteManagementFactory;
    type ConnectionController = MockByteConnectionController;

    fn connection_controller(&self) -> &Arc<Self::ConnectionController> {
        &self.connection_controller
    }
}

impl<NI> ByteNetworkControllerInit<NI, PeerCNNMngmt, MockByteStub, PeerInnCnn>
    for MockByteController
where
    NI: NetworkInformationProvider,
{
    fn initialize_controller(
        reconf: ReconfigurationMessageHandler,
        network_info: Arc<NI>,
        config: MockByteManagementFactory,
        stub_controllers: PeerCNNMngmt,
    ) -> atlas_common::error::Result<Self>
    where
        Self: Sized,
        NI: NetworkInformationProvider,
    {
        todo!()
    }
}

#[derive(Clone)]
struct MockByteConnectionController {
    /// Receiver for mock messages,
    /// This channel is what is going to be used in other nodes
    /// to send messages to this node
    /// (In that case, it would be the [ChannelSyncTx] end)
    rx: ChannelSyncRx<WireMessage>,

    connected: BTreeMap<NodeId, MockByteStub>,
}

impl NetworkConnectionController for MockByteConnectionController {
    fn has_connection(&self, node: &NodeId) -> bool {
        self.connected.contains_key(node)
    }

    fn currently_connected_node_count(&self) -> usize {
        self.connected.len()
    }

    fn currently_connected_nodes(&self) -> Vec<NodeId> {
        self.connected.keys().cloned().collect()
    }

    fn connect_to_node(
        self: &Arc<Self>,
        node: NodeId,
    ) -> Vec<OneShotRx<atlas_common::error::Result<()>>> {
        todo!()
    }

    fn disconnect_from_node(self: &Arc<Self>, node: &NodeId) -> atlas_common::error::Result<()> {
        todo!()
    }
}

/// The factory for connection controllers
/// This factory contains information on all the registered endpoints,
/// such that it can be used to create the byte level network controller
/// and then the byte level network controller can be used to create the
/// individual node connections (with the help of this factory as well,
/// of course)
#[derive(Clone)]
struct MockByteManagementFactory {
    /// All the registered endpoints for the byte level network
    end_points: Arc<BTreeMap<NodeId, (MockByteStub, ChannelSyncRx<WireMessage>)>>,
}

impl MockByteManagementFactory {
    fn initialize_factory(node_count: u32) -> Self {
        let mut connected = BTreeMap::default();

        for node_id in 0..node_count {
            let node_id = NodeId::from(node_id);

            let (tx, rx) = channel::new_bounded_sync(100, Some(format!("{:?}", node_id).as_str()));

            connected.insert(node_id, (MockByteStub(tx), rx));
        }

        Self {
            end_points: Arc::new(connected),
        }
    }

    fn initialize_controller_for(&self, node: NodeId) -> MockByteConnectionController {
        let (_, rx) = self.end_points.get(&node).unwrap();

        MockByteConnectionController {
            rx: rx.clone(),
            connected: Default::default(),
        }
    }

    fn initialize_stub_for(&self, from: NodeId, to: NodeId) -> MockByteStub {
        let (stub, _) = self.end_points.get(&to).unwrap();

        stub.clone()
    }
}

#[cfg(test)]
mod conn_testing {
    use atlas_communication::NetworkManagement;
    use std::collections::BTreeMap;

    fn setup_nodes(node_count: u32) -> () {}

    #[test]
    pub fn test_connection() {}
}
