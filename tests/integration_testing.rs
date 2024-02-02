pub mod integration_testing {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use atlas_common::crypto::signature::{KeyPair, PublicKey};
    use atlas_common::node_id::{NodeId, NodeType};
    use atlas_common::peer_addr::PeerAddr;
    use atlas_communication::reconfiguration::NetworkInformationProvider;

    pub struct NodeInfo<K> {
        id: NodeId,
        addr: PeerAddr,
        node_type: NodeType,
        key: K
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
}
#[cfg(test)]
mod conn_testing {

    #[test]
    pub fn test_connection() {
        
        
        
    }

}
