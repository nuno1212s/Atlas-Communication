use std::collections::BTreeMap;
use std::iter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use either::Either;
use log::{debug, error};
use smallvec::SmallVec;

use atlas_common::{Err, socket, threadpool};
use atlas_common::crypto::hash::Digest;
use atlas_common::crypto::signature::KeyPair;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::prng::ThreadSafePrng;
use atlas_common::socket::SyncListener;
use atlas_metrics::metrics::metric_duration;

use crate::{FullNetworkNode, NetworkNode, NetworkSendError};
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::config::MioConfig;
use crate::conn_utils::ConnCounts;
use crate::message::{NetworkMessageKind, SerializedMessage, StoredMessage, StoredSerializedNetworkMessage, StoredSerializedProtocolMessage, WireMessage};
use crate::message_signing::DefaultProtocolSignatureVerifier;
use crate::metric::THREADPOOL_PASS_TIME_ID;
use crate::mio_tcp::connections::{Connections, PeerConnection};
use crate::mio_tcp::connections::conn_establish::pending_conn::{NetworkUpdateHandler, PendingConnHandle};
use crate::mio_tcp::connections::epoll_group::{init_worker_group_handle, initialize_worker_group};
use crate::protocol_node::ProtocolNetworkNode;
use crate::reconfiguration_node::{NetworkInformationProvider, ReconfigurationMessageHandler, ReconfigurationNode};
use crate::serialize::{Buf, Serializable};

mod connections;

const NODE_QUORUM_SIZE: usize = 32;

type SendTos<RM, PM, CM> = SmallVec<[SendTo<RM, PM, CM>; NODE_QUORUM_SIZE]>;

/// The node that handles the TCP connections
pub struct MIOTcpNode<NI, RM, PM, CM>
    where NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    id: NodeId,
    // The thread safe random number generator
    rng: Arc<ThreadSafePrng>,
    /// General network information and reconfiguration logic
    reconfiguration: Arc<NI>,
    // The connections that are currently being maintained by us to other peers
    connections: Arc<Connections<NI, RM, PM, CM>>,
    // Handles the incoming reconfiguration messages, which will be handled separately from the
    // Rest of the protocol requests
    reconfig_handling: Arc<ReconfigurationMessageHandler<StoredMessage<RM::Message>>>,
    //Handles the incoming connections' buffering and request collection
    //This is polled by the proposer for client requests and by the
    client_pooling: Arc<PeerIncomingRqHandling<StoredMessage<PM::Message>, StoredMessage<CM::Message>>>,
}

impl<NI, RM, PM, CM> MIOTcpNode<NI, RM, PM, CM>
    where
        NI: NetworkInformationProvider + 'static,
        RM: Serializable + 'static,
        PM: Serializable + 'static,
        CM: Serializable + 'static {
    fn setup_connection(id: &NodeId, server_addr: &SocketAddr) -> Result<SyncListener> {
        socket::bind_sync_server(server_addr.clone()).context(format!("Failed to setup connection with socket {:?}", server_addr))
    }

    /// Create the send tos for a given target
    fn send_tos(&self, shared: Option<&Arc<KeyPair>>, targets: impl Iterator<Item=NodeId>, flush: bool)
                -> (Option<SendTo<RM, PM, CM>>, Option<SendTos<RM, PM, CM>>, Vec<NodeId>) {
        let mut send_to_me = None;
        let mut send_tos: Option<SendTos<RM, PM>> = None;

        let mut failed = Vec::new();

        let my_id = self.id();

        let nonce = self.rng.next_state();

        for id in targets {
            if id == my_id {
                send_to_me = Some(SendTo {
                    my_id,
                    peer_id: id,
                    shared: shared.cloned(),
                    nonce,
                    reconfig_handling: self.reconfig_handling.clone(),
                    peer_cnn: SendToPeer::Me(self.client_pooling.loopback_connection().clone()),
                    flush,
                    rq_send_time: Instant::now(),
                })
            } else {
                match self.connections.get_connection(&id) {
                    None => {
                        match self.connections.get_pending_connection(&id) {
                            None => {
                                failed.push(id)
                            }
                            Some(conn) => {
                                let send_to = match &mut send_tos {
                                    None => {
                                        send_tos = Some(SmallVec::new());

                                        send_tos.as_mut().unwrap()
                                    }
                                    Some(send_to) => {
                                        send_to
                                    }
                                };

                                send_to.push(SendTo {
                                    my_id,
                                    peer_id: id.clone(),
                                    shared: shared.cloned(),
                                    nonce,
                                    reconfig_handling: self.reconfig_handling.clone(),
                                    peer_cnn: SendToPeer::PendingPeer(conn),
                                    flush,
                                    rq_send_time: Instant::now(),
                                });
                            }
                        }
                    }
                    Some(conn) => {
                        let send_to = match &mut send_tos {
                            None => {
                                send_tos = Some(SmallVec::new());

                                send_tos.as_mut().unwrap()
                            }
                            Some(send_to) => {
                                send_to
                            }
                        };

                        send_to.push(SendTo {
                            my_id,
                            peer_id: id.clone(),
                            shared: shared.cloned(),
                            nonce,
                            reconfig_handling: self.reconfig_handling.clone(),
                            peer_cnn: SendToPeer::Peer(conn),
                            flush,
                            rq_send_time: Instant::now(),
                        });
                    }
                }
            }
        }

        (send_to_me, send_tos, failed)
    }

    fn serialize_send_impl(send_to_me: Option<SendTo<RM, PM, CM>>, send_to_others: Option<SendTos<RM, PM, CM>>,
                           message: NetworkMessageKind<RM, PM, CM>) {
        let start = Instant::now();

        threadpool::execute(move || {
            metric_duration(THREADPOOL_PASS_TIME_ID, start.elapsed());

            match crate::cpu_workers::serialize_digest_no_threadpool(&message) {
                Ok((buffer, digest)) => {
                    Self::send_impl(send_to_me, send_to_others, message, buffer, digest);
                }
                Err(err) => {
                    error!("Failed to serialize message {:?}", err);
                }
            }
        });
    }

    fn send_impl(send_to_me: Option<SendTo<RM, PM, CM>>, send_to_others: Option<SendTos<RM, PM, CM>>,
                 msg: NetworkMessageKind<RM, PM, CM>, buffer: Buf, digest: Digest, ) {
        if let Some(send_to) = send_to_me {
            send_to.value(Either::Left((msg, buffer.clone(), digest.clone())));
        }

        if let Some(send_to) = send_to_others {
            for send in send_to {
                send.value(Either::Right((buffer.clone(), digest.clone())));
            }
        }
    }

    fn send_serialized_impl(send_to_me: Option<SendTo<RM, PM, CM>>, send_to_others: Option<SendTos<RM, PM, CM>>,
                            mut messages: BTreeMap<NodeId, StoredSerializedNetworkMessage<RM, PM, CM>>) {
        if let Some(send_to) = send_to_me {
            let message = messages.remove(&send_to.peer_id).unwrap();

            send_to.value_serialized(message);
        }

        if let Some(send_to) = send_to_others {
            for send in send_to {
                let message = messages.remove(&send.peer_id).unwrap();

                send.value_serialized(message);
            }
        }
    }
}

impl<NI, RM, PM, CM> ProtocolNetworkNode<PM> for MIOTcpNode<NI, RM, PM, CM>
    where NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    type IncomingRqHandler = PeerIncomingRqHandling<StoredMessage<PM::Message>, StoredMessage<CM::Message>>;
    type NetworkSignatureVerifier = DefaultProtocolSignatureVerifier<RM, PM, CM, NI>;

    fn node_incoming_rq_handling(&self) -> &Arc<Self::IncomingRqHandler> {
        &self.client_pooling
    }

    fn send(&self, message: PM::Message, target: NodeId, flush: bool) -> Result<()> {
        let nmk = NetworkMessageKind::from_system(message);

        let (send_to_me, send_to_others, failed) =
            self.send_tos(None, iter::once(target), flush);

        if !failed.is_empty() {
            return Err!(NetworkSendError::PeerNotFound(target));
        }

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        Ok(())
    }

    fn send_signed(&self, message: PM::Message, target: NodeId, flush: bool) -> Result<()> {
        let nmk = NetworkMessageKind::from_system(message);

        let keys = Some(self.reconfiguration.get_key_pair());

        let (send_to_me, send_to_others, failed) =
            self.send_tos(keys, iter::once(target), flush);

        if !failed.is_empty() {
            return Err!(NetworkSendError::PeerNotFound(target));
        }

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        Ok(())
    }

    fn broadcast(&self, message: PM::Message, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let nmk = NetworkMessageKind::from_system(message);

        let (send_to_me, send_to_others, failed) =
            self.send_tos(None, targets, true);

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
    }

    fn broadcast_signed(&self, message: PM::Message, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let nmk = NetworkMessageKind::from_system(message);

        let keys = Some(self.reconfiguration.get_key_pair());

        let (send_to_me, send_to_others, failed) =
            self.send_tos(keys, target, true);

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
    }

    fn serialize_digest_message(&self, message: PM::Message) -> Result<(SerializedMessage<PM::Message>, Digest)> {
        let nmk = NetworkMessageKind::<RM, PM>::from_system(message);

        let key_pair = Some(&**self.reconfiguration.get_key_pair());

        let nonce = self.rng.next_state();

        match crate::cpu_workers::serialize_digest_no_threadpool(&nmk) {
            Ok((buffer, digest)) => {
                let msg = match nmk {
                    NetworkMessageKind::System(sys) => {
                        SerializedMessage::new(sys.into(), buffer)
                    }
                    _ => unreachable!()
                };

                Ok((msg, digest))
            }
            Err(err) => {
                error!("Failed to serialize message {:?}", err);

                Err!(err)
            }
        }
    }

    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedProtocolMessage<PM::Message>>) -> std::result::Result<(), Vec<NodeId>> {
        let targets = messages.keys().cloned().into_iter();

        let (send_to_me, send_to_others, failed) = self.send_tos(None,
                                                                 targets, true);

        let mut mapped_serialized_messages = BTreeMap::new();

        for (id, message) in messages.into_iter() {
            let (header, message) = message.into_inner();

            let (pm, buf) = message.into_inner();

            let nmk = NetworkMessageKind::from_system(pm);

            let message = StoredSerializedNetworkMessage::new(header, SerializedMessage::new(nmk, buf));

            mapped_serialized_messages.insert(id, message);
        }

        threadpool::execute(move || {
            Self::send_serialized_impl(send_to_me, send_to_others, mapped_serialized_messages);
        });

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
    }
}

impl<NI, RM, PM, CM> ProtocolNetworkNode<CM> for MIOTcpNode<NI, RM, PM, CM>
    where NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    type IncomingRqHandler = PeerIncomingRqHandling<StoredMessage<PM::Message>, StoredMessage<CM::Message>>;
    type NetworkSignatureVerifier = DefaultProtocolSignatureVerifier<RM, PM, CM, NI>;

    fn node_incoming_rq_handling(&self) -> &Arc<Self::IncomingRqHandler> {
        &self.client_pooling
    }

    fn send(&self, message: CM::Message, target: NodeId, flush: bool) -> Result<()> {
        let nmk = NetworkMessageKind::from_c_system(message);

        let (send_to_me, send_to_others, failed) =
            self.send_tos(None, iter::once(target), flush);

        if !failed.is_empty() {
            return Err!(NetworkSendError::PeerNotFound(target));
        }

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        Ok(())
    }

    fn send_signed(&self, message: CM::Message, target: NodeId, flush: bool) -> Result<()> {
        let nmk = NetworkMessageKind::from_c_system(message);

        let keys = Some(self.reconfiguration.get_key_pair());

        let (send_to_me, send_to_others, failed) =
            self.send_tos(keys, iter::once(target), flush);

        if !failed.is_empty() {
            return Err!(NetworkSendError::PeerNotFound(target));
        }

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        Ok(())
    }

    fn broadcast(&self, message: CM::Message, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let nmk = NetworkMessageKind::from_c_system(message);

        let (send_to_me, send_to_others, failed) =
            self.send_tos(None, targets, true);

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
    }

    fn broadcast_signed(&self, message: CM::Message, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let nmk = NetworkMessageKind::from_c_system(message);

        let keys = Some(self.reconfiguration.get_key_pair());

        let (send_to_me, send_to_others, failed) =
            self.send_tos(keys, target, true);

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
    }

    fn serialize_digest_message(&self, message: CM::Message) -> Result<(SerializedMessage<CM::Message>, Digest)> {
        let nmk = NetworkMessageKind::<RM, PM>::from_c_system(message);

        let key_pair = Some(&**self.reconfiguration.get_key_pair());

        let nonce = self.rng.next_state();

        match crate::cpu_workers::serialize_digest_no_threadpool(&nmk) {
            Ok((buffer, digest)) => {
                let msg = match nmk {
                    NetworkMessageKind::System(sys) => {
                        SerializedMessage::new(sys.into(), buffer)
                    }
                    _ => unreachable!()
                };

                Ok((msg, digest))
            }
            Err(err) => {
                error!("Failed to serialize message {:?}", err);

                Err!(err)
            }
        }
    }

    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedProtocolMessage<PM::Message>>) -> std::result::Result<(), Vec<NodeId>> {
        let targets = messages.keys().cloned().into_iter();

        let (send_to_me, send_to_others, failed) = self.send_tos(None,
                                                                 targets, true);

        let mut mapped_serialized_messages = BTreeMap::new();

        for (id, message) in messages.into_iter() {
            let (header, message) = message.into_inner();

            let (pm, buf) = message.into_inner();

            let nmk = NetworkMessageKind::from_system(pm);

            let message = StoredSerializedNetworkMessage::new(header, SerializedMessage::new(nmk, buf));

            mapped_serialized_messages.insert(id, message);
        }

        threadpool::execute(move || {
            Self::send_serialized_impl(send_to_me, send_to_others, mapped_serialized_messages);
        });

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
    }
}


impl<NI, RM, PM, CM> NetworkNode for MIOTcpNode<NI, RM, PM, CM>
    where NI: 'static + NetworkInformationProvider,
          PM: 'static + Serializable,
          CM: 'static + Serializable,
          RM: 'static + Serializable {
    type ConnectionManager = Connections<NI, RM, PM, CM>;
    type NetworkInfoProvider = NI;

    fn id(&self) -> NodeId {
        self.id
    }

    fn node_connections(&self) -> &Arc<Self::ConnectionManager> {
        &self.connections
    }

    fn network_info_provider(&self) -> &Arc<Self::NetworkInfoProvider> {
        &self.reconfiguration
    }
}

impl<NI, RM, PM, CM> ReconfigurationNode<RM> for MIOTcpNode<NI, RM, PM, CM>
    where NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    type IncomingReconfigRqHandler = ReconfigurationMessageHandler<StoredMessage<RM::Message>>;
    type ReconfigurationNetworkUpdate = ReconfigurationMessageHandler<StoredMessage<RM::Message>>;

    fn reconfiguration_network_update(&self) -> &Arc<Self::ReconfigurationNetworkUpdate> {
        &self.reconfig_handling
    }

    fn reconfiguration_message_handler(&self) -> &Arc<Self::IncomingReconfigRqHandler> {
        &self.reconfig_handling
    }

    fn send_reconfig_message(&self, message: RM::Message, target: NodeId) -> Result<()> {
        let nmk = NetworkMessageKind::from_reconfig(message);

        let keys = Some(self.reconfiguration.get_key_pair());

        let (send_to_me, send_to_others, failed) =
            self.send_tos(keys, iter::once(target), true);

        if !failed.is_empty() {
            return Err!(NetworkSendError::PeerNotFound(target));
        }

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        Ok(())
    }

    fn broadcast_reconfig_message(&self, message: RM::Message, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let nmk = NetworkMessageKind::from_reconfig(message);

        let keys = Some(self.reconfiguration.get_key_pair());

        let (send_to_me, send_to_others, failed) =
            self.send_tos(keys, target, true);

        Self::serialize_send_impl(send_to_me, send_to_others, nmk);

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
    }
}

impl<NI, RM, PM, CM> FullNetworkNode<NI, RM, PM, CM> for MIOTcpNode<NI, RM, PM, CM>
    where NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    type Config = MioConfig;

    async fn bootstrap(id: NodeId, network_info_provider: Arc<NI>, node_config: Self::Config) -> Result<Self> where NI: NetworkInformationProvider {
        let MioConfig { node_config: cfg, worker_count } = node_config;

        debug!("Initializing sockets.");

        let tcp_config = cfg.tcp_config;

        let conn_counts = ConnCounts::from_tcp_config(&tcp_config);

        let reconfig_message_handler = Arc::new(ReconfigurationMessageHandler::initialize());

        let network = tcp_config.network_config;

        let rng = Arc::new(ThreadSafePrng::new());

        debug!("{:?} // Initializing node reference", id);

        //Setup all the peer message reception handling.
        let peers = Arc::new(PeerIncomingRqHandling::new(
            id,
            network_info_provider.get_own_node_type(),
            cfg.client_pool_config,
        ));

        let (handle, receivers) = init_worker_group_handle::<NI, RM, PM>(worker_count as u32);

        let connections = Arc::new(Connections::initialize_connections(
            id,
            network_info_provider.clone(),
            handle.clone(),
            conn_counts.clone(),
            reconfig_message_handler.clone(),
            peers.clone(),
        )?);

        NetworkUpdateHandler::initialize_update_handler(
            connections.registered_servers().clone(),
            connections.pending_server_connections().clone(),
            reconfig_message_handler.clone(),
            connections.clone(),
        );

        initialize_worker_group(connections.clone(), receivers)?;

        let addr = network_info_provider.get_own_addr();

        let listener = Self::setup_connection(&id, addr.socket())?;

        connections.setup_tcp_server_worker(listener);

        let network_node = Self {
            id,
            rng,
            connections,
            reconfig_handling: reconfig_message_handler,
            client_pooling: peers,
            reconfiguration: network_info_provider.clone(),
        };

        Ok(network_node)
    }
}

/// Some information about a message about to be sent to a peer
struct SendTo<RM, PM, CM>
    where RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    my_id: NodeId,
    peer_id: NodeId,
    shared: Option<Arc<KeyPair>>,
    nonce: u64,
    reconfig_handling: Arc<ReconfigurationMessageHandler<StoredMessage<RM::Message>>>,
    peer_cnn: SendToPeer<RM, PM, CM>,
    flush: bool,
    rq_send_time: Instant,
}

/// The information about the connection itself which can either be a loopback
/// or a peer connection
enum SendToPeer<RM, PM, CM>
    where RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    Me(Arc<ConnectedPeer<StoredMessage<PM::Message>, StoredMessage<CM::Message>>>),
    Peer(Arc<PeerConnection<RM, PM, CM>>),
    PendingPeer(PendingConnHandle),
}

impl<RM, PM, CM> SendTo<RM, PM, CM>
    where RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    fn value(self, msg: Either<(NetworkMessageKind<RM, PM, CM>, Buf, Digest), (Buf, Digest)>) {
        let key_pair = match &self.shared {
            None => {
                None
            }
            Some(key_pair) => {
                Some(&**key_pair)
            }
        };

        match (self.peer_cnn, msg) {
            (SendToPeer::Me(conn), Either::Left((msg, buf, digest))) => {
                let message = WireMessage::new(self.my_id, self.peer_id,
                                               buf, self.nonce, Some(digest), key_pair);

                let (header, _) = message.into_inner();

                match msg {
                    NetworkMessageKind::ReconfigurationMessage(reconfig_msg) => {
                        self.reconfig_handling.push_request(StoredMessage::new(header, reconfig_msg.into())).unwrap();
                    }
                    NetworkMessageKind::System(sys_msg) => {
                        conn.push_request(StoredMessage::new(header, sys_msg.into())).unwrap();
                    }
                    _ => {
                        unreachable!()
                    }
                }
            }
            (SendToPeer::Peer(peer), Either::Right((buf, digest))) => {
                let message = WireMessage::new(self.my_id, self.peer_id,
                                               buf, self.nonce, Some(digest), key_pair);

                peer.peer_message(message, None).unwrap();
            }
            (SendToPeer::PendingPeer(peer), Either::Right((buf, digest))) => {
                let message = WireMessage::new(self.my_id, self.peer_id,
                                               buf, self.nonce, Some(digest), key_pair);

                peer.peer_message(message).unwrap();
            }
            (_, _) => { unreachable!() }
        }
    }

    fn value_serialized(self, msg: StoredSerializedNetworkMessage<RM, PM, CM>) {
        match self.peer_cnn {
            SendToPeer::Me(peer_conn) => {
                let (header, msg) = msg.into_inner();

                let (msg, _) = msg.into_inner();

                match msg {
                    NetworkMessageKind::ReconfigurationMessage(reconfig_msg) => {
                        self.reconfig_handling.push_request(StoredMessage::new(header, reconfig_msg.into())).unwrap();
                    }
                    NetworkMessageKind::System(sys_msg) => {
                        peer_conn.push_request(StoredMessage::new(header, sys_msg.into())).unwrap();
                    }
                    _ => {
                        unreachable!()
                    }
                }
            }
            SendToPeer::Peer(peer_cnn) => {
                let (header, msg) = msg.into_inner();

                let (_, buf) = msg.into_inner();

                let wm = WireMessage::from_parts(header, buf).unwrap();

                peer_cnn.peer_message(wm, None).unwrap();
            }
            SendToPeer::PendingPeer(pending_conn) => {
                let (header, msg) = msg.into_inner();

                let (msg, buf) = msg.into_inner();

                match msg {
                    NetworkMessageKind::ReconfigurationMessage(reconf) => {
                        let wm = WireMessage::from_parts(header, buf).unwrap();

                        pending_conn.peer_message(wm).unwrap();
                    }
                    NetworkMessageKind::Ping(_) => {}
                    _ => {
                        error!("Should not be sending system messages to pending connections");
                    }
                }
            }
        }
    }
}