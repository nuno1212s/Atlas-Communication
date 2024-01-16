use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use enum_dispatch::enum_dispatch;

use dashmap::DashMap;
use log::{debug, error, trace};
use thiserror::Error;

use atlas_common::{channel, Err};
use atlas_common::channel::{ChannelMultRx, ChannelMultTx, ChannelSyncRx, ChannelSyncTx, TryRecvError};
use atlas_common::error::*;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::node_id::NodeType;
use atlas_metrics::metrics::metric_duration;
use crate::client_pooling::client_handling::{ClientPeer, ConnectedPeersGroup};
use crate::client_pooling::replica_handling::{ConnectedReplica, ReplicaHandling};

use crate::config::ClientPoolConfig;
use crate::metric::CLIENT_POOL_BATCH_PASSING_TIME_ID;
use crate::{NodeId, NodeIncomingRqHandler};

mod replica_handling;
mod client_handling;

fn channel_init<T>(capacity: usize) -> (ChannelMultTx<T>, ChannelMultRx<T>) {
    channel::new_bounded_mult(capacity)
}

fn client_channel_init<T>(capacity: usize) -> (ChannelMultTx<T>, ChannelMultRx<T>) {
    channel::new_bounded_mult(capacity)
}

/// A batch sent from the client pools to be processed is composed of the Vec of requests
/// and the instant at which it was created and pushed in the queue
type ClientRqBatchOutput<T> = (Vec<T>, Instant);

type ReplicaRqOutput<T> = (T, Instant);

pub struct ConnectedPeerManagement<RT, CT> {
    connected_clients: DashMap<NodeId, Arc<ConnectedPeer<RT, CT>>>,
    connected_counts: BTreeMap<NodeType, AtomicUsize>,
}

///Handles the communication between two peers (replica - replica, replica - client)
///Only handles reception of requests, not transmission
/// It's also built on top of the default networking layer, which handles
/// actually serializing the messages. This only handles already serialized messages.
pub struct PeerIncomingRqHandling<RT: Send + 'static, CT: Send + 'static> {
    batch_size: usize,
    //Our own ID
    own_id: NodeId,
    //The loopback channel to our own node reception
    peer_loopback: Arc<ConnectedPeer<RT, CT>>,
    // Management of the peers that are currently connected to us
    connected_peers: ConnectedPeerManagement<RT, CT>,
    //Replica connection handling
    replica_handling: Arc<ReplicaHandling<RT>>,
    //Client request collection handling (Pooled), is only available on the replicas
    client_handling: Option<Arc<ConnectedPeersGroup<CT>>>,
    client_tx: Option<ChannelSyncTx<ClientRqBatchOutput<CT>>>,
    client_rx: Option<ChannelSyncRx<ClientRqBatchOutput<CT>>>,
}

const NODE_CHAN_BOUND: usize = 1024;
const DEFAULT_CLIENT_QUEUE: usize = 16384;
const DEFAULT_REPLICA_QUEUE: usize = 131072;

///We make this class Sync and send since the clients are going to be handled by a single class
///And the replicas are going to be handled by another class.
/// There is no possibility of 2 threads accessing the client_rx or replica_rx concurrently
unsafe impl<RT, CT> Sync for PeerIncomingRqHandling<RT, CT> where RT: Send, CT: Send {}

unsafe impl<RT, CT> Send for PeerIncomingRqHandling<RT, CT> where RT: Send, CT: Send {}

impl<RT, CT> PeerIncomingRqHandling<RT, CT> where RT: Send, CT: Send {
    pub fn new(id: NodeId, node_type: NodeType, config: ClientPoolConfig) -> PeerIncomingRqHandling<RT, CT> {
        //We only want to setup client handling if we are a replica
        let client_handling;

        let client_channel;

        let ClientPoolConfig {
            batch_size, clients_per_pool, batch_timeout_micros, batch_sleep_micros
        } = config;

        match node_type {
            NodeType::Replica => {
                let (client_tx, client_rx) = channel::new_bounded_sync(NODE_CHAN_BOUND,
                                                                       Some("Client Pool Handle"));

                client_handling = Some(ConnectedPeersGroup::new(DEFAULT_CLIENT_QUEUE,
                                                                batch_size,
                                                                client_tx.clone(),
                                                                id,
                                                                clients_per_pool,
                                                                batch_timeout_micros,
                                                                batch_sleep_micros));

                client_channel = Some((client_tx, client_rx));
            }
            NodeType::Client => {
                client_handling = None;
                client_channel = None;
            }
        }

        let replica_handling = ReplicaHandling::new(NODE_CHAN_BOUND);

        let loopback_address = replica_handling.init_client(id);

        let (cl_tx, cl_rx) = if let Some((cl_tx, cl_rx)) = client_channel {
            (Some(cl_tx), Some(cl_rx))
        } else {
            (None, None)
        };

        let peers = PeerIncomingRqHandling {
            batch_size,
            own_id: id,
            peer_loopback: loopback_address,
            connected_peers: ConnectedPeerManagement::init(),
            replica_handling,
            client_handling,
            client_tx: cl_tx,
            client_rx: cl_rx,
        };

        peers
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    ///Initialize a new peer connection
    /// This will be used by the networking layer to deliver the received messages to the
    /// Actual system
    pub fn init_peer_conn(&self, peer: NodeId, node_type: NodeType) -> Result<Arc<ConnectedPeer<RT, CT>>> {
        debug!("Initializing peer connection for peer {:?} on peer {:?}", peer, self.own_id);

        let conn = match node_type {
            NodeType::Replica => {
                let rep_conn =
                    self.replica_handling.init_client(peer);

                ConnectedPeer {
                    peer_id: peer,
                    node_type,
                    peer_info: ConnectedPeerInfo::UnpooledConnection(rep_conn),
                }
            }
            NodeType::Client => {
                let client_conn = self.client_handling.as_ref()
                    .ok_or(ClientPoolError::CannotConnectToClients)?
                    .init_client(peer);

                ConnectedPeer {
                    peer_id: peer,
                    node_type,
                    peer_info: ConnectedPeerInfo::PoolConnection(client_conn),
                }
            }
        };


        self.connected_peers.register_connection(conn);

        Ok(conn)
    }

    ///Get the incoming request queue for a given node
    pub fn resolve_peer_conn(&self, peer: NodeId, node_type: NodeType) -> Option<Arc<ConnectedPeer<RT, CT>>> {
        if peer == self.own_id {
            return Some(self.peer_loopback.clone());
        }

        return match node_type {
            NodeType::Replica => {
                self.replica_handling.resolve_connection(peer)
            }
            NodeType::Client => {
                self.client_handling.as_ref()
                    .ok_or(ClientPoolError::CannotConnectToClients).unwrap()
                    .get_client_conn(peer)
            }
        };
    }

    ///Get our loopback request queue
    pub fn loopback_connection(&self) -> &Arc<ConnectedPeer<RT, CT>> {
        &self.peer_loopback
    }

    fn get_client_rx(&self) -> Result<&ChannelSyncRx<ClientRqBatchOutput<CT>>> {
        return match &self.client_rx {
            None => {
                Err!(ClientPoolError::NoClientsConnected)
            }
            Some(rx) => {
                Ok(rx)
            }
        };
    }

    ///Count the amount of clients present (not including replicas)
    ///Returns None if this is a client and therefore has no client conns
    pub fn client_count(&self) -> Option<usize> {
        self.connected_peers.connected_nodes(NodeType::Client)
    }

    ///Count the replicas connected
    pub fn replica_count(&self) -> usize {
        self.connected_peers.connected_nodes(NodeType::Replica)
    }
}

impl<RT, CT> NodeIncomingRqHandler<RT> for PeerIncomingRqHandling<RT, CT> {
    
    fn pending_rqs(&self) -> usize {
        self.replica_handling.pending_requests()
    }

    fn receive_requests(&self, timeout: Option<Duration>) -> Result<MaybeVec<RT>> {
        self.replica_handling.receive_from_replicas(timeout).map(|ele| MaybeVec::from_one(ele))
    }

    fn try_receive_requests(&self) -> Result<Option<MaybeVec<RT>>> {
        Ok(self.replica_handling.try_rcv_from_replicas().map(|opt| MaybeVec::from_one(opt)))
    }
}

impl<RT, CT> NodeIncomingRqHandler<CT> for PeerIncomingRqHandling<RT, CT> {
    fn pending_rqs(&self) -> usize {
        match self.client_handling.as_ref() {
            None => {
                0
            }
            Some(client_handling) => {
                todo!("Still have to implement client request counting")
            }
        }
    }

    fn receive_requests(&self, timeout: Option<Duration>) -> Result<MaybeVec<CT>> {
        let rx = self.get_client_rx()?;

        match timeout {
            None => {
                let (vec, time_created) = rx.recv()?;

                metric_duration(CLIENT_POOL_BATCH_PASSING_TIME_ID, time_created.elapsed());

                Ok(vec)
            }
            Some(timeout) => {
                match rx.recv_timeout(timeout) {
                    Ok((vec, time_created)) => {
                        metric_duration(CLIENT_POOL_BATCH_PASSING_TIME_ID, time_created.elapsed());

                        Ok(vec)
                    }
                    Err(err) => {
                        match err {
                            TryRecvError::Timeout => {
                                Ok(MaybeVec::None)
                            }
                            _ => {
                                Err!(err)
                            }
                        }
                    }
                }
            }
        }
    }

    fn try_receive_requests(&self) -> Result<Option<MaybeVec<CT>>> {
        let rx = self.get_client_rx()?;

        match rx.try_recv() {
            Ok((msgs, time_created)) => {
                metric_duration(CLIENT_POOL_BATCH_PASSING_TIME_ID, time_created.elapsed());

                Ok(Some(MaybeVec::from_many(msgs)))
            }
            Err(err) => {
                match &err {
                    TryRecvError::ChannelEmpty => {
                        Ok(None)
                    }
                    _ => {
                        Err!(err)
                    }
                }
            }
        }
    }
}

impl<RT, CT> ConnectedPeerManagement<RT, CT>
    where RT: Send, CT: Send {
    fn init() -> Self {
        let mut connected_counts = Default::default();

        NodeType::iter().for_each(|n_type| {
            connected_counts.insert(n_type, AtomicUsize::new(0));
        });

        Self {
            connected_clients: Default::default(),
            connected_counts: connected_counts,
        }
    }

    pub fn connected_nodes(&self, node_type: NodeType) -> usize {
        let connected_count = self.connected_counts.get(node_type).expect("This should be impossible as we populate the map on initialization");

        connected_count.load(Ordering::Relaxed)
    }

    fn incr_connected_count(&self, node_type: NodeType, count: usize) {
        let connected_count = self.connected_counts.get(node_type).expect("This should be impossible as we populate the map on initialization");

        connected_count.fetch_add(count, Ordering::Relaxed);
    }

    fn decr_connected_count(&self, node_type: NodeType, count: usize) {
        let connected_count = self.connected_counts.get(node_type).expect("This should be impossible as we populate the map on initialization");

        connected_count.fetch_sub(count, Ordering::Relaxed);
    }

    pub fn register_connection(&self, connection: ConnectedPeer<RT, CT>) {
        match self.connected_clients.insert(connection.peer_id, connection.clone()) {
            None => {
                self.incr_connected_count(connection.node_type, 1);
            }
            Some(old) => {
                //When we insert a new channel, we want the old channel to become closed.
                old.disconnect();
            }
        };
    }

    pub fn resolve_connection(&self, peer_id: NodeId) -> Option<ConnectedPeer<RT, CT>> {
        self.connected_clients.get(&peer_id).map(|reference| {
            reference.value().clone()
        })
    }

    pub fn remove_connection(&self, peer_id: NodeId) {
        match self.connected_clients.remove(&peer_id) {
            None => {}
            Some((node, conn)) => {
                self.decr_connected_count(conn.node_type, 1);
            }
        }
    }
}

/// The connected peer information.
#[derive(Clone)]
pub struct ConnectedPeer<RT, CT> where RT: Send, CT: Send {
    peer_id: NodeId,
    node_type: NodeType,
    peer_info: ConnectedPeerInfo<RT, CT>,
}


pub trait PeerConn<T> {
    // Is this connection disconnected?
    fn is_dc(&self) -> bool;

    // Disconnect this connection
    fn disconnect(&self);

    ///Dump n requests into the provided vector
    ///Returns the amount of requests that were dumped into the array
    fn dump_requests(&self, replacement_vec: Vec<T>) -> std::result::Result<Vec<T>, Vec<T>>;

    fn push_request(&self, msg: T) -> Result<()>;
}

///Represents a connected peer
///
///Can either be a pooled peer with an individual queue and a thread that will collect all requests
///Or an unpooled connection that puts the messages straight into the channel where the consumer
///Will collect.
pub enum ConnectedPeerInfo<RT, CT> where RT: Send, CT: Send {
    PoolConnection(ClientPeer<CT>),
    UnpooledConnection(ConnectedReplica<RT>),
}

impl<RT, CT> ConnectedPeer<RT, CT> where RT: Send, CT: Send {
    pub fn peer_id(&self) -> NodeId {
        self.peer_id
    }

    pub fn client_id(&self) -> &NodeId {
        self.peer_id
    }

    pub fn is_dc(&self) -> bool {
        match &self.peer_info {
            ConnectedPeerInfo::PoolConnection(c) => {
                c.is_dc()
            }
            ConnectedPeerInfo::UnpooledConnection(c) => {
                c.is_dc()
            }
        }
    }

    pub fn disconnect(&self) {
        trace!("Force disconnecting node {:?}", self.client_id());

        match &self.peer_info {
            ConnectedPeerInfo::PoolConnection(c) => {
                c.disconnect()
            }
            ConnectedPeerInfo::UnpooledConnection(c) => {
                c.disconnect()
            }
        }
    }

    ///Dump n requests into the provided vector
    ///Returns the amount of requests that were dumped into the array
    pub fn dump_client_requests(&self, replacement_vec: Vec<CT>) -> std::result::Result<Vec<CT>, Vec<CT>> {
        trace!("Dumping requests into provided vec to client {:?}", self.client_id());

        match &self.peer_info {
            ConnectedPeerInfo::PoolConnection(c) => {
                c.dump_requests(replacement_vec)
            }
            _ => Err(SendPeerError::AttemptToPushClientMessageToReplicaConn(self.peer_id))
        }
        self.peer_info.dump_requests(replacement_vec)
    }

    pub fn push_system_request(&self, msg: RT) -> Result<()> {
        match &self.peer_info {
            ConnectedPeerInfo::UnpooledConnection(c) => {
                c.push_request(msg)
            }
            _ => Err(SendPeerError::AttemptToPushReplicaMessageToClientConn(self.peer_id))
        }
    }

    pub fn push_client_request(&self, msg: CT) -> Result<()> {
        match &self.peer_info {
            ConnectedPeerInfo::PoolConnection(conn) => {
                conn.push_request(msg)
            }
            _ => Err(SendPeerError::AttemptToPushClientMessageToReplicaConn(self.peer_id))
        }
    }
}

#[derive(Error, Debug)]
pub enum ClientPoolError {
    #[error("This error is meant to be used to close the pool")]
    ClosePool,
    #[error("The unpooled connection is closed {0:?}")]
    UnpooledConnectionClosed(NodeId),
    #[error("The pooled connection is closed {0:?}")]
    PooledConnectionClosed(NodeId),
    #[error("Failed to allocate client pool ID")]
    FailedToAllocateClientPoolID,
    #[error("Failed to receive from clients as there are no clients connected")]
    NoClientsConnected,
    #[error("Failed to get client connection as we can't connect to other clients")]
    CannotConnectToClients,
}

#[derive(Error, Debug)]
pub enum SendPeerError {
    #[error("Attempted to push replica bound message, {0}")]
    AttemptToPushReplicaMessageToClientConn(NodeId),
    #[error("Attempted to push client bound message to another replica {0}")]
    AttemptToPushClientMessageToReplicaConn(NodeId),
}