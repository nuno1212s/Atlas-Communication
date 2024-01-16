use std::sync::Arc;
use std::time::{Duration, Instant};
use log::error;
use atlas_common::{channel, Err};
use atlas_common::error::*;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, TryRecvError};
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::node_id::NodeId;
use atlas_metrics::metrics::metric_duration;
use crate::client_pooling::{ClientPoolError, ConnectedPeer, PeerConn, ReplicaRqOutput};
use crate::metric::REPLICA_RQ_PASSING_TIME_ID;

#[derive(Clone)]
pub struct ConnectedReplica<T> {
    peer_id: NodeId,
    sender: ChannelSyncTx<ReplicaRqOutput<T>>,
}

///Handling replicas is different from handling clients
///We want to handle the requests differently because in communication between replicas
///Latency is extremely important and we have to minimize it to the least amount possible
/// So in this implementation, we will just use a single channel to receive and collect
/// all messages
///
/// FIXME: See if having a multiple channel approach with something like a select is
/// worth the overhead of having to pool multiple channels. We may also get problems with fairness.
/// Probably not worth it
pub struct ReplicaHandling<T> where T: Send {
    capacity: usize,
    //The channel we push replica sent requests into
    channel_tx_replica: ChannelSyncTx<ReplicaRqOutput<T>>,
    //The channel used to read requests that were pushed by replicas
    channel_rx_replica: ChannelSyncRx<ReplicaRqOutput<T>>,
}

impl<T> ReplicaHandling<T> where T: Send {
    pub fn new(capacity: usize) -> Arc<Self> {
        let (sender, receiver) = channel::new_unbounded_sync(Some("Replica message channel"));

        Arc::new(
            Self {
                capacity,
                channel_rx_replica: receiver,
                channel_tx_replica: sender,
            }
        )
    }

    pub fn init_client(&self, peer_id: NodeId) -> ConnectedReplica<T> {
        let peer = ConnectedReplica {
            peer_id,
            sender: self.channel_tx_replica.clone(),
        };

        peer
    }

    pub fn pending_requests(&self) -> usize {
        self.channel_rx_replica.len()
    }

    pub fn receive_from_replicas(&self, timeout: Option<Duration>) -> Option<T> {
        return match timeout {
            None => {
                // This channel is always active,
                let (message, instant) = self.channel_rx_replica.recv().unwrap();

                metric_duration(REPLICA_RQ_PASSING_TIME_ID, instant.elapsed());

                Some(message)
            }
            Some(timeout) => {
                let result = self.channel_rx_replica.recv_timeout(timeout);

                match result {
                    Ok((item, instant)) => {
                        metric_duration(REPLICA_RQ_PASSING_TIME_ID, instant.elapsed());

                        Some(item)
                    }
                    Err(err) => {
                        match err {
                            TryRecvError::Timeout | TryRecvError::ChannelEmpty => {
                                None
                            }
                            TryRecvError::ChannelDc => {
                                // Since we always hold at least one reference to the TX side,
                                // We know it will never disconnect
                                unreachable!()
                            }
                        }
                    }
                }
            }
        };
    }

    pub fn try_rcv_from_replicas(&self) -> Option<T> {
        match self.channel_rx_replica.try_recv() {
            Ok(message) => {
                Some(message)
            }
            Err(err) => {
                match err {
                    TryRecvError::ChannelDc => {
                        unreachable!("We have an TX reference which lives as long as this RX one, so this is impossible")
                    }
                    _ => None
                }
            }
        }
    }
}

impl<T> PeerConn<T> for ConnectedReplica<T> {
    fn is_dc(&self) -> bool {
        false
    }

    fn disconnect(&self) {}

    fn dump_requests(&self, replacement_vec: MaybeVec<T>) -> std::result::Result<Vec<T>, Vec<T>> {
        Ok(vec![])
    }

    fn push_request(&self, msg: T) -> atlas_common::error::Result<()> {
        match self.sender.send_return((msg, Instant::now())) {
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                error!("Failed to deliver data from {:?} because {:?}", self.client_id(), err);

                Err!(ClientPoolError::UnpooledConnectionClosed(self.peer_id.clone()))
            }
        }
    }
}