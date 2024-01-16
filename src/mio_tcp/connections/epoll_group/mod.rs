use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use anyhow::Context;
use log::error;
use mio::{Token};
use thiserror::Error;
use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::socket::MioSocket;
use crate::mio_tcp::connections::{Connections, PeerConnection};
use crate::mio_tcp::connections::conn_util::{ReadingBuffer, WritingBuffer};
use crate::mio_tcp::connections::epoll_group::epoll_workers::EpollWorker;
use crate::reconfiguration_node::NetworkInformationProvider;
use crate::serialize::Serializable;

pub mod epoll_workers;

pub type EpollWorkerId = u32;

// This will just handle creating and deleting connections so it can be small
pub const DEFAULT_WORKER_CHANNEL: usize = 128;

pub(crate) fn init_worker_group_handle<NI, RM, PM, CM>(worker_count: u32) -> (EpollWorkerGroupHandle<RM, PM, CM>, Vec<ChannelSyncRx<EpollWorkerMessage<RM, PM, CM>>>)
    where NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    let mut workers = Vec::with_capacity(worker_count as usize);

    let mut receivers = Vec::with_capacity(worker_count as usize);

    for _ in 0..worker_count {
        let (tx, rx) = channel::new_bounded_sync(DEFAULT_WORKER_CHANNEL, Some("Worker Group Handle"));

        workers.push(tx);
        receivers.push(rx);
    }

    (EpollWorkerGroupHandle {
        workers,
        round_robin: AtomicUsize::new(0),
    }, receivers)
}

pub(crate) fn initialize_worker_group<NI, RM, PM, CM>(connections: Arc<Connections<NI, RM, PM, CM>>, receivers: Vec<ChannelSyncRx<EpollWorkerMessage<RM, PM, CM>>>) -> Result<()>
    where NI: NetworkInformationProvider + 'static,
          RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    for (worker_id, rx) in receivers.into_iter().enumerate() {
        let worker = EpollWorker::new(worker_id as u32, connections.clone(), rx)?;

        std::thread::Builder::new().name(format!("Epoll Worker {}", worker_id))
            .spawn(move || {
                if let Err(err) = worker.epoll_worker_loop() {
                    error!("Epoll worker {} failed with error: {:?}", worker_id,err);
                }
            }).expect("Failed to launch worker thread");
    }

    Ok(())
}

/// A handle to the worker group that handles the epoll events
/// Allows us to register new connections to the epoll workers
pub struct EpollWorkerGroupHandle<RM, PM, CM>
    where RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    workers: Vec<ChannelSyncTx<EpollWorkerMessage<RM, PM, CM>>>,
    round_robin: AtomicUsize,
}

pub(crate) struct NewConnection<RM, PM, CM>
    where RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    conn_id: u32,
    peer_id: NodeId,
    my_id: NodeId,
    socket: MioSocket,
    reading_info: ReadingBuffer,
    writing_info: Option<WritingBuffer>,
    peer_conn: Arc<PeerConnection<RM, PM, CM>>,
}

pub(crate) enum EpollWorkerMessage<RM, PM, CM>
    where RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    NewConnection(NewConnection<RM, PM, CM>),
    CloseConnection(Token),
}

impl<RM, PM, CM> EpollWorkerGroupHandle<RM, PM, CM>
    where RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    /// Assigns a socket to any given worker
    pub(super) fn assign_socket_to_worker(&self, conn_details: NewConnection<RM, PM, CM>) -> atlas_common::error::Result<()> {
        let round_robin = self.round_robin.fetch_add(1, Ordering::Relaxed);

        let epoll_worker = round_robin % self.workers.len();

        let worker = self.workers.get(epoll_worker)
            .ok_or(WorkerError::FailedToAllocateWorkerForConnection(epoll_worker, conn_details.peer_id, conn_details.conn_id))?;

        worker.send(EpollWorkerMessage::NewConnection(conn_details))
            .context("Failed to send new connection message to epoll worker")?;

        Ok(())
    }

    /// Order a disconnection of a given connection from a worker
    pub fn disconnect_connection_from_worker(&self, epoll_worker: EpollWorkerId, conn_id: Token) -> atlas_common::error::Result<()> {
        let worker = self.workers.get(epoll_worker as usize)
            .ok_or(WorkerError::FailedToGetWorkerForConnection(epoll_worker, conn_id))?;

        worker.send(EpollWorkerMessage::CloseConnection(conn_id))
            .context(format!("Failed to close connection in worker {:?}, {:?}", epoll_worker, conn_id))?;

        Ok(())
    }
}

impl<RM, PM, CM> Clone for EpollWorkerGroupHandle<RM, PM, CM>
    where RM: Serializable + 'static,
          PM: Serializable + 'static,
          CM: Serializable + 'static {
    fn clone(&self) -> Self {
        Self {
            workers: self.workers.clone(),
            round_robin: AtomicUsize::new(0),
        }
    }
}

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("Failed to allocate worker (planned {0:?}) for connection {2} with node {1:?}")]
    FailedToAllocateWorkerForConnection(usize, NodeId, u32),
    #[error("Failed to get the corresponding worker for connection {0:?}, {1:?}")]
    FailedToGetWorkerForConnection(EpollWorkerId, Token),
}