use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use log::{error, info};
use atlas_common::channel::ChannelSyncTx;
use atlas_common::Err;
use atlas_common::node_id::NodeId;
use atlas_metrics::metrics::metric_duration;
use crate::client_pooling::{ClientPoolError, ClientRqBatchOutput, ConnectedPeer, PeerConn};
use crate::metric::CLIENT_POOL_COLLECT_TIME_ID;

pub type ClientPeer<T> = Arc<ConnectedClientPeer<T>>;

pub struct ConnectedClientPeer<T> {
    queue: Mutex<Vec<T>>,
    disconnect: AtomicBool
}

///Client pool design, where each pool contains a number of clients (Maximum of BATCH_SIZE clients
/// per pool). This is to prevent starvation for each client, as when we are performing
/// the fair collection of requests from the clients, if there are more clients than batch size
/// then we will get very unfair distribution of requests
///
/// This will push Vecs of T types into the ChannelTx provided
/// The type T is not wrapped in any other way
/// no socket handling is done here
/// This is just built on top of the actual per client connection socket stuff and each socket
/// should push items into its own ConnectedPeer instance
pub struct ConnectedPeersGroup<T: Send + 'static> {
    own_id: NodeId,
    //We can use mutexes here since there will only be concurrency on client connections and dcs
    //And since each client has his own reference to push data to, this only needs to be accessed by the thread
    //That's producing the batches and the threads of clients connecting and disconnecting
    client_pools: Mutex<BTreeMap<usize, Arc<ConnectedPeersPool<T>>>>,
    batch_transmission: ChannelSyncTx<ClientRqBatchOutput<T>>,
    per_client_cache: usize,
    //What batch size should we target for each batch (there is no set limit on requests,
    //Just a hint on when it should move on)
    batch_target_size: usize,
    //How much time can be spent gathering batches
    batch_timeout_micros: u64,
    //How much time should the thread sleep in between batch collection
    batch_sleep_micros: u64,
    clients_per_pool: usize,
    //Counter used to keep track of the created pools
    pool_id_counter: AtomicUsize,
}

pub struct ConnectedPeersPool<T: Send + 'static> {
    pool_id: usize,
    //We can use mutexes here since there will only be concurrency on client connections and dcs
    //And since each client has his own reference to push data to, this only needs to be accessed by the thread
    //That's producing the batches and the threads of clients connecting and disconnecting
    connected_clients: Mutex<Vec<ClientPeer<T>>>,
    batch_transmission: ChannelSyncTx<ClientRqBatchOutput<T>>,
    finish_execution: AtomicBool,
    owner: Arc<ConnectedPeersGroup<T>>,
    batch_size: usize,
    client_limit: usize,
    batch_timeout_micros: u64,
    batch_sleep_micros: u64,
}

impl<T> ConnectedPeersGroup<T> where T: Send + 'static {
    pub fn new(per_client_bound: usize, batch_size: usize,
               batch_transmission: ChannelSyncTx<ClientRqBatchOutput<T>>,
               own_id: NodeId, clients_per_pool: usize, batch_timeout_micros: u64,
               batch_sleep_micros: u64) -> Arc<Self> {
        Arc::new(Self {
            own_id,
            client_pools: Mutex::new(BTreeMap::new()),
            per_client_cache: per_client_bound,
            batch_timeout_micros,
            batch_sleep_micros,
            batch_target_size: batch_size,
            batch_transmission,
            clients_per_pool,
            pool_id_counter: AtomicUsize::new(0),
        })
    }

    fn get_pool_id(&self) -> atlas_common::error::Result<usize> {
        const IT_LIMIT: usize = 100;

        let mut it_count = 0;

        let pool_id = loop {
            let pool_id = self.pool_id_counter.fetch_add(1, Ordering::Relaxed);

            it_count += 1;

            if it_count >= IT_LIMIT {
                return Err!(ClientPoolError::FailedToAllocateClientPoolID);
            }

            if !self.client_pools.lock().unwrap().contains_key(&pool_id) {
                break pool_id;
            }
        };

        Ok(pool_id)
    }

    pub fn init_client(self: &Arc<Self>, peer_id: NodeId) -> ClientPeer<T> {
        let connected_client = Arc::new(ConnectedClientPeer {
            queue: Mutex::new(Vec::with_capacity(self.per_client_cache)),
            disconnect: AtomicBool::new(false),
        });

        let mut clone_queue = connected_client.clone();

        {
            let guard = self.client_pools.lock().unwrap();

            for (_pool_id, pool) in &*guard {
                match pool.attempt_to_add(clone_queue) {
                    Ok(_) => {
                        return connected_client;
                    }
                    Err(queue) => {
                        clone_queue = queue;
                    }
                }
            }
        }

        //In the case all the pools are already full, allocate a new pool
        let pool_id = match self.get_pool_id() {
            Ok(pool_id) => {
                pool_id
            }
            Err(_err) => {
                panic!("Failed to allocate new pool id");
            }
        };

        let pool = ConnectedPeersPool::new(
            pool_id,
            self.batch_target_size,
            self.batch_transmission.clone(),
            Arc::clone(self),
            self.clients_per_pool,
            self.batch_timeout_micros,
            self.batch_sleep_micros);

        match pool.attempt_to_add(clone_queue) {
            Ok(_) => {}
            Err(_e) => {
                panic!("Failed to add pool to pool list.")
            }
        };
        {
            let mut guard = self.client_pools.lock().unwrap();

            let pool_clone = pool.clone();

            guard.insert(pool.pool_id, pool);

            let id = guard.len();

            pool_clone.start(id as u32);
        }


        connected_client
    }

    fn del_pool(&self, pool_id: usize) -> bool {
        println!("{:?} // DELETING POOL {}", self.own_id, pool_id);

        match self.client_pools.lock().unwrap().remove(&pool_id) {
            None => { false }
            Some(pool) => {
                pool.shutdown();
                println!("{:?} // DELETED POOL {}", self.own_id, pool_id);

                true
            }
        }
    }
}

impl<T> ConnectedPeersPool<T> where T: Send {
    //We mark the owner as static since if the pool is active then
    //The owner also has to be active
    pub fn new(pool_id: usize, batch_size: usize, batch_transmission: ChannelSyncTx<ClientRqBatchOutput<T>>,
               owner: Arc<ConnectedPeersGroup<T>>, client_per_pool: usize,
               batch_timeout_micros: u64, batch_sleep_micros: u64) -> Arc<Self> {
        let result = Self {
            pool_id,
            connected_clients: Mutex::new(Vec::new()),
            batch_size,
            batch_transmission,
            batch_timeout_micros,
            batch_sleep_micros,
            client_limit: client_per_pool,
            finish_execution: AtomicBool::new(false),
            owner,
        };

        let pool = Arc::new(result);

        pool
    }

    pub fn start(self: Arc<Self>, pool_id: u32) {

        //Spawn the thread that will collect client requests
        //and then send the batches to the channel.
        std::thread::Builder::new()
            .name(format!("Peer pool collector thread #{}", pool_id))
            .spawn(move || {
                loop {
                    if self.finish_execution.load(Ordering::Relaxed) {
                        break;
                    }

                    let vec = match self.collect_requests(self.batch_size, &self.owner) {
                        Ok(vec) => { vec }
                        Err(err) => {
                            match err {
                                ClientPoolError::ClosePool => {
                                    //The pool is empty, so to save CPU, delete it
                                    self.owner.del_pool(self.pool_id);

                                    self.finish_execution.store(true, Ordering::SeqCst);

                                    break;
                                }
                                _ => { break; }
                            }
                        }
                    };

                    if !vec.is_empty() {
                        self.batch_transmission.send_return((vec, Instant::now()))
                            .expect("Failed to send proposed batch");

                        // Sleep for a determined amount of time to allow clients to send requests
                        let three_quarters_sleep = (self.batch_sleep_micros / 4) * 3;
                        let five_quarters_sleep = (self.batch_sleep_micros / 4) * 5;

                        let sleep_micros = fastrand::u64(three_quarters_sleep..=five_quarters_sleep);

                        std::thread::sleep(Duration::from_micros(sleep_micros));
                    }

                    // backoff.spin();
                }
            }).unwrap();
    }

    pub fn attempt_to_add(&self, client: ClientPeer<T>) -> std::result::Result<(), ClientPeer<T>> {
        let mut guard = self.connected_clients.lock().unwrap();

        if guard.len() < self.client_limit {
            guard.push(client);

            return Ok(());
        }

        Err(client)
    }

    pub fn attempt_to_remove(&self, client_id: &NodeId) -> std::result::Result<bool, ()> {
        let mut guard = self.connected_clients.lock().unwrap();

        return match guard.iter().position(|client| client.client_id().eq(client_id)) {
            None => {
                Err(())
            }
            Some(position) => {
                guard.swap_remove(position);

                Ok(guard.is_empty())
            }
        };
    }

    pub fn collect_requests(&self, batch_target_size: usize, owner: &Arc<ConnectedPeersGroup<T>>) -> std::result::Result<Vec<T>, ClientPoolError> {
        let start = Instant::now();

        let vec_size = std::cmp::max(batch_target_size, self.owner.per_client_cache);

        let mut batch = Vec::with_capacity(vec_size);

        let guard = self.connected_clients.lock().unwrap();

        let mut dced = Vec::new();

        let mut connected_peers = Vec::with_capacity(guard.len());

        if guard.len() == 0 {
            return Err!(ClientPoolError::ClosePool);
        }

        for connected_peer in &*guard {
            connected_peers.push(Arc::clone(connected_peer));
        }

        drop(guard);

        let start_point = fastrand::usize(0..connected_peers.len());

        let ind_limit = usize::MAX;

        let start_time = Instant::now();

        let mut replacement_vec = Vec::with_capacity(self.owner.per_client_cache);

        for index in 0..ind_limit {
            let client = &connected_peers[(start_point + index) % connected_peers.len()];

            if client.is_dc() {
                dced.push(client.client_id().clone());

                //Assign the remaining slots to the next client
                continue;
            }

            //Collect all possible requests from each client

            let mut rqs_dumped = match client.dump_requests(replacement_vec) {
                Ok(rqs) => { rqs }
                Err(vec) => {
                    dced.push(client.client_id().clone());

                    replacement_vec = vec;
                    continue;
                }
            };

            batch.append(&mut rqs_dumped);

            //The previous vec is now the new vec of the next node
            replacement_vec = rqs_dumped;

            if index % connected_peers.len() == 0 {
                //We have done a full circle on the requests

                if batch.len() >= batch_target_size {
                    //We only check on each complete revolution since if we didn't do that
                    //We could have a situation where a single client's requests were
                    //Enough to fill an entire batch, so the rest of the clients
                    //Wouldn't even be checked
                    break;
                } else {
                    let current_time = Instant::now();

                    if current_time.duration_since(start_time).as_micros() >= self.batch_timeout_micros as u128 {
                        //Check if a given amount of time limit has passed, to prevent us getting
                        //Stuck while checking for requests
                        break;
                    }

                    std::thread::yield_now();
                }
            }
        }

        //This might cause some lag since it has to access the intmap, but
        //Should be fine as it will only happen on client dcs
        if !dced.is_empty() {
            let mut guard = self.connected_clients.lock().unwrap();

            for node in &dced {
                //This is O(n*c) but there isn't really a much better way to do it I guess
                let option = guard.iter().position(|x| {
                    x.client_id().0 == node.0
                });

                match option {
                    None => {
                        //The client was already removed from the guard
                    }
                    Some(option) => {
                        guard.swap_remove(option);
                    }
                }
            }

            //If the pool is empty, delete it
            let should_delete_pool = guard.is_empty();

            drop(guard);

            owner.del_cached_clients(dced);

            if should_delete_pool {
                return Err!(ClientPoolError::ClosePool);
            }
        }

        metric_duration(CLIENT_POOL_COLLECT_TIME_ID, start.elapsed());

        Ok(batch)
    }

    pub fn shutdown(&self) {
        info!("{:?} // Pool {} is shutting down", self.owner.own_id, self.pool_id);

        self.finish_execution.store(true, Ordering::Relaxed);
    }
}

impl<T> PeerConn<T> for ConnectedClientPeer<T> {
    fn is_dc(&self) -> bool {
        self.disconnect.load(Ordering::Relaxed)
    }

    fn disconnect(&self) {
        self.disconnect.store(true, Ordering::Relaxed)
    }

    fn dump_requests(&self, replacement_vec: Vec<T>) -> Result<Vec<T>, Vec<T>> {
        let mut guard = self.queue.lock().unwrap();

        match &mut *guard {
            None => {
                Err(replacement_vec)
            }
            Some(rqs) => {
                Ok(std::mem::replace(rqs, replacement_vec))
            }
        }
    }

    fn push_request(&self, msg: T) -> atlas_common::error::Result<()> {

        let mut sender_guard = self.queue.lock().unwrap();

        match &mut *sender_guard {
            None => {
                error!("Failed to send to client {:?} as he was already disconnected", self.peer_id);

                Err!(ClientPoolError::PooledConnectionClosed(self.peer_id.clone()))
            }
            Some(sender) => {
                //We don't clone and ditch the lock since each replica
                //has a thread dedicated to receiving his requests, but only the single thread
                //So, no more than one thread will be trying to acquire this lock at the same time
                sender.push(msg);

                Ok(())
            }
        }
    }
}