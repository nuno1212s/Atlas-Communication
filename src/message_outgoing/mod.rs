use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use bytes::Bytes;
use either::Either;
use getset::{CopyGetters, Getters};
use log::{error, trace};
use smallvec::SmallVec;

use atlas_common::crypto::hash::Digest;
use atlas_common::crypto::signature::KeyPair;
use atlas_common::node_id::NodeId;
use atlas_common::prng::ThreadSafePrng;
use atlas_common::quiet_unwrap;
use atlas_metrics::metrics::metric_duration;

use crate::byte_stub::{ByteNetworkStub, PeerConnectionManager};
use crate::byte_stub::outgoing::PeerOutgoingConnection;
use crate::lookup_table::{LookupTable, MessageModule, ModMessageWrapped};
use crate::message::{Buf, StoredSerializedMessage, WireMessage};
use crate::metric::{COMM_SERIALIZE_SIGN_TIME_ID, THREADPOOL_PASS_TIME_ID};
use crate::reconfiguration::NetworkInformationProvider;
use crate::serialization;
use crate::serialization::Serializable;

const NODE_QUORUM_SIZE: usize = 32;

type SendTos<CN, R, O, S, A> = SmallVec<[SendTo<CN, R, O, S, A>; NODE_QUORUM_SIZE]>;

#[derive(Getters, CopyGetters)]
pub struct SendTo<CN, R, O, S, A>
    where
        R: Serializable,
        O: Serializable,
        S: Serializable,
        A: Serializable,
{
    from: NodeId,
    #[get = "pub"]
    to: NodeId,
    shared: Option<Arc<KeyPair>>,
    nonce: u64,
    peer: PeerOutgoingConnection<CN, R, O, S, A>,
    #[get_copy = "pub(crate)"]
    authenticated_state: bool,
}

impl<CN, R, O, S, A> SendTo<CN, R, O, S, A>
    where
        R: Serializable,
        O: Serializable,
        S: Serializable,
        A: Serializable,
        CN: Clone,
{
    pub fn initialize_send_tos_serialized<NI, L>(
        conn_manager: &PeerConnectionManager<NI, CN, R, O, S, A, L>,
        shared: Option<&Arc<KeyPair>>,
        rng: &Arc<ThreadSafePrng>,
        target: NodeId,
    ) -> (Option<Self>, Option<Self>)
        where
            L: Clone + Send,
            NI: NetworkInformationProvider,
    {
        let mut send_to_me = None;
        let mut send_tos = None;

        let mut failed_lookup = Vec::new();

        let nonce = rng.next_state();

        let option = conn_manager.get_connection_to_node(&target);

        match option {
            None => {
                failed_lookup.push(target);
            }
            Some(stub) => {
                let connection = stub.outgoing_connection().clone();

                let send_to = SendTo {
                    from: conn_manager.node_id(),
                    to: target,
                    shared: shared.cloned(),
                    nonce,
                    peer: connection,
                    authenticated_state: stub.is_authenticated(),
                };

                if target == conn_manager.node_id() {
                    send_to_me = Some(send_to);
                } else {
                    send_tos = Some(send_to);
                }
            }
        }

        (send_to_me, send_tos)
    }

    pub type InitializedSendTos = (Option<Self>, Option<SendTos<CN, R, O, S, A>>);
    
    pub fn initialize_send_tos<NI, L>(
        conn_manager: &PeerConnectionManager<NI, CN, R, O, S, A, L>,
        shared: Option<&Arc<KeyPair>>,
        rng: &Arc<ThreadSafePrng>,
        targets: impl Iterator<Item=NodeId>,
    ) -> Self::InitializedSendTos
        where
            L: LookupTable<R, O, S, A>,
            NI: NetworkInformationProvider,
    {
        let mut send_to_me = None;
        let mut send_tos = Some(SmallVec::new());

        let mut failed_lookup = Vec::new();

        let nonce = rng.next_state();

        targets.for_each(|target| {
            let option = conn_manager.get_connection_to_node(&target);

            match option {
                None => {
                    failed_lookup.push(target);
                }
                Some(stub) => {
                    let connection = stub.outgoing_connection().clone();

                    let send_to = SendTo {
                        from: conn_manager.node_id(),
                        to: target,
                        shared: shared.cloned(),
                        nonce,
                        peer: connection,
                        authenticated_state: stub.is_authenticated(),
                    };

                    if target == conn_manager.node_id() {
                        send_to_me = Some(send_to);
                    } else {
                        if send_tos.is_none() {
                            send_tos = Some(SmallVec::new());
                        }

                        send_tos.as_mut().unwrap().push(send_to);
                    }
                }
            }
        });

        (send_to_me, send_tos)
    }

    pub type TypedMessage = (ModMessageWrapped<R, O, S, A>, Buf, Digest);
    pub type SerializedMessage = (MessageModule, Buf, Digest);

    pub fn value(
        self,
        msg: Either<Self::TypedMessage, Self::SerializedMessage>,
    ) where
        CN: ByteNetworkStub,
    {
        let key_pair = self.shared.as_deref();

        match (self.peer, msg) {
            (PeerOutgoingConnection::LoopbackStub(stub), Either::Left((msg, buf, digest))) => {
                let wire_msg = WireMessage::new(
                    self.from,
                    self.to,
                    msg.get_module(),
                    buf,
                    self.nonce,
                    Some(digest),
                    key_pair,
                );

                let (header, _, _) = wire_msg.into_inner();

                stub.handle_message(header, msg);
            }
            (PeerOutgoingConnection::OutgoingStub(stub), Either::Right((msg_mod, buf, digest))) => {
                let wire_msg = WireMessage::new(
                    self.from,
                    self.to,
                    msg_mod,
                    buf,
                    self.nonce,
                    Some(digest),
                    key_pair,
                );

                stub.dispatch_message(wire_msg)
                    .context(format!("Failed to send message to node {:?} ", self.to))
                    .unwrap();
            }
            _ => unreachable!(),
        }
    }

    pub fn value_ser(self, msg: StoredSerializedMessage<ModMessageWrapped<R, O, S, A>>)
        where
            CN: ByteNetworkStub,
    {
        match (self.peer, msg) {
            (
                PeerOutgoingConnection::LoopbackStub(stub),
                StoredSerializedMessage {
                    header, message, ..
                },
            ) => {
                stub.handle_message(header, message.into_inner().0);
            }
            (
                PeerOutgoingConnection::OutgoingStub(stub),
                StoredSerializedMessage {
                    header, message, ..
                },
            ) => {
                let (msg, buf) = message.into_inner();

                let module = msg.get_module();

                let wire_msg = WireMessage::from_parts(header, module, buf).unwrap();

                stub.dispatch_message(wire_msg)
                    .context(format!("Failed to send message to node {:?} ", self.to))
                    .unwrap();
            }
        }
    }
}

pub fn send_message_to_targets<NI, CN, R, O, S, A, L>(
    conn_manager: &PeerConnectionManager<NI, CN, R, O, S, A, L>,
    shared: Option<&Arc<KeyPair>>,
    rng: &Arc<ThreadSafePrng>,
    message: ModMessageWrapped<R, O, S, A>,
    targets: impl Iterator<Item=NodeId>,
) where
    CN: ByteNetworkStub + 'static,
    R: Serializable + 'static,
    O: Serializable + 'static,
    S: Serializable + 'static,
    A: Serializable + 'static,
    L: LookupTable<R, O, S, A>,
    NI: NetworkInformationProvider,
{
    if shared.is_none() {
        trace!(
            "Sending message from module {:?} without authentication",
            message.get_module()
        );
    }

    let (send_to_me, send_tos) = SendTo::initialize_send_tos(conn_manager, shared, rng, targets);

    let alloc_time = Instant::now();
    
    atlas_common::threadpool::execute(move || {
        metric_duration(THREADPOOL_PASS_TIME_ID, alloc_time.elapsed());
        
        let serialize_time_start = Instant::now();
        
        let mut buf = Vec::new();

        let message_module = message.get_module();

        let digest = match &message {
            ModMessageWrapped::Reconfiguration(reconf) => {
                serialization::serialize_digest::<Vec<u8>, R>(reconf, &mut buf)
            }
            ModMessageWrapped::Protocol(protocol) => {
                serialization::serialize_digest::<Vec<u8>, O>(protocol, &mut buf)
            }
            ModMessageWrapped::StateProtocol(state) => {
                serialization::serialize_digest::<Vec<u8>, S>(state, &mut buf)
            }
            ModMessageWrapped::Application(application) => {
                serialization::serialize_digest::<Vec<u8>, A>(application, &mut buf)
            }
        };

        let digest = quiet_unwrap!(digest);

        let buf = Bytes::from(buf);

        if let Some(send_to_me) = send_to_me {
            send_to_me.value(Either::Left((message, buf.clone(), digest)));
        }

        if let Some(send_tos) = send_tos {
            send_tos.into_iter().for_each(|send_to| {
                if send_to.authenticated_state() {
                    send_to.value(Either::Right((message_module.clone(), buf.clone(), digest)));
                } else {
                    match &message_module {
                        MessageModule::Reconfiguration => {
                            send_to.value(Either::Right((
                                message_module.clone(),
                                buf.clone(),
                                digest,
                            )));
                        }
                        _ => {
                            error!(
                                "Attempted to send message to node {:?} while unauthenticated",
                                send_to.to()
                            );
                        }
                    }
                }
            });
        }

        metric_duration(COMM_SERIALIZE_SIGN_TIME_ID, serialize_time_start.elapsed());
    });
}

pub fn send_serialized_message_to_target<NI, CN, R, O, S, A, L>(
    conn_manager: &PeerConnectionManager<NI, CN, R, O, S, A, L>,
    message: StoredSerializedMessage<ModMessageWrapped<R, O, S, A>>,
    target: NodeId,
) where
    CN: ByteNetworkStub + 'static,
    R: Serializable + 'static,
    O: Serializable + 'static,
    S: Serializable + 'static,
    A: Serializable + 'static,
    L: LookupTable<R, O, S, A>,
    NI: NetworkInformationProvider,
{
    let (send_to_me, send_tos) =
        SendTo::initialize_send_tos_serialized(conn_manager, None, conn_manager.rng(), target);

    atlas_common::threadpool::execute(move || {
        if let Some(send_to_me) = send_to_me {
            send_to_me.value_ser(message);
        } else if let Some(send_to) = send_tos {
            if send_to.authenticated_state() {
                send_to.value_ser(message)
            } else {
                match &message.message().original().get_module() {
                    MessageModule::Reconfiguration => send_to.value_ser(message),
                    _ => {
                        error!(
                            "Attempted to send message to node {:?} while unauthenticated",
                            send_to.to()
                        );
                    }
                }
            }
        }
    });
}
