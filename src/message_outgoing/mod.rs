use std::sync::Arc;

use bytes::Bytes;
use either::Either;
use smallvec::SmallVec;

use atlas_common::crypto::hash::Digest;
use atlas_common::crypto::signature::KeyPair;
use atlas_common::node_id::NodeId;
use atlas_common::prng::ThreadSafePrng;

use crate::byte_stub::{ByteNetworkStub, PeerConnectionManager};
use crate::byte_stub::outgoing::PeerOutgoingConnection;
use crate::lookup_table::{LookupTable, MessageModule, ModMessageWrapped};
use crate::message::{Buf, StoredSerializedMessage, WireMessage};
use crate::serialization;
use crate::serialization::Serializable;

const NODE_QUORUM_SIZE: usize = 32;

type SendTos<CN, R, O, S, A> = SmallVec<[SendTo<CN, R, O, S, A>; NODE_QUORUM_SIZE]>;

pub struct SendTo<CN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable {
    from: NodeId,
    to: NodeId,
    shared: Option<Arc<KeyPair>>,
    nonce: u64,
    peer: PeerOutgoingConnection<CN, R, O, S, A>,
}

impl<CN, R, O, S, A> SendTo<CN, R, O, S, A>
    where R: Serializable, O: Serializable,
          S: Serializable, A: Serializable, {
    pub fn initialize_send_tos_serialized<L>(conn_manager: &PeerConnectionManager<CN, R, O, S, A, L>, shared: Option<&Arc<KeyPair>>,
                                             rng: &Arc<ThreadSafePrng>, target: NodeId,
    ) -> (Option<Self>, Option<Self>) {
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
                let connection = stub.outgoing_connection();

                let send_to = SendTo {
                    from: conn_manager.node_id(),
                    to: target,
                    shared: shared.cloned(),
                    nonce,
                    peer: connection,
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

    pub fn initialize_send_tos<L>(conn_manager: &PeerConnectionManager<CN, R, O, S, A, L>, shared: Option<&Arc<KeyPair>>,
                                  rng: &Arc<ThreadSafePrng>, targets: impl Iterator<Item=NodeId>)
                                  -> (Option<Self>, Option<SendTos<CN, R, O, S, A>>)
        where L: LookupTable<R, O, S, A> {
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
                    let connection = stub.outgoing_connection();

                    let send_to = SendTo {
                        from: conn_manager.node_id(),
                        to: target,
                        shared: shared.cloned(),
                        nonce,
                        peer: connection,
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

    pub fn value(self, msg: Either<(ModMessageWrapped<R, O, S, A>, Buf, Digest), (MessageModule, Buf, Digest)>)
        where CN: ByteNetworkStub {
        let key_pair = match &self.shared {
            None => {
                None
            }
            Some(key_pair) => {
                Some(&**key_pair)
            }
        };

        match (self.peer.stub(), msg) {
            (PeerOutgoingConnection::LoopbackStub(stub), Either::Left((msg, buf, digest))) => {
                let wire_msg = WireMessage::new(self.from, self.to, msg.get_module(), buf, self.nonce, Some(digest), key_pair);

                let (header, _, _) = wire_msg.into_inner();

                stub.handle_message(header, msg);
            }
            (PeerOutgoingConnection::OutgoingStub(stub), Either::Right((msg_mod, buf, digest))) => {
                let wire_msg = WireMessage::new(self.from, self.to, msg_mod, buf, self.nonce, Some(digest), key_pair);

                stub.dispatch_message(wire_msg).unwrap();
            }
            _ => unreachable!()
        }
    }

    pub fn value_ser(self, msg: StoredSerializedMessage<ModMessageWrapped<R, O, S, A>>)
        where CN: ByteNetworkStub {
        let key_pair = match &self.shared {
            None => {
                None
            }
            Some(key_pair) => {
                Some(&**key_pair)
            }
        };

        match (self.peer.stub(), msg) {
            (PeerOutgoingConnection::LoopbackStub(stub), StoredSerializedMessage { header, message, .. }) => {
                stub.handle_message(header, message.into_inner().0);
            }
            (PeerOutgoingConnection::OutgoingStub(stub), StoredSerializedMessage { header, message, .. }) => {
                let (msg, buf) = message.into_inner();

                let module = msg.get_module();

                let wire_msg = WireMessage::from_parts(header, module, buf);

                stub.dispatch_message(wire_msg).unwrap();
            }
            _ => unreachable!()
        }
    }
}

pub fn send_message_to_targets<CN, R, O, S, A, L>(conn_manager: &PeerConnectionManager<CN, R, O, S, A, L>,
                                                  shared: Option<&Arc<KeyPair>>,
                                                  rng: &Arc<ThreadSafePrng>,
                                                  message: ModMessageWrapped<R, O, S, A>,
                                                  targets: impl Iterator<Item=NodeId>, )
    where CN: ByteNetworkStub,
          R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          L: LookupTable<R, O, S, A> {
    let (send_to_me, send_tos) = SendTo::initialize_send_tos(conn_manager, shared, rng, targets);

    atlas_common::threadpool::execute(move || {
        let mut buf = Vec::new();

        let message_module = message.get_module();

        let digest = match &message {
            ModMessageWrapped::Reconfiguration(reconf) => {
                serialization::serialize_digest::<Vec<u8>, R>(reconf, &mut buf)?
            }
            ModMessageWrapped::Protocol(protocol) => {
                serialization::serialize_digest::<Vec<u8>, O>(protocol, &mut buf)?
            }
            ModMessageWrapped::StateProtocol(state) => {
                serialization::serialize_digest::<Vec<u8>, S>(state, &mut buf)?
            }
            ModMessageWrapped::Application(application) => {
                serialization::serialize_digest::<Vec<u8>, A>(application, &mut buf)?
            }
        };

        let buf = Bytes::from(buf);

        if let Some(send_to_me) = send_to_me {
            send_to_me.value(Either::Left((message, buf.clone(), digest.clone())));
        }

        if let Some(mut send_tos) = send_tos {
            send_tos.into_iter().for_each(|send_to| {
                send_to.value(Either::Right((message_module, buf.clone(), digest.clone())));
            });
        }
    });
}

pub fn send_serialized_message_to_target<CN, R, O, S, A, L>(conn_manager: &PeerConnectionManager<CN, R, O, S, A, L>,
                                                            message: StoredSerializedMessage<ModMessageWrapped<R, O, S, A>>,
                                                            target: NodeId)
    where CN: ByteNetworkStub,
          R: Serializable, O: Serializable,
          S: Serializable, A: Serializable,
          L: LookupTable<R, O, S, A> {
    let (send_to_me, send_tos) = SendTo::initialize_send_tos_serialized(conn_manager, None, conn_manager.rng(), target);

    atlas_common::threadpool::execute(move || {
        if let Some(send_to_me) = send_to_me {
            send_to_me.value_ser(message);
        } else if let Some(mut send_to) = send_tos {
            send_to.value_ser(message.clone())
        }
    });
    
}