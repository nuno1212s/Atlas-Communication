use thiserror::Error;
use atlas_common::crypto::hash::Digest;
use atlas_common::Err;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;

use crate::lookup_table::{LookupTable, MessageModuleSerialization, PeerStubLookupTable};
use crate::message::{Buf, Header, verify_validity, WireMessage};
use crate::reconfiguration_node::NetworkInformationProvider;
use crate::serialization;
use crate::serialization::{deserialize_message, Serializable};

/// Verify the validity of a message, for any serializable type.
/// Performs the serialization and follow up digest and signature verification of the byte message.
///
/// Guarantees AUTHENTICITY, INTEGRITY and NON-REPUDIATION of the message.
pub fn verify_message_validity<M>(network_info: &impl NetworkInformationProvider, header: &Header, message: &M::Message) -> Result<()>
    where M: Serializable {
    let mut msg = Vec::new();

    serialization::serialize_message::<Vec<u8>, M>(&mut msg, message)?;

    let message_bytes = Buf::from(msg);

    verify_ser_message_validity(network_info, header, &message_bytes)?;

    Ok(())
}

/// Verifies the validity of a serialized network message
///
/// Guarantees AUTHENTICITY, INTEGRITY and NON-REPUDIATION of the message.
pub(crate) fn verify_ser_message_validity(network_info: &impl NetworkInformationProvider, header: &Header, message: &Buf) -> Result<()> {
    let digest = serialization::digest_message(message)?;

    if *header.digest() != digest {
        return Err!(IngestionError::DigestDoesNotMatch(digest, header.digest().clone()));
    }

    let pub_key = network_info.get_public_key(&header.from());

    if let Some(key) = pub_key {
        if !verify_validity(header, message, true, Some(&key)) {
            Err!(IngestionError::InvalidSignature)
        } else {
            Ok(())
        }
    } else {
        Err!(IngestionError::NodeNotKnown(header.from()))
    }
}

/// Process a message received from the byte layer of the network.
/// Requires the lookup table to be able to get the appropriate type to deserialize the message.
/// Then, stubs are retrieved from the peer stub lookup table and the message is pushed to the appropriate stub.
pub(crate) fn process_wire_message_message<R, O, S, A>(message: WireMessage,
                                                       network_info: &impl NetworkInformationProvider,
                                                       lookup_table: &impl LookupTable<R, O, S, A>,
                                                       stubs: &impl PeerStubLookupTable<R, O, S, A>) -> Result<()>
    where R: Serializable,
          O: Serializable,
          S: Serializable,
          A: Serializable
{
    if let Err(e) = verify_ser_message_validity(network_info, message.header(), message.payload_buf()) {
        return Err(e);
    }

    let (header, module, message) = message.into_inner();

    // FIXME: Is this part with the lookup table even necessary? We just directly type in the types anyways so I think it is redundant.
    let serialization_mod = lookup_table.get_module_for_message(&module);

    let stub = stubs.get_stub_for_message(&module);

    match serialization_mod {
        MessageModuleSerialization::Reconfiguration(_) => {
            let m = deserialize_message::<&[u8], R>(&message)?;

            stub.push_reconfiguration(header, m)?;
        }
        MessageModuleSerialization::Protocol(_) => {
            let m = deserialize_message::<&[u8], O>(&message)?;

            stub.push_protocol(header, m)?;
        }
        MessageModuleSerialization::StateProtocol(_) => {
            let m = deserialize_message::<&[u8], S>(&message)?;

            stub.push_state_protocol(header, m)?;
        }
        MessageModuleSerialization::Application(_) => {
            let m = deserialize_message::<&[u8], A>(&message)?;

            stub.push_application(header, m)?;
        }
    }

    Ok(())
}

/// The error type for ingestion of messages
#[derive(Error, Debug)]
pub enum IngestionError {
    #[error("The digest of the message {0:?} does not match the digest in the header {1:?}")]
    DigestDoesNotMatch(Digest, Digest),
    #[error("We do not know the node {0:?} (No Public key available)")]
    NodeNotKnown(NodeId),
    #[error("The message we have received has an invalid signature")]
    InvalidSignature,
}
