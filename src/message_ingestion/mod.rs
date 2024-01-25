use atlas_common::error::*;

use crate::lookup_table::{LookupTable, MessageModuleSerialization, PeerStubLookupTable};
use crate::message::WireMessage;
use crate::serialization::{deserialize_message, Serializable};

/// Process a message received from the byte layer of the network.
/// Requires the lookup table to be able to get the appropriate type to deserialize the message.
/// Then, stubs are retrieved from the peer stub lookup table and the message is pushed to the appropriate stub.
pub(crate) fn process_wire_message_message<R, O, S, A>(message: WireMessage,
                                                       lookup_table: impl LookupTable<R, O, S, A>,
                                                       stubs: impl PeerStubLookupTable<R, O, S, A>) -> Result<()>
    where R: Serializable,
          O: Serializable,
          S: Serializable,
          A: Serializable
{

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

