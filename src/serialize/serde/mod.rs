use std::io::{Read, Write};
use anyhow::Context;
use atlas_common::error::*;
use crate::message::NetworkMessageKind;
use crate::serialize::Serializable;

pub fn serialize_message<W, RM, PM, CM>(
    m: &NetworkMessageKind<RM, PM, CM>,
    w: &mut W,
) -> Result<()> where
    W: Write + AsMut<[u8]>,
    RM: Serializable,
    PM: Serializable,
    CM: Serializable {
    bincode::serde::encode_into_std_write(m, w, bincode::config::standard())
        .context(format!("Failed to serialize message {} bytes len", w.as_mut().len()))?;

    Ok(())
}

pub fn deserialize_message<R, RM, PM, CM>(
    r: R
) -> Result<NetworkMessageKind<RM, PM, CM>, >
    where RM: Serializable,
          PM: Serializable,
          CM: Serializable,
          R: Read + AsRef<[u8]> {
    let msg = bincode::serde::decode_borrowed_from_slice(r.as_ref(), bincode::config::standard())
        .context("Failed to deserialize message")?;

    Ok(msg)
}

/*
TODO
pub fn serialize_consensus<W, D>(
    m: &ConsensusMessage<D::Request>,
    w: &mut W,
) -> Result<()> where
    W: Write + AsMut<[u8]>,
    D: SharedData {

    bincode::serde::encode_into_std_write(m,  w, bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, format!("Failed to serialize message {} bytes len", w.as_mut().len()).as_str())?;

    Ok(())
}

pub fn deserialize_consensus<R, D>(
    r: R
) -> Result<ConsensusMessage<D::Request>> where D: SharedData, R: Read + AsRef<[u8]> {
    let msg =  bincode::serde::decode_borrowed_from_slice(r.as_ref(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to deserialize message")?;

    Ok(msg)
}*/