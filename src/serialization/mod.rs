#[cfg(feature = "serialize_capnp")]
pub(super) mod capnp;

#[cfg(feature = "serialize_serde")]
pub(super) mod serde;


use std::io::{Read, Write};
use std::sync::Arc;
use atlas_common::crypto::hash::{Context, Digest};
use atlas_common::serialization_helper::SerType;
use atlas_common::error::*;
use crate::message::{Buf, Header};

pub fn serialize_message<W, T>(w: &mut W, message: &T::Message) -> Result<()>
    where T: Serializable,
          W: Write + AsRef<[u8]> + AsMut<[u8]> {

    #[cfg(feature = "serialize_capnp")]
    capnp::serialize_message::<W, RM, PM>(w, msg)?;

    #[cfg(feature = "serialize_serde")]
    serde::serialize_message::<W, T>(message, w)?;

    Ok(())
}


pub fn deserialize_message<R, T>(r: R) -> Result<T::Message>
    where R: Read + AsRef<[u8]>, T: Serializable {
    #[cfg(feature = "serialize_capnp")]
        let result = capnp::deserialize_message::<R, T>(r)?;

    #[cfg(feature = "serialize_serde")]
        let result = serde::deserialize_message::<R, T>(r)?;

    Ok(result)
}

pub fn serialize_digest<W, T>(message: &T::Message, w: &mut W) -> Result<Digest>
    where W: Write + AsRef<[u8]> + AsMut<[u8]>, T: Serializable {
    serialize_message::<W,T>(w, message)?;

    let mut ctx = Context::new();
    ctx.update(w.as_ref());
    Ok(ctx.finish())
}

pub fn digest_message(message: Buf) -> Result<Digest> {
    let mut ctx = Context::new();
    ctx.update(message.as_ref());
    Ok(ctx.finish())
}

pub trait InternalMessageVerifier<M> {
    /// Verify the internals of a given message type.
    /// This isn't meant to verify the integrity and authenticity of the entire message, as that has already been performed.
    /// This is used in cases where messages contain forwarded messages from other members, which must be verified as well
    /// or other similar cases.
    fn verify_message<NI>(info_provider: &Arc<NI>, header: &Header, message: &M) -> atlas_common::error::Result<()>;
        //where NI: NetworkInformationProvider;
}

/// The trait that should be implemented for all systems which wish to use this communication method
pub trait Serializable: Send {
    /// The message type
    type Message: SerType;

    type Verifier: InternalMessageVerifier<Self::Message>;
}