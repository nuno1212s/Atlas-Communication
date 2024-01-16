//! This module contains types associated with messages traded
//! between the system processes.

use std::fmt::{Debug, Formatter};
use std::io;
use std::io::Write;
use std::mem::MaybeUninit;
use std::ops::Deref;
use bytes::Bytes;
use futures::{AsyncWrite, AsyncWriteExt};

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};
use thiserror::Error;

use atlas_common::error::*;

use atlas_common::crypto::hash::{Context, Digest};
use atlas_common::crypto::signature::{KeyPair, PublicKey, Signature};
use atlas_common::Err;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};

use crate::serialize::{Buf, Serializable};

// convenience type
pub type StoredSerializedNetworkMessage<RM, PM, CM> = StoredMessage<SerializedMessage<NetworkMessageKind<RM, PM, CM>>>;

pub type StoredSerializedProtocolMessage<M> = StoredMessage<SerializedMessage<M>>;

pub struct SerializedMessage<M> {
    original: M,
    raw: Buf,
}

impl<M> SerializedMessage<M> {
    pub fn new(original: M, raw: Buf) -> Self {
        Self { original, raw }
    }

    pub fn original(&self) -> &M {
        &self.original
    }

    pub fn raw(&self) -> &Buf {
        &self.raw
    }

    pub fn into_inner(self) -> (M, Buf) {
        (self.original, self.raw)
    }
}

/// Contains a system message as well as its respective header.
/// Convenience type to allow to store messages more directly, instead of having
/// the entire network message wrapper (with type of message, etc)
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct StoredMessage<M> {
    header: Header,
    message: M,
}

impl<M> StoredMessage<M> {
    /// Constructs a new `StoredMessage`.
    pub fn new(header: Header, message: M) -> Self {
        Self { header, message }
    }

    /// Returns the stored message's header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns the stored system message.
    pub fn message(&self) -> &M {
        &self.message
    }

    /// Return the inner types of this `StoredMessage`.
    pub fn into_inner(self) -> (Header, M) {
        (self.header, self.message)
    }
}

impl<M> Orderable for StoredMessage<M> where M: Orderable {
    fn sequence_number(&self) -> SeqNo {
        self.message().sequence_number()
    }
}

impl<M> Clone for StoredMessage<M> where M: Clone {
    fn clone(&self) -> Self {
        Self { header: self.header.clone(), message: self.message.clone() }
    }
}

impl<M> Debug for StoredMessage<M> where M: Debug {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StoredMessage {{ header: {:?}, message: {:?} }}", self.header, self.message)
    }
}

///
/// The messages that are going to be sent over the network
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct NetworkMessage<RM, PM, CM> where RM: Serializable, PM: Serializable, CM: Serializable {
    pub header: Header,
    pub message: NetworkMessageKind<RM, PM, CM>,
}

impl<RM, PM, CM> NetworkMessage<RM, PM, CM> where RM: Serializable, PM: Serializable, CM: Serializable {
    pub fn new(header: Header, message: NetworkMessageKind<RM, PM, CM>) -> Self {
        Self { header, message }
    }

    pub fn into_inner(self) -> (Header, NetworkMessageKind<RM, PM, CM>) {
        (self.header, self.message)
    }
}

impl<RM, PM, CM> From<(Header, NetworkMessageKind<RM, PM, CM>)> for NetworkMessage<RM, PM, CM>
    where RM: Serializable,
          PM: Serializable,
          CM: Serializable {
    fn from(value: (Header, NetworkMessageKind<RM, PM, CM>)) -> Self {
        NetworkMessage { header: value.0, message: value.1 }
    }
}

/// The type of network message you want to send
/// To initialize a System message, you should use the [`From<PM::Message>`] implementation
/// that is available.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum NetworkMessageKind<RM, PM, CM> where PM: Serializable, RM: Serializable, CM: Serializable {
    ReconfigurationMessage(Reconfig<RM::Message>),
    Ping(PingMessage),
    System(System<PM::Message>),
    Orderable(ClientSys<CM::Message>),
}

impl<RM, PM, CM> Clone for NetworkMessageKind<RM, PM, CM> where RM: Serializable, PM: Serializable, CM: Serializable {
    fn clone(&self) -> Self {
        match self {
            NetworkMessageKind::Ping(ping) => { NetworkMessageKind::Ping(ping.clone()) }
            NetworkMessageKind::System(sys) => { NetworkMessageKind::System(sys.clone()) }
            NetworkMessageKind::Orderable(sys) => { NetworkMessageKind::Orderable(sys.clone()) }
            NetworkMessageKind::ReconfigurationMessage(reconfig) => { NetworkMessageKind::ReconfigurationMessage(reconfig.clone()) }
        }
    }
}

impl<RM, PM, CM> NetworkMessageKind<RM, PM, CM> where RM: Serializable, PM: Serializable, CM: Serializable {
    pub fn from_system(msg: PM::Message) -> Self {
        NetworkMessageKind::System(System::from(msg))
    }

    pub fn from_c_system(msg: CM::Message) -> Self { NetworkMessageKind::Orderable(ClientSys::from(msg)) }

    pub fn from_reconfig(msg: RM::Message) -> Self {
        NetworkMessageKind::ReconfigurationMessage(Reconfig::from(msg))
    }

    pub fn deref_system(&self) -> &PM::Message {
        match self {
            NetworkMessageKind::System(sys) => {
                sys
            }
            _ => unreachable!()
        }
    }

    pub fn deref_c_system(&self) -> &CM::Message {
        match self {
            NetworkMessageKind::Orderable(sys) => {
                sys
            }
            _ => unreachable!()
        }
    }

    pub fn deref_reconfig(&self) -> &RM::Message {
        match self {
            NetworkMessageKind::ReconfigurationMessage(reconfig) => {
                reconfig
            }
            _ => unreachable!()
        }
    }

    pub fn into(self) -> PM::Message {
        match self {
            NetworkMessageKind::System(sys_msg) => {
                sys_msg.inner
            }
            _ => unreachable!()
        }
    }

    pub fn into_system(self) -> PM::Message {
        match self {
            NetworkMessageKind::System(sys_msg) => {
                sys_msg.inner
            }
            _ => unreachable!()
        }
    }

    pub fn into_c_system(self) -> CM::Message {
        match self {
            NetworkMessageKind::Orderable(ord) => {
                ord.inner
            }
            _ => unreachable!()
        }
    }

    pub fn into_reconfig(self) -> RM::Message {
        match self {
            NetworkMessageKind::ReconfigurationMessage(reconfig_msg) => {
                reconfig_msg.inner
            }
            _ => unreachable!()
        }
    }
}

impl<RM, PM, CM> From<System<PM::Message>> for NetworkMessageKind<RM, PM, CM> where RM: Serializable, PM: Serializable, CM: Serializable {
    fn from(value: System<PM::Message>) -> Self {
        NetworkMessageKind::System(value)
    }
}

impl<RM, PM, CM> From<ClientSys<CM::Message>> for NetworkMessageKind<RM, PM, CM> where RM: Serializable, PM: Serializable, CM: Serializable {
    fn from(value: ClientSys<CM::Message>) -> Self {
        NetworkMessageKind::Orderable(value)
    }
}

impl<RM, PM, CM> Debug for NetworkMessageKind<RM, PM, CM> where RM: Serializable, PM: Serializable, CM: Serializable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkMessageKind::Ping(ping) => {
                write!(f, "Ping message Request ({})", ping.is_request())
            }
            NetworkMessageKind::System(_) => {
                write!(f, "System message")
            }
            NetworkMessageKind::ReconfigurationMessage(_) => {
                write!(f, "Reconfiguration message")
            }
            NetworkMessageKind::Orderable(_) => {
                write!(f, "Orderable message")
            }
        }
    }
}

/// A client system message, used by clients and replicas in order to communicate
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ClientSys<M: Clone> {
    inner: M,
}

impl<M> ClientSys<M> where M: Clone {
    pub fn into(self) -> M { self.inner }
}

impl<M: Clone> Deref for ClientSys<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M: Clone> From<M> for ClientSys<M> {
    fn from(value: M) -> Self {
        Self {
            inner: value
        }
    }
}

/// A system message, relating to the protocol that is utilizing this communication framework
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct System<M: Clone> {
    inner: M,
}

impl<M> System<M> where M: Clone {
    pub fn into(self) -> M {
        self.inner
    }
}

impl<M: Clone> Deref for System<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M: Clone> From<M> for System<M> {
    fn from(value: M) -> Self {
        System { inner: value }
    }
}


/// A system message, relating to the protocol that is utilizing this communication framework
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct Reconfig<M: Clone> {
    inner: M,
}

impl<M> Reconfig<M> where M: Clone {
    pub fn into(self) -> M {
        self.inner
    }
}

impl<M: Clone> Deref for Reconfig<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M: Clone> From<M> for Reconfig<M> {
    fn from(value: M) -> Self {
        Self { inner: value }
    }
}

/// A header that is sent before a message in transit in the wire.
///
/// A fixed amount of `Header::LENGTH` bytes are read before
/// a message is read. Contains the protocol version, message
/// length, as well as other metadata.
#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(C, packed)]
pub struct Header {
    // manually align memory for cross platform compat
    pub(crate) _align: u32,
    // the protocol version
    pub(crate) version: u32,
    // origin of the message
    pub(crate) from: u32,
    // destination of the message
    pub(crate) to: u32,
    // a random number
    pub(crate) nonce: u64,
    // length of the payload
    pub(crate) length: u64,
    // the digest of the serialized payload
    pub(crate) digest: [u8; Digest::LENGTH],
    // sign(hash(le(from) + le(to) + le(nonce) + hash(serialize(payload))))
    pub(crate) signature: [u8; Signature::LENGTH],
}

#[cfg(feature = "serialize_serde")]
impl serde::Serialize for Header {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
    {
        // TODO: improve this, to avoid allocating a `Vec`
        let mut bytes = vec![0; Self::LENGTH];
        let hdr: &[u8; Self::LENGTH] = unsafe { std::mem::transmute(self) };
        bytes.copy_from_slice(&hdr[..]);
        serde_bytes::serialize(&bytes, serializer)
    }
}

#[cfg(feature = "serialize_serde")]
impl<'de> serde::Deserialize<'de> for Header {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Header, D::Error>
        where
            D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        let mut hdr: [u8; Self::LENGTH] = [0; Self::LENGTH];
        hdr.copy_from_slice(&bytes);
        Ok(unsafe { std::mem::transmute(hdr) })
    }
}

/// A message to be sent over the wire. The payload should be a serialized
/// `SystemMessage`, for correctness.
#[derive(Debug)]
pub struct WireMessage {
    pub(crate) header: Header,
    pub(crate) payload: Bytes,
}

/// A generic `WireMessage`, for different `AsRef<[u8]>` types.
#[derive(Clone, Debug)]
pub struct OwnedWireMessage<T> {
    pub(crate) header: Header,
    pub(crate) payload: T,
}

///
/// Ping messages
/// @{

///Contains a boolean representing if this is a request.
///If it is a ping request, should be set to true,
///ping responses should be false
///
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct PingMessage {
    request: bool,
}

impl PingMessage {
    pub fn new(is_request: bool) -> Self {
        Self {
            request: is_request
        }
    }

    pub fn is_request(&self) -> bool {
        self.request
    }
}

///}@

// FIXME: perhaps use references for serializing and deserializing,
// to save a stack allocation? probably overkill
impl Header {
    /// The size of the memory representation of the `Header` in bytes.
    pub const LENGTH: usize = std::mem::size_of::<Self>();

    unsafe fn serialize_into_unchecked(self, buf: &mut [u8]) {
        #[cfg(target_endian = "big")]
        {
            self.version = self.version.to_le();
            self.nonce = self.nonce.to_le();
            self.from = self.from.to_le();
            self.to = self.to.to_le();
            self.length = self.length.to_le();
        }
        let hdr: [u8; Self::LENGTH] = std::mem::transmute(self);
        (&mut buf[..Self::LENGTH]).copy_from_slice(&hdr[..]);
    }

    /// Serialize a `Header` into a byte buffer of appropriate size.
    pub fn serialize_into(self, buf: &mut [u8]) -> Result<()> {
        if buf.len() < Self::LENGTH {
            return Err!(MessageErrors::InvalidSizeSerDest(buf.len()));
        }
        Ok(unsafe { self.serialize_into_unchecked(buf) })
    }

    unsafe fn deserialize_from_unchecked(buf: &[u8]) -> Self {
        let mut hdr: [u8; Self::LENGTH] = {
            let hdr = MaybeUninit::uninit();
            hdr.assume_init()
        };
        (&mut hdr[..]).copy_from_slice(&buf[..Self::LENGTH]);
        #[cfg(target_endian = "big")]
        {
            hdr.version = hdr.version.to_be();
            hdr.nonce = hdr.nonce.to_be();
            hdr.from = hdr.from.to_be();
            hdr.to = hdr.to.to_le();
            hdr.length = hdr.length.to_be();
        }
        std::mem::transmute(hdr)
    }

    /// Deserialize a `Header` from a byte buffer of appropriate size.
    pub fn deserialize_from(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::LENGTH {
            return Err!(MessageErrors::InvalidSizeHeader(buf.len()));
        }
        Ok(unsafe { Self::deserialize_from_unchecked(buf) })
    }

    /// Reports the current version of the wire protocol,
    /// i.e. `WireMessage::CURRENT_VERSION`.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The originating `NodeId`.
    pub fn from(&self) -> NodeId {
        self.from.into()
    }

    /// The destination `NodeId`.
    pub fn to(&self) -> NodeId {
        self.to.into()
    }

    /// The length of the payload associated with this `Header`.
    pub fn payload_length(&self) -> usize {
        self.length as usize
    }

    /// The signature of this `Header` and associated payload.
    pub fn signature(&self) -> &Signature {
        unsafe { std::mem::transmute(&self.signature) }
    }

    /// The digest of the associated payload serialized data.
    pub fn digest(&self) -> &Digest {
        unsafe { std::mem::transmute(&self.digest) }
    }

    /// Hashes the digest of the associated message's payload
    /// with this header's nonce.
    ///
    /// This is useful for attaining a unique identifier for
    /// a particular client request.
    pub fn unique_digest(&self) -> Digest {
        self.digest().entropy(self.nonce.to_le_bytes())
    }

    /// Returns the nonce associated with this `Header`.
    pub fn nonce(&self) -> u64 {
        self.nonce
    }
}

/*
impl From<WireMessage<'_>> for OwnedWireMessage<Box<[u8]>> {
    fn from(wm: WireMessage<'_>) -> Self {
        OwnedWireMessage {
            header: wm.header,
            payload: Vec::from(wm.payload).into_boxed_slice(),
        }
    }
}

impl From<WireMessage<'_>> for OwnedWireMessage<Vec<u8>> {
    fn from(wm: WireMessage<'_>) -> Self {
        OwnedWireMessage {
            header: wm.header,
            payload: Vec::from(wm.payload),
        }
    }
}

impl<T: Array<Item=u8>> From<WireMessage<'_>> for OwnedWireMessage<SmallVec<T>> {
    fn from(wm: WireMessage<'_>) -> Self {
        OwnedWireMessage {
            header: wm.header,
            payload: SmallVec::from(wm.payload),
        }
    }
}

impl<T: AsRef<[u8]>> OwnedWireMessage<T> {
    /// Returns a reference to a `WireMessage`.
    pub fn borrowed<'a>(&'a self) -> WireMessage<'a> {
        WireMessage {
            header: self.header,
            payload: self.payload,
        }
    }
}
*/

impl WireMessage {
    /// The current version of the wire protocol.
    pub const CURRENT_VERSION: u32 = 0;

    /// Wraps a `Header` and a byte array payload into a `WireMessage`.
    pub fn from_parts(header: Header, payload: Buf) -> Result<Self> {
        let wm = Self { header, payload };
        if !wm.is_valid(None, true) {
            return Err!(MessageErrors::InvalidWireMessage);
        }
        Ok(wm)
    }

    pub fn from_header(header: Header) -> Result<Self> {
        let wm = Self { header, payload: Buf::new() };
        if !wm.is_valid(None, false) {
            return Err!(MessageErrors::InvalidWireMessage);
        }
        Ok(wm)
    }

    /// Constructs a new message to be sent over the wire.
    pub fn new(
        from: NodeId,
        to: NodeId,
        payload: Buf,
        nonce: u64,
        digest: Option<Digest>,
        sk: Option<&KeyPair>,
    ) -> Self {
        let digest = digest
            // safety: digests have repr(transparent)
            .map(|d| unsafe { std::mem::transmute(d) })
            // if payload length is 0
            .unwrap_or([0; Digest::LENGTH]);

        let signature = sk
            .map(|sk| {
                let signature = crate::message_signing::sign_parts(
                    sk,
                    from.into(),
                    to.into(),
                    nonce,
                    &digest[..],
                );
                // safety: signatures have repr(transparent)
                unsafe { std::mem::transmute(signature) }
            })
            .unwrap_or([0; Signature::LENGTH]);

        let (from, to) = (from.into(), to.into());

        let header = Header {
            _align: 0,
            version: Self::CURRENT_VERSION,
            length: payload.len() as u64,
            signature,
            digest,
            nonce,
            from,
            to,
        };

        Self { header, payload }
    }

    /// Retrieve the inner `Header` and payload byte buffer stored
    /// inside the `WireMessage`.
    pub fn into_inner(self) -> (Header, Buf) {
        (self.header, self.payload)
    }

    /// Returns a reference to the `Header` of the `WireMessage`.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns a reference to the payload bytes of the `WireMessage`.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Checks for the correctness of the `WireMessage`. This implies
    /// checking its signature, if a `PublicKey` is provided.
    pub fn is_valid(&self, public_key: Option<&PublicKey>, check_payload_len: bool) -> bool {
        let preliminary_check_failed =
            self.header.version != WireMessage::CURRENT_VERSION
                || (check_payload_len && self.header.length != self.payload.len() as u64);

        if preliminary_check_failed {
            return false;
        }

        public_key
            .map(|pk| {
                crate::message_signing::verify_parts(
                    pk,
                    self.header.signature(),
                    self.header.from,
                    self.header.to,
                    self.header.nonce,
                    &self.header.digest[..],
                ).is_ok()
            })
            .unwrap_or(true)
    }

    /// Serialize a `WireMessage` into an async writer.
    pub async fn write_to<W: AsyncWrite + Unpin>(&self, mut w: W, flush: bool) -> io::Result<()> {
        let mut buf = [0; Header::LENGTH];
        self.header.serialize_into(&mut buf[..]).unwrap();

        w.write_all(&buf[..]).await?;

        if self.payload.len() > 0 {
            w.write_all(&self.payload[..]).await?;
        }

        if flush {
            w.flush().await?;
        }

        Ok(())
    }

    /// Serialize a `WireMessage` into an async writer.
    pub fn write_to_sync<W: Write>(&self, mut w: W, flush: bool) -> io::Result<()> {
        let mut buf = [0; Header::LENGTH];
        self.header.serialize_into(&mut buf[..]).unwrap();

        w.write_all(&buf[..]);

        if self.payload.len() > 0 {
            w.write_all(&self.payload[..]);
        }

        if flush {
            w.flush();
        }

        Ok(())
    }

    /// Converts this `WireMessage` into an owned one.
    pub fn with_owned_buffer<T: AsRef<[u8]>>(self, buf: T) -> Option<OwnedWireMessage<T>> {
        let buf_p = buf.as_ref()[0] as *const u8;
        let payload_p = &self.payload[0] as *const u8;

        // both point to the same memory region, safe
        if buf_p == payload_p {
            Some(OwnedWireMessage {
                header: self.header,
                payload: buf,
            })
        } else {
            None
        }
    }
}

impl Debug for Header {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let version = self.version;
        let length = self.length;
        let signature = self.signature.chunks(4).next().unwrap();
        let digest = self.digest.chunks(4).next().unwrap();
        let nonce = self.nonce;
        let from = self.from;
        let to = self.to;

        write!(f, "Header {{ version: {}, length: {}, signature: {:x?}, digest: {:x?}, nonce: {}, from: {}, to: {} }}",
               version, length, signature, digest, nonce, from, to
        )
    }
}

#[derive(Debug, Error)]
pub enum MessageErrors {
    #[error("The wire message is not valid")]
    InvalidWireMessage,
    #[error("The header has an invalid size {0}")]
    InvalidSizeHeader(usize),
    #[error("Destination header is too small {0}")]
    InvalidSizeSerDest(usize),
}

#[cfg(test)]
mod tests {
    use atlas_common::crypto::hash::Digest;
    use atlas_common::crypto::signature::Signature;
    use crate::message::{WireMessage, Header};

    #[test]
    fn test_header_serialize() {
        let old_header = Header {
            _align: 0,
            version: WireMessage::CURRENT_VERSION,
            signature: [0; Signature::LENGTH],
            digest: [0; Digest::LENGTH],
            nonce: 0,
            from: 0,
            to: 3,
            length: 0,
        };
        let mut buf = [0; Header::LENGTH];
        old_header.serialize_into(&mut buf[..])
            .expect("Serialize failed");
        let new_header = Header::deserialize_from(&buf[..])
            .expect("Deserialize failed");
        assert_eq!(old_header, new_header);
    }
}
