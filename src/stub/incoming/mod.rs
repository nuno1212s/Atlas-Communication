use crate::byte_stub::StubEndpoint;
use crate::reconfiguration_node::NetworkInformationProvider;
use crate::serialization::Serializable;
use crate::stub::{ApplicationStub, BatchedNetworkStub, OperationStub, ReconfigurationStub, RegularNetworkStub, StateProtocolStub};

/// The regular network stub implementation for the reconfiguration stub
impl<NI, CN, BN, R, O, S, A, L> RegularNetworkStub<R> for ReconfigurationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable, S: Serializable, A: Serializable,
          NI: NetworkInformationProvider, BN: Clone {
    type Incoming = StubEndpoint<R::Message>;

    fn incoming_stub(&self) -> &Self::Incoming {
        &self.stub_endpoint
    }
}

/// The regular network stub implementation for the operation stub
impl<NI, CN, BN, R, O, S, A, L> RegularNetworkStub<O> for OperationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable, S: Serializable, A: Serializable,
          NI: NetworkInformationProvider, BN: Clone
{
    type Incoming = StubEndpoint<O::Message>;

    fn incoming_stub(&self) -> &Self::Incoming {
        &self.stub_endpoint
    }
}

impl<NI, CN, BN, R, O, S, A, L> RegularNetworkStub<S> for StateProtocolStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable, S: Serializable, A: Serializable,
          NI: NetworkInformationProvider, BN: Clone
{
    type Incoming = StubEndpoint<S::Message>;

    fn incoming_stub(&self) -> &Self::Incoming {
        &self.stub_endpoint
    }
}


/// The application stub is special
/// Application stubs can either be regular stubs (if we are a client)
/// or pooled, batched stubs (if we are a replica)
/// Therefore, we must have both implementations since they are both valid
impl<NI, CN, BN, R, O, S, A, L> RegularNetworkStub<A> for ApplicationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable, S: Serializable, A: Serializable,
          NI: NetworkInformationProvider, BN: Clone
{
    type Incoming = StubEndpoint<A::Message>;

    fn incoming_stub(&self) -> &Self::Incoming {
        &self.stub_endpoint
    }
}

impl<NI, CN, BN, R, O, S, A, L> BatchedNetworkStub<A> for ApplicationStub<NI, CN, BN, R, O, S, A, L>
    where R: Serializable, O: Serializable, S: Serializable, A: Serializable,
          NI: NetworkInformationProvider, BN: Clone {
    type Incoming = StubEndpoint<A::Message>;

    fn incoming_stub(&self) -> &Self::Incoming {
        &self.stub_endpoint
    }
}