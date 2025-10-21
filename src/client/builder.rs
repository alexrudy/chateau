//! Builder pattern for clients

use std::marker::PhantomData;

use crate::services::SharedService;

use super::conn::ConnectionError;
use super::conn::connector::ConnectorLayer;
use super::conn::service::ClientExecutorService;
use super::conn::{Connection, Protocol, Transport};
use super::pool::{Key, PoolableConnection, PoolableStream};
use super::{ConnectionPoolLayer, PoolConfig};

/// Sentinel indicating a client needs to pick a transport.
#[derive(Debug, Clone, Copy)]
pub struct NeedsTransport;

/// Sentinel indicating a client needs to pick a protocol.
#[derive(Debug, Clone, Copy)]
pub struct NeedsProtocol;

/// Sentinel indicating a client needs to pick a request type.
#[derive(Debug, Clone, Copy)]
pub struct NeedsRequest;

/// Builder-pattern for clients
#[derive(Debug)]
pub struct ClientBuilder<T, P, R> {
    transport: T,
    protocol: P,
    request: PhantomData<fn(R)>,
}

impl ClientBuilder<NeedsTransport, NeedsProtocol, NeedsRequest> {
    /// Create a new builder struct
    pub fn new() -> Self {
        ClientBuilder {
            transport: NeedsTransport,
            protocol: NeedsProtocol,
            request: PhantomData,
        }
    }
}

impl Default for ClientBuilder<NeedsTransport, NeedsProtocol, NeedsRequest> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P, R> ClientBuilder<NeedsTransport, P, R> {
    /// Set the selected transport
    pub fn with_transport<T>(self, transport: T) -> ClientBuilder<T, P, R> {
        ClientBuilder {
            transport,
            protocol: self.protocol,
            request: self.request,
        }
    }
}

impl<T, R> ClientBuilder<T, NeedsProtocol, R> {
    /// Set the selected protocol
    pub fn with_protocol<P>(self, protocol: P) -> ClientBuilder<T, P, R> {
        ClientBuilder {
            transport: self.transport,
            protocol,
            request: self.request,
        }
    }
}

impl<T, P, R> ClientBuilder<T, P, R>
where
    T: Transport<R> + Clone + Send + Sync + 'static,
    T::Future: Send + 'static,
    P: Protocol<T::IO, R> + Clone + Send + Sync + 'static,
    <P as Protocol<T::IO, R>>::Future: Send + 'static,
    T::IO: Unpin + Send + Sync + 'static,
    R: Send + 'static,
{
    /// Build a simple client service.
    #[allow(clippy::type_complexity)]
    pub fn build_service(
        self,
    ) -> SharedService<
        R,
        <<P as Protocol<<T as Transport<R>>::IO, R>>::Connection as Connection<R>>::Response,
        ConnectionError<
            <T as Transport<R>>::Error,
            <P as Protocol<<T as Transport<R>>::IO, R>>::Error,
            <<P as Protocol<<T as Transport<R>>::IO, R>>::Connection as Connection<R>>::Error,
        >,
    > {
        tower::ServiceBuilder::new()
            .layer(SharedService::layer())
            .layer(ConnectorLayer::new(self.transport, self.protocol))
            .service(ClientExecutorService::new())
    }
}

impl<T, P, R> ClientBuilder<T, P, R>
where
    T: Transport<R> + Clone + Send + Sync + 'static,
    T::Future: Send + 'static,
    P: Protocol<T::IO, R> + Clone + Send + Sync + 'static,
    P::Connection: PoolableConnection<R>,
    <P as Protocol<T::IO, R>>::Future: Send + 'static,
    T::IO: PoolableStream + Unpin + Send + Sync + 'static,
    R: Send + 'static,
{
    /// Build a client service with connection pooling
    #[allow(clippy::type_complexity)]
    pub fn build_with_pool<K>(
        self,
        config: PoolConfig,
    ) -> SharedService<
        R,
        <<P as Protocol<<T as Transport<R>>::IO, R>>::Connection as Connection<R>>::Response,
        ConnectionError<
            <T as Transport<R>>::Error,
            <P as Protocol<<T as Transport<R>>::IO, R>>::Error,
            <<P as Protocol<<T as Transport<R>>::IO, R>>::Connection as Connection<R>>::Error,
        >,
    >
    where
        K: Key<R> + Send + 'static,
    {
        tower::ServiceBuilder::new()
            .layer(SharedService::layer())
            .layer(
                ConnectionPoolLayer::<_, _, _, K>::new(self.transport, self.protocol)
                    .with_pool(config),
            )
            .service(ClientExecutorService::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    use static_assertions::assert_impl_all;

    #[cfg(feature = "mock")]
    use crate::client::conn::protocol::mock::{MockProtocol, MockRequest, MockSender};
    #[cfg(feature = "mock")]
    use crate::client::conn::transport::mock::MockTransport;

    // Static assertions for sentinel types
    assert_impl_all!(NeedsTransport: Debug, Clone, Copy, Send, Sync);
    assert_impl_all!(NeedsProtocol: Debug, Clone, Copy, Send, Sync);
    assert_impl_all!(NeedsRequest: Debug, Clone, Copy, Send, Sync);

    // Static assertions for ClientBuilder with different type states
    assert_impl_all!(ClientBuilder< NeedsTransport, NeedsProtocol, NeedsRequest>: Debug, Default);
    assert_impl_all!(ClientBuilder< NeedsTransport, NeedsProtocol, NeedsRequest>: Debug);

    #[cfg(feature = "mock")]
    mod mock_tests {
        use super::*;

        // Complex static assertions for fully configured builder
        assert_impl_all!(
            ClientBuilder< MockTransport, MockProtocol, MockRequest>: Debug
        );

        // Static assertions for build methods - these test the complex generic constraints
        #[allow(dead_code)]
        type ComplexServiceType = SharedService<
            MockRequest,
            <MockSender as Connection<MockRequest>>::Response,
            ConnectionError<
                <MockTransport as Transport<MockRequest>>::Error,
                <MockProtocol as Protocol<
                    <MockTransport as Transport<MockRequest>>::IO,
                    MockRequest,
                >>::Error,
                <MockSender as Connection<MockRequest>>::Error,
            >,
        >;

        // This tests that the build_service method's return type compiles correctly
        fn _assert_build_service_type() -> ComplexServiceType {
            let builder: ClientBuilder<MockTransport, MockProtocol, MockRequest> = ClientBuilder {
                transport: MockTransport::single(),
                protocol: MockProtocol::default(),
                request: std::marker::PhantomData,
            };

            builder.build_service()
        }

        #[test]
        fn test_client_builder_new() {
            let builder = ClientBuilder::new();
            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_client_builder_default() {
            let builder = ClientBuilder::default();
            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_builder_with_transport() {
            let builder = ClientBuilder::new().with_transport(MockTransport::single());

            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_builder_with_protocol() {
            let builder = ClientBuilder::new().with_protocol(MockProtocol::default());

            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_builder_method_chaining() {
            let builder = ClientBuilder::new()
                .with_transport(MockTransport::single())
                .with_protocol(MockProtocol::default());

            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_builder_different_order_chaining() {
            let builder = ClientBuilder::new()
                .with_protocol(MockProtocol::default())
                .with_transport(MockTransport::single());

            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_builder_static_address_chaining() {
            let builder = ClientBuilder::new()
                .with_transport(MockTransport::reusable())
                .with_protocol(MockProtocol::default());

            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_build_service() {
            let builder: ClientBuilder<MockTransport, MockProtocol, MockRequest> = ClientBuilder {
                transport: MockTransport::single(),
                protocol: MockProtocol::default(),
                request: std::marker::PhantomData,
            };

            let service = builder.build_service();
            let debug_str = format!("{service:?}");
            assert!(debug_str.contains("SharedService"));
        }
    }

    #[test]
    fn test_type_state_transitions() {
        // Test that the type-state machine works correctly
        let _builder_needs_all = ClientBuilder::new();
        let _builder_has_transport = ClientBuilder::new().with_transport(());
        let _builder_has_protocol = ClientBuilder::new().with_protocol(());
    }

    // Additional static assertions for complex generic constraints in build methods
    #[cfg(feature = "mock")]
    mod build_constraints {
        use super::*;
        use crate::client::pool::Key;

        // Test that MockSender implements PoolableConnection for pool building
        assert_impl_all!(MockSender: PoolableConnection<MockRequest>);

        // Helper type alias to test build_with_pool constraints
        type PoolServiceType = SharedService<
            MockRequest,
            <MockSender as Connection<MockRequest>>::Response,
            ConnectionError<
                <MockTransport as Transport<MockRequest>>::Error,
                <MockProtocol as Protocol<
                    <MockTransport as Transport<MockRequest>>::IO,
                    MockRequest,
                >>::Error,
                <MockSender as Connection<MockRequest>>::Error,
            >,
        >;

        // Simple key implementation for testing
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct TestKey(String);

        impl Key<MockRequest> for TestKey {
            fn build_key(_request: &MockRequest) -> Result<Self, crate::client::pool::KeyError> {
                Ok(TestKey("test".to_string()))
            }
        }

        assert_impl_all!(TestKey: Key<MockRequest>, Send);

        // This function tests that build_with_pool compiles with proper constraints
        #[allow(dead_code)]
        fn _assert_build_with_pool_type() -> PoolServiceType {
            use crate::client::PoolConfig;

            let builder: ClientBuilder<MockTransport, MockProtocol, MockRequest> = ClientBuilder {
                transport: MockTransport::reusable(),
                protocol: MockProtocol::default(),
                request: std::marker::PhantomData,
            };

            builder.build_with_pool::<TestKey>(PoolConfig::default())
        }
    }

    // Test that the complex generic constraints actually work
    #[cfg(feature = "mock")]
    #[test]
    fn test_generic_constraint_compilation() {
        use crate::client::conn::protocol::mock::{MockProtocol, MockRequest};
        use crate::client::conn::transport::mock::MockTransport;

        // This test verifies that all the complex generic constraints in the impl blocks compile correctly
        let builder: ClientBuilder<MockTransport, MockProtocol, MockRequest> = ClientBuilder {
            transport: MockTransport::single(),
            protocol: MockProtocol::default(),
            request: std::marker::PhantomData,
        };

        // The fact that this compiles means all the trait bounds are satisfied
        let _service = builder.build_service();
    }
}
