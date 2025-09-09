//! Builder pattern for clients

use std::marker::PhantomData;

use crate::services::SharedService;

use super::conn::ConnectionError;
use super::conn::connector::ConnectorLayer;
use super::conn::dns::{Resolver, StaticResolver};
use super::conn::service::ClientExecutorService;
use super::conn::{Connection, Protocol, Transport};
use super::pool::{Key, PoolableConnection, PoolableStream};
use super::{ConnectionPoolLayer, PoolConfig};

/// Sentinel indicating a client needs to pick a resolver.
#[derive(Debug, Clone, Copy)]
pub struct NeedsResolver;

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
pub struct ClientBuilder<D, T, P, R> {
    resolver: D,
    transport: T,
    protocol: P,
    request: PhantomData<fn(R)>,
}

impl ClientBuilder<NeedsResolver, NeedsTransport, NeedsProtocol, NeedsRequest> {
    /// Create a new builder struct
    pub fn new() -> Self {
        ClientBuilder {
            resolver: NeedsResolver,
            transport: NeedsTransport,
            protocol: NeedsProtocol,
            request: PhantomData,
        }
    }
}

impl Default for ClientBuilder<NeedsResolver, NeedsTransport, NeedsProtocol, NeedsRequest> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, P, R> ClientBuilder<NeedsResolver, T, P, R> {
    /// Use a custom resolver
    pub fn with_resolver<D>(self, resolver: D) -> ClientBuilder<D, T, P, R> {
        ClientBuilder {
            resolver,
            transport: self.transport,
            protocol: self.protocol,
            request: self.request,
        }
    }

    /// Use a static resolver, which always returns a single address.
    pub fn with_static_address<A>(self, address: A) -> ClientBuilder<StaticResolver<A>, T, P, R> {
        ClientBuilder {
            resolver: StaticResolver::new(address),
            transport: self.transport,
            protocol: self.protocol,
            request: self.request,
        }
    }
}

impl<D, P, R> ClientBuilder<D, NeedsTransport, P, R> {
    /// Set the selected transport
    pub fn with_transport<T>(self, transport: T) -> ClientBuilder<D, T, P, R> {
        ClientBuilder {
            resolver: self.resolver,
            transport,
            protocol: self.protocol,
            request: self.request,
        }
    }
}

impl<D, T, R> ClientBuilder<D, T, NeedsProtocol, R> {
    /// Set the selected protocol
    pub fn with_protocol<P>(self, protocol: P) -> ClientBuilder<D, T, P, R> {
        ClientBuilder {
            resolver: self.resolver,
            transport: self.transport,
            protocol,
            request: self.request,
        }
    }
}

impl<D, T, P, R> ClientBuilder<D, T, P, R>
where
    D: Resolver<R> + Clone + Send + Sync + 'static,
    D::Address: Send + Sync + 'static,
    D::Error: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Clone + Send + Sync + 'static,
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
        <<P as Protocol<<T as Transport<<D as Resolver<R>>::Address>>::IO, R>>::Connection as Connection<R>>::Response,
        ConnectionError<
            <D as Resolver<R>>::Error,
            <T as Transport<<D as Resolver<R>>::Address>>::Error,
            <P as Protocol<<T as Transport<<D as Resolver<R>>::Address>>::IO, R>>::Error,
            <<P as Protocol<<T as Transport<<D as Resolver<R>>::Address>>::IO, R>>::Connection as Connection<R>>::Error
        >>
    {
        tower::ServiceBuilder::new()
            .layer(SharedService::layer())
            .layer(ConnectorLayer::new(
                self.resolver,
                self.transport,
                self.protocol,
            ))
            .service(ClientExecutorService::new())
    }
}

impl<D, T, P, R> ClientBuilder<D, T, P, R>
where
    D: Resolver<R> + Clone + Send + Sync + 'static,
    D::Address: Send + Sync + 'static,
    D::Error: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Clone + Send + Sync + 'static,
    T::Future: Send + 'static,
    P: Protocol<T::IO, R> + Clone + Send + Sync + 'static,
    P::Connection: PoolableConnection<R>,
    <P as Protocol<T::IO, R>>::Future: Send + 'static,
    T::IO: PoolableStream + Unpin + Send + Sync + 'static,
    R: Send + 'static,
{
    /// Build a client service with connection pooling
    #[allow(clippy::type_complexity)]
    pub fn build_with_pool<K>(self, config: PoolConfig) -> SharedService<
        R,
        <<P as Protocol<<T as Transport<<D as Resolver<R>>::Address>>::IO, R>>::Connection as Connection<R>>::Response,
        ConnectionError<
            <D as Resolver<R>>::Error,
            <T as Transport<<D as Resolver<R>>::Address>>::Error,
            <P as Protocol<<T as Transport<<D as Resolver<R>>::Address>>::IO, R>>::Error,
            <<P as Protocol<<T as Transport<<D as Resolver<R>>::Address>>::IO, R>>::Connection as Connection<R>>::Error
        >>
    where K: Key<R> + Send + 'static,
    {
        tower::ServiceBuilder::new()
            .layer(SharedService::layer())
            .layer(
                ConnectionPoolLayer::<_, _, _, _, K>::new(
                    self.resolver,
                    self.transport,
                    self.protocol,
                )
                .with_pool(config),
            )
            .service(ClientExecutorService::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;
    use std::net::SocketAddr;

    use static_assertions::assert_impl_all;

    #[cfg(feature = "mock")]
    use crate::client::conn::protocol::mock::{MockProtocol, MockRequest, MockSender};
    #[cfg(feature = "mock")]
    use crate::client::conn::transport::mock::{MockResolver, MockTransport};

    // Static assertions for sentinel types
    assert_impl_all!(NeedsResolver: Debug, Clone, Copy, Send, Sync);
    assert_impl_all!(NeedsTransport: Debug, Clone, Copy, Send, Sync);
    assert_impl_all!(NeedsProtocol: Debug, Clone, Copy, Send, Sync);
    assert_impl_all!(NeedsRequest: Debug, Clone, Copy, Send, Sync);

    // Static assertions for ClientBuilder with different type states
    assert_impl_all!(ClientBuilder<NeedsResolver, NeedsTransport, NeedsProtocol, NeedsRequest>: Debug, Default);
    assert_impl_all!(ClientBuilder<StaticResolver<SocketAddr>, NeedsTransport, NeedsProtocol, NeedsRequest>: Debug);

    #[cfg(feature = "mock")]
    mod mock_tests {
        use super::*;

        // Complex static assertions for fully configured builder
        assert_impl_all!(
            ClientBuilder<MockResolver, MockTransport, MockProtocol, MockRequest>: Debug
        );

        // Static assertions for build methods - these test the complex generic constraints
        #[allow(dead_code)]
        type ComplexServiceType =
            SharedService<
                MockRequest,
                <MockSender as Connection<MockRequest>>::Response,
                ConnectionError<
                    <MockResolver as Resolver<MockRequest>>::Error,
                    <MockTransport as Transport<
                        <MockResolver as Resolver<MockRequest>>::Address,
                    >>::Error,
                    <MockProtocol as Protocol<
                        <MockTransport as Transport<
                            <MockResolver as Resolver<MockRequest>>::Address,
                        >>::IO,
                        MockRequest,
                    >>::Error,
                    <MockSender as Connection<MockRequest>>::Error,
                >,
            >;

        // This tests that the build_service method's return type compiles correctly
        fn _assert_build_service_type() -> ComplexServiceType {
            let builder: ClientBuilder<MockResolver, MockTransport, MockProtocol, MockRequest> =
                ClientBuilder {
                    resolver: MockResolver {},
                    transport: MockTransport::single(),
                    protocol: MockProtocol::default(),
                    request: std::marker::PhantomData,
                };

            builder.build_service()
        }

        #[test]
        fn test_sentinel_types() {
            let needs_resolver = NeedsResolver;
            let needs_transport = NeedsTransport;
            let needs_protocol = NeedsProtocol;
            let needs_request = NeedsRequest;

            let debug_resolver = format!("{needs_resolver:?}");
            let debug_transport = format!("{needs_transport:?}");
            let debug_protocol = format!("{needs_protocol:?}");
            let debug_request = format!("{needs_request:?}");

            assert!(debug_resolver.contains("NeedsResolver"));
            assert!(debug_transport.contains("NeedsTransport"));
            assert!(debug_protocol.contains("NeedsProtocol"));
            assert!(debug_request.contains("NeedsRequest"));
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
        fn test_builder_with_resolver() {
            let builder = ClientBuilder::new().with_resolver(MockResolver {});

            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_builder_with_static_address() {
            let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
            let builder = ClientBuilder::new().with_static_address(addr);

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
                .with_resolver(MockResolver {})
                .with_transport(MockTransport::single())
                .with_protocol(MockProtocol::default());

            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_builder_different_order_chaining() {
            let builder = ClientBuilder::new()
                .with_protocol(MockProtocol::default())
                .with_transport(MockTransport::single())
                .with_resolver(MockResolver {});

            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_builder_static_address_chaining() {
            let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
            let builder = ClientBuilder::new()
                .with_static_address(addr)
                .with_transport(MockTransport::reusable())
                .with_protocol(MockProtocol::default());

            let debug_str = format!("{builder:?}");
            assert!(debug_str.contains("ClientBuilder"));
        }

        #[test]
        fn test_build_service() {
            let builder: ClientBuilder<MockResolver, MockTransport, MockProtocol, MockRequest> =
                ClientBuilder {
                    resolver: MockResolver {},
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
    fn test_static_resolver_integration() {
        let addr = SocketAddr::from(([192, 168, 1, 1], 9000));
        let static_resolver = StaticResolver::new(addr);

        let builder = ClientBuilder::new().with_resolver(static_resolver);

        let debug_str = format!("{builder:?}");
        assert!(debug_str.contains("ClientBuilder"));
    }

    #[test]
    fn test_type_state_transitions() {
        // Test that the type-state machine works correctly
        let _builder_needs_all = ClientBuilder::new();
        let _builder_has_resolver = ClientBuilder::new().with_resolver(StaticResolver::new("test"));
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
        type PoolServiceType =
            SharedService<
                MockRequest,
                <MockSender as Connection<MockRequest>>::Response,
                ConnectionError<
                    <MockResolver as Resolver<MockRequest>>::Error,
                    <MockTransport as Transport<
                        <MockResolver as Resolver<MockRequest>>::Address,
                    >>::Error,
                    <MockProtocol as Protocol<
                        <MockTransport as Transport<
                            <MockResolver as Resolver<MockRequest>>::Address,
                        >>::IO,
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

            let builder: ClientBuilder<MockResolver, MockTransport, MockProtocol, MockRequest> =
                ClientBuilder {
                    resolver: MockResolver {},
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
        use crate::client::conn::transport::mock::{MockResolver, MockTransport};

        // This test verifies that all the complex generic constraints in the impl blocks compile correctly
        let builder: ClientBuilder<MockResolver, MockTransport, MockProtocol, MockRequest> =
            ClientBuilder {
                resolver: MockResolver {},
                transport: MockTransport::single(),
                protocol: MockProtocol::default(),
                request: std::marker::PhantomData,
            };

        // The fact that this compiles means all the trait bounds are satisfied
        let _service = builder.build_service();
    }
}
