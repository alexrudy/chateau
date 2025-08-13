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
