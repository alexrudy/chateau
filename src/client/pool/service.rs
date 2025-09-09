use std::fmt;
use std::future::Future;
use std::task::Poll;

use pin_project::pin_project;

use crate::BoxError;
use crate::client::conn::Connection;
use crate::client::conn::ConnectionError;
use crate::client::conn::Protocol;
use crate::client::conn::Transport;
use crate::client::conn::dns::Resolver;
use crate::client::pool;
use crate::client::pool::Checkout;
use crate::client::pool::Connector;
use crate::client::pool::PoolableConnection;
use crate::client::pool::Pooled;

use super::PoolableStream;

/// Layer which adds connection pooling and converts
/// to an inner service which accepts `ExecuteRequest`
/// from an outer service which accepts `http::Request`.
pub struct ConnectionPoolLayer<D, T, P, R, K> {
    resolver: D,
    transport: T,
    protocol: P,
    pool: Option<pool::Config>,
    _body: std::marker::PhantomData<fn(R, K) -> ()>,
}

impl<D: fmt::Debug, T: fmt::Debug, P: fmt::Debug, R, K> fmt::Debug
    for ConnectionPoolLayer<D, T, P, R, K>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionPoolLayer")
            .field("resolver", &self.resolver)
            .field("transport", &self.transport)
            .field("protocol", &self.protocol)
            .field("pool", &self.pool)
            .finish()
    }
}

impl<D, T, P, R, K> ConnectionPoolLayer<D, T, P, R, K> {
    /// Layer for connection pooling.
    pub fn new(resolver: D, transport: T, protocol: P) -> Self {
        Self {
            resolver,
            transport,
            protocol,
            pool: None,
            _body: std::marker::PhantomData,
        }
    }

    /// Set the connection pool configuration.
    pub fn with_pool(mut self, pool: pool::Config) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Set the connection pool configuration to an optional value.
    pub fn with_optional_pool(mut self, pool: Option<pool::Config>) -> Self {
        self.pool = pool;
        self
    }

    /// Disable connection pooling.
    pub fn without_pool(mut self) -> Self {
        self.pool = None;
        self
    }
}

impl<D, T, P, R, K> Clone for ConnectionPoolLayer<D, T, P, R, K>
where
    D: Clone,
    T: Clone,
    P: Clone,
{
    fn clone(&self) -> Self {
        Self {
            resolver: self.resolver.clone(),
            transport: self.transport.clone(),
            protocol: self.protocol.clone(),
            pool: self.pool.clone(),
            _body: std::marker::PhantomData,
        }
    }
}

impl<D, T, P, S, R, K> tower::layer::Layer<S> for ConnectionPoolLayer<D, T, P, R, K>
where
    D: Resolver<R> + Clone + Send + 'static,
    T: Transport<D::Address> + Clone + Send + Sync + 'static,
    P: Protocol<T::IO, R> + Clone + Send + Sync + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
    K: pool::Key<R>,
{
    type Service = ConnectionPoolService<D, T, P, S, R, K>;

    fn layer(&self, service: S) -> Self::Service {
        let pool = self.pool.clone().map(pool::Pool::new);

        ConnectionPoolService {
            resolver: self.resolver.clone(),
            transport: self.transport.clone(),
            protocol: self.protocol.clone(),
            service,
            pool,
            _body: std::marker::PhantomData,
        }
    }
}

/// A service which gets a connection from a possible connection pool and passes it to
/// an inner service to execute that request.
///
/// This service will accept request objects, but expects the inner service
/// to accept request objects bundled with the connection.
///
/// The inner service should execute the request
/// on the connection and return the response.
#[derive(Debug)]
pub struct ConnectionPoolService<D, T, P, S, R, K>
where
    D: Resolver<R> + Send + 'static,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
    K: pool::Key<R>,
{
    pub(super) resolver: D,
    pub(super) transport: T,
    pub(super) protocol: P,
    pub(super) service: S,
    pub(super) pool: Option<pool::Pool<P::Connection, R, K>>,
    pub(super) _body: std::marker::PhantomData<fn(R)>,
}

impl<D, T, P, S, R, K> ConnectionPoolService<D, T, P, S, R, K>
where
    D: Resolver<R> + Send + 'static,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
    K: pool::Key<R>,
{
    /// Create a new client with the given transport, protocol, and pool configuration.
    pub fn new(resolver: D, transport: T, protocol: P, service: S, pool: pool::Config) -> Self {
        Self {
            resolver,
            transport,
            protocol,
            service,
            pool: Some(pool::Pool::new(pool)),
            _body: std::marker::PhantomData,
        }
    }

    /// Disable connection pooling for this client.
    pub fn without_pool(self) -> Self {
        Self { pool: None, ..self }
    }
}

impl<D, T, P, S, R, K> Clone for ConnectionPoolService<D, T, P, S, R, K>
where
    D: Resolver<R> + Clone + Send + 'static,
    T: Transport<D::Address> + Clone,
    P: Protocol<T::IO, R> + Clone,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
    S: Clone,
    K: pool::Key<R>,
{
    fn clone(&self) -> Self {
        Self {
            resolver: self.resolver.clone(),
            protocol: self.protocol.clone(),
            transport: self.transport.clone(),
            pool: self.pool.clone(),
            service: self.service.clone(),
            _body: std::marker::PhantomData,
        }
    }
}

impl<D, T, P, S, R, K> ConnectionPoolService<D, T, P, S, R, K>
where
    D: Resolver<R> + Clone + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Clone,
    T::IO: Unpin,
    P: Protocol<T::IO, R> + Clone + Send + Sync + 'static,
    <P as Protocol<T::IO, R>>::Connection: PoolableConnection<R> + Send + 'static,
    R: Send + 'static,
    K: pool::Key<R>,
    S: tower::Service<(Pooled<P::Connection, R>, R)>,
{
    #[allow(clippy::type_complexity)]
    fn connect_to(
        &self,
        request: R,
    ) -> Result<
        Checkout<D, T, P, R>,
        ConnectionError<D::Error, T::Error, <P as Protocol<T::IO, R>>::Error, S::Error>,
    > {
        let key: K = K::build_key(&request)?;
        let resolver = self.resolver.clone();
        let protocol = self.protocol.clone();
        let transport = self.transport.clone();

        let multiplex = protocol.multiplex();
        let connector = Connector::new(resolver, transport, protocol, request);

        if let Some(pool) = self.pool.as_ref() {
            tracing::trace!(?key, "checking out connection");
            Ok(pool.checkout(key, multiplex, connector))
        } else {
            tracing::trace!(?key, "detatched connection");
            Ok(Checkout::detached(connector))
        }
    }
}

impl<D, P, C, T, S, R, K> tower::Service<R> for ConnectionPoolService<D, T, P, S, R, K>
where
    D: Resolver<R> + Clone + Send,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    C: Connection<R> + PoolableConnection<R>,
    P: Protocol<T::IO, R, Connection = C> + Clone + Send + Sync + 'static,
    T: Transport<D::Address> + Clone + Send + 'static,
    T::IO: PoolableStream + Unpin,
    R: Send,
    S: tower::Service<(Pooled<C, R>, R), Response = C::Response> + Clone + Send + 'static,
    K: pool::Key<R>,
{
    type Response = C::Response;
    type Error = ConnectionError<D::Error, T::Error, <P as Protocol<T::IO, R>>::Error, S::Error>;
    type Future = ResponseFuture<D, T, P, C, S, R>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: R) -> Self::Future {
        match self.connect_to(request) {
            Ok(checkout) => ResponseFuture::new(checkout, self.service.clone()),
            Err(error) => ResponseFuture::error(error),
        }
    }
}

/// A future that resolves to a response.
#[pin_project]
pub struct ResponseFuture<D, T, P, C, S, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    P: Protocol<T::IO, R, Connection = C> + Send + 'static,
    C: Connection<R> + PoolableConnection<R>,
    S: tower::Service<(Pooled<C, R>, R), Response = C::Response> + Send + 'static,
    R: Send + 'static,
{
    #[pin]
    inner: ResponseFutureState<D, T, P, C, S, R>,
    _body: std::marker::PhantomData<fn(R)>,
}

impl<D, T, P, C, S, R> fmt::Debug for ResponseFuture<D, T, P, C, S, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    P: Protocol<T::IO, R, Connection = C> + Send + 'static,
    C: Connection<R> + PoolableConnection<R>,
    S: tower::Service<(Pooled<C, R>, R), Response = C::Response> + Send + 'static,
    R: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseFuture").finish()
    }
}

impl<D, T, P, C, S, R> ResponseFuture<D, T, P, C, S, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    P: Protocol<T::IO, R, Connection = C> + Send + 'static,
    C: Connection<R> + PoolableConnection<R>,
    S: tower::Service<(Pooled<C, R>, R), Response = C::Response> + Send + 'static,
    R: Send + 'static,
{
    fn new(checkout: Checkout<D, T, P, R>, service: S) -> Self {
        Self {
            inner: ResponseFutureState::Checkout { checkout, service },
            _body: std::marker::PhantomData,
        }
    }

    #[allow(clippy::type_complexity)]
    fn error(
        error: ConnectionError<D::Error, T::Error, <P as Protocol<T::IO, R>>::Error, S::Error>,
    ) -> Self {
        Self {
            inner: ResponseFutureState::ConnectionError(Some(error)),
            _body: std::marker::PhantomData,
        }
    }
}

impl<D, T, P, C, S, R> Future for ResponseFuture<D, T, P, C, S, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    <T as Transport<D::Address>>::Error: Into<BoxError>,
    P: Protocol<T::IO, R, Connection = C> + Send + 'static,
    <P as Protocol<T::IO, R>>::Error: Into<BoxError>,
    C: Connection<R> + PoolableConnection<R>,
    S: tower::Service<(Pooled<C, R>, R), Response = C::Response> + Send + 'static,
    R: Send,
{
    #[allow(clippy::type_complexity)]
    type Output = Result<
        C::Response,
        ConnectionError<D::Error, T::Error, <P as Protocol<T::IO, R>>::Error, S::Error>,
    >;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();
            let next = match this.inner.as_mut().project() {
                ResponseFutureStateProj::Checkout {
                    mut checkout,
                    service,
                } => match checkout.as_mut().poll(cx) {
                    Poll::Ready(Ok(conn)) => ResponseFutureState::Request(
                        service.call((conn, checkout.take_request_pinned())),
                    ),
                    Poll::Ready(Err(error)) => {
                        return Poll::Ready(Err(error.into()));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                ResponseFutureStateProj::Request(fut) => match fut.poll(cx) {
                    Poll::Ready(Ok(response)) => {
                        return Poll::Ready(Ok(response));
                    }
                    Poll::Ready(Err(error)) => {
                        return Poll::Ready(Err(ConnectionError::Service(error)));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                ResponseFutureStateProj::ConnectionError(error) => {
                    return Poll::Ready(Err(error.take().expect("error polled again")));
                }
            };
            this.inner.set(next);
        }
    }
}

#[pin_project(project=ResponseFutureStateProj)]
#[allow(clippy::large_enum_variant)]
enum ResponseFutureState<D, T, P, C, S, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    P: Protocol<T::IO, R, Connection = C> + Send + 'static,
    C: Connection<R> + PoolableConnection<R>,
    S: tower::Service<(Pooled<C, R>, R), Response = C::Response> + Send + 'static,
    R: Send + 'static,
{
    Checkout {
        #[pin]
        checkout: Checkout<D, T, P, R>,
        service: S,
    },

    #[allow(clippy::type_complexity)]
    ConnectionError(
        Option<ConnectionError<D::Error, T::Error, <P as Protocol<T::IO, R>>::Error, S::Error>>,
    ),
    Request(#[pin] S::Future),
}
