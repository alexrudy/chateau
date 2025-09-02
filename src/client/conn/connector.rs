//! Connectors couple a transport with a protocol to create a connection.
//!
//! In a high-level client, the connector is integrated with the connection pool, to facilitate
//! connection re-use and pre-emption. The connector here is instead meant to be used without
//! a connection pool, when it is known that a new connection should be created every time
//! that the service gets called.
//!
//! This can be useful if you are developing or testing a transport or protocol implementation.
//! Creating a `Connector` object and awaiting it will give you a connection to the server,
//! which will obey the `Connection` trait.

use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

use std::task::Context;
use std::task::Poll;
use std::task::ready;

use pin_project::pin_project;
use thiserror::Error;

use crate::client::conn::ConnectionError;
use crate::client::conn::Protocol;
use crate::client::conn::Transport;
use crate::info::ConnectionInfo;
use crate::info::HasConnectionInfo;

use crate::client::conn::connection::Connection;

use super::dns::Resolver;

pub(in crate::client) struct ConnectorMeta {
    overall_span: tracing::Span,
    resolver_span: Option<tracing::Span>,
    transport_span: Option<tracing::Span>,
    protocol_span: Option<tracing::Span>,
}

impl ConnectorMeta {
    pub(in crate::client) fn new() -> Self {
        let overall_span = tracing::Span::current();

        Self {
            overall_span,
            resolver_span: None,
            transport_span: None,
            protocol_span: None,
        }
    }

    #[allow(dead_code)]
    pub(in crate::client) fn current(&self) -> &tracing::Span {
        &self.overall_span
    }

    pub(in crate::client) fn resolver(&mut self) -> &tracing::Span {
        self.resolver_span
            .get_or_insert_with(|| tracing::trace_span!(parent: &self.overall_span, "resolver"))
    }

    pub(in crate::client) fn transport(&mut self) -> &tracing::Span {
        self.transport_span
            .get_or_insert_with(|| tracing::trace_span!(parent: &self.overall_span, "transport"))
    }

    pub(in crate::client) fn protocol(&mut self) -> &tracing::Span {
        self.protocol_span
            .get_or_insert_with(|| tracing::trace_span!(parent: &self.overall_span, "protocol"))
    }
}

/// Error that can occur during the connection process.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error<Resolver, Transport, Protocol> {
    /// Error resolving address from request
    #[error("resolving address")]
    Resolving(#[source] Resolver),

    /// Error occurred during the connection
    #[error("creating connection")]
    Connecting(#[source] Transport),

    /// Error occurred during the handshake
    #[error("handshaking connection")]
    Handshaking(#[source] Protocol),

    /// Connection can't even be attempted
    #[error("connection closed")]
    Unavailable,
}

#[pin_project(project = ConnectorStateProjected)]
#[allow(clippy::large_enum_variant)]
enum ConnectorState<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    PollReadyResolver {
        resolver: Option<D>,
        transport: Option<T>,
        protocol: Option<P>,
    },
    Resolve {
        #[pin]
        future: D::Future,
        transport: Option<T>,
        protocol: Option<P>,
    },

    PollReadyTransport {
        address: Option<D::Address>,
        transport: Option<T>,
        protocol: Option<P>,
    },
    Connect {
        #[pin]
        future: T::Future,
        protocol: Option<P>,
    },
    PollReadyHandshake {
        protocol: Option<P>,
        stream: Option<T::IO>,
    },
    Handshake {
        #[pin]
        future: <P as Protocol<T::IO, R>>::Future,
        info: ConnectionInfo<D::Address>,
    },
}

impl<D, T, P, R> fmt::Debug for ConnectorState<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorState::PollReadyResolver { .. } => f.debug_tuple("PollReadyResolver").finish(),
            ConnectorState::Resolve { .. } => f.debug_tuple("Resolve").finish(),
            ConnectorState::PollReadyTransport { .. } => {
                f.debug_tuple("PollReadyTransport").finish()
            }
            ConnectorState::Connect { .. } => f.debug_tuple("Connect").finish(),
            ConnectorState::PollReadyHandshake { .. } => {
                f.debug_tuple("PollReadyHandshake").finish()
            }
            ConnectorState::Handshake { .. } => f.debug_tuple("Handshake").finish(),
        }
    }
}

/// A connector combines the futures required to connect to a transport
/// and then complete the transport's associated startup handshake.
#[pin_project]
pub struct Connector<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    #[pin]
    state: ConnectorState<D, T, P, R>,
    request: Option<R>,
    shareable: bool,
}

impl<D, T, P, R> fmt::Debug for Connector<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connector")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl<D, T, P, R> Connector<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    /// Create a new connection from a transport connector and a protocol.
    pub fn new(resolver: D, transport: T, protocol: P, request: R) -> Self {
        //TODO: Fix this
        let shareable = false;

        Self {
            state: ConnectorState::PollReadyResolver {
                resolver: Some(resolver),
                transport: Some(transport),
                protocol: Some(protocol),
            },
            shareable,
            request: Some(request),
        }
    }

    /// Unwrap the connector returning just the inner request while it is pinned
    pub(crate) fn take_request_pinned(mut self: Pin<&mut Self>) -> R {
        self.as_mut()
            .project()
            .request
            .take()
            .expect("Request unavailalbe after polling")
    }

    /// Unwrap the connector returning just the inner request.
    pub(crate) fn take_request_unpinned(&mut self) -> R {
        self.request
            .take()
            .expect("Request unavailalbe after polling")
    }
}

#[allow(type_alias_bounds)]
type ConnectorError<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
= Error<D::Error, <T as Transport<D::Address>>::Error, <P as Protocol<T::IO, R>>::Error>;

impl<D, T, P, R> Connector<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    #[allow(clippy::type_complexity)]
    pub(in crate::client) fn poll_connector<F>(
        self: Pin<&mut Self>,
        notify: F,
        meta: &mut ConnectorMeta,
        cx: &mut Context<'_>,
    ) -> Poll<Result<P::Connection, ConnectorError<D, T, P, R>>>
    where
        F: FnOnce(),
    {
        let mut connector_projected = self.project();
        let mut notifier = Some(notify);

        loop {
            match connector_projected.state.as_mut().project() {
                ConnectorStateProjected::PollReadyResolver {
                    resolver,
                    transport,
                    protocol,
                } => {
                    let _entered = meta.resolver().enter();
                    {
                        let resolver = resolver.as_mut().unwrap();
                        if let Err(error) = ready!(resolver.poll_ready(cx)) {
                            return Poll::Ready(Err(Error::Resolving(error)));
                        }
                    }
                    let mut resolver = resolver
                        .take()
                        .expect("connector polled in invalid state (resolver)");
                    let future = resolver.resolve(
                        connector_projected
                            .request
                            .as_ref()
                            .expect("connector polled in invalid state (request)"),
                    );
                    let transport = transport.take();
                    let protocol = protocol.take();

                    tracing::trace!("resolver ready");
                    connector_projected.state.set(ConnectorState::Resolve {
                        future,
                        transport,
                        protocol,
                    });
                }
                ConnectorStateProjected::Resolve {
                    future,
                    transport,
                    protocol,
                } => {
                    let _entered = meta.resolver().enter();
                    let address = match ready!(future.poll(cx)) {
                        Ok(address) => address,
                        Err(error) => return Poll::Ready(Err(Error::Resolving(error))),
                    };

                    let transport = transport.take();
                    let protocol = protocol.take();
                    connector_projected
                        .state
                        .set(ConnectorState::PollReadyTransport {
                            address: Some(address),
                            transport,
                            protocol,
                        })
                }
                ConnectorStateProjected::PollReadyTransport {
                    address,
                    transport,
                    protocol,
                } => {
                    let _entered = meta.transport().enter();
                    {
                        let transport = transport.as_mut().unwrap();
                        if let Err(error) = ready!(transport.poll_ready(cx)) {
                            return Poll::Ready(Err(Error::Connecting(error)));
                        }
                    }

                    let mut transport = transport
                        .take()
                        .expect("connector polled in invalid state (transport)");
                    let future = transport.connect(
                        address
                            .take()
                            .expect("connector polled in invalid state (address)"),
                    );
                    let protocol = protocol.take();

                    tracing::trace!("transport ready");
                    connector_projected
                        .state
                        .set(ConnectorState::Connect { future, protocol });
                }

                ConnectorStateProjected::Connect { future, protocol } => {
                    let _entered = meta.transport().enter();
                    let stream = match ready!(future.poll(cx)) {
                        Ok(stream) => stream,
                        Err(error) => return Poll::Ready(Err(Error::Connecting(error))),
                    };
                    let protocol = protocol.take();

                    tracing::trace!("transport connected");
                    connector_projected
                        .state
                        .set(ConnectorState::PollReadyHandshake {
                            protocol,
                            stream: Some(stream),
                        });
                }

                ConnectorStateProjected::PollReadyHandshake { protocol, stream } => {
                    let _entered = meta.protocol().enter();

                    {
                        let protocol = protocol.as_mut().unwrap();
                        if let Err(error) =
                            ready!(<P as Protocol<T::IO, R>>::poll_ready(protocol, cx))
                        {
                            return Poll::Ready(Err(Error::Handshaking(error)));
                        }
                    }

                    let stream = stream
                        .take()
                        .expect("future polled in invalid state: stream is None");

                    let info = stream.info();

                    let future = protocol
                        .as_mut()
                        .expect("future polled in invalid state: protocol is None")
                        .connect(stream);

                    if *connector_projected.shareable {
                        if let Some(notifier) = notifier.take() {
                            notifier();
                        }
                    }

                    tracing::trace!("handshake ready");

                    connector_projected
                        .state
                        .set(ConnectorState::Handshake { future, info });
                }

                ConnectorStateProjected::Handshake { future, .. } => {
                    let _entered = meta.protocol().enter();

                    return future.poll(cx).map(|result| match result {
                        Ok(conn) => {
                            tracing::debug!("connection ready");
                            Ok(conn)
                        }
                        Err(error) => Err(Error::Handshaking(error)),
                    });
                }
            }
        }
    }
}

/// A future that resolves to a connection.
#[pin_project]
pub struct ConnectorFuture<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    #[pin]
    connector: Connector<D, T, P, R>,
    meta: ConnectorMeta,
}

impl<D, T, P, R> fmt::Debug for ConnectorFuture<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ConnectorFuture")
            .field(&self.connector)
            .finish()
    }
}

impl<D, T, P, R> Future for ConnectorFuture<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    type Output = Result<(P::Connection, R), ConnectorError<D, T, P, R>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        let connection = ready!(this.connector.as_mut().poll_connector(|| (), this.meta, cx));
        Poll::Ready(connection.map(|c| {
            (
                c,
                this.connector
                    .project()
                    .request
                    .take()
                    .expect("connector polled in invalid state (request)"),
            )
        }))
    }
}

impl<D, T, P, R> IntoFuture for Connector<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
{
    type Output = Result<(P::Connection, R), ConnectorError<D, T, P, R>>;
    type IntoFuture = ConnectorFuture<D, T, P, R>;

    fn into_future(self) -> Self::IntoFuture {
        let meta = ConnectorMeta::new();

        ConnectorFuture {
            connector: self,
            meta,
        }
    }
}

/// A layer which provides a connection for a request.
///
/// No pooling is done.
#[derive(Debug, Clone)]
pub struct ConnectorLayer<D, T, P> {
    resolver: D,
    transport: T,
    protocol: P,
}

impl<D, T, P> ConnectorLayer<D, T, P> {
    /// Create a new `ConnectorLayer` wrapping the given transport and protocol.
    pub fn new(resolver: D, transport: T, protocol: P) -> Self {
        Self {
            resolver,
            transport,
            protocol,
        }
    }
}

impl<S, D, T, P> tower::layer::Layer<S> for ConnectorLayer<D, T, P>
where
    D: Clone,
    T: Clone,
    P: Clone,
{
    type Service = ConnectorService<S, D, T, P>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectorService::new(
            inner,
            self.resolver.clone(),
            self.transport.clone(),
            self.protocol.clone(),
        )
    }
}

/// A service that opens a connection with a given transport and protocol.
#[derive(Debug, Clone)]
pub struct ConnectorService<S, D, T, P> {
    inner: S,
    resolver: D,
    transport: T,
    protocol: P,
}

impl<S, D, T, P> ConnectorService<S, D, T, P> {
    /// Create a new `ConnectorService` wrapping the given service, transport, and protocol.
    pub fn new(inner: S, resolver: D, transport: T, protocol: P) -> Self {
        Self {
            inner,
            resolver,
            transport,
            protocol,
        }
    }
}

impl<S, D, T, P, Req> tower::Service<Req> for ConnectorService<S, D, T, P>
where
    D: Resolver<Req> + Clone + Send + Sync + 'static,
    P: Protocol<T::IO, Req> + Clone + Send + Sync + 'static,
    P::Connection: Connection<Req>,
    T: Transport<D::Address> + Clone + Send + 'static,
    T::IO: Unpin,
    S: tower::Service<
            (P::Connection, Req),
            Response = <<P as Protocol<<T as Transport<D::Address>>::IO, Req>>::Connection as Connection<Req>>::Response,
        > + Clone
        + Send
        + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = ConnectionError<D::Error, T::Error, <P as Protocol<T::IO, Req>>::Error, S::Error>;
    type Future = self::future::ResponseFuture<D, T, P, P::Connection, S, Req, S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.transport
            .poll_ready(cx)
            .map_err(ConnectionError::Connecting)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let connector =
            Connector::new(self.resolver.clone(), self.transport.clone(), self.protocol.clone(), req);

        self::future::ResponseFuture::new(connector, self.inner.clone())
    }
}

mod future {
    use super::{Connector, ConnectorMeta};

    use std::fmt;
    use std::future::Future;
    use std::task::Poll;

    use pin_project::pin_project;

    use crate::client::conn::ConnectionError;
    use crate::client::conn::{Connection, Protocol, Transport, dns::Resolver};

    /// A future that resolves to an HTTP response.
    #[pin_project]
    pub struct ResponseFuture<D, T, P, C, S, Req, Res>
    where
        D: Resolver<Req>,
        T: Transport<D::Address> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        #[pin]
        inner: ResponseFutureState<D, T, P, C, S, Req, Res>,
        meta: ConnectorMeta,

        _body: std::marker::PhantomData<fn(Req) -> Res>,
    }

    impl<D, T, P, C, S, Req, Res> fmt::Debug for ResponseFuture<D, T, P, C, S, Req, Res>
    where
        D: Resolver<Req>,
        T: Transport<D::Address> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ResponseFuture").finish()
        }
    }

    impl<D, T, P, C, S, Req, Res> ResponseFuture<D, T, P, C, S, Req, Res>
    where
        D: Resolver<Req>,
        T: Transport<D::Address> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        pub(super) fn new(connector: Connector<D, T, P, Req>, service: S) -> Self {
            Self {
                inner: ResponseFutureState::Connect { connector, service },
                meta: ConnectorMeta::new(),
                _body: std::marker::PhantomData,
            }
        }

        #[allow(dead_code, clippy::type_complexity)]
        fn error(
            error: ConnectionError<
                D::Error,
                T::Error,
                <P as Protocol<<T as Transport<D::Address>>::IO, Req>>::Error,
                S::Error,
            >,
        ) -> Self {
            Self {
                inner: ResponseFutureState::ConnectionError(Some(error)),
                meta: ConnectorMeta::new(),
                _body: std::marker::PhantomData,
            }
        }
    }

    impl<D, T, P, C, S, Req, Res> Future for ResponseFuture<D, T, P, C, S, Req, Res>
    where
        D: Resolver<Req>,
        T: Transport<D::Address> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        type Output = Result<
            Res,
            ConnectionError<
                D::Error,
                T::Error,
                <P as Protocol<<T as Transport<D::Address>>::IO, Req>>::Error,
                S::Error,
            >,
        >;

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Self::Output> {
            loop {
                let mut this = self.as_mut().project();
                let next = match this.inner.as_mut().project() {
                    ResponseFutureStateProj::Connect {
                        mut connector,
                        service,
                    } => match connector.as_mut().poll_connector(|| (), this.meta, cx) {
                        Poll::Ready(Ok(conn)) => ResponseFutureState::Request(
                            service.call((
                                conn,
                                connector
                                    .project()
                                    .request
                                    .take()
                                    .expect("request polled again"),
                            )),
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
                        return Poll::Ready(Err(error.take().expect("error polled after return")));
                    }
                };
                this.inner.set(next);
            }
        }
    }

    #[pin_project(project=ResponseFutureStateProj)]
    #[allow(clippy::large_enum_variant)]
    enum ResponseFutureState<D, T, P, C, S, Req, Res>
    where
        D: Resolver<Req>,
        T: Transport<D::Address> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        Connect {
            #[pin]
            connector: Connector<D, T, P, Req>,
            service: S,
        },
        ConnectionError(
            #[allow(clippy::type_complexity)]
            Option<
                ConnectionError<
                    D::Error,
                    T::Error,
                    <P as Protocol<<T as Transport<D::Address>>::IO, Req>>::Error,
                    S::Error,
                >,
            >,
        ),
        Request(#[pin] S::Future),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use static_assertions::assert_impl_all;

    assert_impl_all!(ConnectorMeta: Send, Sync);
    assert_impl_all!(Error<std::io::Error, std::io::Error, std::io::Error>: std::error::Error, Send, Sync);

    #[test]
    fn test_error_variants() {
        use std::convert::Infallible;

        let resolving_error: Error<String, Infallible, Infallible> =
            Error::Resolving("test".to_string());
        assert_eq!(format!("{resolving_error}"), "resolving address");

        let connecting_error: Error<Infallible, String, Infallible> =
            Error::Connecting("test".to_string());
        assert_eq!(format!("{connecting_error}"), "creating connection");

        let handshaking_error: Error<Infallible, Infallible, String> =
            Error::Handshaking("test".to_string());
        assert_eq!(format!("{handshaking_error}"), "handshaking connection");

        let unavailable_error: Error<Infallible, Infallible, Infallible> = Error::Unavailable;
        assert_eq!(format!("{unavailable_error}"), "connection closed");
    }

    #[test]
    fn test_error_equality() {
        use std::convert::Infallible;

        let error1: Error<String, Infallible, Infallible> = Error::Resolving("test".to_string());
        let error2: Error<String, Infallible, Infallible> = Error::Resolving("test".to_string());
        let error3: Error<String, Infallible, Infallible> =
            Error::Resolving("different".to_string());

        assert_eq!(error1, error2);
        assert_ne!(error1, error3);
    }

    #[cfg(feature = "mock")]
    mod mock_tests {
        use super::*;
        use crate::client::conn::protocol::mock::{MockProtocol, MockRequest};
        use crate::client::conn::transport::mock::{MockResolver, MockTransport};

        #[test]
        fn test_connector_new() {
            let resolver = MockResolver {};
            let transport = MockTransport::single();
            let protocol = MockProtocol::default();
            let request = MockRequest;

            let connector = Connector::new(resolver, transport, protocol, request);

            assert!(!connector.shareable);
            assert!(connector.request.is_some());
        }

        #[test]
        fn test_connector_debug() {
            let resolver = MockResolver {};
            let transport = MockTransport::single();
            let protocol = MockProtocol::default();
            let request = MockRequest;

            let connector = Connector::new(resolver, transport, protocol, request);
            let debug_str = format!("{connector:?}");

            assert!(debug_str.contains("Connector"));
            assert!(debug_str.contains("state"));
        }

        #[test]
        fn test_connector_layer_new() {
            let resolver = MockResolver {};
            let transport = MockTransport::single();
            let protocol = MockProtocol::default();

            let layer = ConnectorLayer::new(resolver, transport, protocol);

            let debug_str = format!("{layer:?}");
            assert!(debug_str.contains("ConnectorLayer"));
        }

        #[test]
        fn test_connector_service_new() {
            let inner_service = tower::service_fn(
                |_: (crate::client::conn::protocol::mock::MockSender, MockRequest)| async {
                    Ok::<_, std::convert::Infallible>(
                        crate::client::conn::protocol::mock::MockResponse,
                    )
                },
            );
            let resolver = MockResolver {};
            let transport = MockTransport::single();
            let protocol = MockProtocol::default();

            let service = ConnectorService::new(inner_service, resolver, transport, protocol);

            let debug_str = format!("{service:?}");
            assert!(debug_str.contains("ConnectorService"));
        }
    }
}
