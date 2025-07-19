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
use std::marker::PhantomData;
use std::pin::Pin;

use std::task::Context;
use std::task::Poll;
use std::task::ready;

use pin_project::pin_project;
use thiserror::Error;

use crate::client::conn::Protocol;
use crate::client::conn::Transport;
use crate::client::pool::KeyError;
use crate::info::ConnectionInfo;
use crate::info::HasConnectionInfo;

use crate::client::conn::connection::Connection;

pub(in crate::client) struct ConnectorMeta {
    overall_span: tracing::Span,
    transport_span: Option<tracing::Span>,
    protocol_span: Option<tracing::Span>,
}

impl ConnectorMeta {
    pub(in crate::client) fn new() -> Self {
        let overall_span = tracing::Span::current();

        Self {
            overall_span,
            transport_span: None,
            protocol_span: None,
        }
    }

    #[allow(dead_code)]
    pub(in crate::client) fn current(&self) -> &tracing::Span {
        &self.overall_span
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
pub enum Error<Transport, Protocol> {
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

/// Error that can occur during the connection and serving process.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConnectionError<T, P, S> {
    /// Error occurred during the connection
    #[error("creating connection")]
    Connecting(#[source] T),

    /// Error occurred during the handshake
    #[error("handshaking connection")]
    Handshaking(#[source] P),

    /// Error occurred during the service
    #[error("service: {0}")]
    Service(#[source] S),

    /// Connection can't even be attempted
    #[error("connection closed")]
    Unavailable,

    /// Error returned when building a key
    #[error("key error: {0}")]
    Key(#[from] KeyError),
}

impl<T, P, S> From<Error<T, P>> for ConnectionError<T, P, S> {
    fn from(value: Error<T, P>) -> Self {
        match value {
            Error::Connecting(t) => ConnectionError::Connecting(t),
            Error::Handshaking(p) => ConnectionError::Handshaking(p),
            Error::Unavailable => ConnectionError::Unavailable,
        }
    }
}

#[pin_project(project = ConnectorStateProjected)]
#[allow(clippy::large_enum_variant)]
enum ConnectorState<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
{
    PollReadyTransport {
        address: Option<A>,
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
        info: ConnectionInfo<<T::IO as HasConnectionInfo>::Addr>,
    },
}

impl<T, A, P, R> fmt::Debug for ConnectorState<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
    A: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorState::PollReadyTransport { address, .. } => f
                .debug_struct("PollReadyTransport")
                .field("address", &address.as_ref().unwrap())
                .finish(),
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
pub struct Connector<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
{
    #[pin]
    state: ConnectorState<T, A, P, R>,
    shareable: bool,
}

impl<T, A, P, R> fmt::Debug for Connector<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
    A: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connector")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl<T, A, P, R> Connector<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
{
    /// Create a new connection from a transport connector and a protocol.
    pub fn new(transport: T, protocol: P, address: A) -> Self {
        //TODO: Fix this
        let shareable = false;

        Self {
            state: ConnectorState::PollReadyTransport {
                address: Some(address),
                transport: Some(transport),
                protocol: Some(protocol),
            },
            shareable,
        }
    }
}

#[allow(type_alias_bounds)]
type ConnectorError<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
= Error<<T as Transport<A>>::Error, <P as Protocol<T::IO, R>>::Error>;

impl<T, A, P, R> Connector<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
{
    pub(in crate::client) fn poll_connector<F>(
        self: Pin<&mut Self>,
        notify: F,
        meta: &mut ConnectorMeta,
        cx: &mut Context<'_>,
    ) -> Poll<Result<P::Connection, ConnectorError<T, A, P, R>>>
    where
        F: FnOnce(),
    {
        let mut connector_projected = self.project();
        let mut notifier = Some(notify);

        loop {
            match connector_projected.state.as_mut().project() {
                ConnectorStateProjected::PollReadyTransport {
                    address: parts,
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
                        .expect("future polled in invalid state: transport is None");
                    let future = transport.connect(
                        parts
                            .take()
                            .expect("future polled in invalid state: parts is None"),
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
pub struct ConnectorFuture<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
{
    #[pin]
    connector: Connector<T, A, P, R>,
    meta: ConnectorMeta,
}

impl<T, A, P, R> fmt::Debug for ConnectorFuture<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
    A: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ConnectorFuture")
            .field(&self.connector)
            .finish()
    }
}

impl<T, A, P, R> Future for ConnectorFuture<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
{
    type Output = Result<P::Connection, ConnectorError<T, A, P, R>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();

        this.connector.poll_connector(|| (), this.meta, cx)
    }
}

impl<T, A, P, R> IntoFuture for Connector<T, A, P, R>
where
    T: Transport<A>,
    P: Protocol<T::IO, R>,
{
    type Output = Result<P::Connection, ConnectorError<T, A, P, R>>;
    type IntoFuture = ConnectorFuture<T, A, P, R>;

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
pub struct ConnectorLayer<T, A, P> {
    transport: T,
    protocol: P,
    address: PhantomData<fn(A)>,
}

impl<T, A, P> ConnectorLayer<T, A, P> {
    /// Create a new `ConnectorLayer` wrapping the given transport and protocol.
    pub fn new(transport: T, protocol: P) -> Self {
        Self {
            transport,
            protocol,
            address: PhantomData,
        }
    }
}

impl<S, T, A, P> tower::layer::Layer<S> for ConnectorLayer<T, A, P>
where
    T: Clone,
    P: Clone,
{
    type Service = ConnectorService<S, T, A, P>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectorService::new(inner, self.transport.clone(), self.protocol.clone())
    }
}

/// A trait to mark a request type as capable of providing an address
pub trait Addressable<Addr> {
    /// Get the correct address type for this request
    fn address(&self) -> Addr;
}

/// A service that opens a connection with a given transport and protocol.
#[derive(Debug, Clone)]
pub struct ConnectorService<S, T, A, P> {
    inner: S,
    transport: T,
    protocol: P,
    address: PhantomData<fn(A)>,
}

impl<S, T, A, P> ConnectorService<S, T, A, P> {
    /// Create a new `ConnectorService` wrapping the given service, transport, and protocol.
    pub fn new(inner: S, transport: T, protocol: P) -> Self {
        Self {
            inner,
            transport,
            protocol,
            address: PhantomData,
        }
    }
}

impl<S, T, A, P, Req> tower::Service<Req> for ConnectorService<S, T, A, P>
where
    Req: Addressable<A>,
    P: Protocol<T::IO, Req> + Clone + Send + Sync + 'static,
    P::Connection: Connection<Req>,
    T: Transport<A> + Clone + Send + 'static,
    T::IO: Unpin,
    A: Send,
    S: tower::Service<
            (P::Connection, Req),
            Response = <<P as Protocol<<T as Transport<A>>::IO, Req>>::Connection as Connection<Req>>::Response,
        > + Clone
        + Send
        + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = ConnectionError<T::Error, <P as Protocol<T::IO, Req>>::Error, S::Error>;
    type Future = self::future::ResponseFuture<T, A, P, P::Connection, S, Req, S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.transport
            .poll_ready(cx)
            .map_err(|error| ConnectionError::Connecting(error))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let connector =
            Connector::new(self.transport.clone(), self.protocol.clone(), req.address());

        self::future::ResponseFuture::new(connector, req, self.inner.clone())
    }
}

mod future {
    use super::{Connector, ConnectorMeta};

    use std::fmt;
    use std::future::Future;
    use std::task::Poll;

    use pin_project::pin_project;

    use super::ConnectionError;
    use crate::client::conn::{Connection, Protocol, Transport};

    /// A future that resolves to an HTTP response.
    #[pin_project]
    pub struct ResponseFuture<T, A, P, C, S, Req, Res>
    where
        T: Transport<A> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        #[pin]
        inner: ResponseFutureState<T, A, P, C, S, Req, Res>,
        meta: ConnectorMeta,

        _body: std::marker::PhantomData<fn(Req) -> Res>,
    }

    impl<T, A, P, C, S, Req, Res> fmt::Debug for ResponseFuture<T, A, P, C, S, Req, Res>
    where
        T: Transport<A> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ResponseFuture").finish()
        }
    }

    impl<T, A, P, C, S, Req, Res> ResponseFuture<T, A, P, C, S, Req, Res>
    where
        T: Transport<A> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        pub(super) fn new(connector: Connector<T, A, P, Req>, request: Req, service: S) -> Self {
            Self {
                inner: ResponseFutureState::Connect {
                    connector,
                    request: Some(request),
                    service,
                },
                meta: ConnectorMeta::new(),
                _body: std::marker::PhantomData,
            }
        }

        #[allow(dead_code)]
        fn error(
            error: ConnectionError<
                T::Error,
                <P as Protocol<<T as Transport<A>>::IO, Req>>::Error,
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

    impl<T, A, P, C, S, Req, Res> Future for ResponseFuture<T, A, P, C, S, Req, Res>
    where
        T: Transport<A> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        type Output = Result<
            Res,
            ConnectionError<
                T::Error,
                <P as Protocol<<T as Transport<A>>::IO, Req>>::Error,
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
                        connector,
                        request,
                        service,
                    } => match connector.poll_connector(|| (), this.meta, cx) {
                        Poll::Ready(Ok(conn)) => ResponseFutureState::Request(
                            service.call((conn, request.take().expect("request polled again"))),
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
    enum ResponseFutureState<T, A, P, C, S, Req, Res>
    where
        T: Transport<A> + Send + 'static,
        P: Protocol<T::IO, Req, Connection = C> + Send + 'static,
        C: Connection<Req>,
        S: tower::Service<(C, Req), Response = Res> + Send + 'static,
    {
        Connect {
            #[pin]
            connector: Connector<T, A, P, Req>,
            request: Option<Req>,
            service: S,
        },
        ConnectionError(
            Option<
                ConnectionError<
                    T::Error,
                    <P as Protocol<<T as Transport<A>>::IO, Req>>::Error,
                    S::Error,
                >,
            >,
        ),
        Request(#[pin] S::Future),
    }
}
