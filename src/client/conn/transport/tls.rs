//! Wrap a transport with TLS

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::ClientConfig as TlsClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};

use super::{TlsConnectionError, Transport};
use crate::client::conn::stream::tls::TlsStream;
use crate::info::HasConnectionInfo;

/// Trait for types which can describe a TLS domain for TLS connections
pub trait TlsAddress {
    /// Get the TLS domain to use for TLS conenctions
    fn domain(&self) -> Option<&str>;
}

/// Wrapper around a transport which adds TLS encryption when connecting
/// to a static hostname.
#[derive(Debug, Clone)]
pub struct StaticHostTlsTransport<T> {
    transport: T,
    config: Arc<TlsClientConfig>,
    host: Box<str>,
}

impl<T> StaticHostTlsTransport<T> {
    /// Create a new `TlsTransport`
    pub fn new(transport: T, config: Arc<TlsClientConfig>, host: impl Into<Box<str>>) -> Self {
        Self {
            transport,
            config,
            host: host.into(),
        }
    }

    /// Returns the inner transport and the TLS configuration.
    pub fn into_parts(self) -> (T, Arc<TlsClientConfig>) {
        (self.transport, self.config)
    }

    /// Returns a reference to the inner transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Returns a mutable reference to the inner transport.
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    /// Returns a reference to the TLS configuration.
    pub fn config(&self) -> &Arc<TlsClientConfig> {
        &self.config
    }

    /// Target hostnmae used for TLS connections on this transport
    pub fn host(&self) -> &str {
        &self.host
    }
}

impl<T, A> tower::Service<A> for StaticHostTlsTransport<T>
where
    T: Transport<A>,
    <T as Transport<A>>::IO: HasConnectionInfo<Addr = A> + AsyncRead + AsyncWrite + Unpin,
    A: Clone + Send + Unpin,
{
    type Response = TlsStream<T::IO>;
    type Error = TlsConnectionError<T::Error>;
    type Future = future::TlsConnectionFuture<T, A>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.transport
            .poll_ready(cx)
            .map_err(TlsConnectionError::Connection)
    }

    fn call(&mut self, req: A) -> Self::Future {
        let config = self.config.clone();
        let host = self.host.clone();
        let future = self.transport.connect(req);

        future::TlsConnectionFuture::new(future, config, host.into())
    }
}

/// Transport via TLS
#[derive(Debug, Clone)]
pub struct TlsTransport<T> {
    transport: T,
    config: Arc<TlsClientConfig>,
}

impl<T> TlsTransport<T> {
    /// Create a new `TlsTransport`
    pub fn new(transport: T, config: Arc<TlsClientConfig>) -> Self {
        Self { transport, config }
    }

    /// Returns the inner transport and the TLS configuration.
    pub fn into_parts(self) -> (T, Arc<TlsClientConfig>) {
        (self.transport, self.config)
    }

    /// Returns a reference to the inner transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Returns a mutable reference to the inner transport.
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    /// Returns a reference to the TLS configuration.
    pub fn config(&self) -> &Arc<TlsClientConfig> {
        &self.config
    }
}

impl<T, A> tower::Service<A> for TlsTransport<T>
where
    T: Transport<A>,
    <T as Transport<A>>::IO: HasConnectionInfo<Addr = A> + AsyncRead + AsyncWrite + Unpin,
    A: TlsAddress + Clone + Send + Unpin,
{
    type Response = TlsStream<T::IO>;
    type Error = TlsConnectionError<T::Error>;
    type Future = future::TlsConnectionFuture<T, A>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.transport
            .poll_ready(cx)
            .map_err(TlsConnectionError::Connection)
    }

    fn call(&mut self, req: A) -> Self::Future {
        let config = self.config.clone();
        let Some(host) = req.domain().map(String::from) else {
            return future::TlsConnectionFuture::error(TlsConnectionError::NoDomain);
        };

        let future = self.transport.connect(req);

        future::TlsConnectionFuture::new(future, config, host)
    }
}

pub(in crate::client::conn::transport) mod future {
    use std::fmt;
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use pin_project::pin_project;
    use tokio::io::{AsyncRead, AsyncWrite};

    use crate::stream::tls::TlsHandshakeStream as _;

    use super::super::Transport;
    use super::*;

    #[pin_project(project = StateProject, project_replace = StateProjectOwned)]
    enum State<T, A>
    where
        T: Transport<A>,
    {
        Connecting {
            #[pin]
            future: T::Future,
            config: Arc<TlsClientConfig>,
            domain: String,
        },

        Handshake {
            stream: TlsStream<T::IO>,
        },

        Error {
            error: TlsConnectionError<<T as Transport<A>>::Error>,
        },

        Invalid,
    }

    impl<T, A> fmt::Debug for State<T, A>
    where
        T: Transport<A>,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                State::Connecting { .. } => f.debug_struct("Connecting").finish(),
                State::Handshake { .. } => f.debug_struct("Handshake").finish(),
                State::Error { .. } => f.debug_struct("Error").finish(),
                State::Invalid => f.debug_struct("Invalid").finish(),
            }
        }
    }

    #[pin_project]
    #[derive(Debug)]
    pub struct TlsConnectionFuture<T, A>
    where
        T: Transport<A>,
    {
        #[pin]
        state: State<T, A>,
        span: Option<tracing::Span>,
    }

    impl<T, A> TlsConnectionFuture<T, A>
    where
        T: Transport<A>,
    {
        pub(super) fn new(future: T::Future, config: Arc<TlsClientConfig>, domain: String) -> Self {
            Self {
                state: State::Connecting {
                    future,
                    config,
                    domain,
                },
                span: None,
            }
        }

        pub(super) fn error(error: TlsConnectionError<T::Error>) -> Self {
            Self {
                state: State::Error { error },
                span: None,
            }
        }
    }

    impl<T, A> Future for TlsConnectionFuture<T, A>
    where
        T: Transport<A>,
        <T as Transport<A>>::IO: HasConnectionInfo<Addr = A> + AsyncRead + AsyncWrite + Unpin,
        A: Clone + Send + Unpin,
    {
        type Output = Result<TlsStream<T::IO>, TlsConnectionError<T::Error>>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut this = self.project();
            loop {
                match this.state.as_mut().project() {
                    StateProject::Connecting {
                        future,
                        config,
                        domain,
                    } => match future.poll(cx) {
                        Poll::Ready(Ok(stream)) => {
                            let _guard = this
                                .span
                                .get_or_insert_with(|| tracing::trace_span!("tls"))
                                .enter();
                            tracing::trace!("Transport connected. TLS handshake starting");
                            let stream = TlsStream::new(stream, domain, config.clone());
                            this.state.set(State::Handshake { stream });
                        }
                        Poll::Ready(Err(e)) => {
                            tracing::trace!(?e, "Transport connection error");
                            return Poll::Ready(Err(TlsConnectionError::Connection(e)));
                        }
                        Poll::Pending => return Poll::Pending,
                    },
                    StateProject::Handshake { stream } => {
                        let _guard = this
                            .span
                            .get_or_insert_with(|| tracing::trace_span!("tls"))
                            .enter();
                        match stream.poll_handshake(cx) {
                            Poll::Ready(Ok(())) => {
                                let StateProjectOwned::Handshake { stream } =
                                    this.state.project_replace(State::Invalid)
                                else {
                                    unreachable!();
                                };

                                tracing::trace!("TLS handshake complete");
                                return Poll::Ready(Ok(stream));
                            }
                            Poll::Ready(Err(e)) => {
                                tracing::trace!(?e, "Transport handshake error");
                                return Poll::Ready(Err(TlsConnectionError::Handshake(e)));
                            }
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                    StateProject::Error { .. } => {
                        let StateProjectOwned::Error { error } =
                            this.state.project_replace(State::Invalid)
                        else {
                            unreachable!();
                        };

                        return Poll::Ready(Err(error));
                    }
                    StateProject::Invalid => panic!("polled after ready"),
                };
            }
        }
    }
}
