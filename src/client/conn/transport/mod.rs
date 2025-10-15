//! Transport streams for connecting to remote servers.
//!
//! Transports are responsible for establishing a connection to a remote server, shuffling bytes back and forth,

use std::future::Future;
#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use rustls::client::ClientConfig;
#[cfg(feature = "tls")]
use thiserror::Error;
use tower::Service;

use self::oneshot::Oneshot;

#[cfg(feature = "tls")]
pub use self::tls::{StaticHostTlsTransport, TlsRequest, TlsTransport};
#[cfg(feature = "tls")]
use crate::client::default_tls_config;

use crate::info::HasConnectionInfo;

#[cfg(feature = "duplex")]
pub mod duplex;
#[cfg(feature = "mock")]
pub mod mock;
pub mod tcp;
#[cfg(feature = "tls")]
pub mod tls;
#[cfg(target_family = "unix")]
pub mod unix;

/// A transport provides data transmission between two endpoints.
///
/// To implement a transport stream, implement a [`tower::Service`] which accepts an address and returns
/// an IO stream, which must be compatible with a [`super::Protocol`].
pub trait Transport<Req>: Send {
    /// The type of IO stream used by this transport
    type IO: HasConnectionInfo + Send + 'static;

    /// Error returned when connection fails
    type Error: std::error::Error + Send + Sync + 'static;

    /// The future type returned by this service
    type Future: Future<Output = Result<Self::IO, <Self as Transport<Req>>::Error>> + Send + 'static;

    /// Connect to a remote server and return a stream.
    fn connect(&mut self, req: &Req) -> <Self as Transport<Req>>::Future;

    /// Poll the transport to see if it is ready to accept a new connection.
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), <Self as Transport<Req>>::Error>>;
}

impl<T, IO, Req, F, E> Transport<Req> for T
where
    T: for<'a> Service<&'a Req, Response = IO, Future = F, Error = E>,
    T: Clone + Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
    F: Future<Output = Result<IO, E>> + Send + 'static,
    IO: HasConnectionInfo + Send + 'static,
    Req: Send,
{
    type IO = IO;
    type Error = E;
    type Future = F;

    fn connect(&mut self, req: &Req) -> Self::Future {
        self.call(req)
    }

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), <Self as Transport<Req>>::Error>> {
        Service::poll_ready(self, cx)
    }
}

/// Extension trait for Transports to provide additional configuration options.
pub trait TransportExt<Req>: Transport<Req> {
    #[cfg(feature = "tls")]
    /// Wrap the transport in a TLS layer.
    fn with_tls(self, config: Arc<ClientConfig>) -> TlsTransport<Self>
    where
        Self: Sized,
    {
        TlsTransport::new(self, config)
    }

    #[cfg(feature = "tls")]
    /// Wrap the transport in a TLS layer configured with a default client configuration.
    fn with_default_tls(self) -> TlsTransport<Self>
    where
        Self: Sized,
    {
        TlsTransport::new(self, default_tls_config().into())
    }

    /// Create a future which uses the given transport to connect after calling poll_ready.
    fn oneshot(self, request: Req) -> Oneshot<Self, Req>
    where
        Self: Sized,
    {
        Oneshot::new(self, request)
    }
}

impl<T, Req> TransportExt<Req> for T where T: Transport<Req> {}

/// An error returned when a TLS connection attempt fails
#[cfg(feature = "tls")]
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TlsConnectionError<E> {
    /// An error occured while trying to connect via the underlying transport
    /// before any TLS handshake was attempted.
    #[error(transparent)]
    Connection(#[from] E),

    /// An error occured during the TLS handshake.
    #[error("TLS handshake failed: {0}")]
    Handshake(#[source] std::io::Error),

    /// The request did not contain a domain, making TLS certificate verification impossible.
    #[error("No domain found in URI")]
    NoDomain,

    /// The TLS feature is disabled, but TLS was requested.
    #[error("TLS is not enabled, can't connect to https")]
    TlsDisabled,
}

mod oneshot {
    use std::{fmt, future::Future, task::ready};

    use super::Transport;

    #[pin_project::pin_project(project=OneshotStateProj)]
    enum OneshotState<T, R>
    where
        T: Transport<R>,
    {
        Pending { transport: T, request: Option<R> },
        Ready(#[pin] T::Future),
    }

    impl<T, R> fmt::Debug for OneshotState<T, R>
    where
        T: Transport<R>,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                OneshotState::Pending { .. } => f.debug_struct("OneshotState::Pending").finish(),
                OneshotState::Ready(_) => f.debug_struct("OneshotState::Ready").finish(),
            }
        }
    }

    #[derive(Debug)]
    #[pin_project::pin_project]
    pub struct Oneshot<T, R>
    where
        T: Transport<R>,
    {
        #[pin]
        state: OneshotState<T, R>,
    }

    impl<T, R> Oneshot<T, R>
    where
        T: Transport<R>,
    {
        pub fn new(transport: T, request: R) -> Self {
            Self {
                state: OneshotState::Pending {
                    transport,
                    request: Some(request),
                },
            }
        }
    }

    impl<T, R> Future for Oneshot<T, R>
    where
        T: Transport<R>,
    {
        type Output = Result<T::IO, T::Error>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            let mut this = self.project();
            loop {
                match this.state.as_mut().project() {
                    OneshotStateProj::Pending { transport, request } => {
                        ready!(transport.poll_ready(cx))?;
                        let fut = transport.connect(request.as_ref().unwrap());
                        this.state.set(OneshotState::Ready(fut));
                    }
                    OneshotStateProj::Ready(fut) => {
                        return fut.poll(cx);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::Ready;

    use static_assertions::assert_obj_safe;
    assert_obj_safe!(Transport<(), IO=(), Error=(), Future=Ready<Result<(),()>>>);
}
