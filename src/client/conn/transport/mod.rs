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
pub use self::tls::TlsTransport;
#[cfg(feature = "tls")]
use crate::client::default_tls_config;

use crate::info::HasConnectionInfo;

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
pub trait Transport<Addr>: Send {
    /// The type of IO stream used by this transport
    type IO: HasConnectionInfo<Addr = Addr> + Send + 'static;

    /// Error returned when connection fails
    type Error: std::error::Error + Send + Sync + 'static;

    /// The future type returned by this service
    type Future: Future<Output = Result<Self::IO, <Self as Transport<Addr>>::Error>>
        + Send
        + 'static;

    /// Connect to a remote server and return a stream.
    fn connect(&mut self, req: Addr) -> <Self as Transport<Addr>>::Future;

    /// Poll the transport to see if it is ready to accept a new connection.
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), <Self as Transport<Addr>>::Error>>;
}

impl<T, IO, Addr> Transport<Addr> for T
where
    T: Service<Addr, Response = IO>,
    T: Clone + Send + Sync + 'static,
    T::Error: std::error::Error + Send + Sync + 'static,
    T::Future: Send + 'static,
    IO: HasConnectionInfo<Addr = Addr> + Send + 'static,
    Addr: Send,
{
    type IO = IO;
    type Error = T::Error;
    type Future = T::Future;

    fn connect(&mut self, req: Addr) -> <Self as Service<Addr>>::Future {
        self.call(req)
    }

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), <Self as Transport<Addr>>::Error>> {
        Service::poll_ready(self, cx)
    }
}

/// Extension trait for Transports to provide additional configuration options.
pub trait TransportExt<Addr>: Transport<Addr> {
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
    fn oneshot(self, request: Addr) -> Oneshot<Self, Addr>
    where
        Self: Sized,
    {
        Oneshot::new(self, request)
    }
}

impl<T, Addr> TransportExt<Addr> for T where T: Transport<Addr> {}

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
    enum OneshotState<T, A>
    where
        T: Transport<A>,
    {
        Pending { transport: T, request: Option<A> },
        Ready(#[pin] T::Future),
    }

    impl<T, A> fmt::Debug for OneshotState<T, A>
    where
        T: Transport<A>,
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
    pub struct Oneshot<T, A>
    where
        T: Transport<A>,
    {
        #[pin]
        state: OneshotState<T, A>,
    }

    impl<T, A> Oneshot<T, A>
    where
        T: Transport<A>,
    {
        pub fn new(transport: T, request: A) -> Self {
            Self {
                state: OneshotState::Pending {
                    transport,
                    request: Some(request),
                },
            }
        }
    }

    impl<T, A> Future for Oneshot<T, A>
    where
        T: Transport<A>,
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
                        let fut = transport.connect(request.take().unwrap());
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
