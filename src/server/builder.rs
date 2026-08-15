#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use rustls::ServerConfig;
use tower::make::Shared;

use crate::rt::TokioExecutor;

use super::Accept;
use super::Server;

#[cfg(feature = "tls")]
use super::conn::tls::{TlsAcceptor, acceptor::TlsAcceptExt};

/// Indicates that the Server requires an acceptor.
#[derive(Debug, Clone, Copy, Default)]
pub struct NeedsAcceptor {
    _priv: (),
}

/// Indicates that the Server requires a protocol.
#[derive(Debug, Clone, Copy, Default)]
pub struct NeedsProtocol {
    _priv: (),
}

/// Indicates that the Server requires a service.
#[derive(Debug, Clone, Copy, Default)]
pub struct NeedsService {
    _priv: (),
}

/// Indicates that the Server requires an executor.
#[derive(Debug, Clone, Copy, Default)]
pub struct NeedsExecutor {
    _priv: (),
}

impl<P, S, B, E> Server<NeedsAcceptor, P, S, B, E> {
    /// Set the acceptor to use for incoming connections.
    pub fn with_acceptor<A>(self, acceptor: A) -> Server<A, P, S, B, E>
    where
        A: Accept,
    {
        Server {
            acceptor,
            make_service: self.make_service,
            protocol: self.protocol,
            executor: self.executor,
            request: self.request,
        }
    }
}

impl<A, S, B, E> Server<A, NeedsProtocol, S, B, E> {
    /// Set the protocol to use for incoming connections.
    pub fn with_protocol<P>(self, protocol: P) -> Server<A, P, S, B, E> {
        Server {
            acceptor: self.acceptor,
            make_service: self.make_service,
            protocol,
            executor: self.executor,
            request: self.request,
        }
    }
}

impl<A, P, B, E> Server<A, P, NeedsService, B, E> {
    /// Set the make service to use for handling incoming connections.
    ///
    /// A [MakeService][tower::make::MakeService] is a factory for creating [Service][tower::Service] instances. It is
    /// used to create a new [Service][tower::Service] for each incoming connection.
    ///
    /// If you have a service that is [Clone], you can use [Server::with_shared_service]
    /// to wrap it in a [Shared] and avoid constructing a new make service.
    pub fn with_make_service<S>(self, make_service: S) -> Server<A, P, S, B, E> {
        Server {
            acceptor: self.acceptor,
            make_service,
            protocol: self.protocol,
            executor: self.executor,
            request: self.request,
        }
    }

    /// Wrap a [Clone] service in a [Shared] to use as a make service.
    pub fn with_shared_service<S>(self, service: S) -> Server<A, P, Shared<S>, B, E> {
        Server {
            acceptor: self.acceptor,
            make_service: Shared::new(service),
            protocol: self.protocol,
            executor: self.executor,
            request: self.request,
        }
    }
}

impl<A, P, S, B> Server<A, P, S, B, NeedsExecutor> {
    /// Set the executor for this server
    ///
    /// The executor is used to drive connections to completion asynchronously.
    pub fn with_executor<E>(self, executor: E) -> Server<A, P, S, B, E> {
        Server {
            acceptor: self.acceptor,
            make_service: self.make_service,
            protocol: self.protocol,
            executor,
            request: self.request,
        }
    }

    /// Use a tokio multi-threaded excutor via [tokio::task::spawn]
    ///
    /// This executor is a suitable default, but does require Send and 'static
    /// bounds in some places to allow futures to be moved between threads.
    ///
    /// Consider the [TokioCurrentThreadExecutor][crate::rt::TokioCurrentThreadExecutor]
    /// if releasing the Send and 'static bounds is necessary.
    ///
    /// Neither executor provides good monitoring for in-progress tasks, that is left to
    /// either middleware for the service, or the protocol's connection type. However,
    /// it would be easy to implement more sophisticated executors - they just accept
    /// a [Future] and arrange for it to be polled to completion.
    pub fn with_tokio(self) -> Server<A, P, S, B, TokioExecutor> {
        self.with_executor(TokioExecutor::new())
    }
}

impl<A, P, S, B, E> Server<A, P, S, B, E> {
    /// Set the body to use for handling requests.
    ///
    /// Usually this method can be called with inferred
    /// types, as it exists only to specify the input body
    /// type, since many protocols are generic over a suitable
    /// range of body types.
    pub fn with_request<B2>(self) -> Server<A, P, S, B2, E> {
        Server {
            acceptor: self.acceptor,
            make_service: self.make_service,
            protocol: self.protocol,
            executor: self.executor,
            request: Default::default(),
        }
    }
}

#[cfg(feature = "tls")]
impl<A, P, S, B, E> Server<A, P, S, B, E>
where
    A: Accept,
{
    /// Add TLS to this server
    pub fn with_tls(self, config: Arc<ServerConfig>) -> Server<TlsAcceptor<A>, P, S, B, E> {
        Server {
            acceptor: self.acceptor.with_tls(config),
            protocol: self.protocol,
            make_service: self.make_service,
            executor: self.executor,
            request: self.request,
        }
    }
}

impl<A, P, S, B, E> Server<A, P, S, B, E> {
    /// Map the service to a new value.
    pub fn map_service<F, U>(self, f: F) -> Server<A, P, U, B, E>
    where
        F: FnOnce(S) -> U,
    {
        Server {
            acceptor: self.acceptor,
            make_service: f(self.make_service),
            protocol: self.protocol,
            executor: self.executor,
            request: self.request,
        }
    }
}
