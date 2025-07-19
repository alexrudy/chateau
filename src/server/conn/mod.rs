//! Server-side connection builders.

use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;
use tower::BoxError;
pub(super) mod drivers;

#[cfg(feature = "tls")]
pub mod tls;

/// A transport protocol for serving connections.
///
/// This is not meant to be the "accept" part of a server, but instead the connection
/// management and serving part.
pub trait Protocol<S, IO, Req> {
    /// The response type for the protocol.
    type Response: Send + 'static;

    /// The error when a connection has a problem.
    type Error: Into<BoxError>;

    /// The connection future, used to drive a connection IO to completion.
    type Connection: Connection + Future<Output = Result<(), Self::Error>> + 'static;

    /// Serve a connection with possible upgrades.
    ///
    /// Implementing this method does not guarantee that a protocol can be upgraded,
    /// just that we can serve the connection.
    fn serve_connection(&self, stream: IO, service: S) -> Self::Connection;
}

/// A connection that can be gracefully shutdown.
pub trait Connection {
    /// Gracefully shutdown the connection.
    fn graceful_shutdown(self: Pin<&mut Self>);
}

/// An async generator of new connections
pub trait Accept {
    /// The connection type for this acceptor
    type Connection: Unpin + 'static;

    /// The error type for this acceptor
    type Error: Into<BoxError>;

    /// Poll for a new connection
    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Connection, Self::Error>>;
}

/// Extension trait for Accept
pub trait AcceptExt: Accept {
    /// Wrap the acceptor in a future that resolves to a single connection.
    fn accept(self) -> AcceptOne<Self>
    where
        Self: Sized,
    {
        AcceptOne::new(self)
    }
}

impl<A> AcceptExt for A where A: Accept {}

/// A future that resolves to a single connection
#[derive(Debug)]
#[pin_project]
pub struct AcceptOne<A> {
    #[pin]
    inner: A,
}

impl<A> AcceptOne<A> {
    fn new(inner: A) -> Self {
        AcceptOne { inner }
    }
}

impl<A> Future for AcceptOne<A>
where
    A: Accept,
{
    type Output = Result<A::Connection, A::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.project().inner.poll_accept(cx)
    }
}
