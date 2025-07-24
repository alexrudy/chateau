//! Protocol describes how http requests and responses are transmitted over a connection.
//!
//! There are three protocols provided here: HTTP/1.1, HTTP/2, and an automatically
//! negotiated protocol which can be either HTTP/1.1 or HTTP/2 based on the connection
//! protocol and ALPN negotiation.

#[cfg(feature = "mock")]
pub mod mock;

#[cfg(feature = "codec")]
pub mod framed;

use std::future::Future;

use tower::Service;

use super::Connection;

use crate::info::HasConnectionInfo;

/// Protocols (like HTTP) define how data is sent and received over a connection.
///
/// A protocol is a service which accepts a [`ProtocolRequest`] and returns a connection.
///
/// The request contains a transport stream and the HTTP protocol to use for the connection.
///
/// The connection is responsible for sending and receiving HTTP requests and responses.
///
///
pub trait Protocol<IO, Req>
where
    IO: HasConnectionInfo,
    Self: Service<IO, Response = Self::Connection>,
{
    /// Error returned when connection fails
    type Error: std::error::Error + Send + Sync + 'static;

    /// The type of connection returned by this service
    type Connection: Connection<Req>;

    /// The type of the handshake future
    type Future: Future<Output = Result<Self::Connection, <Self as Protocol<IO, Req>>::Error>>
        + Send
        + 'static;

    /// Connect to a remote server and return a connection.
    ///
    /// The protocol version is provided to facilitate the correct handshake procedure.
    fn connect(&mut self, transport: IO) -> <Self as Protocol<IO, Req>>::Future;

    /// Poll the protocol to see if it is ready to accept a new connection.
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), <Self as Protocol<IO, Req>>::Error>>;

    /// Can this protocol multiplex on the same connection?
    fn multiplex(&self) -> bool {
        false
    }
}

impl<T, C, IO, Req> Protocol<IO, Req> for T
where
    IO: HasConnectionInfo,
    T: Service<IO, Response = C> + Send + 'static,
    T::Error: std::error::Error + Send + Sync + 'static,
    T::Future: Send + 'static,
    C: Connection<Req>,
{
    type Error = T::Error;
    type Connection = C;
    type Future = T::Future;

    fn connect(&mut self, transport: IO) -> <Self as Protocol<IO, Req>>::Future {
        self.call(transport)
    }

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), <Self as Protocol<IO, Req>>::Error>> {
        Service::poll_ready(self, cx)
    }
}
