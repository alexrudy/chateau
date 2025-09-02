//! Connections are responsible for sending and receiving HTTP requests and responses
//! over an arbitrary two-way stream of bytes.

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

/// A connection to a remote server which can send and recieve requests/responses.
pub trait Connection<Req> {
    /// The response type for this connection
    type Response: 'static;

    /// The error type for this connection
    type Error: std::error::Error + Send + Sync + 'static;

    /// The future type returned by this service
    type Future: Future<Output = Result<Self::Response, Self::Error>> + Send + 'static;

    /// Send a request to the remote server and return the response.
    fn send_request(&mut self, request: Req) -> Self::Future;

    /// Poll the connection to see if it is ready to accept a new request.
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>>;
}

/// Extension trait for `Connection` providing additional methods.
pub trait ConnectionExt<R>: Connection<R> {
    /// Future which resolves when the connection is ready to accept a new request.
    fn when_ready(&mut self) -> WhenReady<'_, Self, R> {
        WhenReady::new(self)
    }
}

impl<T, R> ConnectionExt<R> for T where T: Connection<R> {}

/// A future which resolves when the connection is ready again
#[derive(Debug)]
pub struct WhenReady<'a, C, R>
where
    C: Connection<R> + ?Sized,
{
    conn: &'a mut C,
    _private: std::marker::PhantomData<fn(R)>,
}

impl<'a, C, R> WhenReady<'a, C, R>
where
    C: Connection<R> + ?Sized,
{
    pub(crate) fn new(conn: &'a mut C) -> Self {
        Self {
            conn,
            _private: std::marker::PhantomData,
        }
    }
}

impl<C, R> Future for WhenReady<'_, C, R>
where
    C: Connection<R> + ?Sized,
{
    type Output = Result<(), C::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.conn.poll_ready(cx)
    }
}
