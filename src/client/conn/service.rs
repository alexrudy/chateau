//! Request execution service

use std::task::{Context, Poll};

use super::Connection;

/// A service which serves as the base of
/// a client, which does the execution of
/// a request on a connection.
#[derive(Debug, Default, Clone)]
pub struct ClientExecutorService {
    _priv: (),
}

impl ClientExecutorService {
    /// Create a new client executor service.
    pub fn new() -> Self {
        Self { _priv: () }
    }
}

impl<C, R> tower::Service<(C, R)> for ClientExecutorService
where
    C: Connection<R>,
{
    type Response = C::Response;
    type Error = C::Error;
    type Future = C::Future;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The connection in the request is assumed to be ready.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, (mut conn, req): (C, R)) -> Self::Future {
        conn.send_request(req)
    }
}
