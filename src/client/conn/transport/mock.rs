//! A transport full of empty implementations, suitable for testing behavior of transport-dependent code.

use std::convert::Infallible;
use std::future::{Ready, ready};
use std::task::{Context, Poll};

use thiserror::Error;

use crate::BoxFuture;
use crate::client::conn::Connector;
use crate::client::conn::protocol::mock::{MockProtocol, MockRequest};
use crate::client::conn::stream::mock::{MockAddress, MockStream};

/// An error that can occur when creating a mock transport.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("connection error")]
pub struct MockConnectionError;

#[derive(Debug)]
enum TransportMode {
    SingleUse,
    Reusable,
    ConnectionError,
    Channel(Option<tokio::sync::oneshot::Receiver<MockStream>>),
}

/// An address resolver that always returns mock addresses
#[derive(Debug, Clone)]
pub struct MockResolver {}

impl tower::Service<&MockRequest> for MockResolver {
    type Error = Infallible;
    type Response = MockAddress;
    type Future = Ready<Result<MockAddress, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: &MockRequest) -> Self::Future {
        ready(Ok(MockAddress))
    }
}

/// A mock transport that can be used to test connection behavior.
#[derive(Debug)]
pub struct MockTransport {
    mode: TransportMode,
}

impl Clone for MockTransport {
    fn clone(&self) -> Self {
        Self {
            mode: match &self.mode {
                TransportMode::Channel(_) => TransportMode::ConnectionError,
                TransportMode::SingleUse => TransportMode::SingleUse,
                TransportMode::Reusable => TransportMode::Reusable,
                TransportMode::ConnectionError => TransportMode::ConnectionError,
            },
        }
    }
}

impl MockTransport {
    /// Create a new transport with the specified reuse mode.
    pub fn new(reuse: bool) -> Self {
        let mode = if reuse {
            TransportMode::Reusable
        } else {
            TransportMode::SingleUse
        };
        Self { mode }
    }

    /// Transport which can be used only once
    pub fn single() -> Self {
        Self::new(false)
    }

    /// Transport which can be reused
    pub fn reusable() -> Self {
        Self::new(true)
    }

    /// Transport which returns an error during connection attempts
    pub fn error() -> Self {
        Self {
            mode: TransportMode::ConnectionError,
        }
    }

    /// Transport which returns a stream from a oneshot channel
    pub fn channel(rx: tokio::sync::oneshot::Receiver<MockStream>) -> Self {
        Self {
            mode: TransportMode::Channel(Some(rx)),
        }
    }

    /// Create a new connector for the transport.
    pub fn connector(
        self,
        request: MockRequest,
    ) -> Connector<MockResolver, Self, MockProtocol, MockRequest> {
        Connector::new(MockResolver {}, self, MockProtocol::default(), request)
    }
}

impl tower::Service<MockAddress> for MockTransport {
    type Response = MockStream;

    type Error = MockConnectionError;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: MockAddress) -> Self::Future {
        let reuse = match &mut self.mode {
            TransportMode::SingleUse => false,
            TransportMode::Reusable => true,
            TransportMode::ConnectionError => return Box::pin(ready(Err(MockConnectionError))),
            TransportMode::Channel(rx) => {
                if let Some(rx) = rx.take() {
                    return Box::pin(async move {
                        match rx.await {
                            Ok(stream) => Ok(stream),
                            Err(_) => Err(MockConnectionError),
                        }
                    });
                }

                return Box::pin(ready(Err(MockConnectionError)));
            }
        };

        let conn = MockStream::new(reuse);
        Box::pin(async move { Ok(conn) })
    }
}

#[cfg(test)]
mod tests {

    use crate::client::conn::Transport;

    use super::*;

    use static_assertions::assert_impl_all;

    assert_impl_all!(MockConnectionError: std::error::Error, Send, Sync);
    assert_impl_all!(MockTransport: Transport<MockAddress>);
}
