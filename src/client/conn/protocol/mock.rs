//! Mock protocol implementation for testing purposes.

use std::future::{Ready, ready};

use thiserror::Error;

use crate::client::conn::Connection;
use crate::client::conn::stream::mock::{MockStream, StreamID};
use crate::client::pool::{PoolableConnection, PoolableStream};

/// Fake request
#[derive(Debug)]
pub struct MockRequest;

/// Fake response
#[derive(Debug)]
pub struct MockResponse;

/// Fake error
#[derive(Debug, thiserror::Error)]
#[error("mock error")]
pub struct MockError;

/// A minimal protocol sender for testing purposes.
#[derive(Debug, Clone)]
pub struct MockSender {
    id: StreamID,
    stream: MockStream,
}

impl MockSender {
    /// The unique identifier for the stream.
    pub fn id(&self) -> StreamID {
        self.id
    }

    /// Close the connection and stream
    pub fn close(&self) {
        self.stream.close();
    }

    /// Create a single-use mock sender.
    pub fn single() -> Self {
        Self {
            id: StreamID::new(),
            stream: MockStream::single(),
        }
    }

    /// Create a new reusable mock sender.
    pub fn reusable() -> Self {
        Self {
            id: StreamID::new(),
            stream: MockStream::reusable(),
        }
    }

    /// Create a new mock sender.
    pub fn new() -> Self {
        Self::reusable()
    }
}

impl Default for MockSender {
    fn default() -> Self {
        Self::reusable()
    }
}

impl Connection<MockRequest> for MockSender {
    type Response = MockResponse;

    type Error = MockProtocolError;

    type Future = Ready<Result<MockResponse, Self::Error>>;

    fn send_request(&mut self, _: MockRequest) -> Self::Future {
        ready(Ok(MockResponse))
    }

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl PoolableConnection<MockRequest> for MockSender {
    fn is_open(&self) -> bool {
        self.stream.is_open()
    }

    fn can_share(&self) -> bool {
        self.stream.can_share()
    }

    fn reuse(&mut self) -> Option<Self> {
        Some(self.clone())
    }
}

/// Error type for the mock protocol.
#[derive(Debug, Default, Error, PartialEq, Eq)]
#[error("mock protocol error")]
pub struct MockProtocolError {
    _private: (),
}

/// A simple protocol for returning empty responses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MockProtocol {
    _private: (),
}

impl tower::Service<MockStream> for MockProtocol {
    type Response = MockSender;

    type Error = MockError;
    type Future = Ready<Result<MockSender, MockError>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: MockStream) -> Self::Future {
        ready(Ok(MockSender {
            id: StreamID::new(),
            stream: req,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::client::conn::Protocol;

    use super::*;

    use static_assertions::assert_impl_all;

    assert_impl_all!(MockSender: Connection<MockRequest>, PoolableConnection<MockRequest>);
    assert_impl_all!(MockProtocol: Protocol<MockStream, MockRequest>);
}
