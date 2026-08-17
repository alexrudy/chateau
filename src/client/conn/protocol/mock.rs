//! Mock protocol implementation for testing purposes.

use std::future::{Ready, ready};

use thiserror::Error;

use crate::client::conn::Connection;
use crate::client::conn::stream::mock::{MockStream, StreamID};
use crate::client::pool::{PoolableConnection, PoolableStream};

use super::Protocol;

/// Fake request
#[derive(Debug)]
pub struct MockRequest;

/// Fake response
#[derive(Debug)]
pub struct MockResponse;

/// Fake error
#[derive(Debug, thiserror::Error, PartialEq)]
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
    multiplex: bool,
    multiplex_ready: Option<bool>,
    fail_ready: bool,
    fail_handshake: bool,
    _private: (),
}

impl MockProtocol {
    /// Creates a new mock protocol.
    pub fn new(multiplex: bool) -> Self {
        Self {
            multiplex,
            multiplex_ready: None,
            fail_ready: false,
            fail_handshake: false,
            _private: (),
        }
    }

    /// Overrides the answer returned by [`Protocol::multiplex_ready`],
    /// regardless of what the connected stream reports via
    /// [`PoolableStream::can_share`].
    ///
    /// This is useful for simulating protocols whose multiplexing decision
    /// can only be made once the transport is connected (e.g. ALPN
    /// negotiation over TLS), and which may report a different answer than
    /// [`Protocol::multiplex`] gave before connecting.
    pub fn with_multiplex_ready(mut self, multiplex_ready: bool) -> Self {
        self.multiplex_ready = Some(multiplex_ready);
        self
    }

    /// Makes [`Protocol::poll_ready`] return an error, simulating a protocol
    /// that fails before it ever attempts the handshake (e.g. because setup
    /// state is unavailable).
    pub fn with_ready_error(mut self) -> Self {
        self.fail_ready = true;
        self
    }

    /// Makes the handshake future returned by [`Protocol::connect`] resolve
    /// to an error, simulating a protocol that fails during the handshake
    /// itself, after the transport has already connected.
    pub fn with_handshake_error(mut self) -> Self {
        self.fail_handshake = true;
        self
    }
}

impl tower::Service<MockStream> for MockProtocol {
    type Response = MockSender;

    type Error = MockError;
    type Future = Ready<Result<MockSender, MockError>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.fail_ready {
            return std::task::Poll::Ready(Err(MockError));
        }
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: MockStream) -> Self::Future {
        if self.fail_handshake {
            return ready(Err(MockError));
        }
        ready(Ok(MockSender {
            id: StreamID::new(),
            stream: req,
        }))
    }
}

impl Protocol<MockStream, MockRequest> for MockProtocol {
    type Error = MockError;
    type Connection = MockSender;
    type Future = Ready<Result<MockSender, MockError>>;

    fn connect(
        &mut self,
        transport: MockStream,
    ) -> <Self as Protocol<MockStream, MockRequest>>::Future {
        tower::Service::call(self, transport)
    }

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), <Self as Protocol<MockStream, MockRequest>>::Error>> {
        tower::Service::poll_ready(self, cx)
    }

    fn multiplex(&self) -> bool {
        self.multiplex
    }

    /// Falls back to the connected stream's [`PoolableStream::can_share`] when
    /// no explicit override has been set with [`MockProtocol::with_multiplex_ready`].
    fn multiplex_ready(&self, io: &MockStream) -> bool {
        self.multiplex_ready.unwrap_or_else(|| io.can_share())
    }
}

#[cfg(test)]
mod tests {
    use crate::client::conn::Protocol;
    use std::task::{Context, Poll};

    use super::*;

    use static_assertions::assert_impl_all;

    assert_impl_all!(MockSender: Connection<MockRequest>, PoolableConnection<MockRequest>);
    assert_impl_all!(MockProtocol: Protocol<MockStream, MockRequest>);
    assert_impl_all!(MockRequest: Send, Sync);
    assert_impl_all!(MockResponse: Send, Sync);
    assert_impl_all!(MockError: std::error::Error, Send, Sync);
    assert_impl_all!(MockProtocolError: std::error::Error, Send, Sync);

    #[test]
    fn test_mock_sender_new() {
        let sender = MockSender::new();
        assert!(sender.stream.can_share());
        assert!(sender.stream.is_open());
    }

    #[test]
    fn test_mock_sender_single() {
        let sender = MockSender::single();
        assert!(!sender.stream.can_share());
        assert!(sender.stream.is_open());
    }

    #[test]
    fn test_mock_sender_reusable() {
        let sender = MockSender::reusable();
        assert!(sender.stream.can_share());
        assert!(sender.stream.is_open());
    }

    #[test]
    fn test_mock_sender_default() {
        let sender = MockSender::default();
        assert!(sender.stream.can_share());
        assert!(sender.stream.is_open());
    }

    #[test]
    fn test_mock_sender_id() {
        let sender1 = MockSender::new();
        let sender2 = MockSender::new();

        assert_ne!(sender1.id(), sender2.id());
    }

    #[test]
    fn test_mock_sender_close() {
        let sender = MockSender::new();
        assert!(sender.stream.is_open());

        sender.close();
        assert!(!sender.stream.is_open());
    }

    #[test]
    fn test_mock_sender_clone() {
        let sender1 = MockSender::new();
        let sender2 = sender1.clone();

        assert_eq!(sender1.id(), sender2.id());
    }

    #[tokio::test]
    async fn test_mock_sender_send_request() {
        let mut sender = MockSender::new();
        let request = MockRequest;

        let future = sender.send_request(request);
        let result = future.await;

        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_sender_poll_ready() {
        let mut sender = MockSender::new();
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let result = sender.poll_ready(&mut cx);
        assert!(matches!(result, Poll::Ready(Ok(()))));
    }

    #[test]
    fn test_mock_sender_poolable_connection() {
        let mut sender = MockSender::reusable();

        assert!(sender.is_open());
        assert!(sender.can_share());

        let reused = sender.reuse();
        assert!(reused.is_some());

        let cloned = reused.unwrap();
        assert_eq!(sender.id(), cloned.id());
    }

    #[test]
    fn test_mock_sender_poolable_connection_single() {
        let mut sender = MockSender::single();

        assert!(sender.is_open());
        assert!(!sender.can_share());

        let reused = sender.reuse();
        assert!(reused.is_some());
    }

    #[test]
    fn test_mock_protocol_multiplex_ready_default_uses_stream() {
        let protocol = MockProtocol::new(false);

        let shareable = MockStream::reusable();
        assert!(Protocol::multiplex_ready(&protocol, &shareable));

        let single_use = MockStream::single();
        assert!(!Protocol::multiplex_ready(&protocol, &single_use));
    }

    #[test]
    fn test_mock_protocol_multiplex_ready_can_diverge_from_multiplex() {
        let protocol = MockProtocol::new(true);
        assert!(Protocol::multiplex(&protocol));

        // The eager answer optimistically assumed multiplexing, but the actual
        // connected stream reports it cannot be shared.
        let single_use = MockStream::single();
        assert!(!Protocol::multiplex_ready(&protocol, &single_use));
    }

    #[test]
    fn test_mock_protocol_multiplex_ready_override() {
        let protocol = MockProtocol::new(true).with_multiplex_ready(false);
        assert!(Protocol::multiplex(&protocol));

        // Even though the eager answer is `true` and the stream itself reports
        // that it can share, the explicit override wins.
        let shareable = MockStream::reusable();
        assert!(!Protocol::multiplex_ready(&protocol, &shareable));
    }

    #[test]
    fn test_mock_protocol_with_ready_error() {
        use tower::Service;

        let mut protocol = MockProtocol::new(true).with_ready_error();

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let result = Service::poll_ready(&mut protocol, &mut cx);
        assert!(matches!(result, Poll::Ready(Err(MockError))));
    }

    #[tokio::test]
    async fn test_mock_protocol_with_handshake_error() {
        use tower::Service;

        let mut protocol = MockProtocol::new(true).with_handshake_error();
        let stream = MockStream::reusable();

        let result = protocol.call(stream).await;
        assert!(matches!(result, Err(MockError)));
    }

    #[test]
    fn test_mock_protocol_default() {
        let protocol = MockProtocol::default();
        let debug_str = format!("{protocol:?}");
        assert!(debug_str.contains("MockProtocol"));
    }

    #[test]
    fn test_mock_protocol_clone() {
        let protocol1 = MockProtocol::default();
        let protocol2 = protocol1.clone();

        assert_eq!(protocol1, protocol2);
    }

    #[tokio::test]
    async fn test_mock_protocol_service() {
        use tower::Service;

        let mut protocol = MockProtocol::default();
        let stream = MockStream::new(true);

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let poll_result = tower::Service::poll_ready(&mut protocol, &mut cx);
        assert!(matches!(poll_result, Poll::Ready(Ok(()))));

        let future = protocol.call(stream);
        let result = future.await;

        assert!(result.is_ok());
        let sender = result.unwrap();
        assert!(sender.stream.can_share());
    }

    #[test]
    fn test_mock_error() {
        let error = MockError;
        let error_str = format!("{error}");
        assert_eq!(error_str, "mock error");

        let debug_str = format!("{error:?}");
        assert!(debug_str.contains("MockError"));
    }

    #[test]
    fn test_mock_protocol_error() {
        let error = MockProtocolError::default();
        let error_str = format!("{error}");
        assert_eq!(error_str, "mock protocol error");

        let debug_str = format!("{error:?}");
        assert!(debug_str.contains("MockProtocolError"));
    }

    #[test]
    fn test_mock_request_debug() {
        let request = MockRequest;
        let debug_str = format!("{request:?}");
        assert!(debug_str.contains("MockRequest"));
    }

    #[test]
    fn test_mock_response_debug() {
        let response = MockResponse;
        let debug_str = format!("{response:?}");
        assert!(debug_str.contains("MockResponse"));
    }
}
