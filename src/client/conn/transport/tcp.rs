//! TCP transport implementation for client connections.
//!
//! This module contains the [`TcpTransport`] type, which is a [`tower::Service`] that connects to
//! remote addresses using TCP. It also contains the [`TcpTransportConfig`] type, which is used to
//! configure TCP connections.
//!
//! Normally, you will not need to use this module directly. Instead, you can use the [`Client`][crate::client::Client]
//! type from the [`client`][crate::client] module, which uses the [`TcpTransport`] internally by default.
//!
//! See [`Client::builder`](crate::client::Client::builder) for creating a client that uses the TCP transport.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::FutureExt as _;
use thiserror::Error;
use tokio::net::TcpSocket;
use tokio::task::JoinError;
use tracing::{Instrument, trace, warn};

use crate::BoxError;
use crate::client::conn::dns::{IpVersion, SocketAddrs};
use crate::happy_eyeballs::{EyeballSet, HappyEyeballsError};
use crate::stream::tcp::TcpStream;

/// A trait for resolving a TCP address from a request.
pub trait TcpResolver<Req> {
    /// Error returned when TCP resolution fails
    type Error;

    /// Future used to resovle TCP addresses
    type Future: Future<Output = Result<SocketAddrs, Self::Error>>;

    /// Check if the resolver is ready to resovle.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    /// Return a future to resolve an address.
    fn resolve(&mut self, req: Req) -> Self::Future;
}

impl<T, Req> TcpResolver<Req> for T
where
    T: tower::Service<Req, Response = SocketAddrs>,
{
    type Error = T::Error;

    type Future = T::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <T as tower::Service<Req>>::poll_ready(self, cx)
    }

    fn resolve(&mut self, req: Req) -> Self::Future {
        self.call(req)
    }
}

/// A TCP connector for client connections.
///
/// This type is a [`tower::Service`] that connects to remote addresses using TCP.
///
/// The connector can be configured with a [`TcpTransportConfig`] to control
/// various aspects of the TCP connection, such as timeouts, buffer sizes, and
/// local addresses to bind to.
///
#[cfg_attr(
    feature = "tls",
    doc = "If the `tls` feature is enabled, the connector can also be configured with
    a [`rustls::ClientConfig`] to enable TLS support"
)]
///
/// This connector implements the happy-eyeballs algorithm for connecting to
/// remote addresses, which allows for faster connection times by trying
/// multiple addresses in parallel, regardless of whether they are IPv4 or IPv6.
#[derive(Debug)]
pub struct TcpTransport<R> {
    resolver: R,
    config: Arc<TcpTransportConfig>,
}

impl<R: Clone> Clone for TcpTransport<R> {
    fn clone(&self) -> Self {
        Self {
            resolver: self.resolver.clone(),
            config: self.config.clone(),
        }
    }
}

impl<R: Default> Default for TcpTransport<R> {
    fn default() -> Self {
        Self {
            resolver: R::default(),
            config: Arc::new(TcpTransportConfig::default()),
        }
    }
}

impl<R> TcpTransport<R> {
    /// Create a new TCP transport
    pub fn new(resolver: R, config: Arc<TcpTransportConfig>) -> Self {
        Self { resolver, config }
    }
}

impl<R> TcpTransport<R> {
    /// Get the configuration for the TCP connector.
    pub fn config(&self) -> &TcpTransportConfig {
        &self.config
    }

    /// Mutable, copy-on-write access to the TCP Transport configuration
    pub fn config_mut(&mut self) -> &mut TcpTransportConfig {
        Arc::make_mut(&mut self.config)
    }
}

type BoxFuture<'a, T, E> = crate::BoxFuture<'a, Result<T, E>>;

impl<Resolver, Request> tower::Service<Request> for TcpTransport<Resolver>
where
    Resolver: TcpResolver<Request> + Clone + Send + 'static,
    Resolver::Error: std::error::Error + Send + Sync + 'static,
    Resolver::Future: Send + 'static,
{
    type Response = TcpStream;
    type Error = TcpConnectionError;
    type Future = BoxFuture<'static, Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let config = self.config.clone();
        let span = tracing::trace_span!("tcp");

        let resolve = self.resolver.resolve(req);

        Box::pin(
            async move {
                let addrs = resolve
                    .await
                    .map_err(TcpConnectionError::msg("resolving"))?;
                let stream = config.connect(addrs).await?;

                if let Ok(peer_addr) = stream.peer_addr() {
                    trace!(peer.addr = %peer_addr, "tcp connected");
                } else {
                    trace!("tcp connected");
                }

                Ok(stream)
            }
            .instrument(span),
        )
    }
}

/// Represents a single attempt to connect to a remote address.
///
/// This exists to allow us to move the SocketAddr but borrow the TcpTransportConfig.
struct TcpConnectionAttempt<'c> {
    address: SocketAddr,
    config: &'c TcpTransportConfig,
}

impl TcpConnectionAttempt<'_> {
    /// Make a single connection attempt.
    async fn connect(self) -> Result<TcpStream, TcpConnectionError> {
        let connect = connect(&self.address, self.config.connect_timeout, self.config)?;
        connect.await
    }
}

impl<'c> TcpConnectionAttempt<'c> {
    fn new(address: SocketAddr, config: &'c TcpTransportConfig) -> Self {
        Self { address, config }
    }
}

/// Error type for TCP connections.
#[derive(Debug, Error)]
pub struct TcpConnectionError {
    message: String,
    #[source]
    source: Option<BoxError>,
}

impl TcpConnectionError {
    #[allow(dead_code)]
    pub(super) fn new<S>(message: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            message: message.into(),
            source: None,
        }
    }

    pub(super) fn msg<S, E>(message: S) -> impl FnOnce(E) -> Self
    where
        S: Into<String>,
        E: std::error::Error + Send + Sync + 'static,
    {
        move |error| Self {
            message: message.into(),
            source: Some(error.into()),
        }
    }

    pub(super) fn build<S, E>(message: S, error: E) -> Self
    where
        S: Into<String>,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            message: message.into(),
            source: Some(Box::new(error)),
        }
    }
}

impl From<JoinError> for TcpConnectionError {
    fn from(value: JoinError) -> Self {
        Self::build("tcp connection panic", value)
    }
}

impl fmt::Display for TcpConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref source) = self.source {
            write!(f, "{}: {}", self.message, source)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl From<TcpConnectionError> for io::Error {
    fn from(value: TcpConnectionError) -> Self {
        if let Some(original) = value
            .source
            .as_ref()
            .and_then(|r| r.downcast_ref::<io::Error>())
        {
            io::Error::new(original.kind(), original.to_string())
        } else {
            io::Error::other(value)
        }
    }
}

/// Configuration for TCP connections.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TcpTransportConfig {
    /// The timeout for connecting to a remote address.
    pub connect_timeout: Option<Duration>,

    /// The timeout for keep-alive connections.
    pub keep_alive_timeout: Option<Duration>,

    /// The timeout for happy eyeballs algorithm.
    pub happy_eyeballs_timeout: Option<Duration>,

    /// The number of concurrent connection atttempts to make.
    pub happy_eyeballs_concurrency: Option<usize>,

    /// The local IPv4 address to bind to.
    pub local_address_ipv4: Option<Ipv4Addr>,

    /// The local IPv6 address to bind to.
    pub local_address_ipv6: Option<Ipv6Addr>,

    /// Whether to disable Nagle's algorithm.
    pub nodelay: bool,

    /// Whether to reuse the local address.
    pub reuse_address: bool,

    /// The size of the send buffer.
    pub send_buffer_size: Option<usize>,

    /// The size of the receive buffer.
    pub recv_buffer_size: Option<usize>,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Some(Duration::from_secs(10)),
            keep_alive_timeout: Some(Duration::from_secs(90)),
            happy_eyeballs_timeout: Some(Duration::from_secs(30)),
            happy_eyeballs_concurrency: Some(2),
            local_address_ipv4: None,
            local_address_ipv6: None,
            nodelay: true,
            reuse_address: true,
            send_buffer_size: None,
            recv_buffer_size: None,
        }
    }
}

impl TcpTransportConfig {
    /// Connect via TCP using the happy eyeballs algorithm
    ///
    /// This uses the happy eyeballs algorithm to connect to the first address
    /// that succeeds, prefering the IP address version which matches the socket
    /// binding address when provided.
    pub async fn connect(&self, mut addrs: SocketAddrs) -> Result<TcpStream, TcpConnectionError> {
        if self.happy_eyeballs_timeout.is_some() {
            addrs.sort_preferred(IpVersion::from_binding(
                self.local_address_ipv4,
                self.local_address_ipv6,
            ));
        }

        let delay = if addrs.is_empty() {
            self.happy_eyeballs_timeout
        } else {
            self.happy_eyeballs_timeout
                .map(|duration| duration / (addrs.len()) as u32)
        };

        tracing::trace!(?delay, timeout=?self.happy_eyeballs_timeout, "happy eyeballs");
        let mut attempts = EyeballSet::new(
            delay,
            self.happy_eyeballs_timeout,
            self.happy_eyeballs_concurrency,
        );

        while let Some(address) = addrs.pop() {
            let span: tracing::Span = tracing::trace_span!("connect", %address);
            let attempt = TcpConnectionAttempt::new(address, self);
            attempts.push(async { attempt.connect().instrument(span).await });
        }

        tracing::trace!("Starting {} connection attempts", attempts.len());

        attempts.finish().await.map_err(|err| match err {
            HappyEyeballsError::Error(err) => err,
            HappyEyeballsError::Timeout(elapsed) => {
                tracing::trace!("tcp timed out after {}ms", elapsed.as_millis());
                TcpConnectionError::new(format!(
                    "Connection attempts timed out after {}ms",
                    elapsed.as_millis()
                ))
            }
            HappyEyeballsError::NoProgress => {
                tracing::trace!("tcp exhausted connection candidates");
                TcpConnectionError::new("Exhausted connection candidates")
            }
        })
    }

    /// Connect to a single address.
    ///
    /// Prefer the [TcpTransportConfig::connect] method when you have multiple addresses available, as they can be raced.
    pub async fn connect_to_addr(&self, addr: SocketAddr) -> Result<TcpStream, TcpConnectionError> {
        let connect = connect(&addr, self.connect_timeout, self)?;
        connect
            .await
            .map_err(TcpConnectionError::msg("tcp connect error"))
    }
}

/// A simple TCP transport that uses a single connection attempt.
pub struct SimpleTcpTransport<R> {
    resolver: R,
    config: Arc<TcpTransportConfig>,
}

impl<R> fmt::Debug for SimpleTcpTransport<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleTcpTransport").finish()
    }
}

impl<R: Clone> Clone for SimpleTcpTransport<R> {
    fn clone(&self) -> Self {
        Self {
            resolver: self.resolver.clone(),
            config: self.config.clone(),
        }
    }
}

impl<R> SimpleTcpTransport<R> {
    /// Create a new simple TCP transport with the given configuration and resolver.
    pub fn new(resolver: R, config: TcpTransportConfig) -> Self {
        Self {
            resolver,
            config: Arc::new(config),
        }
    }
}

impl<R, Request> tower::Service<Request> for SimpleTcpTransport<R>
where
    R: TcpResolver<Request> + Clone + Send + 'static,
    R::Error: std::error::Error + Send + Sync + 'static,
    R::Future: Send + 'static,
{
    type Response = TcpStream;
    type Error = TcpConnectionError;
    type Future = BoxFuture<'static, Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let transport = self.config.clone();

        let span = tracing::trace_span!(
            "tcp",
            ip = tracing::field::Empty,
            port = tracing::field::Empty
        );
        let resolve = self.resolver.resolve(req);

        let span_handle = span.clone();
        async move {
            let mut addrs = resolve
                .await
                .map_err(TcpConnectionError::msg("resolving"))?;

            let addr = addrs
                .pop()
                .ok_or_else(|| TcpConnectionError::new("No address found"))?;

            span_handle.record("ip", addr.ip().to_string());
            span_handle.record("port", addr.port());

            let stream = transport.connect_to_addr(addr).await?;

            if let Ok(peer_addr) = stream.peer_addr() {
                trace!(peer.addr = %peer_addr, "tcp connected");
            } else {
                trace!("tcp connected");
                trace!("no peer address available");
            }

            Ok(stream)
        }
        .instrument(span)
        .boxed()
    }
}

fn bind_local_address(
    socket: &socket2::Socket,
    dst_addr: &SocketAddr,
    local_addr_ipv4: &Option<Ipv4Addr>,
    local_addr_ipv6: &Option<Ipv6Addr>,
) -> io::Result<()> {
    match (*dst_addr, local_addr_ipv4, local_addr_ipv6) {
        (SocketAddr::V4(_), Some(addr), _) => {
            socket.bind(&SocketAddr::new((*addr).into(), 0).into())?;
        }
        (SocketAddr::V6(_), _, Some(addr)) => {
            socket.bind(&SocketAddr::new((*addr).into(), 0).into())?;
        }
        _ => {}
    }

    Ok(())
}

pub(crate) fn connect(
    addr: &SocketAddr,
    connect_timeout: Option<Duration>,
    config: &TcpTransportConfig,
) -> Result<impl Future<Output = Result<TcpStream, TcpConnectionError>>, TcpConnectionError> {
    use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};

    let domain = Domain::for_address(*addr);
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(TcpConnectionError::msg("tcp open error"))?;
    tracing::trace!("tcp socket opened");

    let socket = tracing::trace_span!("socket::options").in_scope(|| {
        // When constructing a Tokio `TcpSocket` from a raw fd/socket, the user is
        // responsible for ensuring O_NONBLOCK is set.
        socket
            .set_nonblocking(true)
            .map_err(TcpConnectionError::msg("tcp set_nonblocking error"))?;

        if let Some(dur) = config.keep_alive_timeout {
            let conf = TcpKeepalive::new().with_time(dur);
            if let Err(e) = socket.set_tcp_keepalive(&conf) {
                warn!("tcp set_keepalive error: {}", e);
            }
        }

        bind_local_address(
            &socket,
            addr,
            &config.local_address_ipv4,
            &config.local_address_ipv6,
        )
        .map_err(TcpConnectionError::msg("tcp bind local address"))?;

        #[allow(unsafe_code)]
        let socket = unsafe {
            // Safety: `from_raw_fd` is only safe to call if ownership of the raw
            // file descriptor is transferred. Since we call `into_raw_fd` on the
            // socket2 socket, it gives up ownership of the fd and will not close
            // it, so this is safe.
            use std::os::unix::io::{FromRawFd, IntoRawFd};
            TcpSocket::from_raw_fd(socket.into_raw_fd())
        };

        if config.reuse_address {
            if let Err(e) = socket.set_reuseaddr(true) {
                warn!("tcp set_reuse_address error: {}", e);
            }
        }

        if let Some(size) = config.send_buffer_size {
            if let Err(e) = socket.set_send_buffer_size(size.try_into().unwrap_or(u32::MAX)) {
                warn!("tcp set_buffer_size error: {}", e);
            }
        }

        if let Some(size) = config.recv_buffer_size {
            if let Err(e) = socket.set_recv_buffer_size(size.try_into().unwrap_or(u32::MAX)) {
                warn!("tcp set_recv_buffer_size error: {}", e);
            }
        }
        Ok::<_, TcpConnectionError>(socket)
    })?;

    let span = tracing::trace_span!("socket::connect", remote.addr = %addr);
    let connect = socket.connect(*addr).instrument(span);
    Ok(async move {
        match connect_timeout {
            Some(dur) => match tokio::time::timeout(dur, connect).await {
                Ok(Ok(s)) => Ok(TcpStream::client(s)),
                Ok(Err(e)) => Err(e),
                Err(e) => {
                    tracing::trace!(timeout=?dur, "connection timed out");
                    Err(io::Error::new(io::ErrorKind::TimedOut, e))
                }
            },
            None => connect.await.map(TcpStream::client),
        }
        .map_err(TcpConnectionError::msg("tcp connect error"))
    })
}

#[cfg(test)]
mod test {

    use std::convert::Infallible;

    use tokio::net::TcpListener;
    use tower::{Service, ServiceExt as _};

    use crate::info::HasConnectionInfo as _;

    use super::*;

    #[derive(Debug, Clone)]
    struct MockResolver<A> {
        addr: A,
    }

    impl<A> MockResolver<A> {
        fn new(addr: A) -> Self {
            Self { addr }
        }
    }

    impl<A, R> tower::Service<R> for MockResolver<A>
    where
        A: Clone,
    {
        type Response = A;

        type Error = Infallible;

        type Future = std::future::Ready<Result<A, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: R) -> Self::Future {
            std::future::ready(Ok(self.addr.clone()))
        }
    }

    async fn connect_transport<T>(transport: T, listener: TcpListener) -> (TcpStream, TcpStream)
    where
        T: Service<(), Response = TcpStream>,
        <T as Service<()>>::Error: std::fmt::Debug,
    {
        tokio::join!(async { transport.oneshot(()).await.unwrap() }, async {
            let (stream, addr) = listener.accept().await.unwrap();
            TcpStream::server(stream, addr)
        })
    }

    #[tokio::test]
    async fn test_transport() {
        let _ = tracing_subscriber::fmt::try_init();

        let bind = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = bind.local_addr().unwrap().port();

        let config = TcpTransportConfig::default();

        let transport = TcpTransport::new(
            MockResolver::new(SocketAddrs::from(bind.local_addr().unwrap())),
            config.into(),
        );

        let (stream, _) = connect_transport(transport, bind).await;

        let info = stream.info();
        assert_eq!(
            *info.remote_addr(),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)
        );
    }

    #[tokio::test]
    async fn test_transport_connect_to_addrs() {
        let _ = tracing_subscriber::fmt::try_init();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let addrs = vec![
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port + 1),
        ];

        let resolver = MockResolver::new(SocketAddrs::from_iter(addrs.into_iter()));

        let config = TcpTransportConfig::default();
        let transport = TcpTransport::new(resolver, config.into());

        let (conn, _): (TcpStream, TcpStream) = connect_transport(transport, listener).await;

        let info = conn.info();
        assert_eq!(
            *info.remote_addr(),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)
        );
    }

    #[tokio::test]
    async fn test_simple_transport() {
        let _ = tracing_subscriber::fmt::try_init();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let config = TcpTransportConfig::default();
        let transport = SimpleTcpTransport::new(
            MockResolver::new(SocketAddrs::from(listener.local_addr().unwrap())),
            config,
        );

        let (conn, _) = connect_transport(transport, listener).await;

        let info = conn.info();
        assert_eq!(
            *info.remote_addr(),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)
        );
    }
}
