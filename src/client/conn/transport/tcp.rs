//! TCP transport implementation for client connections.
//!
//! This module contains the [`TcpTransport`] type, which is a [`tower::Service`] that connects to
//! remote addresses using TCP. It also contains the [`TcpTransportConfig`] type, which is used to
//! configure TCP connections.
//!
//! Normally, you will not need to use this module directly. Instead, you can use the [`Client`][crate::client::Client]
//! type from the [`client`][crate::client] module, which uses the [`TcpTransport`] internally by default.
//!
//! See [`Client::build_tcp_http`][crate::client::Client::build_tcp_http] for the default constructor which uses the TCP transport.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::future::FutureExt as _;
use thiserror::Error;
use tokio::net::TcpSocket;
use tokio::task::JoinError;
use tracing::{Instrument, trace, warn};

use crate::BoxError;
use crate::client::conn::dns::{IpVersion, SocketAddrs};
use crate::happy_eyeballs::{EyeballSet, HappyEyeballsError};
use crate::stream::tcp::TcpStream;

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
pub struct TcpTransport {
    config: Arc<TcpTransportConfig>,
}

impl Clone for TcpTransport {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
        }
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        TcpTransport::new(TcpTransportConfig::default().into())
    }
}

impl TcpTransport {
    /// Create a new TCP transport
    pub fn new(config: Arc<TcpTransportConfig>) -> Self {
        Self { config }
    }
}

impl TcpTransport {
    /// Get the configuration for the TCP connector.
    pub fn config(&self) -> &TcpTransportConfig {
        &self.config
    }
}

type BoxFuture<'a, T, E> = crate::BoxFuture<'a, Result<T, E>>;

impl tower::Service<SocketAddrs> for TcpTransport {
    type Response = TcpStream;
    type Error = TcpConnectionError;
    type Future = BoxFuture<'static, Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: SocketAddrs) -> Self::Future {
        let transport = std::mem::replace(self, self.clone());

        let span = tracing::trace_span!("tcp");

        Box::pin(
            async move {
                let stream = transport.connecting(req).connect().await?;

                if let Ok(peer_addr) = stream.peer_addr() {
                    trace!(peer.addr = %peer_addr, "tcp connected");
                } else {
                    trace!("tcp connected");
                }

                let stream = stream.into();

                Ok(stream)
            }
            .instrument(span),
        )
    }
}

impl tower::Service<SocketAddr> for TcpTransport {
    type Response = <Self as tower::Service<SocketAddrs>>::Response;
    type Error = <Self as tower::Service<SocketAddrs>>::Error;
    type Future = <Self as tower::Service<SocketAddrs>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as tower::Service<SocketAddrs>>::poll_ready(self, cx)
    }

    fn call(&mut self, req: SocketAddr) -> Self::Future {
        let addrs = SocketAddrs::from(req);
        <Self as tower::Service<SocketAddrs>>::call(self, addrs)
    }
}

impl TcpTransport {
    /// Create a new `TcpConnecting` future.
    fn connecting(&self, mut addrs: SocketAddrs) -> TcpConnecting<'_> {
        if self.config.happy_eyeballs_timeout.is_some() {
            addrs.sort_preferred(IpVersion::from_binding(
                self.config.local_address_ipv4,
                self.config.local_address_ipv6,
            ));
        }

        TcpConnecting::new(addrs, &self.config)
    }
}

/// Establish a TCP connection to a set of addresses with a given config.
///
/// This is a low-level method which allows for library-level re-use of things like the happy-eyeballs algorithm
/// and connection attempt management.
pub async fn connect_to_addrs<A>(
    config: &TcpTransportConfig,
    addrs: A,
) -> Result<TcpStream, TcpConnectionError>
where
    A: IntoIterator<Item = SocketAddr>,
{
    let mut addrs = SocketAddrs::from_iter(addrs);
    if config.happy_eyeballs_timeout.is_some() {
        addrs.sort_preferred(IpVersion::from_binding(
            config.local_address_ipv4,
            config.local_address_ipv6,
        ));
    }

    let connecting = TcpConnecting::new(addrs, config);
    connecting.connect().await
}

/// Future which implements the happy eyeballs algorithm for connecting to a remote address.
///
/// This follows the algorithm described in [RFC8305](https://tools.ietf.org/html/rfc8305),
/// which allows for faster connection times by trying multiple addresses in parallel,
/// regardless of whether they are IPv4 or IPv6.
pub(crate) struct TcpConnecting<'c> {
    addresses: SocketAddrs,
    config: &'c TcpTransportConfig,
}

impl<'c> TcpConnecting<'c> {
    /// Create a new `TcpConnecting` future.
    pub(crate) fn new(addresses: SocketAddrs, config: &'c TcpTransportConfig) -> Self {
        Self { addresses, config }
    }

    /// Connect to the remote address using the happy eyeballs algorithm.
    async fn connect(mut self) -> Result<TcpStream, TcpConnectionError> {
        let delay = if self.addresses.is_empty() {
            self.config.happy_eyeballs_timeout
        } else {
            self.config
                .happy_eyeballs_timeout
                .map(|duration| duration / (self.addresses.len()) as u32)
        };

        tracing::trace!(?delay, timeout=?self.config.happy_eyeballs_timeout, "happy eyeballs");
        let mut attempts = EyeballSet::new(
            delay,
            self.config.happy_eyeballs_timeout,
            self.config.happy_eyeballs_concurrency,
        );

        while let Some(address) = self.addresses.pop() {
            let span: tracing::Span = tracing::trace_span!("connect", %address);
            let attempt = TcpConnectionAttempt::new(address, self.config);
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

/// A simple TCP transport that uses a single connection attempt.
pub struct SimpleTcpTransport {
    config: Arc<TcpTransportConfig>,
}

impl fmt::Debug for SimpleTcpTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleTcpTransport").finish()
    }
}

impl Clone for SimpleTcpTransport {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
        }
    }
}

impl SimpleTcpTransport {
    /// Create a new simple TCP transport with the given configuration and resolver.
    pub fn new(config: TcpTransportConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl SimpleTcpTransport {
    async fn connect_to_addr(&self, addr: SocketAddr) -> Result<TcpStream, TcpConnectionError> {
        let connect = connect(&addr, self.config.connect_timeout, &self.config)?;
        connect
            .await
            .map_err(TcpConnectionError::msg("tcp connect error"))
    }
}

impl tower::Service<SocketAddr> for SimpleTcpTransport {
    type Response = TcpStream;
    type Error = TcpConnectionError;
    type Future = BoxFuture<'static, Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: SocketAddr) -> Self::Future {
        let transport = std::mem::replace(self, self.clone());

        let span = tracing::trace_span!("tcp", ip = %req.ip(), port = %req.port());

        async move {
            let stream = transport.connect_to_addr(req).await?;

            if let Ok(peer_addr) = stream.peer_addr() {
                trace!(peer.addr = %peer_addr, "tcp connected");
            } else {
                trace!("tcp connected");
                trace!("no peer address available");
            }

            let stream = stream.into();

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

    let guard = tracing::trace_span!("socket::options").entered();

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

    drop(guard);

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

    use tokio::net::TcpListener;
    use tower::{Service, ServiceExt as _};

    use crate::info::HasConnectionInfo as _;

    use super::*;

    async fn connect_transport<T, R>(
        addr: R,
        transport: T,
        listener: TcpListener,
    ) -> (TcpStream, TcpStream)
    where
        T: Service<R, Response = TcpStream>,
        <T as Service<R>>::Error: std::fmt::Debug,
    {
        tokio::join!(
            async { transport.oneshot(addr.into()).await.unwrap() },
            async {
                let (stream, addr) = listener.accept().await.unwrap();
                TcpStream::server(stream, addr)
            }
        )
    }

    #[tokio::test]
    async fn test_transport() {
        let _ = tracing_subscriber::fmt::try_init();

        let bind = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = bind.local_addr().unwrap().port();

        let config = TcpTransportConfig::default();

        let transport = TcpTransport::new(config.into());

        let (stream, _) = connect_transport(
            SocketAddrs::from(bind.local_addr().unwrap()),
            transport,
            bind,
        )
        .await;

        let info = stream.info();
        assert_eq!(
            *info.remote_addr(),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)
        );
    }

    #[tokio::test]
    async fn test_transport_connect_to_addrs() {
        let _ = tracing_subscriber::fmt::try_init();

        let bind = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = bind.local_addr().unwrap().port();

        let config = TcpTransportConfig::default();
        let transport = TcpTransport::new(config.into());

        let addrs = vec![
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port + 1),
        ];

        let (conn, _): (TcpStream, TcpStream) = tokio::join!(
            async {
                transport
                    .oneshot(SocketAddrs::from_iter(addrs.into_iter()))
                    .await
                    .unwrap()
            },
            async {
                let (stream, addr) = bind.accept().await.unwrap();
                TcpStream::server(stream, addr)
            }
        );

        let info = conn.info();
        assert_eq!(
            *info.remote_addr(),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)
        );
    }

    #[tokio::test]
    async fn test_simple_transport() {
        let _ = tracing_subscriber::fmt::try_init();

        let bind = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = bind.local_addr().unwrap().port();

        let config = TcpTransportConfig::default();
        let transport = SimpleTcpTransport::new(config);

        let (conn, _) = connect_transport(bind.local_addr().unwrap(), transport, bind).await;

        let info = conn.info();
        assert_eq!(
            *info.remote_addr(),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)
        );
    }
}
