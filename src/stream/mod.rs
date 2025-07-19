//! Network stream abstractions and implementations.
//!
//! This module provides unified abstractions for working with different types of network streams,
//! including TCP, UDP, and TLS connections. Each stream type implements common traits for
//! asynchronous I/O while providing specialized functionality for their respective protocols.
//!
//! # Modules
//!
//! - [`tcp`] - TCP stream wrapper with improved address semantics for servers
//! - [`tls`] - TLS handshake support and optional TLS stream dispatching
//! - [`udp`] - UDP message-based abstractions compatible with tower services
//!
//! # Examples
//!
//! ## TCP Streams
//! ```rust,no_run
//! use chateau::stream::tcp::TcpStream;
//! use tokio::net::TcpListener;
//!
//! # async fn example() -> std::io::Result<()> {
//! let listener = TcpListener::bind("127.0.0.1:8080").await?;
//! let (stream, addr) = listener.accept().await?;
//! let tcp_stream = TcpStream::server(stream, addr);
//!
//! // Server streams retain the remote address even when
//! // the underlying socket can't provide it
//! let remote = tcp_stream.peer_addr()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## UDP Connections
//! ```rust,no_run
//! use chateau::stream::udp::{UdpListener, UdpMessage};
//! use tokio::net::UdpSocket;
//!
//! # async fn example() -> std::io::Result<()> {
//! let socket = UdpSocket::bind("127.0.0.1:8080").await?;
//! let listener = UdpListener::new(socket, 1024, 16);
//!
//! // UDP listener provides connection-like semantics
//! // for message-based protocol
//! # Ok(())
//! # }
//! ```

pub mod tcp;
#[cfg(feature = "tls")]
pub mod tls;
pub mod udp;
