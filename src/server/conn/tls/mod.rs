//! Implementation of a server side TLS stream on top of tokio_rustls
//! which additionally provides connection information after the
//! handshake has been completed.

use std::task::{Context, Poll, ready};
use std::{fmt, io};
use std::{future::Future, pin::Pin};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::Accept;

use crate::info::tls::{TlsConnectionInfoReceiver, TlsConnectionInfoSender, channel};
use crate::info::{ConnectionInfo, HasConnectionInfo, TlsConnectionInfo};
use crate::stream::tls::{TlsHandshakeInfo, TlsHandshakeStream};

pub mod acceptor;
pub mod info;

pub use self::acceptor::TlsAcceptor;
pub use self::info::TlsConnectionInfoLayer;

/// State tracks the process of accepting a connection and turning it into a stream.
enum TlsState<IO> {
    Handshake(tokio_rustls::Accept<IO>),
    Streaming(tokio_rustls::server::TlsStream<IO>),
}

impl<IO> fmt::Debug for TlsState<IO> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlsState::Handshake(_) => f.write_str("State::Handshake"),
            TlsState::Streaming(_) => f.write_str("State::Streaming"),
        }
    }
}

/// A TLS stream, generic over the underlying IO.
#[derive(Debug)]
pub struct TlsStream<IO>
where
    IO: HasConnectionInfo,
{
    state: TlsState<IO>,
    tx: TlsConnectionInfoSender,
    pub(crate) rx: TlsConnectionInfoReceiver,
}

impl<IO> TlsHandshakeStream for TlsStream<IO>
where
    IO: HasConnectionInfo + AsyncRead + AsyncWrite + Send + Unpin,
    IO::Addr: Unpin,
{
    fn poll_handshake(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.handshake(cx, |_, _| Poll::Ready(Ok(())))
    }
}

impl<IO> TlsHandshakeInfo for TlsStream<IO>
where
    IO: HasConnectionInfo + AsyncRead + AsyncWrite + Send + Unpin,
    IO::Addr: Unpin,
{
    fn recv(&self) -> TlsConnectionInfoReceiver {
        self.rx.clone()
    }
}

impl<IO> TlsStream<IO>
where
    IO: HasConnectionInfo + AsyncRead + AsyncWrite + Unpin,
{
    fn handshake<F, R>(&mut self, cx: &mut Context, action: F) -> Poll<io::Result<R>>
    where
        F: FnOnce(&mut tokio_rustls::server::TlsStream<IO>, &mut Context) -> Poll<io::Result<R>>,
    {
        match self.state {
            TlsState::Handshake(ref mut accept) => match ready!(Pin::new(accept).poll(cx)) {
                Ok(mut stream) => {
                    // Take some action here when the handshake happens

                    let (io, server_info) = stream.get_ref();
                    let info = TlsConnectionInfo::server(server_info);
                    self.tx.send(info.clone());

                    let host = info.server_name.as_deref().unwrap_or("-");
                    tracing::trace!(local=%io.info().local_addr(), remote=%io.info().remote_addr(), %host,  "TLS Handshake complete");

                    // Back to processing the stream
                    let result = action(&mut stream, cx);
                    self.state = TlsState::Streaming(stream);
                    result
                }
                Err(err) => Poll::Ready(Err(err)),
            },
            TlsState::Streaming(ref mut stream) => action(stream, cx),
        }
    }
}

impl<IO> TlsStream<IO>
where
    IO: HasConnectionInfo,
    IO::Addr: Clone,
{
    /// Create a new `TlsStream` from an `Accept` future.
    ///
    /// This adapts the underlying `rustls` stream to provide connection information.
    pub fn new(accept: Accept<IO>) -> Self {
        let (tx, rx) = channel();

        Self {
            state: TlsState::Handshake(accept),
            tx,
            rx,
        }
    }
}

impl<IO> HasConnectionInfo for TlsStream<IO>
where
    IO: HasConnectionInfo,
    IO::Addr: Clone,
{
    type Addr = IO::Addr;

    fn info(&self) -> ConnectionInfo<Self::Addr> {
        match &self.state {
            TlsState::Handshake(a) => a
                .get_ref()
                .map(|io| io.info())
                .expect("connection info available without tls handshake"),
            TlsState::Streaming(s) => s.get_ref().0.info(),
        }
    }
}

impl<IO> AsyncRead for TlsStream<IO>
where
    IO: HasConnectionInfo + AsyncRead + AsyncWrite + Unpin,
    IO::Addr: Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        let pin = self.get_mut();
        pin.handshake(cx, |stream, cx| Pin::new(stream).poll_read(cx, buf))
    }
}

impl<IO> AsyncWrite for TlsStream<IO>
where
    IO: HasConnectionInfo + AsyncRead + AsyncWrite + Unpin,
    IO::Addr: Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let pin = self.get_mut();
        pin.handshake(cx, |stream, cx| Pin::new(stream).poll_write(cx, buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.state {
            TlsState::Handshake(_) => Poll::Ready(Ok(())),
            TlsState::Streaming(ref mut stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.state {
            TlsState::Handshake(_) => Poll::Ready(Ok(())),
            TlsState::Streaming(ref mut stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
