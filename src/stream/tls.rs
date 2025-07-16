//! Support for braided streams which include Transport Layer security
//! and so involve a negotiation component.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

pub use crate::info::TlsConnectionInfo;
use crate::info::tls::TlsConnectionInfoReceiver;
use futures_core::Future;
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};

/// Future returned by `TlsHandshakeStream::finish_handshake`.
///
/// This future resolves when the handshake is complete.
#[pin_project]
#[derive(Debug)]
pub struct Handshaking<'a, T: ?Sized> {
    inner: &'a mut T,
}

impl<'a, T: ?Sized> Handshaking<'a, T> {
    fn new(inner: &'a mut T) -> Self {
        Handshaking { inner }
    }
}

impl<T> Future for Handshaking<'_, T>
where
    T: TlsHandshakeStream + ?Sized,
{
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll_handshake(cx)
    }
}

/// A stream that supports a TLS handshake.
pub trait TlsHandshakeStream {
    /// Poll the handshake to completion.
    fn poll_handshake(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>>;

    /// Finish the TLS handshake.
    ///
    /// This method will drive the connection asynchronosly allowing you to wait
    /// for the TLS handshake to complete. If this method is not called, the TLS handshake
    /// will be completed the first time the connection is used.
    fn finish_handshake(&mut self) -> Handshaking<'_, Self> {
        Handshaking::new(self)
    }
}

/// A stream which can provide information about the TLS handshake.
#[cfg(feature = "server")]
pub(crate) trait TlsHandshakeInfo: TlsHandshakeStream {
    fn recv(&self) -> TlsConnectionInfoReceiver;
}

/// Dispatching wrapper for optionally supporting TLS
#[derive(Debug)]
#[pin_project(project=BraidProjection)]
pub enum OptTlsStream<Tls, NoTls> {
    /// A stream without TLS
    NoTls(#[pin] NoTls),

    /// A stream with TLS
    Tls(#[pin] Tls),
}

impl<Tls, NoTls> TlsHandshakeStream for OptTlsStream<Tls, NoTls>
where
    Tls: TlsHandshakeStream + Unpin,
    NoTls: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_handshake(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self {
            OptTlsStream::NoTls(_) => Poll::Ready(Ok(())),
            OptTlsStream::Tls(stream) => stream.poll_handshake(cx),
        }
    }
}

#[cfg(feature = "server")]
impl<Tls, NoTls> TlsHandshakeInfo for OptTlsStream<Tls, NoTls>
where
    Tls: TlsHandshakeInfo + Unpin,
    NoTls: AsyncRead + AsyncWrite + Send + Unpin,
{
    fn recv(&self) -> TlsConnectionInfoReceiver {
        match self {
            OptTlsStream::NoTls(_) => TlsConnectionInfoReceiver::empty(),
            OptTlsStream::Tls(stream) => stream.recv(),
        }
    }
}

macro_rules! dispatch {
    ($driver:ident.$method:ident($($args:expr),+)) => {

        match $driver.project() {
            BraidProjection::NoTls(stream) => stream.$method($($args),+),
            BraidProjection::Tls(stream) => stream.$method($($args),+),
        }
    };
}

impl<Tls, NoTls> AsyncRead for OptTlsStream<Tls, NoTls>
where
    Tls: AsyncRead,
    NoTls: AsyncRead,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        dispatch!(self.poll_read(cx, buf))
    }
}

impl<Tls, NoTls> AsyncWrite for OptTlsStream<Tls, NoTls>
where
    Tls: AsyncWrite,
    NoTls: AsyncWrite,
{
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        dispatch!(self.poll_write(cx, buf))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        dispatch!(self.poll_flush(cx))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        dispatch!(self.poll_shutdown(cx))
    }
}

impl<Tls, NoTls> From<NoTls> for OptTlsStream<Tls, NoTls> {
    fn from(stream: NoTls) -> Self {
        Self::NoTls(stream)
    }
}

/// Extension trait for `TlsHandshakeStream`.
pub trait TlsHandshakeExt: TlsHandshakeStream {
    /// Perform a TLS handshake.
    fn handshake(&mut self) -> TlsHandshakeFuture<'_, Self>
    where
        Self: Sized,
    {
        TlsHandshakeFuture::new(self)
    }
}

impl<T> TlsHandshakeExt for T where T: TlsHandshakeStream {}

/// A future that resolves once the TLS handshake is complete.
#[derive(Debug)]
#[pin_project]
pub struct TlsHandshakeFuture<'s, S> {
    stream: &'s mut S,
}

impl<'s, S> TlsHandshakeFuture<'s, S> {
    fn new(stream: &'s mut S) -> Self {
        Self { stream }
    }
}

impl<S> Future for TlsHandshakeFuture<'_, S>
where
    S: TlsHandshakeStream,
{
    type Output = Result<(), io::Error>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().stream.poll_handshake(cx)
    }
}
