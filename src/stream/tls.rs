//! TLS (Transport Layer Security) stream support and handshake management.
//!
//! This module provides abstractions for working with TLS-enabled streams that require
//! a handshake negotiation phase. It includes support for both mandatory and optional
//! TLS configurations, allowing streams to gracefully handle both encrypted and
//! unencrypted connections.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, io};

pub use crate::info::TlsConnectionInfo;
#[cfg(feature = "server")]
use crate::info::tls::TlsConnectionInfoReceiver;
use crate::info::{ConnectionInfo, HasConnectionInfo, HasTlsConnectionInfo};
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
///
/// This trait provides the core functionality for managing TLS handshakes
/// on streams. Implementations should handle the TLS negotiation protocol
/// and provide polling-based handshake completion.
pub trait TlsHandshakeStream {
    /// Poll the handshake to completion.
    ///
    /// This method should be called repeatedly until it returns `Poll::Ready(Ok(()))`
    /// indicating the handshake is complete, or `Poll::Ready(Err(_))` if the handshake failed.
    /// Returns `Poll::Pending` if the handshake is still in progress.
    fn poll_handshake(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>>;

    /// Finish the TLS handshake.
    ///
    /// This method will drive the connection asynchronously, allowing you to wait
    /// for the TLS handshake to complete. If this method is not called, the TLS handshake
    /// will be completed the first time the connection is used for I/O operations.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use chateau::stream::tls::TlsHandshakeStream;
    ///
    /// # async fn example<T: TlsHandshakeStream>(mut stream: T) -> std::io::Result<()> {
    /// // Explicitly complete the handshake before doing any I/O
    /// stream.finish_handshake().await?;
    /// # Ok(())
    /// # }
    /// ```
    fn finish_handshake(&mut self) -> Handshaking<'_, Self> {
        Handshaking::new(self)
    }
}

#[cfg(feature = "server")]
/// A stream which can provide information about the TLS handshake.
pub(crate) trait TlsHandshakeInfo: TlsHandshakeStream {
    fn recv(&self) -> TlsConnectionInfoReceiver;
}

/// Dispatching wrapper for optionally supporting TLS.
///
/// This enum allows code to work with streams that may or may not use TLS,
/// providing a unified interface for both cases. The `Tls` variant contains
/// a TLS-enabled stream, while `NoTls` contains a plain stream.
///
/// All async I/O operations are automatically dispatched to the appropriate
/// underlying stream type.
#[derive(Debug)]
#[pin_project(project=OptTlsProjection)]
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

impl<Tls, NoTls, A> HasConnectionInfo for OptTlsStream<Tls, NoTls>
where
    A: fmt::Debug + fmt::Display + Send + 'static,
    Tls: HasConnectionInfo<Addr = A>,
    NoTls: HasConnectionInfo<Addr = A>,
{
    type Addr = A;
    fn info(&self) -> ConnectionInfo<A> {
        match self {
            OptTlsStream::NoTls(stream) => stream.info(),
            OptTlsStream::Tls(stream) => stream.info(),
        }
    }
}

impl<Tls, NoTls, A> HasTlsConnectionInfo for OptTlsStream<Tls, NoTls>
where
    A: fmt::Debug + fmt::Display + Send + 'static,
    Tls: HasConnectionInfo<Addr = A> + HasTlsConnectionInfo,
    NoTls: HasConnectionInfo<Addr = A>,
{
    fn tls_info(&self) -> Option<&TlsConnectionInfo> {
        match self {
            OptTlsStream::NoTls(_) => None,
            OptTlsStream::Tls(stream) => stream.tls_info(),
        }
    }
}

macro_rules! dispatch {
    ($driver:ident.$method:ident($($args:expr),+)) => {

        match $driver.project() {
            OptTlsProjection::NoTls(stream) => stream.$method($($args),+),
            OptTlsProjection::Tls(stream) => stream.$method($($args),+),
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
