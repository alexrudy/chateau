//! User datagram protocol support

mod message;

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::BytesMut;
use tokio::net::UdpSocket;
use tracing::trace;

pub use self::message::UdpMessage;
use crate::server::Accept;

/// A listener for a UDP socket to match the Accept trait.
#[derive(Debug)]
pub struct UdpListener {
    socket: Arc<tokio::net::UdpSocket>,
    recv_buffer: BytesMut,
    max_recv_buffer: usize,
}

impl UdpListener {
    /// Create a new UDP listener
    pub fn new(socket: tokio::net::UdpSocket, recv_buffer_size: usize) -> Self {
        Self {
            socket: Arc::new(socket),
            recv_buffer: BytesMut::with_capacity(recv_buffer_size),
            max_recv_buffer: recv_buffer_size,
        }
    }
}

/// A UDP "Connection", consisting of one inbound message and a responder to send
/// an unlimited number of responses.
#[derive(Debug)]
pub struct UdpConnection {
    message: Option<UdpMessage>,
    addr: SocketAddr,
    socket: Arc<UdpSocket>,
}

impl UdpConnection {
    /// Take the message that was recieved to start this "connection"
    pub fn take(&mut self) -> Option<UdpMessage> {
        self.message.take()
    }

    /// Access the socket from the connection for socket methods.
    pub fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    /// Send a datagram to the address from the inbound message.
    pub async fn send(&self, data: &[u8]) -> Result<(), io::Error> {
        self.socket.send_to(data, self.addr).await.map(|_| ())
    }

    /// Send a datagram to a specific address
    pub async fn send_to(&self, data: &[u8], addr: SocketAddr) -> Result<(), io::Error> {
        self.socket.send_to(data, addr).await.map(|_| ())
    }
}

impl Accept for UdpListener {
    type Connection = UdpConnection;
    type Error = io::Error;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Connection, Self::Error>> {
        if self.recv_buffer.capacity() < self.max_recv_buffer {
            let additional = self.max_recv_buffer - self.recv_buffer.capacity();
            self.recv_buffer.reserve(additional);
        }
        let UdpListener {
            socket,
            recv_buffer,
            ..
        } = &mut *self;
        let mut buf = tokio::io::ReadBuf::uninit(recv_buffer.spare_capacity_mut());
        match socket.poll_recv_from(cx, &mut buf) {
            Poll::Ready(Ok(addr)) => {
                let n = buf.filled().len();
                trace!("UDP: Received datagram n={}", n);

                // SAFETY: We _just_ read n bytes into buf, which is just
                // an exclusive reference into this.recv_buffer, so we know
                // that we can set the length here.
                #[allow(unsafe_code)]
                unsafe {
                    recv_buffer.set_len(n);
                }

                let data = recv_buffer.split().freeze();
                let message = UdpMessage::new(data, addr);
                let connection = UdpConnection {
                    message: Some(message),
                    addr,
                    socket: socket.clone(),
                };
                Poll::Ready(Ok(connection))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}
