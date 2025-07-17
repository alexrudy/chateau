//! Abstractions to help apply UDP sockets to tower services

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, io};

use bytes::{Bytes, BytesMut};
use futures_core::future::BoxFuture;
use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::server::Accept;

/// Represents a UDP message which is ready to be sent or has been recieved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpMessage {
    data: Bytes,
    addr: SocketAddr,
    cursor: usize,
}

impl UdpMessage {
    /// Creates a new UDP message with the given data and address.
    pub fn new(data: Bytes, addr: SocketAddr) -> Self {
        Self {
            data,
            addr,
            cursor: 0,
        }
    }

    /// Returns the data and address of the message.
    pub fn into_parts(self) -> (Bytes, SocketAddr) {
        (self.data, self.addr)
    }

    /// Reference the data in the message.
    pub fn bytes(&self) -> &Bytes {
        &self.data
    }

    /// The destination address for the message
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Advance the internal send cursor.
    fn advance(&mut self, sent: usize) -> bool {
        self.cursor += sent;
        self.cursor >= self.data.len()
    }

    /// Get the unsent slice of this message.
    pub fn slice(&self) -> &[u8] {
        &self.data[self.cursor..]
    }
}

impl From<(Vec<u8>, SocketAddr)> for UdpMessage {
    fn from((data, addr): (Vec<u8>, SocketAddr)) -> Self {
        UdpMessage::new(Bytes::from(data), addr)
    }
}

impl From<(Box<[u8]>, SocketAddr)> for UdpMessage {
    fn from((data, addr): (Box<[u8]>, SocketAddr)) -> Self {
        UdpMessage::new(Bytes::from(data), addr)
    }
}

impl From<(Bytes, SocketAddr)> for UdpMessage {
    fn from((data, addr): (Bytes, SocketAddr)) -> Self {
        UdpMessage::new(data, addr)
    }
}

enum ListenerState {
    Permit(BoxFuture<'static, Result<mpsc::OwnedPermit<UdpMessage>, mpsc::error::SendError<()>>>),
    Recv(Option<mpsc::OwnedPermit<UdpMessage>>),
    Closed(Option<io::Error>),
}

impl fmt::Debug for ListenerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ListenerState::Permit(_) => f.debug_tuple("Permit").finish(),
            ListenerState::Recv(_) => f.debug_tuple("Recv").finish(),
            ListenerState::Closed(_) => f.debug_tuple("Closed").finish(),
        }
    }
}

enum SenderState {
    Polling,
    Send(UdpMessage),
}

impl fmt::Debug for SenderState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            SenderState::Polling => f.debug_tuple("Polling").finish(),
            SenderState::Send(_) => f.debug_tuple("Send").finish(),
        }
    }
}

/// A listener for a UDP socket to match the Accept trait.
#[derive(Debug)]
#[pin_project::pin_project]
pub struct UdpListener {
    #[pin]
    socket: tokio::net::UdpSocket,
    listen_state: ListenerState,
    send_state: SenderState,
    recv_buffer: BytesMut,
    max_recv_buffer: usize,
    #[pin]
    outbound: mpsc::Receiver<UdpMessage>,
    sender: Option<mpsc::Sender<UdpMessage>>,
}

impl UdpListener {
    /// Create a new UDP listener
    pub fn new(
        socket: tokio::net::UdpSocket,
        recv_buffer_size: usize,
        send_channel_size: usize,
    ) -> Self {
        let (sender, outbound) = mpsc::channel(send_channel_size);
        Self {
            socket,
            listen_state: ListenerState::Permit(Box::pin(sender.clone().reserve_owned())),
            recv_buffer: BytesMut::with_capacity(recv_buffer_size),
            max_recv_buffer: recv_buffer_size,
            send_state: SenderState::Polling,
            outbound,
            sender: Some(sender),
        }
    }

    fn poll_listen(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<UdpConnection>, io::Error>> {
        let this = self.as_mut().project();

        loop {
            match this.listen_state {
                ListenerState::Permit(future) => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(permit)) => {
                        *this.listen_state = ListenerState::Recv(Some(permit));
                    }
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "response channel closed",
                        )));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ListenerState::Recv(permit) if permit.is_some() => {
                    if this.recv_buffer.capacity() < *this.max_recv_buffer {
                        this.recv_buffer
                            .reserve((*this.max_recv_buffer) - this.recv_buffer.capacity());
                    }

                    let mut buf = tokio::io::ReadBuf::uninit(this.recv_buffer.spare_capacity_mut());
                    match this.socket.poll_recv_from(cx, &mut buf) {
                        Poll::Ready(Ok(addr)) => {
                            let data = this.recv_buffer.split().freeze();
                            let message = UdpMessage::new(data, addr);
                            let connection = UdpConnection {
                                message,
                                responder: UdpResponder {
                                    response: permit.take().unwrap(),
                                },
                            };

                            if let Some(sender) = this.sender.clone() {
                                *this.listen_state =
                                    ListenerState::Permit(Box::pin(sender.reserve_owned()));
                                return Poll::Ready(Ok(Some(connection)));
                            } else {
                                return Poll::Ready(Ok(None));
                            }
                        }
                        Poll::Ready(Err(error)) => {
                            debug!("Error reading UDP packet: {error}");
                            *this.listen_state = ListenerState::Closed(Some(error));
                            return Poll::Pending;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ListenerState::Recv(_) => {
                    panic!("UDP Listener: Missing permit in listener state Recv")
                }
                ListenerState::Closed(error) => {
                    if let Some(error) = error.take() {
                        return Poll::Ready(Err(error));
                    }
                }
            }
        }
    }

    fn poll_send(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Error> {
        let mut this = self.as_mut().project();

        loop {
            match this.send_state {
                SenderState::Polling => {
                    match this.outbound.poll_recv(cx) {
                        Poll::Ready(Some(msg)) => *this.send_state = SenderState::Send(msg),
                        Poll::Ready(None) => {
                            // Stream has closed, return an error so we don't accept anything else.
                            return Poll::Ready(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "UDP Response queue closed",
                            ));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                SenderState::Send(udp_message) => {
                    match this
                        .socket
                        .poll_send_to(cx, udp_message.slice(), udp_message.addr)
                    {
                        Poll::Ready(Ok(n)) => {
                            if udp_message.advance(n) {
                                trace!("Sent UDP message");
                                *this.send_state = SenderState::Polling;
                            }
                        }
                        Poll::Ready(Err(error)) => {
                            debug!("Error while sending message: {error}");
                            return Poll::Ready(error);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

/// A sender which respects the UDP socket queue
#[derive(Debug, Clone)]
pub struct UdpSender {
    sender: mpsc::Sender<UdpMessage>,
}

impl UdpSender {
    /// Sends a message over the UDP socket.
    pub async fn send(
        &self,
        message: impl Into<UdpMessage>,
    ) -> Result<(), mpsc::error::SendError<UdpMessage>> {
        self.sender.send(message.into()).await
    }
}

/// A handle to send a message over the UDP socket.
#[derive(Debug)]
pub struct UdpResponder {
    response: mpsc::OwnedPermit<UdpMessage>,
}

impl UdpResponder {
    /// Sends a message to a remote, and returns a sender that can be used
    /// to send additional messages.
    pub fn send(self, message: impl Into<UdpMessage>) -> UdpSender {
        let sender = self.response.send(message.into());
        UdpSender { sender }
    }
}

/// A UDP "Connection", consisting of one inbound message and a responder to send
/// an unlimited number of responses.
#[derive(Debug)]
pub struct UdpConnection {
    message: UdpMessage,
    responder: UdpResponder,
}

impl UdpConnection {
    /// Returns the message and responder of the connected UDP stream.
    pub fn into_parts(self) -> (UdpMessage, UdpResponder) {
        (self.message, self.responder)
    }
}

impl Accept for UdpListener {
    type Connection = UdpConnection;
    type Error = io::Error;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Connection, Self::Error>> {
        match self.as_mut().poll_send(cx) {
            Poll::Ready(error) => return Poll::Ready(Err(error)),
            Poll::Pending => {}
        }

        match self.as_mut().poll_listen(cx) {
            Poll::Ready(Ok(Some(conn))) => Poll::Ready(Ok(conn)),
            Poll::Ready(Ok(None)) => self.as_mut().poll_send(cx).map(Err),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}
