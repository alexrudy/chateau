//! UDP Socket wrapper

use std::io;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

/// A UDP socket
#[derive(Debug)]
pub struct UdpStream {
    stream: tokio::net::UdpSocket,
}

impl UdpStream {
    /// Bind a UDP socket to the given address
    pub async fn bind(addr: &SocketAddr) -> io::Result<Self> {
        let stream = tokio::net::UdpSocket::bind(addr).await?;
        Ok(Self { stream })
    }

    /// Send data to the given address
    pub async fn send_to(&self, buf: &[u8], addr: &SocketAddr) -> io::Result<usize> {
        self.stream.send_to(buf, addr).await
    }

    /// Receive data from the socket
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.stream.recv_from(buf).await
    }
}

impl Deref for UdpStream {
    type Target = tokio::net::UdpSocket;
    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for UdpStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl From<tokio::net::UdpSocket> for UdpStream {
    fn from(stream: tokio::net::UdpSocket) -> Self {
        Self { stream }
    }
}
