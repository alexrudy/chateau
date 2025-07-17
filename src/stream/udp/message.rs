use std::net::SocketAddr;

use bytes::Bytes;

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

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The destination address for the message
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Advance the internal send cursor.
    pub(crate) fn advance(&mut self, sent: usize) -> bool {
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

pub struct UdpMessageCursor {
    message: Bytes,
    cursor: usize,
}

impl UdpMessageCursor {
    pub fn new(message: Bytes) -> Self {
        Self { message, cursor: 0 }
    }

    /// Advance the internal send cursor.
    pub fn advance(&mut self, sent: usize) -> bool {
        self.cursor += sent;
        self.cursor >= self.message.len()
    }

    /// Get the unsent slice of this message.
    pub fn slice(&self) -> &[u8] {
        &self.message[self.cursor..]
    }
}
