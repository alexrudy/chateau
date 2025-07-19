//! Connection Information

use std::fmt;

#[cfg(feature = "tls")]
pub mod tls;
#[cfg(feature = "tls")]
pub use self::tls::HasTlsConnectionInfo;
#[cfg(feature = "tls")]
pub use self::tls::TlsConnectionInfo;
#[doc(hidden)]
pub use crate::stream::duplex::DuplexAddr;
#[doc(hidden)]
pub use crate::stream::unix::UnixAddr;

/// Information about a connection to a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo<Addr> {
    /// The local address for this connection.
    pub local_addr: Addr,

    /// The remote address for this connection.
    pub remote_addr: Addr,
}

impl<Addr> Default for ConnectionInfo<Addr>
where
    Addr: Default,
{
    fn default() -> Self {
        Self {
            local_addr: Addr::default(),
            remote_addr: Addr::default(),
        }
    }
}

impl ConnectionInfo<DuplexAddr> {
    /// Connection info for a duplex stream.
    pub fn duplex() -> Self {
        Self::default()
    }
}

impl<Addr> ConnectionInfo<Addr> {
    /// The local address for this connection
    pub fn local_addr(&self) -> &Addr {
        &self.local_addr
    }

    /// The remote address for this connection
    pub fn remote_addr(&self) -> &Addr {
        &self.remote_addr
    }

    /// Map the addresses in this connection info to a new type.
    pub fn map<T, F>(self, f: F) -> ConnectionInfo<T>
    where
        F: Fn(Addr) -> T,
    {
        ConnectionInfo {
            local_addr: f(self.local_addr),
            remote_addr: f(self.remote_addr),
        }
    }
}

/// Trait for types which can provide connection information.
pub trait HasConnectionInfo {
    /// The address type for this connection.
    type Addr: fmt::Display + fmt::Debug + Send;

    /// Get the connection information for this stream.
    fn info(&self) -> ConnectionInfo<Self::Addr>;
}
