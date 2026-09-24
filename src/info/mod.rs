//! Connection Information
//!
//! Streams report their local and remote addresses via [`HasConnectionInfo`], which
//! produces a [`ConnectionInfo`] typed over the transport's address type (e.g.
//! [`SocketAddr`] for TCP, [`UnixAddr`] for unix sockets).
//!
//! When connection information needs to cross an API boundary where the concrete
//! transport is not known (for example, when stored in HTTP request extensions),
//! use the type-erased form, `ConnectionInfo` (i.e. `ConnectionInfo<AnyAddr>`),
//! which can be produced from any typed info with [`ConnectionInfo::erase`] or
//! [`From`]. Consumers can then recover a concrete address type with
//! [`ConnectionInfo::remote_addr_as`] / [`ConnectionInfo::local_addr_as`], which
//! also see through wrapper address types that implement [`Address::inner`].
//!
//! ```
//! # use std::net::SocketAddr;
//! # use chateau::info::ConnectionInfo;
//! let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
//! let typed = ConnectionInfo { local_addr: addr, remote_addr: addr };
//!
//! let erased: ConnectionInfo = typed.erase();
//! assert_eq!(erased.remote_addr_as::<SocketAddr>(), Some(&addr));
//! assert_eq!(erased.remote_addr().to_string(), "127.0.0.1:8080");
//! ```

use std::any::Any;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

pub mod tls;
pub use self::tls::HasTlsConnectionInfo;
pub use self::tls::TlsConnectionInfo;
#[cfg(feature = "duplex")]
#[doc(hidden)]
pub use crate::stream::duplex::DuplexAddr;
#[doc(hidden)]
pub use crate::stream::unix::UnixAddr;

/// An address for one end of a connection.
///
/// Address types which wrap other address types (e.g. an enum over several
/// transports' addresses) should implement [`Address::inner`] to expose the
/// wrapped address, so that [`AnyAddr::downcast_ref`] can find it.
///
/// ```
/// # use std::fmt;
/// # use std::net::SocketAddr;
/// # use chateau::info::{Address, AnyAddr, UnixAddr};
/// #[derive(Debug, Clone)]
/// enum EitherAddr {
///     Tcp(SocketAddr),
///     Unix(UnixAddr),
/// }
///
/// impl fmt::Display for EitherAddr {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         match self {
///             EitherAddr::Tcp(addr) => addr.fmt(f),
///             EitherAddr::Unix(addr) => addr.fmt(f),
///         }
///     }
/// }
///
/// impl Address for EitherAddr {
///     fn inner(&self) -> Option<&dyn Address> {
///         match self {
///             EitherAddr::Tcp(addr) => Some(addr),
///             EitherAddr::Unix(addr) => Some(addr),
///         }
///     }
/// }
///
/// let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
/// let any = AnyAddr::new(EitherAddr::Tcp(addr));
/// assert_eq!(any.downcast_ref::<SocketAddr>(), Some(&addr));
/// assert!(any.downcast_ref::<EitherAddr>().is_some());
/// assert!(any.downcast_ref::<UnixAddr>().is_none());
/// ```
pub trait Address: Any + fmt::Display + fmt::Debug + Send + Sync {
    /// The address wrapped by this address, if any.
    ///
    /// Wrapper address types should return the active inner address here.
    fn inner(&self) -> Option<&dyn Address> {
        None
    }
}

impl Address for SocketAddr {}

/// A type-erased [`Address`].
///
/// This is cheap to clone, and can be converted back to a concrete address
/// type with [`AnyAddr::downcast_ref`].
#[derive(Clone)]
pub struct AnyAddr(Arc<dyn Address>);

impl AnyAddr {
    /// Erase the type of an address.
    ///
    /// Wrapping an `AnyAddr` returns it unchanged, rather than nesting it.
    pub fn new<A: Address>(addr: A) -> Self {
        let mut slot = Some(addr);
        if let Some(any) = (&mut slot as &mut dyn Any).downcast_mut::<Option<AnyAddr>>() {
            return any.take().expect("slot is populated");
        }
        Self(Arc::new(slot.expect("slot is populated")))
    }

    /// Get a reference to the address as a concrete type.
    ///
    /// This checks the stored address first, and then each address exposed by
    /// successive calls to [`Address::inner`], returning the first match.
    pub fn downcast_ref<T: Address>(&self) -> Option<&T> {
        let mut current: &dyn Address = &*self.0;
        loop {
            if let Some(addr) = (current as &dyn Any).downcast_ref::<T>() {
                return Some(addr);
            }
            current = current.inner()?;
        }
    }

    /// Does this address (or any address it wraps) have type `T`?
    pub fn is<T: Address>(&self) -> bool {
        self.downcast_ref::<T>().is_some()
    }

    /// Get a reference to the underlying address as a trait object.
    pub fn as_dyn(&self) -> &dyn Address {
        &*self.0
    }
}

impl fmt::Debug for AnyAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for AnyAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&*self.0, f)
    }
}

impl Address for AnyAddr {
    fn inner(&self) -> Option<&dyn Address> {
        Some(&*self.0)
    }
}

/// Information about a connection to a stream.
///
/// The address type defaults to [`AnyAddr`], a type-erased address. Use the
/// erased form when storing connection information somewhere keyed by type
/// (e.g. HTTP request extensions), so that consumers can always look up
/// `ConnectionInfo` without knowing which transport produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo<Addr = AnyAddr> {
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

#[cfg(feature = "duplex")]
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

impl<Addr: Address> ConnectionInfo<Addr> {
    /// Erase the address type of this connection info.
    pub fn erase(self) -> ConnectionInfo {
        self.map(AnyAddr::new)
    }
}

impl ConnectionInfo {
    /// The local address for this connection, as a concrete address type.
    ///
    /// Returns `None` if the address is not (and does not wrap) a `T`.
    pub fn local_addr_as<T: Address>(&self) -> Option<&T> {
        self.local_addr.downcast_ref()
    }

    /// The remote address for this connection, as a concrete address type.
    ///
    /// Returns `None` if the address is not (and does not wrap) a `T`.
    pub fn remote_addr_as<T: Address>(&self) -> Option<&T> {
        self.remote_addr.downcast_ref()
    }
}

macro_rules! impl_from_typed {
    ($($ty:ty $(: $feature:literal)?),* $(,)?) => {
        $(
            $(#[cfg(feature = $feature)])?
            impl From<ConnectionInfo<$ty>> for ConnectionInfo {
                fn from(info: ConnectionInfo<$ty>) -> Self {
                    info.erase()
                }
            }
        )*
    };
}

// A blanket `impl<A: Address> From<ConnectionInfo<A>> for ConnectionInfo` would
// conflict with the reflexive `From<T> for T`, so provide impls for the built-in
// address types. Other address types can use `ConnectionInfo::erase`.
impl_from_typed!(SocketAddr, UnixAddr, DuplexAddr: "duplex");

/// Trait for types which can provide connection information.
pub trait HasConnectionInfo {
    /// The address type for this connection.
    type Addr: Address;

    /// Get the connection information for this stream.
    fn info(&self) -> ConnectionInfo<Self::Addr>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum BraidAddr {
        Tcp(SocketAddr),
        Unix(UnixAddr),
    }

    impl fmt::Display for BraidAddr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                BraidAddr::Tcp(addr) => addr.fmt(f),
                BraidAddr::Unix(addr) => addr.fmt(f),
            }
        }
    }

    impl Address for BraidAddr {
        fn inner(&self) -> Option<&dyn Address> {
            match self {
                BraidAddr::Tcp(addr) => Some(addr),
                BraidAddr::Unix(addr) => Some(addr),
            }
        }
    }

    fn socket() -> SocketAddr {
        "127.0.0.1:8080".parse().unwrap()
    }

    #[test]
    fn erase_and_downcast() {
        let info = ConnectionInfo {
            local_addr: socket(),
            remote_addr: socket(),
        };
        let erased: ConnectionInfo = info.into();

        assert_eq!(erased.local_addr_as::<SocketAddr>(), Some(&socket()));
        assert_eq!(erased.remote_addr_as::<SocketAddr>(), Some(&socket()));
        assert!(erased.remote_addr_as::<UnixAddr>().is_none());
        assert_eq!(erased.remote_addr().to_string(), "127.0.0.1:8080");
        assert_eq!(format!("{:?}", erased.remote_addr()), "127.0.0.1:8080");
    }

    #[test]
    fn downcast_through_wrapper() {
        let info = ConnectionInfo {
            local_addr: BraidAddr::Tcp(socket()),
            remote_addr: BraidAddr::Unix(UnixAddr::unnamed()),
        }
        .erase();

        assert_eq!(info.local_addr_as::<SocketAddr>(), Some(&socket()));
        assert_eq!(
            info.local_addr_as::<BraidAddr>(),
            Some(&BraidAddr::Tcp(socket()))
        );
        assert!(info.local_addr_as::<UnixAddr>().is_none());

        assert_eq!(
            info.remote_addr_as::<UnixAddr>(),
            Some(&UnixAddr::unnamed())
        );
        assert!(info.remote_addr_as::<SocketAddr>().is_none());
    }

    #[test]
    fn erase_is_idempotent() {
        let once = AnyAddr::new(socket());
        let twice = AnyAddr::new(once.clone());
        assert!(Arc::ptr_eq(&once.0, &twice.0));

        let info = ConnectionInfo {
            local_addr: socket(),
            remote_addr: socket(),
        }
        .erase()
        .erase();
        assert_eq!(info.remote_addr_as::<SocketAddr>(), Some(&socket()));
    }

    #[test]
    fn erased_info_is_extension_compatible() {
        fn assert_extension<T: Clone + Send + Sync + 'static>() {}
        assert_extension::<ConnectionInfo>();
    }
}
