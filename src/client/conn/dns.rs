//! DNS resolution utilities.

use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// A collection of socket addresses.
#[derive(Debug, Clone, Default)]
pub struct SocketAddrs(VecDeque<SocketAddr>);

impl SocketAddrs {
    #[allow(dead_code)]
    pub(crate) fn set_port(&mut self, port: u16) {
        for addr in &mut self.0 {
            addr.set_port(port)
        }
    }

    #[allow(dead_code)]
    pub(crate) fn peek(&self) -> Option<SocketAddr> {
        self.0.front().copied()
    }

    pub(crate) fn pop(&mut self) -> Option<SocketAddr> {
        self.0.pop_front()
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn sort_preferred(&mut self, prefer: Option<IpVersion>) {
        let mut v4_idx = None;
        let mut v6_idx = None;

        for (idx, addr) in self.0.iter().enumerate() {
            match (addr.version(), v4_idx, v6_idx) {
                (IpVersion::V4, None, _) => {
                    v4_idx = Some(idx);
                }
                (IpVersion::V6, _, None) => {
                    v6_idx = Some(idx);
                }
                (_, Some(_), Some(_)) => break,
                _ => {}
            }
        }

        let v4: Option<SocketAddr>;
        let v6: Option<SocketAddr>;
        if v4_idx.zip(v6_idx).is_some_and(|(v4, v6)| v4 > v6) {
            v4 = v4_idx.and_then(|idx| self.0.remove(idx));
            v6 = v6_idx.and_then(|idx| self.0.remove(idx));
        } else {
            v6 = v6_idx.and_then(|idx| self.0.remove(idx));
            v4 = v4_idx.and_then(|idx| self.0.remove(idx));
        }

        match (prefer, v4, v6) {
            (Some(IpVersion::V4), Some(addr_v4), Some(addr_v6)) => {
                self.0.push_front(addr_v6);
                self.0.push_front(addr_v4);
            }
            (Some(IpVersion::V6), Some(addr_v4), Some(addr_v6)) => {
                self.0.push_front(addr_v4);
                self.0.push_front(addr_v6);
            }

            (_, Some(addr_v4), Some(addr_v6)) => {
                self.0.push_front(addr_v4);
                self.0.push_front(addr_v6);
            }
            (_, Some(addr_v4), None) => {
                self.0.push_front(addr_v4);
            }
            (_, None, Some(addr_v6)) => {
                self.0.push_front(addr_v6);
            }
            _ => {}
        }
    }
}

impl From<SocketAddr> for SocketAddrs {
    fn from(value: SocketAddr) -> Self {
        let mut addrs = VecDeque::with_capacity(1);
        addrs.push_front(value);
        SocketAddrs(addrs)
    }
}

impl FromIterator<SocketAddr> for SocketAddrs {
    fn from_iter<T: IntoIterator<Item = SocketAddr>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl IntoIterator for SocketAddrs {
    type Item = SocketAddr;
    type IntoIter = std::collections::vec_deque::IntoIter<SocketAddr>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a SocketAddrs {
    type Item = &'a SocketAddr;
    type IntoIter = std::collections::vec_deque::Iter<'a, SocketAddr>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Extension trait for `IpAddr` and `SocketAddr` to get the IP version.
pub trait IpVersionExt {
    /// Get the IP version of this address.
    fn version(&self) -> IpVersion;
}

/// IP version.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum IpVersion {
    /// IPv4
    V4,

    /// IPv6
    V6,
}

impl IpVersion {
    pub(super) fn from_binding(
        ip_v4_address: Option<Ipv4Addr>,
        ip_v6_address: Option<Ipv6Addr>,
    ) -> Option<Self> {
        match (ip_v4_address, ip_v6_address) {
            // Prefer IPv6 if both are available.
            (Some(_), Some(_)) => Some(Self::V6),
            (Some(_), None) => Some(Self::V4),
            (None, Some(_)) => Some(Self::V6),
            (None, None) => None,
        }
    }

    /// Is this IP version IPv4?
    #[allow(dead_code)]
    pub fn is_v4(&self) -> bool {
        matches!(self, Self::V4)
    }

    /// Is this IP version IPv6?
    #[allow(dead_code)]
    pub fn is_v6(&self) -> bool {
        matches!(self, Self::V6)
    }
}

impl IpVersionExt for SocketAddr {
    fn version(&self) -> IpVersion {
        match self {
            SocketAddr::V4(_) => IpVersion::V4,
            SocketAddr::V6(_) => IpVersion::V6,
        }
    }
}

impl IpVersionExt for IpAddr {
    fn version(&self) -> IpVersion {
        match self {
            IpAddr::V4(_) => IpVersion::V4,
            IpAddr::V6(_) => IpVersion::V6,
        }
    }
}
