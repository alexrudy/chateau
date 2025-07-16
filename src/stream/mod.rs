//! Utilities for working with different types of network streams.

pub mod tcp;
#[cfg(feature = "tls")]
pub mod tls;
pub mod udp;
