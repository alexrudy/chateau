//! Information about a TLS connection.
//!
//! TLS information is a bit tricky, because it is realy only available after the handshake is complete.

use rustls::{HandshakeKind, ProtocolVersion, SupportedCipherSuite, pki_types::CertificateDer};

use super::HasConnectionInfo;

#[cfg(all(feature = "server", feature = "tls"))]
pub use self::channel::{TlsConnectionInfoReceiver, TlsConnectionInfoSender, channel};

/// Information about a TLS connection.
#[derive(Debug, Clone, PartialEq)]
pub struct TlsConnectionInfo {
    /// The server name used for this connection, as provided by the SNI
    /// extension.
    pub server_name: Option<String>,

    /// Whether the server name was validated against the certificate.
    pub validated_server_name: bool,

    /// The application layer protocol negotiated for this connection.
    pub alpn: Option<String>,

    /// The peer certificates used for authentication
    pub peer_certificates: Option<Vec<CertificateDer<'static>>>,

    /// Agreed handshake kind
    pub handshake_kind: HandshakeKind,

    /// Agreed TLS Version
    pub tls_version: ProtocolVersion,

    /// Agreed Cipher Suite
    pub cipher_suite: SupportedCipherSuite,
}

#[cfg(feature = "tls")]
impl TlsConnectionInfo {
    /// Create a new TLS connection info for a server connection.
    pub fn server(server_info: &rustls::ServerConnection) -> Self {
        let server_name = server_info
            .server_name()
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());

        let alpn = server_info
            .alpn_protocol()
            .and_then(|s| std::str::from_utf8(s).ok())
            .and_then(|s| s.parse().ok());

        Self {
            server_name,
            validated_server_name: false,
            alpn,
            peer_certificates: server_info
                .peer_certificates()
                .map(|certs| Vec::from(certs)),
            handshake_kind: server_info
                .handshake_kind()
                .expect("tls info before handshake"),
            tls_version: server_info
                .protocol_version()
                .expect("tls info before handshake"),
            cipher_suite: server_info
                .negotiated_cipher_suite()
                .expect("tls info before handshake"),
        }
    }

    /// Create a new TLS connection info for a client connection.
    pub fn client(client_info: &rustls::ClientConnection) -> Self {
        let alpn = client_info
            .alpn_protocol()
            .and_then(|s| std::str::from_utf8(s).ok())
            .and_then(|s| s.parse().ok());

        Self {
            server_name: None,
            validated_server_name: false,
            alpn,
            peer_certificates: client_info
                .peer_certificates()
                .map(|certs| Vec::from(certs)),
            handshake_kind: client_info
                .handshake_kind()
                .expect("tls info before handshake"),
            tls_version: client_info
                .protocol_version()
                .expect("tls info before handshake"),
            cipher_suite: client_info
                .negotiated_cipher_suite()
                .expect("tls info before handshake"),
        }
    }

    /// Mark this TLS information as externally validated.
    pub fn validated(&mut self) {
        self.validated_server_name = true;
    }
}

/// A trait for types that can provide information about a TLS connection.
pub trait HasTlsConnectionInfo: HasConnectionInfo {
    /// Returns information about the TLS connection, if available.
    ///
    /// This method is async because the information may not be available
    /// and so the handshake may have to complete first.
    fn tls_info(&self) -> Option<&TlsConnectionInfo>;
}

#[cfg(all(feature = "server", feature = "tls"))]
mod channel {
    use std::{
        ops::{Deref, DerefMut},
        sync::Arc,
    };
    use tokio::sync::RwLock;

    use crate::info::TlsConnectionInfo;

    /// Create a new TLS info channel
    pub fn channel() -> (TlsConnectionInfoSender, TlsConnectionInfoReceiver) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            TlsConnectionInfoSender { tx: Some(tx) },
            TlsConnectionInfoReceiver::new(rx),
        )
    }

    /// TLS Connection info sender
    #[derive(Debug)]
    pub struct TlsConnectionInfoSender {
        tx: Option<tokio::sync::oneshot::Sender<TlsConnectionInfo>>,
    }

    impl TlsConnectionInfoSender {
        /// Send TLS connection info from the connection to the stream.
        pub fn send(&mut self, info: TlsConnectionInfo) {
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(info);
            }
        }
    }

    #[derive(Debug)]
    enum State {
        Pending(tokio::sync::oneshot::Receiver<TlsConnectionInfo>),
        Received(TlsConnectionInfo),
        Empty,
    }

    /// Receive TLS info sent from the connection
    #[derive(Debug, Clone)]
    pub struct TlsConnectionInfoReceiver {
        state: Arc<RwLock<State>>,
    }

    impl TlsConnectionInfoReceiver {
        /// Create an empty TLS Connection Info (for non-tls backed streams)
        pub fn empty() -> Self {
            Self {
                state: Arc::new(RwLock::new(State::Empty)),
            }
        }

        pub(crate) fn new(inner: tokio::sync::oneshot::Receiver<TlsConnectionInfo>) -> Self {
            Self {
                state: Arc::new(RwLock::new(State::Pending(inner))),
            }
        }

        /// Wait for the TLS connection info to become available.
        ///
        /// Resolves to `None` if the underlying connection never sends TLS
        /// info (e.g. a plaintext connection), or if the connection is
        /// dropped before the handshake completes.
        pub async fn recv(&self) -> Option<TlsConnectionInfo> {
            {
                let state = self.state.read().await;

                match state.deref() {
                    State::Pending(_) => {}
                    State::Received(info) => return Some(info.clone()),
                    State::Empty => return None,
                };
            }

            let mut state = self.state.write().await;

            let rx = match state.deref_mut() {
                State::Pending(rx) => rx,
                State::Received(info) => {
                    return Some(info.clone());
                }
                State::Empty => {
                    return None;
                }
            };

            let tls = rx
                .await
                .expect("connection info was never sent and is not available");

            *state = State::Received(tls.clone());
            Some(tls)
        }
    }
}

#[cfg(all(test, feature = "tls"))]
mod tests {
    use crate::fixtures;

    #[test]
    fn tls_server_info() {
        let (client, server) = fixtures::tls_handshake();

        let info = super::TlsConnectionInfo::server(&server);
        assert_eq!(info.server_name.as_deref(), Some("example.com"));
        assert!(!info.validated_server_name);
        assert_eq!(info.alpn.as_deref(), Some("h2"));
        assert_eq!(info.tls_version, client.protocol_version().unwrap());
        assert_eq!(info.cipher_suite, client.negotiated_cipher_suite().unwrap());
        // The fixture server does not request client certificates.
        assert_eq!(info.peer_certificates, None);
    }

    #[test]
    fn tls_client_info() {
        let (client, server) = fixtures::tls_handshake();

        let info = super::TlsConnectionInfo::client(&client);
        assert_eq!(info.server_name, None);
        assert!(!info.validated_server_name);
        assert_eq!(info.alpn.as_deref(), Some("h2"));
        assert_eq!(info.tls_version, server.protocol_version().unwrap());
        assert_eq!(info.cipher_suite, server.negotiated_cipher_suite().unwrap());
        // The client sees the server's certificate chain.
        assert_eq!(info.peer_certificates.map(|c| c.len()), Some(1));
    }
}
