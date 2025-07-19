use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

use crate::BoxFuture;

#[derive(Debug, Clone)]
pub(crate) struct Sender(Option<tokio::sync::watch::Receiver<()>>);

impl Sender {
    pub(crate) fn send(&mut self) {
        let _ = self.0.take();
        tracing::trace!("sending close signal");
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Receiver(Arc<tokio::sync::watch::Sender<()>>);

impl IntoFuture for Receiver {
    type IntoFuture = Notified;
    type Output = ();

    fn into_future(self) -> Self::IntoFuture {
        Notified(Box::pin(async move {
            self.0.closed().await;
        }))
    }
}

#[pin_project::pin_project]
pub(crate) struct Notified(#[pin] BoxFuture<'static, ()>);

impl Future for Notified {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        self.project().0.poll(cx)
    }
}

pub(crate) fn channel() -> (Sender, Receiver) {
    let (tx, rx) = tokio::sync::watch::channel(());
    (Sender(Some(rx)), Receiver(Arc::new(tx)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_channel_creation() {
        let (sender, receiver) = channel();
        assert!(sender.0.is_some());
        assert!(Arc::strong_count(&receiver.0) == 1);
    }

    #[tokio::test]
    async fn test_sender_send() {
        let (mut sender, _receiver) = channel();

        // First send should work
        sender.send();
        assert!(sender.0.is_none());

        // Second send should not panic (no-op)
        sender.send();
        assert!(sender.0.is_none());
    }

    #[tokio::test]
    async fn test_receiver_notification() {
        let (mut sender, receiver) = channel();

        // Start waiting on receiver
        let receiver_task = tokio::spawn(async move {
            receiver.await;
        });

        // Give receiver time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Send notification
        sender.send();

        // Receiver should complete quickly
        let result = timeout(Duration::from_millis(100), receiver_task).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_receiver_clone() {
        let (mut sender, receiver) = channel();
        let receiver_clone = receiver.clone();

        // Both receivers should be notified
        let receiver1_task = tokio::spawn(async move {
            receiver.await;
        });

        let receiver2_task = tokio::spawn(async move {
            receiver_clone.await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        sender.send();

        let result1 = timeout(Duration::from_millis(100), receiver1_task).await;
        let result2 = timeout(Duration::from_millis(100), receiver2_task).await;

        assert!(result1.is_ok() && result1.unwrap().is_ok());
        assert!(result2.is_ok() && result2.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_sender_clone_behavior() {
        let (mut sender, _receiver) = channel();
        let mut sender_clone = sender.clone();

        // Both senders should have the same receiver initially
        assert!(sender.0.is_some());
        assert!(sender_clone.0.is_some());

        // When one sender sends, it takes the receiver
        sender.send();
        assert!(sender.0.is_none());

        // The clone should still have its own copy of the receiver
        assert!(sender_clone.0.is_some());

        // Clone can also send
        sender_clone.send();
        assert!(sender_clone.0.is_none());
    }

    #[tokio::test]
    async fn test_notified_future_impl() {
        let (mut sender, receiver) = channel();

        // Convert receiver to future
        let notified_future = receiver.into_future();

        let future_task = tokio::spawn(async move {
            notified_future.await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        sender.send();

        let result = timeout(Duration::from_millis(100), future_task).await;
        assert!(result.is_ok() && result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_multiple_sends_same_sender() {
        let (mut sender, receiver) = channel();

        let receiver_task = tokio::spawn(async move {
            receiver.await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Multiple sends should not cause issues
        sender.send();
        sender.send();
        sender.send();

        let result = timeout(Duration::from_millis(100), receiver_task).await;
        assert!(result.is_ok() && result.unwrap().is_ok());
    }

    #[test]
    fn test_debug_impl() {
        let (sender, receiver) = channel();

        // Test that Debug is implemented
        let _sender_debug = format!("{sender:?}");
        let _receiver_debug = format!("{receiver:?}");

        // Should not panic
    }
}
