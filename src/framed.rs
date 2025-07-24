//! Helpers for working with sinks and streams

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, Stream};

/// A joined stream and sink
///
/// Provides the inverse of Stream::split
#[pin_project::pin_project]
#[derive(Debug, Clone)]
pub struct Joined<St, Si> {
    #[pin]
    stream: St,
    #[pin]
    sink: Si,
}

impl<St, Si> Joined<St, Si> {
    /// Join a stream and sink together into a unified object
    /// which implements both traits
    pub fn new(stream: St, sink: Si) -> Self {
        Self { stream, sink }
    }

    /// Reference to the stream
    pub fn get_stream(&self) -> &St {
        &self.stream
    }

    /// Reference to the sink
    pub fn get_sink(&self) -> &Si {
        &self.sink
    }

    /// Pinned reference to the stream
    pub fn pinned_stream(self: Pin<&mut Self>) -> Pin<&mut St> {
        self.project().stream
    }

    /// Pinned reference to the sink
    pub fn pinned_sink(self: Pin<&mut Self>) -> Pin<&mut Si> {
        self.project().sink
    }

    /// Mutable reference to the stream
    pub fn mut_stream(&mut self) -> &mut St {
        &mut self.stream
    }

    /// Mutable reference to the sink
    pub fn mut_sink(&mut self) -> &mut Si {
        &mut self.sink
    }

    /// Split this back into its constituent parts
    pub fn into_parts(self) -> (St, Si) {
        (self.stream, self.sink)
    }
}

impl<St, Si, I> Sink<I> for Joined<St, Si>
where
    Si: Sink<I>,
{
    type Error = Si::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sink.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().sink.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sink.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sink.poll_close(cx)
    }
}

impl<St, Si> Stream for Joined<St, Si>
where
    St: Stream,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}
