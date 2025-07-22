use tokio_util::codec::Framed;

use super::{Connection, Protocol};

pub struct FramedConnection<IO, C> {
    codec: Framed<IO, C>,
}

impl<IO, C> Connection for FramedConnection<IO, C> {
    type Response;

    type Error;

    type Future;

    fn send_request(&mut self, request: Req) -> Self::Future {
        todo!()
    }

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }
}
