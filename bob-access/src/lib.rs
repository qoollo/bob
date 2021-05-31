use std::task::{Context, Poll};

use tower::{Layer, Service};

pub struct AccessControlLayer {}

pub struct AccessControlService<S> {
    service: S,
}

impl<S> Layer<S> for AccessControlLayer {
    type Service = AccessControlService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        todo!()
    }
}

impl<S, Request> Service<Request> for AccessControlService<S>
where
    S: Service<Request>,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        todo!("check credentials/give access rights");
        self.service.call(req)
    }
}
