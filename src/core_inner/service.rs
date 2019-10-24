use super::prelude::*;

type Svc = AddOrigin<Reconnect<Connect<HttpConnector, BoxBody, Uri>, Uri>>;

#[derive(Clone)]
pub struct BobService(Buffer<Svc, HttpRequest<BoxBody>>);

impl BobService {
    pub fn new(uri: Uri) -> Self {
        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        http_connector.set_nodelay(true);

        let settings = Builder::new().http2_only(true).clone();

        let stack = ServiceBuilder::new()
            .layer(LayerFn(|s| AddOrigin::new(s, uri.clone())))
            .into_inner();
        let conn = Connect::new(http_connector, settings);
        let conn = Reconnect::new(conn, uri.clone());
        let conn = stack.layer(conn);
        let conn = Buffer::new(conn, 1024);
        Self(conn)
    }
}

impl Service<HttpRequest<BoxBody>> for BobService {
    type Response = <Buffer<Svc, HttpRequest<BoxBody>> as Service<HttpRequest<BoxBody>>>::Response;
    type Error = <Buffer<Svc, HttpRequest<BoxBody>> as Service<HttpRequest<BoxBody>>>::Error;
    type Future = <Buffer<Svc, HttpRequest<BoxBody>> as Service<HttpRequest<BoxBody>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<BoxBody>) -> Self::Future {
        self.0.call(req)
    }
}

#[derive(Debug, Clone)]
pub struct AddOrigin<T> {
    inner: T,
    origin: Uri,
}

impl<T> AddOrigin<T> {
    pub fn new(inner: T, origin: Uri) -> Self {
        Self { inner, origin }
    }
}

impl<T, ReqBody> Service<HttpRequest<ReqBody>> for AddOrigin<T>
where
    T: Service<HttpRequest<ReqBody>>,
{
    type Response = T::Response;
    type Error = T::Error;
    type Future = T::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        // Split the request into the head and the body.
        let (mut head, body) = req.into_parts();

        // Split the request URI into parts.
        let mut uri: http::uri::Parts = head.uri.into();
        let set_uri = self.origin.clone().into_parts();

        // Update the URI parts, setting hte scheme and authority
        uri.scheme = Some(set_uri.scheme.expect("expected scheme").clone());
        uri.authority = Some(set_uri.authority.expect("expected authority").clone());

        // Update the the request URI
        head.uri = http::Uri::from_parts(uri).expect("valid uri");

        let request = HttpRequest::from_parts(head, body);

        self.inner.call(request)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct LayerFn<F>(pub F);

impl<F, S, Out> Layer<S> for LayerFn<F>
where
    F: Fn(S) -> Out,
{
    type Service = Out;

    fn layer(&self, inner: S) -> Self::Service {
        (self.0)(inner)
    }
}
