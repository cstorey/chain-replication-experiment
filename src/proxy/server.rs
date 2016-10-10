use service::Service;
use futures::{self, Async, Poll};
use super::{ClientRequest, ClientResponse};

use sexp_proto::Error;

/// The main interface to the outside world.
#[derive(Clone,Debug)]
pub struct ProxyService;


impl ProxyService {
    pub fn new() -> Self {
        ProxyService
    }
}

impl Service for ProxyService {
    // The type of the input requests we get.
    type Request = ClientRequest;
    type Response = ClientResponse;
    type Error = Error;
    type Future = futures::Empty<Self::Response, Self::Error>;

    fn poll_ready(&self) -> Async<()> {
        unimplemented!()
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        unimplemented!()
    }
}
