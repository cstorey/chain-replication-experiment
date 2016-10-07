use service::Service;
use futures::{self, Async, Poll};
use messages::{ServerRequest, ServerResponse};

use sexp_proto::Error;

/// The main interface to the outside world.
#[derive(Clone,Debug)]
pub struct ServerService;


impl ServerService {
    pub fn new() -> Self {
        ServerService
    }
}

impl Service for ServerService {
    // The type of the input requests we get.
    type Request = ServerRequest;
    type Response = ServerResponse;
    type Error = Error;
    type Future = futures::Empty<Self::Response, Self::Error>;

    fn poll_ready(&self) -> Async<()> {
        unimplemented!()
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        unimplemented!()
    }
}
