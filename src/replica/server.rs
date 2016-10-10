use service::Service;
use futures::{self, Async, Poll, Future};
use super::{ServerRequest, ServerResponse, LogPos};

use sexp_proto::Error;

/// The main interface to the outside world.
#[derive(Clone,Debug)]
pub struct ServerService;

#[derive(Debug)]
pub struct ReplicaFut;

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
    type Future = ReplicaFut;

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        ReplicaFut
    }
}

impl Future for ReplicaFut {
    type Item = ServerResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(ServerResponse::Done(LogPos::zero())))
    }
}
