use service::Service;
use futures::{self, Async, Poll};
use super::{TailRequest, TailResponse};
use replica::LogPos;

use sexp_proto::Error;

/// The main interface to the outside world.
#[derive(Clone,Debug)]
pub struct TailService;


impl TailService {
    pub fn new() -> Self {
        TailService
    }
}

impl Service for TailService {
    // The type of the input requests we get.
    type Request = TailRequest;
    type Response = TailResponse;
    type Error = Error;
    type Future = futures::Finished<Self::Response, Self::Error>;

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        futures::finished(TailResponse::Done(LogPos::zero()))
    }
}
