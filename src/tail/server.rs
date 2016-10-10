use service::Service;
use futures::{self, Async};
use super::{TailRequest, TailResponse};
use store::Store;
use replica::LogPos;

use sexp_proto::Error;

/// The main interface to the outside world.
#[derive(Clone,Debug)]
pub struct TailService<S> {
    store: S,
}

impl<S: Store> TailService<S> {
    pub fn new(store: S) -> Self {
        TailService { store: store }
    }
}

impl<S: Store> Service for TailService<S> {
    // The type of the input requests we get.
    type Request = TailRequest;
    type Response = TailResponse;
    type Error = Error;
    type Future = futures::Finished<Self::Response, Self::Error>;

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
    fn call(&self, _req: Self::Request) -> Self::Future {
        futures::finished(TailResponse::Done(LogPos::zero()))
    }
}
