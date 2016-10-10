use futures::{Future, Poll, Async, BoxFuture};
use replica::LogPos;
use super::{TailRequest, TailResponse};
use sexp_proto::{self, client as sclient};
use service::Service;
use tokio::reactor::Handle;
use std::sync::Mutex;
use std::net::SocketAddr;
use Error;

// Currently, this is intended to be
#[derive(Debug)]
pub struct TailClient(Mutex<sclient::Client<TailRequest, TailResponse>>);

pub struct TailClientFut(BoxFuture<TailResponse, sexp_proto::Error>);

impl Future for TailClientFut {
    type Item = TailResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().map_err(|e| e.into())
    }
}

impl TailClient {
    pub fn new(handle: Handle, target: &SocketAddr) -> Self {
        let client0 = sclient::connect(handle, target);
        TailClient(Mutex::new(client0))
    }

    pub fn fetch_next(&self, pos: LogPos) -> TailClientFut {
        let req = TailRequest::FetchNextAfter(pos);
        self.call(req)
    }
}

impl Service for TailClient {
    type Request = TailRequest;
    type Response = TailResponse;
    type Error = Error;
    type Future = TailClientFut;

    fn poll_ready(&self) -> Async<()> {
        self.0.lock().expect("unlock").poll_ready()
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        TailClientFut(self.0.lock().expect("unlock").call(req))
    }
}
