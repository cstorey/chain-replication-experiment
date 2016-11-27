use futures::{Future, Poll, Async, BoxFuture};
use super::{TailRequest, TailResponse};
use sexp_proto::{self, client as sclient};
use service::Service;
use tokio::reactor::Handle;
use std::sync::Mutex;
use std::net::SocketAddr;
use Error;


pub type InnerClient = sclient::Client<TailRequest, TailResponse>;
#[derive(Debug)]
pub struct TailClient(Mutex<InnerClient>);

pub struct TailClientFut(BoxFuture<TailResponse, sexp_proto::Error>);

impl Future for TailClientFut {
    type Item = TailResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().map_err(|e| e.into())
    }
}

impl TailClient {
    pub fn connect(handle: Handle, target: &SocketAddr) -> Self {
        let client0 = sclient::connect(handle, target);
        Self::new(client0)
    }
    pub fn new(client: InnerClient) -> Self {
        TailClient(Mutex::new(client))
    }
}

impl Service for TailClient {
    type Request = TailRequest;
    type Response = TailResponse;
    type Error = Error;
    type Future = TailClientFut;

    fn call(&self, req: Self::Request) -> Self::Future {
        TailClientFut(self.0.lock().expect("unlock").call(req))
    }
}
