use futures::{Future, Poll, BoxFuture};
use super::{TailRequest, TailResponse};
use sexp_proto::client as sclient;
use service::Service;
use tokio::reactor::Handle;
use std::sync::Mutex;
use std::net::SocketAddr;
use Error;
use std::io;

pub type InnerClient = sclient::Client<TailRequest, TailResponse>;
#[derive(Debug)]
pub struct TailClient(Mutex<InnerClient>);

pub struct TailClientFut(BoxFuture<TailResponse, io::Error>);

impl Future for TailClientFut {
    type Item = TailResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().map_err(|e| e.into())
    }
}

impl TailClient {
    pub fn connect(handle: Handle, target: &SocketAddr) -> Box<Future<Item = Self, Error = Error>> {
        Box::new(sclient::connect(handle, target)
            .map(|client0| Self::new(client0))
            .map_err(|e| e.into()))
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
