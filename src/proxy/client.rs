use futures::{Future, Poll, Async, BoxFuture};
use replica::{ServerRequest, ServerResponse, LogPos};
use super::{ClientRequest, ClientResponse};
use sexp_proto::{self, client as sclient};
use service::Service;
use tokio::reactor::Handle;
use std::sync::Mutex;
use std::net::SocketAddr;
use std::marker::PhantomData;
use void::Void;


error_chain!{
    links {
        sexp_proto::Error, sexp_proto::ErrorKind, SexpP;
    }
}

// Currently, this is intended to be
#[derive(Debug)]
pub struct ProxyClient(Mutex<sclient::Client<ClientRequest, ClientResponse>>);

pub struct ProxyClientFut(BoxFuture<ClientResponse, sexp_proto::Error>);

impl Future for ProxyClientFut {
    type Item = ClientResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().map_err(|e| e.into())
    }
}

impl ProxyClient {
    pub fn new(handle: Handle, target: &SocketAddr) -> Self {
        let client0 = sclient::connect(handle, target);
        ProxyClient(Mutex::new(client0))
    }

    pub fn log_item(&self, val: &[u8]) -> ProxyClientFut {
        let req = ClientRequest::LogItem(val.to_vec());
        self.call(req)
    }

    pub fn await_commit(&self, pos: LogPos) -> ProxyClientFut {
        let req = ClientRequest::AwaitCommit(pos);
        self.call(req)
    }
}

impl Service for ProxyClient {
    type Request = ClientRequest;
    type Response = ClientResponse;
    type Error = Error;
    type Future = ProxyClientFut;

    fn poll_ready(&self) -> Async<()> {
        self.0.lock().expect("unlock").poll_ready()
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        ProxyClientFut(self.0.lock().expect("unlock").call(req))
    }
}
