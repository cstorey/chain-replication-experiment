use futures::{Future, Poll, BoxFuture};
use super::{ReplicaRequest, ReplicaResponse};
use sexp_proto::client as sclient;
use service::Service;
use tokio::reactor::Handle;
use std::sync::Mutex;
use std::net::SocketAddr;
use std::fmt;
use std::io;
use Error;

pub type InnerClient = sclient::Client<ReplicaRequest, ReplicaResponse>;
#[derive(Debug)]
pub struct ReplicaClient(Mutex<InnerClient>, SocketAddr);

pub struct ReplicaClientFut(BoxFuture<ReplicaResponse, io::Error>);

impl Future for ReplicaClientFut {
    type Item = ReplicaResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().map_err(|e| e.into())
    }
}

impl ReplicaClient {
    pub fn connect(handle: Handle, target: &SocketAddr) -> Box<Future<Item = Self, Error = Error>> {
        let target = target.clone();
        Box::new(sclient::connect(handle, &target)
            .map(move |client0| Self::new(client0, &target))
            .map_err(|e| e.into()))
    }
    pub fn new(client: InnerClient, target: &SocketAddr) -> Self {
        ReplicaClient(Mutex::new(client), target.clone())
    }
}

impl Service for ReplicaClient {
    type Request = ReplicaRequest;
    type Response = ReplicaResponse;
    type Error = Error;
    type Future = ReplicaClientFut;

    fn call(&self, req: Self::Request) -> Self::Future {
        let addr = self.1;
        debug!("{}: call: {:?}", addr, req);
        let f = self.0.lock().expect("unlock").call(req);
        ReplicaClientFut(f)
    }
}


impl fmt::Debug for ReplicaClientFut {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("ReplicaClientFut").finish()
    }
}
