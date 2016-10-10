use futures::{Future, Poll, Async, BoxFuture};
use messages::{ServerRequest, ServerResponse, LogPos};
use sexp_proto::{self, client as sclient};
use service::Service;
use tokio::reactor::Handle;
use std::sync::Mutex;
use std::net::SocketAddr;
use std::marker::PhantomData;
use void::Void;
use std::fmt;


error_chain!{
    links {
        sexp_proto::Error, sexp_proto::ErrorKind, SexpP;
    }
}

#[derive(Debug)]
pub struct ReplicaClient(Mutex<sclient::Client<ServerRequest, ServerResponse>>);

pub struct ReplicaClientFut(BoxFuture<ServerResponse, sexp_proto::Error>);

impl Future for ReplicaClientFut {
    type Item = ServerResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().map_err(|e| e.into())
    }
}

impl ReplicaClient {
    pub fn new(handle: Handle, target: &SocketAddr) -> Self {
        let client0 = sclient::connect(handle, target);
        ReplicaClient(Mutex::new(client0))
    }

    pub fn append_entry(&self,
                        assumed_offset: LogPos,
                        entry_offset: LogPos,
                        datum: Vec<u8>)
                        -> ReplicaClientFut {
        let req = ServerRequest::AppendLogEntry {
            assumed_offset: assumed_offset,
            entry_offset: entry_offset,
            datum: datum,
        };
        self.call(req)
    }

    pub fn await_commit(&self, pos: LogPos) -> ReplicaClientFut {
        let req = ServerRequest::CommitEntriesUpto { offset: pos };
        self.call(req)
    }
}

impl Service for ReplicaClient {
    type Request = ServerRequest;
    type Response = ServerResponse;
    type Error = Error;
    type Future = ReplicaClientFut;

    fn poll_ready(&self) -> Async<()> {
        self.0.lock().expect("unlock").poll_ready()
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        ReplicaClientFut(self.0.lock().expect("unlock").call(req))
    }
}


impl fmt::Debug for ReplicaClientFut {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("ReplicaClientFut").finish()
    }
}