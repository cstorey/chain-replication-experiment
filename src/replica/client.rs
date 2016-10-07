use futures::{Future, Poll, Async, BoxFuture};
use messages::{ServerRequest, ServerResponse, LogPos};
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

#[derive(Debug)]
pub struct ServerClient(Mutex<sclient::Client<ServerRequest, ServerResponse>>);

pub struct ServerClientFut(BoxFuture<ServerResponse, sexp_proto::Error>);

impl Future for ServerClientFut {
    type Item = ServerResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().map_err(|e| e.into())
    }
}

impl ServerClient {
    pub fn new(handle: Handle, target: &SocketAddr) -> Self {
        let client0 = sclient::connect(handle, target);
        ServerClient(Mutex::new(client0))
    }

    pub fn append_entry(&self,
                        assumed_offset: LogPos,
                        entry_offset: LogPos,
                        datum: &[u8])
                        -> ServerClientFut {
        let req = ServerRequest::AppendLogEntry {
            assumed_offset: assumed_offset,
            entry_offset: entry_offset,
            datum: datum.to_vec(),
        };
        self.call(req)
    }

    pub fn await_commit(&self, pos: LogPos) -> ServerClientFut {
        let req = ServerRequest::CommitEntriesUpto { offset: pos };
        self.call(req)
    }
}

impl Service for ServerClient {
    type Request = ServerRequest;
    type Response = ServerResponse;
    type Error = Error;
    type Future = ServerClientFut;

    fn poll_ready(&self) -> Async<()> {
        self.0.lock().expect("unlock").poll_ready()
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        ServerClientFut(self.0.lock().expect("unlock").call(req))
    }
}
