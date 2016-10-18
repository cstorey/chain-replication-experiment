use tokio::reactor::{Core, Handle};
use futures::Future;
use {RamStore, LogPos, sexp_proto, TailService, ServerService};

use std::net::SocketAddr;
use std::io;
use std::fmt;

use proto;

#[derive(Debug)]
pub struct CoreService {
    head: ServerService<RamStore>,
    tail: TailService<RamStore>,
}

pub struct SexpHost {
    head: proto::server::ServerHandle,
    tail: proto::server::ServerHandle,
}

impl fmt::Debug for SexpHost {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SexpHost")
           .field("head/addr", &self.head.local_addr())
           .field("tail/addr", &self.tail.local_addr())
           .finish()
    }
}

impl CoreService {
    pub fn new() -> Self {
        let store = RamStore::new();

        CoreService {
            head: ServerService::new(store.clone()),
            tail: TailService::new(store.clone()),
        }
    }

    pub fn into_server(self,
                       handle: &Handle,
                       head_addr: SocketAddr,
                       tail_addr: SocketAddr)
                       -> Result<SexpHost, io::Error> {
        let CoreService { head, tail } = self;
        let head_host = try!(sexp_proto::server::serve(handle, head_addr, head));
        let tail_host = try!(sexp_proto::server::serve(handle, tail_addr, tail));

        Ok(SexpHost {
            head: head_host,
            tail: tail_host,
        })
    }
}

impl SexpHost {}
