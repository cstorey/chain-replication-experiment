use tokio::reactor::Handle;
use {RamStore, sexp_proto, TailService, ServerService};

use std::net::SocketAddr;
use std::io;
use std::fmt;

use proto;

pub trait Host : Sized {
    type Addr;

    fn build_server(service: CoreService,
                    handle: &Handle,
                    head_addr: Self::Addr,
                    tail_addr: Self::Addr)
                    -> Result<Self, io::Error>;

    fn head_addr(&self) -> Self::Addr;
    fn tail_addr(&self) -> Self::Addr;
}

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
}

impl Host for SexpHost {
    type Addr = SocketAddr;

    fn build_server(service: CoreService,
                    handle: &Handle,
                    head_addr: Self::Addr,
                    tail_addr: Self::Addr)
                    -> Result<Self, io::Error> {
        let CoreService { head, tail } = service;
        let head_host = try!(sexp_proto::server::serve(handle, head_addr, head));
        let tail_host = try!(sexp_proto::server::serve(handle, tail_addr, tail));

        Ok(SexpHost {
            head: head_host,
            tail: tail_host,
        })
    }
    fn head_addr(&self) -> Self::Addr {
        self.head.local_addr().clone()
    }
    fn tail_addr(&self) -> Self::Addr {
        self.tail.local_addr().clone()
    }
}
