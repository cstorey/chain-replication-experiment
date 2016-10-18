use tokio::reactor::Handle;
use {RamStore, sexp_proto, TailService, ServerService};

use std::net::SocketAddr;
use std::io;

pub trait Host : Sized {
    type Addr;

    fn build_server(&mut self,
                    service: CoreService,
                    handle: &Handle,
                    head_addr: Self::Addr,
                    tail_addr: Self::Addr)
                    -> Result<HostConfig<Self::Addr>, io::Error>;
}

#[derive(Debug, Clone)]
pub struct HostConfig<A> {
    pub head: A,
    pub tail: A,
}

#[derive(Debug)]
pub struct CoreService {
    head: ServerService<RamStore>,
    tail: TailService<RamStore>,
}

pub struct SexpHost;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BovineAddr(pub usize);

pub struct SphericalBovine;

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

    fn build_server(&mut self,
                    service: CoreService,
                    handle: &Handle,
                    head_addr: Self::Addr,
                    tail_addr: Self::Addr)
                    -> Result<HostConfig<Self::Addr>, io::Error> {
        let CoreService { head, tail } = service;
        let head_host = try!(sexp_proto::server::serve(handle, head_addr, head));
        let tail_host = try!(sexp_proto::server::serve(handle, tail_addr, tail));

        Ok(HostConfig {
            head: head_host.local_addr().clone(),
            tail: tail_host.local_addr().clone(),
        })
    }
}

impl SphericalBovine {
    pub fn new() -> Self {
        SphericalBovine
    }
}
impl Host for SphericalBovine {
    type Addr = BovineAddr;

    fn build_server(&mut self,
                    service: CoreService,
                    handle: &Handle,
                    head_addr: Self::Addr,
                    tail_addr: Self::Addr)
                    -> Result<HostConfig<Self::Addr>, io::Error> {
        let CoreService { head, tail } = service;
        let head_host = unimplemented!();
        let tail_host = unimplemented!();

        Ok(HostConfig {
            head: head_addr,
            tail: tail_addr,
        })
    }
}
