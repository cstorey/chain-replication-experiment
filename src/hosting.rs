use tokio::reactor::Handle;
use {RamStore, sexp_proto, TailService, ServerService};
use replica::HostConfig;

use std::net::SocketAddr;
use std::io;

#[derive(Debug)]
pub struct CoreService {
    head: ServerService<RamStore>,
    tail: TailService<RamStore>,
}

impl CoreService {
    pub fn new() -> Self {
        let store = RamStore::new();

        CoreService {
            head: ServerService::new(store.clone()),
            tail: TailService::new(store.clone()),
        }
    }

    pub fn build_server(self,
                        handle: &Handle,
                        head_addr: SocketAddr,
                        tail_addr: SocketAddr)
                        -> Result<HostConfig<SocketAddr>, io::Error> {
        let CoreService { head, tail } = self;
        let head_host = try!(sexp_proto::server::serve(handle, head_addr, head));
        let tail_host = try!(sexp_proto::server::serve(handle, tail_addr, tail));

        Ok(HostConfig {
            head: head_host.local_addr().clone(),
            tail: tail_host.local_addr().clone(),
        })
    }
}
