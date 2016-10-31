use futures::Future;
use tokio::reactor::Handle;
use {RamStore, sexp_proto, TailService, ServerService, Replicator, ReplicaClient};
use replica::HostConfig;

use std::net::SocketAddr;
use std::io;

pub fn build_server(handle: &Handle,
                    head_addr: SocketAddr,
                    tail_addr: SocketAddr)
                    -> Result<HostConfig, io::Error> {
    let store = RamStore::new();
    let head = ServerService::new(store.clone());
    let tail = TailService::new(store.clone());

    let downstream = {
        let handle = handle.clone();
        move |addr| ReplicaClient::connect(handle.clone(), &addr)
    };
    let head_host = try!(sexp_proto::server::serve(handle, head_addr, head));
    let tail_host = try!(sexp_proto::server::serve(handle, tail_addr, tail));

    let host_config = HostConfig {
        head: head_host.local_addr().clone(),
        tail: tail_host.local_addr().clone(),
    };

    let replica = Replicator::new(store.clone(), &host_config, downstream);
    handle.spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
    Ok(host_config)
}
