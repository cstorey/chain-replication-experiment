use tokio::reactor::Handle;
use {RamStore, sexp_proto, TailService, ServerService};
use service::{Service, NewService};
use tokio::io::FramedIo;
use futures::{Poll, Async};
use proto::pipeline::Frame;

use std::net::SocketAddr;
use std::io;
use std::sync::{Mutex, Arc};

use std::collections::BTreeMap;
use void::Void;
use std::marker::PhantomData;

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

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BovineAddr(pub usize);

pub struct InnerBovine<S: Service> {
    new_services: BTreeMap<BovineAddr,
                           Box<NewService<Request = S::Request,
                                          Response = S::Response,
                                          Item = S,
                                          Error = S::Error>>>,
}

pub struct SphericalBovine<S: Service> {
    inner: Arc<Mutex<InnerBovine<S>>>,
}

#[derive(Debug)]
pub struct BovinePort<Req, Resp>(PhantomData<(Req, Resp)>);

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

impl<S: Service> SphericalBovine<S> {
    pub fn new() -> Self {
        let inner = InnerBovine { new_services: BTreeMap::new() };
        SphericalBovine { inner: Arc::new(Mutex::new(inner)) }
    }

    fn serve<N>(&self, service: N, handle: &Handle) -> Result<BovineAddr, io::Error>
        where N: NewService<Request = S::Request,
                            Response = S::Response,
                            Item = S,
                            Error = S::Error> + 'static
    {
        // try!(pipeline::Server::new(service, transport))
        let mut inner = self.inner.lock().expect("lock");
        let n = inner.new_services.len();
        let addr = BovineAddr(n);
        assert!(inner.new_services.get(&addr).is_none());
        inner.new_services.insert(addr, Box::new(service));
        Ok(addr)
    }

    fn connect_transport(&self,
                         addr: BovineAddr)
                         -> Result<BovinePort<S::Request, S::Response>, io::Error> {
        unimplemented!()
        // let cloned_ref = self.inner.clone();
        //
        // let mut inner = self.inner.lock().expect("lock");
        // let service = try!(self.new_services.new_service());
        //
        // let n = inner.connections.len();
        // assert!(inner.new_services.get(&n).is_none());
        //
        // inner.new_services.insert(n, Box::new(service));
        // Ok(BovinePort(cloned_ref)
        //
    }
}

impl<S: Service> Host for SphericalBovine<S> {
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

impl<Req, Resp> FramedIo for BovinePort<Req, Resp> {
    type In = Frame<Req, Void, io::Error>;
    type Out = Frame<Resp, Void, io::Error>;

    fn poll_read(&mut self) -> Async<()> {
        unimplemented!()
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        unimplemented!()
    }
    fn poll_write(&mut self) -> Async<()> {
        unimplemented!()
    }
    fn write(&mut self, msg: Self::In) -> Poll<(), io::Error> {
        unimplemented!()
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        unimplemented!()
    }
}



#[cfg(test)]
mod test {
    use super::*;
    use tokio::reactor::{Core, Handle};
    use proto::{self, pipeline, Message};
    use service::{simple_service, Service};
    use void::Void;
    use futures;
    use std::io;

    struct MyService;

    #[test]
    #[ignore]
    fn should_go_moo() {
        let mut core = Core::new().expect("Core::new");
        let service = simple_service(|n: usize| -> Result<usize, ()> { Ok(n + 1) });

        let net = SphericalBovine::new();
        let addr = net.serve(service, &core.handle()).expect("serve");

        let client : proto::Client<usize, usize, futures::stream::Empty<Void, io::Error>, io::Error> =
            pipeline::connect(|| net.connect_transport(addr.clone()), &core.handle());

        let f = client.call(Message::WithoutBody(42));

        let ret = core.run(f).expect("run");
        assert_eq!(ret, 42);
    }
}
