use tokio::reactor::Handle;
use {RamStore, sexp_proto, TailService, ServerService};
use service::{Service, NewService, simple_service};
use tokio::io::FramedIo;
use futures::{self, Poll, Async, Future, stream, BoxFuture};
use proto::pipeline::{self, Frame};
use proto::{self, Message};

use std::net::SocketAddr;
use std::io;
use std::sync::{Mutex, Arc};

use std::collections::{BTreeMap, VecDeque};
use void::Void;
use take::Take;
use std::marker::PhantomData;

use hosting::{HostConfig, Host, CoreService};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BovineAddr(pub usize);

pub struct InnerBovine<S: Service> {
    new_service: Box<NewService<Request = S::Request,
                                Response = S::Response,
                                Item = S,
                                Error = S::Error> + Send>,
    connections: BTreeMap<usize, ()>,
}

pub struct SphericalBovine<S: Service> {
    inner: Arc<Mutex<InnerBovine<S>>>,
}

pub struct BovinePort<S, R>(Arc<Mutex<VecDeque<S>>>, Arc<Mutex<VecDeque<R>>>);

impl<S: Service> SphericalBovine<S>
    where S::Request: Send + 'static,
          S::Response: Send + 'static,
          S::Future: Send + 'static,
          S: Send + Sync + 'static,
          S::Error: From<proto::Error<S::Error>> + Send + 'static
{
    pub fn new<N>(service: N) -> Self
        where N: NewService<Request = S::Request,
                            Response = S::Response,
                            Item = S,
                            Error = S::Error> + Send + 'static
    {
        let inner = InnerBovine {
            new_service: Box::new(service),
            connections: BTreeMap::new(),
        };
        SphericalBovine { inner: Arc::new(Mutex::new(inner)) }
    }

    fn connect_transport(&self,
                         handle: &Handle)
                         -> Result<BovinePort<Frame<Message<S::Request,
                                                            stream::Empty<Void, Void>>,
                                                    Void,
                                                    S::Error>,
                                              Frame<Message<S::Response,
                                                            stream::Empty<Void, Void>>,
                                                    Void,
                                                    S::Error>>,
                                   io::Error> {
        let cloned_ref = self.inner.clone();
        let mut inner = self.inner.lock().expect("lock");

        let n = inner.connections.keys().rev().next().map(|n| n + 1).unwrap_or(0);
        assert!(inner.connections.get(&n).is_none());

        let cts = Arc::new(Mutex::new(VecDeque::new()));
        let stc = Arc::new(Mutex::new(VecDeque::new()));

        let service_transport = BovinePort(stc.clone(), cts.clone());
        let client_transport = BovinePort(cts.clone(), stc.clone());

        let service = try!(inner.new_service.new_service());
        let service = simple_service(move |req: Message<S::Request, _>| -> BoxFuture<Message<S::Response,
                                                                stream::Empty<Void, S::Error>>,
                                                        S::Error> {
            service.call(req.into_inner()).map(Message::WithoutBody).boxed()
        });

        let _: &pipeline::Transport<In = S::Response, /* Message<S::Response, stream::Empty<Void, Void>>, */
                                    Out = Message<S::Request, stream::Empty<Void, Void>>,
                                    BodyIn = Void,
                                    BodyOut = Void,
                                    Error = S::Error> = &service_transport;

        let task_f = try!(pipeline::Server::new(service, service_transport));
        let () = handle.spawn(task_f.map_err(|e| panic!("transport error? {:?}", e)));

        inner.connections.insert(n, ());
        // Ok(client_transport)
        unimplemented!()
    }
}

impl<S: Service> Host for SphericalBovine<S> {
    type Addr = BovineAddr;

    fn build_server(&mut self,
                    _service: CoreService,
                    _handle: &Handle,
                    head_addr: Self::Addr,
                    tail_addr: Self::Addr)
                    -> Result<HostConfig<Self::Addr>, io::Error> {
        // let CoreService { head, tail } = service;
        let _head_host = unimplemented!();
        let _tail_host = unimplemented!();

        Ok(HostConfig {
            head: head_addr,
            tail: tail_addr,
        })
    }
}

impl<S, R> FramedIo for BovinePort<S, R> {
    type In = S;
    type Out = R;

    fn poll_read(&mut self) -> Async<()> {
        let &mut BovinePort(ref inner, ref id) = self;
        unimplemented!()
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        let &mut BovinePort(ref inner, ref id) = self;
        // unimplemented!()
        Ok(Async::NotReady)
    }
    fn poll_write(&mut self) -> Async<()> {
        let &mut BovinePort(ref inner, ref id) = self;
        let inner = inner.lock().expect("lock");
        // let ref service = inner.connections[id];
        // service.poll_ready()
        unimplemented!()
    }
    fn write(&mut self, _msg: Self::In) -> Poll<(), io::Error> {
        let &mut BovinePort(ref inner, ref id) = self;
        let inner = inner.lock().expect("lock");
        // Not quite right, should be a pipeline/ FramedIo thingy.
        // let ref blah: S = inner.connections[id];
        unimplemented!();
        Ok(Async::Ready(()))
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        let &mut BovinePort(ref inner, ref id) = self;
        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::reactor::{Core, Handle};
    use proto::{self, pipeline, Message};
    use service::{simple_service, Service};
    use void::Void;
    use take::Take;
    use futures;
    use std::io;

    #[cfg(never)]
    #[test]
    fn should_go_moo() {
        let mut core = Core::new().expect("Core::new");
        let service = simple_service(|n: usize| -> Result<usize, ()> { Ok(n + 1) });

        let net = SphericalBovine::new(service);

        let handle = core.handle();
        let client_transport = net.connect_transport(&handle).expect("connect_transport");
        let new_transport = Take::new(move || Ok(client_transport));
        let _: &pipeline::NewTransport<In = Message<usize, _>,
                                       Out = usize,
                                       BodyIn = Void,
                                       BodyOut = Void,
                                       Error = io::Error,
                                       Item = BovinePort<_, _>,
                                       Future = futures::Done<_, _>> = &new_transport;
        let client: proto::Client<_, _, futures::stream::Empty<Void, io::Error>, io::Error> =
            pipeline::connect(new_transport, handle);

        // let client : proto::Client<usize, usize, futures::stream::Empty<Void, io::Error>, io::Error> = pipeline::connect(|| , &core.handle());

        let f = client.call(Message::WithoutBody(Message::WithoutBody(42)));

        let ret = core.run(f).expect("run");
        assert_eq!(ret, 42);
    }
}
