use tokio::reactor::Handle;
use {RamStore, sexp_proto, TailService, ServerService};
use service::{Service, NewService, simple_service};
use tokio::io::FramedIo;
use futures::{self, Poll, Async, Future, stream, BoxFuture};
use proto;

use std::net::SocketAddr;
use std::io;
use std::sync::{Mutex, Arc};

use std::collections::{BTreeMap, VecDeque};
use void::Void;
use take::Take;
use std::marker::PhantomData;
use std::fmt;

use hosting::{HostConfig, Host, CoreService, SchedHandle};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BovineAddr(pub usize);

pub struct InnerBovine<S: Service> {
    new_service: Box<NewService<Request = S::Request,
                                Response = S::Response,
                                Item = S,
                                Error = S::Error> + Send>,
    connections: BTreeMap<usize, BoxFuture<(), io::Error>>,
}

pub struct SphericalBovine<S: Service> {
    inner: Arc<Mutex<InnerBovine<S>>>,
}

pub struct BovinePort<S, R>(Arc<Mutex<VecDeque<S>>>, Arc<Mutex<VecDeque<R>>>);

pub struct BovineHostTask<S: Service, T> {
    service: S,
    transport: T,
    pending: Option<S::Future>,
}

impl<S: Service, T> BovineHostTask<S, T> {
    fn new(service: S, transport: T) -> Self {
        BovineHostTask {
            service: service,
            transport: transport,
            pending: None,
        }
    }
}

pub struct BovineHostClient<T: FramedIo> {
    transport: Arc<Mutex<T>>,
    queue: Arc<Mutex<VecDeque<futures::Complete<T::Out>>>>,
}

impl<T: FramedIo> BovineHostClient<T> {
    fn new(transport: T) -> Self {
        BovineHostClient {
            transport: Arc::new(Mutex::new(transport)),
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl<S: Service, T: FramedIo<Out=S::Request, In=Result<S::Response, S::Error>>> Future for BovineHostTask<S, T> {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("BovineHostTask#poll");
        loop {
            if self.transport.poll_write().is_ready() {
                if let Some(mut f) = self.pending.take() {
                    trace!("has pending");
                    match f.poll() {
                        Ok(Async::Ready(resp)) => {
                            trace!("-> done");
                            self.transport.write(Ok(resp)).expect("write resp");
                        }
                        Err(e) => {
                            trace!("-> error");
                            self.transport.write(Err(e)).expect("write resp");
                        }
                        Ok(Async::NotReady) => {
                            self.pending = Some(f);
                        }
                    }
                }
            }
            assert!(self.pending.is_none());
            let req = try_ready!(self.transport.read());
            trace!("-> new req");
            let resp_f = self.service.call(req);
            self.pending = Some(resp_f);
        }
    }
}

pub struct BovineClientFut<T: FramedIo> {
    inner: futures::Oneshot<T::Out>,
    transport: Arc<Mutex<T>>,
    queue: Arc<Mutex<VecDeque<futures::Complete<T::Out>>>>,
}

impl<T: FramedIo> Service for BovineHostClient<T> {
    type Request = T::In;
    type Response = T::Out;
    type Error = futures::Canceled;
    type Future = BovineClientFut<T>;

    fn poll_ready(&self) -> Async<()> {
        trace!("BovineHostClient#poll_ready");
        self.transport.lock().expect("lock").poll_write()
    }

    fn call(&self, _req: Self::Request) -> Self::Future {
        trace!("BovineHostClient#call");
        let (c, p) = futures::oneshot();
        {
            let mut q = self.queue.lock().expect("lock");
            self.transport.lock().expect("lock").write(_req).expect("write");
            q.push_back(c);
        }
        BovineClientFut {
            inner: p,
            transport: self.transport.clone(),
            queue: self.queue.clone(),
        }
    }
}

impl<T: FramedIo> Future for BovineClientFut<T> {
    type Item = T::Out;
    type Error = futures::Canceled;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let &mut BovineClientFut { ref mut inner, ref mut transport, ref mut queue } = self;
        {
            let mut q = queue.lock().expect("lock queue");
            let mut t = transport.lock().expect("lock transport");
            if let Some(c) = q.pop_front() {
                match t.read() {
                    Ok(Async::Ready(val)) => c.complete(val),
                    Ok(Async::NotReady) => q.push_front(c),
                    Err(e) => panic!("Unexpected error on read: {:?}", e),
                }
            }
        }
        inner.poll()
    }
}

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

    fn connect(&self)
               -> Result<BovineHostClient<BovinePort<S::Request, Result<S::Response, S::Error>>>, io::Error> {
        let cloned_ref = self.inner.clone();
        let mut inner = self.inner.lock().expect("lock");

        let n = inner.connections.keys().rev().next().map(|n| n + 1).unwrap_or(0);
        assert!(inner.connections.get(&n).is_none());

        let cts = Arc::new(Mutex::new(VecDeque::new()));
        let stc = Arc::new(Mutex::new(VecDeque::new()));

        let service_transport = BovinePort(stc.clone(), cts.clone());
        let client_transport = BovinePort(cts.clone(), stc.clone());

        let service = try!(inner.new_service.new_service());

        let t = BovineHostTask::new(service, service_transport);

        inner.connections.insert(n, t.boxed());

        let c = BovineHostClient::new(client_transport);
        Ok(c)
    }
}
impl<S: Service> Future for SphericalBovine<S> {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("SphericalBovine#poll");
        let mut inner = self.inner.lock().expect("lock");
        for (addr, serv) in inner.connections.iter_mut() {
            trace!("-> SphericalBovine@{:?}#poll", addr);
            match serv.poll() {
                Ok(Async::Ready(())) => unimplemented!(),
                Ok(Async::NotReady) => (),
                Err(e) => panic!("service {:?} failed with {:?}", addr, e),
            };
            trace!("<- SphericalBovine@{:?}#poll", addr);
        }
        Ok(Async::NotReady)
    }
}

impl<S: Service, H: SchedHandle> Host<H> for SphericalBovine<S> {
    type Addr = BovineAddr;

    fn build_server(&mut self,
                    _service: CoreService,
                    _handle: &H,
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
        trace!("BovinePort#poll_read");
        // let &mut BovinePort(ref inner, ref id) = self;
        unimplemented!()
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        trace!("BovinePort#read");
        let &mut BovinePort(_, ref recv) = self;
        let mut recv = recv.lock().expect("lock");
        trace!("recv queue pre: {:?}", recv.len());
        if let Some(res) = recv.pop_front() {
            debug!("-> value");
            Ok(Async::Ready(res))
        } else {
            Ok(Async::NotReady)
        }
    }
    fn poll_write(&mut self) -> Async<()> {
        trace!("BovinePort#poll_write");
        Async::Ready(())
    }
    fn write(&mut self, msg: Self::In) -> Poll<(), io::Error> {
        trace!("BovinePort#write");
        let &mut BovinePort(ref send, _) = self;
        let mut send = send.lock().expect("lock");
        send.push_back(msg);
        trace!("send queue post: {:?}", send.len());
        Ok(Async::Ready(()))
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        // let &mut BovinePort(ref inner, ref id) = self;
        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::reactor::Core;
    use service::{simple_service, Service};
    use std::io;
    use env_logger;
    use futures::{Async, Future, BoxFuture};
    use futures::task::{self, Unpark};
    use std::sync::Arc;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Unparker;

    impl Unpark for Unparker {
        fn unpark(&self) {
            debug!("Unpark!");
        }
    }

    struct Scheduler {
        tasks: VecDeque<task::Spawn<BoxFuture<(), ()>>>,
    }

    impl Scheduler {
        fn new() -> Self {
            Scheduler { tasks: VecDeque::new() }
        }

        fn spawn<F: Future<Item = (), Error = ()> + 'static + Send>(&mut self, f: F) {
            self.tasks.push_back(task::spawn(f.boxed()));
        }

        fn run<F: Future<Item = (), Error = ()> + 'static + Send>(&mut self, f: F) {
            let unparker = Arc::new(Unparker);
            let mut foreground = VecDeque::new();
            foreground.push_back(task::spawn(f.boxed()));

            loop {
                for q in &mut [&mut foreground, &mut self.tasks] {
                    if let Some(mut t) = q.pop_front() {
                        match t.poll_future(unparker.clone()) {
                            Ok(Async::NotReady) => q.push_back(t),
                            Ok(Async::Ready(())) => { }
                            Err(e) => panic!("Task failed: {:?}", e),
                        };
                    }
                }

                debug!("Foreground: {:?}; Background tasks: {:?}",
                       foreground.len(),
                       self.tasks.len());
                if foreground.is_empty() {
                    break;
                }
            }
        }
    }

    #[test]
    fn should_go_moo() {
        env_logger::init().unwrap_or(());
        let mut core = Core::new().expect("Core::new");
        let service = simple_service(|n: usize| -> Result<usize, io::Error> {
            debug!("Adding: {:?}", n);
            Ok(n + 1)
        });

        let net = SphericalBovine::new(service);

        let handle = core.handle();
        let client = net.connect().expect("connect");

        let mut sched = Scheduler::new();

        let running = Arc::new(AtomicBool::new(false));

        sched.spawn(net.boxed());

        let t = client.call(42)
            .map(move |val| {
                info!("42+1 => {:?}", val);
                assert_eq!(val.expect("result"), 43);
            })
            .map_err(|e| panic!("Call error: {:?}", e));

        sched.run(t)
    }
}
