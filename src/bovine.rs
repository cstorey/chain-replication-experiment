use service::{Service, NewService};
use tokio::io::FramedIo;
use futures::{self, Poll, Async, Future, BoxFuture};

use std::io;
use std::sync::{Mutex, Arc};

use std::collections::{BTreeMap, VecDeque};

use hosting::{HostConfig, Host, CoreService, SchedHandle};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BovineAddr(pub usize);

type Thing<S, R, E> = Box<NewService<Request = S, Response = R, Error = E,
                           Item = Box<Service<Request = S, Response = R, Error = E, Future=BoxFuture<R, E>> + Send>,
                           > + Send>;

pub struct InnerBovine<S, R, E> {
    new_services: BTreeMap<BovineAddr, Box<NewService<Request = S, Response = R, Error = E,
                           Item = Box<Service<Request = S, Response = R, Error = E, Future=BoxFuture<R, E>> + Send>,
                           > + Send>>,
    connections: BTreeMap<BovineAddr, BoxFuture<(), io::Error>>,
}


pub struct SphericalBovine<S, R, E> {
    inner: Arc<Mutex<InnerBovine<S, R, E>>>,
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

impl<T: FramedIo<Out = Result<R, E>>, R, E> Service for BovineHostClient<T> {
    type Request = T::In;
    type Response = R;
    type Error = E;
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

impl<T: FramedIo<Out = Result<R, E>>, R, E> Future for BovineClientFut<T> {
    type Item = R;
    type Error = E;

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
        match inner.poll() {
            Ok(Async::Ready(Ok(r))) => Ok(Async::Ready(r)),
            Ok(Async::Ready(Err(e))) => Err(e),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => panic!("Fixme: {:?}", e),
        }
    }
}


struct ServiceBoxer<N>(N);
struct ServiceBox<S>(S);

impl<N: NewService> NewService for ServiceBoxer<N>
    where N::Item: Send + 'static,
          <N::Item as Service>::Future: Send + 'static
{
    type Request = N::Request;
    type Response = N::Response;
    type Error = N::Error;
    type Item = Box<Service<Request = N::Request,
                Response = N::Response,
                Error = N::Error,
                Future = BoxFuture<N::Response, N::Error>> + Send>;
    fn new_service(&self) -> Result<Self::Item, io::Error> {
        let s = try!(self.0.new_service());
        Ok(Box::new(ServiceBox(s)))
    }
}

impl<S: Service> Service for ServiceBox<S>
    where S::Future: Send + 'static
{
    type Request = S::Request;
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<S::Response, S::Error>;

    fn poll_ready(&self) -> Async<()> {
        self.0.poll_ready()
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        self.0.call(req).boxed()
    }
}

impl<S, R, E> SphericalBovine<S, R, E>
    where S: Send + 'static,
          R: Send + 'static,
          E: Send + 'static
{
    pub fn new() -> Self {
        let inner = InnerBovine {
            new_services: BTreeMap::new(),
            connections: BTreeMap::new(),
        };
        SphericalBovine { inner: Arc::new(Mutex::new(inner)) }
    }

    pub fn listen<N>(&mut self, service: N) -> BovineAddr
        where N: NewService<Request = S, Response = R, Error = E> + Send + 'static,
              N::Item: Send + 'static,
              <N::Item as Service>::Future: Send + 'static
    {
        let mut inner = self.inner.lock().expect("lock");

        let n = inner.new_services.keys().rev().next().map(|n| n.0 + 1).unwrap_or(0);
        let addr = BovineAddr(n);
        assert!(inner.new_services.get(&addr).is_none());

        inner.new_services.insert(addr.clone(), Box::new(ServiceBoxer(service)));
        addr
    }

    fn connect(&self,
               listener: BovineAddr)
               -> Result<BovineHostClient<BovinePort<S, Result<R, E>>>, io::Error> {
        let mut inner = self.inner.lock().expect("lock");

        let n = inner.connections.keys().rev().next().map(|n| n.0 + 1).unwrap_or(0);
        let addr = BovineAddr(n);
        assert!(inner.connections.get(&addr).is_none());

        let cts = Arc::new(Mutex::new(VecDeque::new()));
        let stc = Arc::new(Mutex::new(VecDeque::new()));

        let service_transport = BovinePort(stc.clone(), cts.clone());
        let client_transport = BovinePort(cts.clone(), stc.clone());

        let service = try!(inner.new_services[&listener].new_service());

        let t = BovineHostTask::new(service, service_transport);

        inner.connections.insert(addr, t.boxed());

        let c = BovineHostClient::new(client_transport);
        Ok(c)
    }
}
impl<S, R, E> Future for SphericalBovine<S, R, E> {
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

impl<S, R, E, H: SchedHandle> Host<H> for SphericalBovine<S, R, E> {
    type Addr = BovineAddr;

    fn build_server(&mut self,
                    _service: CoreService,
                    _handle: &H,
                    _head_addr: Self::Addr,
                    _tail_addr: Self::Addr)
                    -> Result<HostConfig<Self::Addr>, io::Error> {
        // let CoreService { head, tail } = service;
        let _head_host = unimplemented!();
        let _tail_host = unimplemented!();

        Ok(HostConfig {
            head: _head_addr,
            tail: _tail_addr,
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
    use service::{simple_service, Service};
    use std::io;
    use env_logger;
    use futures::{Async, Future, BoxFuture, Poll};
    use futures::task::{self, Unpark};
    use std::sync::Arc;
    use std::collections::VecDeque;

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
        let service = simple_service(|n: usize| -> Result<usize, io::Error> {
            debug!("Adding: {:?}", n);
            Ok(n + 1)
        });

        let mut net = SphericalBovine::new();

        let addr = net.listen(service);

        let client = net.connect(addr).expect("connect");

        let mut sched = Scheduler::new();
        sched.spawn(net.boxed());

        let t = client.call(42)
            .map(move |val| {
                info!("42+1 => {:?}", val);
                assert_eq!(val, 43);
            })
            .map_err(|e| panic!("Call error: {:?}", e));

        sched.run(t)
    }

    #[test]
    fn should_support_different_service_types() {
        // Common message type (eg: ByteBufs) but differet types of service.
        let mut net = SphericalBovine::new();
        let adder = simple_service(|n: usize| -> Result<usize, io::Error> { Ok(n + 1) });
        let subber = simple_service(|n: usize| -> Result<usize, io::Error> { Ok(n - 1) });
        let serv_addr0 = net.listen(adder);
        let serv_addr1 = net.listen(subber);

        let client0 = net.connect(serv_addr0).expect("connect");
        let client1 = net.connect(serv_addr1).expect("connect");

        let mut sched = Scheduler::new();
        sched.spawn(net.boxed());

        let t = client0.call(42)
            .join(client1.call(42))
            .map(move |vals| {
                info!("42+-1 => {:?}", vals);
                assert_eq!(vals, (43, 41));
            })
            .map_err(|e| panic!("Call error: {:?}", e));

        sched.run(t)
    }

    #[derive(Clone)]
    struct SexpAdaptor<S>(S);
    impl<S: Service> Service for SexpAdaptor<S>
        where S::Request: de::Deserialize,
              S::Response: ser::Serialize,
              S::Future: Send + 'static
    {
        type Request = Vec<u8>;
        type Response = Vec<u8>;
        type Error = S::Error;
        type Future = BoxFuture<Self::Request, S::Error>;

        fn poll_ready(&self) -> Async<()> {
            self.0.poll_ready()
        }
        fn call(&self, req: Self::Request) -> BoxFuture<Self::Response, Self::Error> {
            debug!("SexpAdaptor#call -> : {:?}", String::from_utf8_lossy(&req));
            let req = sexp::from_bytes(&req).expect("from_bytes");
            self.0.call(req).map(|res| {
                    let res = sexp::as_bytes(&res).expect("as_bytes");
                    debug!("SexpClient#call <- : {:?}", String::from_utf8_lossy(&res));
                    res
            }).boxed()
        }
    }


    #[derive(Clone)]
    struct SexpClient<I, S, R>(I, PhantomData<(S, R)>);
    use serde::{ser, de};
    use spki_sexp as sexp;
    use std::fmt;
    use std::marker::PhantomData;

    impl<I: Service<Request = Vec<u8>, Response = Vec<u8>>, S, R> Service for SexpClient<I, S, R>
        where S: ser::Serialize,
              R: de::Deserialize,
              I::Future: Send + 'static
    {
        type Request = S;
        type Response = R;
        type Error = I::Error;
        type Future = BoxFuture<Self::Response, I::Error>;

        fn poll_ready(&self) -> Async<()> {
            self.0.poll_ready()
        }
        fn call(&self, req: Self::Request) -> BoxFuture<Self::Response, Self::Error> {
            let req = sexp::as_bytes(&req).expect("as_bytes");
            debug!("SexpClient#call -> : {:?}", String::from_utf8_lossy(&req));
            self.0.call(req).map(|res| {
            debug!("SexpClient#call <- : {:?}", String::from_utf8_lossy(&res));
                    sexp::from_bytes(&res).expect("from_bytes")
                    }).boxed()
        }
    }


    #[test]
    fn should_work_with_type_adaptors() {
        env_logger::init().unwrap_or(());
        let service = simple_service(|n: usize| -> Result<usize, io::Error> {
            debug!("Adding: {:?}", n);
            Ok(n + 1)
        });

        let mut net: SphericalBovine<Vec<u8>, Vec<u8>, _> = SphericalBovine::new();
        let _: &Service<Request = usize, Response = usize, Future = _, Error = io::Error> = &service;
        let addr = net.listen(SexpAdaptor(service));

        let client = net.connect(addr).expect("connect");
        let _: &Service<Request = Vec<u8>, Response = Vec<u8>, Future = _, Error = _> =
            &client;

        let client = SexpClient(client, PhantomData);
        let _: &Service<Request = usize, Response = usize, Future = _, Error = io::Error> = &client;

        let mut sched = Scheduler::new();
        sched.spawn(net.boxed());

        let t = Service::call(&client, 42)
            .map(move |val| {
                info!("42+1 => {:?}", val);
                assert_eq!(val, 43);
            })
            .map_err(|e| panic!("Call error: {:?}", e));

        sched.run(t)
    }
}
