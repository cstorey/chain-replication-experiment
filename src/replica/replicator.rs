use futures::{Poll, Future, Async};
use tokio_service::Service;
use store::Store;
use {LogPos, Error};
use std::net::SocketAddr;
use std::mem;
use replica::{ReplicaRequest, ReplicaResponse, LogEntry};

pub trait DownstreamService {
    type Future: Future<Item = ReplicaResponse, Error = Error>;
    fn call(&self, req: ReplicaRequest) -> Self::Future;
}

pub trait NewDownstreamService {
    type Item: DownstreamService;
    fn new_downstream(&self, addr: SocketAddr) -> Self::Item;
}

impl<D, F> DownstreamService for D
    where D: Service<Request = ReplicaRequest,
                     Response = ReplicaResponse,
                     Error = Error,
                     Future = F>,
          F: Future<Item = ReplicaResponse, Error = Error> + Send
{
    type Future = D::Future;

    fn call(&self, req: ReplicaRequest) -> Self::Future {
        Service::call(self, req)
    }
}

impl<F, D> NewDownstreamService for F
    where F: Fn(SocketAddr) -> D,
          D: DownstreamService
{
    type Item = D;
    fn new_downstream(&self, addr: SocketAddr) -> D {
        self(addr)
    }
}

enum ReplicatorState<S: Store, N>
    where N: NewDownstreamService
{
    Idle,
    Fetching(S::FetchFut),
    Forwarding(<N::Item as DownstreamService>::Future),
}


pub struct Replicator<S: Store, N: NewDownstreamService> {
    store: S,
    new_downstream: N,
    downstream: Option<N::Item>,
    last_seen_seq: LogPos,
    state: ReplicatorState<S, N>,
}

impl<S: Store, N: NewDownstreamService> Replicator<S, N> {
    pub fn new(store: S, new_downstream: N) -> Self {
        Replicator {
            store: store,
            new_downstream: new_downstream,
            downstream: None,
            last_seen_seq: LogPos::zero(),
            state: ReplicatorState::Idle,
        }
    }
}

impl<S: Store, N: NewDownstreamService> Drop for Replicator<S, N> {
    fn drop(&mut self) {
        debug!("Drop Replicator");
    }
}

impl<S: Store, N: NewDownstreamService> Future for Replicator<S, N> {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<(), Error> {
        debug!("Replicator#poll");
        loop {
            match mem::replace(&mut self.state, ReplicatorState::Idle) {
                ReplicatorState::Idle => {
                    debug!("Idle, fetching after: {:?}", self.last_seen_seq);
                    let fetch_f = self.store.fetch_next(self.last_seen_seq);
                    self.state = ReplicatorState::Fetching(fetch_f);
                }
                // Now, what have we learnt from this, children? Surely it's
                // that because we're forwarding LogEntries at least, we
                // should make it easy to get a LogEntry into/out of the
                // store, rather than having to futz around with serialisation
                // rubbish.
                ReplicatorState::Fetching(mut fetch_f) => {
                    debug!("Fetching");
                    match try!(fetch_f.poll()) {
                        Async::NotReady => {
                            self.state = ReplicatorState::Fetching(fetch_f);
                            return Ok(Async::NotReady);
                        }
                        Async::Ready((off, val)) => {
                            debug!("Fetched: {:?}", (&off, &val));
                            if let &LogEntry::Config(ref conf) = &val {
                                debug!("logged Config message: {:?}", conf);
                                // FIXME: Well, this is blatantly wrong.
                                debug!("connecting downstream to: {:?}", conf.head);
                                self.downstream = Some(self.new_downstream
                                    .new_downstream(conf.head));
                            };

                            if let Some(ref downstream) = self.downstream {
                                debug!("Forward {:?}", off);
                                let req = ReplicaRequest::AppendLogEntry {
                                    assumed_offset: self.last_seen_seq,
                                    entry_offset: off,
                                    datum: val,
                                };
                                let _: &DownstreamService<Future = _> = downstream;
                                let f = downstream.call(req);
                                self.state = ReplicatorState::Forwarding(f);
                            } else {
                                debug!("No downstream at {:?}", off);
                            }
                            self.last_seen_seq = off;
                        }
                    }
                }
                ReplicatorState::Forwarding(mut f) => {
                    match try!(f.poll()) {
                        Async::NotReady => {
                            self.state = ReplicatorState::Forwarding(f);
                            return Ok(Async::NotReady);
                        }
                        Async::Ready(ReplicaResponse::Done(pos)) => {
                            debug!("woo! {:?}", pos);
                        }
                        Async::Ready(ReplicaResponse::BadSequence(pos)) => {
                            debug!("Bad sequence: resetting to {:?}", pos);
                            self.last_seen_seq = pos;
                        }
                    }
                }
            }
        }
    }
}



#[cfg(test)]
mod test {
    use futures::{self, Future, BoxFuture, Async};
    use futures::stream::Stream;
    use service::Service;
    use tokio::channel;
    use replica::LogPos;
    use store::{RamStore, Store};
    use replica::{ReplicaRequest, ReplicaResponse, LogEntry, HostConfig};
    use errors::Error;
    use std::net::SocketAddr;
    use super::*;
    use tokio::reactor::Core;
    use std::time::Duration;
    use std::io;
    use tokio_timer;
    use env_logger;

    type MessageType = ((SocketAddr, ReplicaRequest), futures::Complete<ReplicaResponse>);
    struct MyMagicalDownstream {
        addr: SocketAddr,
        send: channel::Sender<MessageType>,
    }

    struct MyDownstreamBuilder {
        send: channel::Sender<MessageType>,
    }

    impl Service for MyMagicalDownstream {
        type Request = ReplicaRequest;
        type Response = ReplicaResponse;
        type Error = Error;
        type Future = BoxFuture<ReplicaResponse, Error>;
        fn poll_ready(&self) -> Async<()> {
            Async::Ready(())
        }
        fn call(&self, req: Self::Request) -> Self::Future {
            let (c, p) = futures::oneshot();
            match self.send.send(((self.addr.clone(), req), c)) {
                Ok(()) => (),
                Err(e) => return futures::failed(e.into()).boxed(),
            };
            p.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe").into())
                .boxed()
        }
    }

    impl NewDownstreamService for MyDownstreamBuilder {
        type Item = MyMagicalDownstream;
        fn new_downstream(&self, addr: SocketAddr) -> Self::Item {
            MyMagicalDownstream {
                addr: addr,
                send: self.send.clone(),
            }
        }
    }

    #[test]
    fn should_start_replicating_to_downstream_on_committed_config_message() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();
        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();

        let mut core = Core::new().expect("core::new");
        let (tx, rx) = channel::channel(&core.handle()).expect("channel");


        let downstream = MyDownstreamBuilder { send: tx };

        let replica = Replicator::new(store.clone(), downstream);

        core.handle().spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
        debug!("Spawned replica task");

        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let config: HostConfig<::std::net::SocketAddr> = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };
        let off = LogPos::zero();
        let _ = append_entry(&store, &mut core, off, LogEntry::Config(config.clone()));

        let (_rx, _response, _assumed0, _off0, entry0) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        assert_eq!(entry0, LogEntry::Config(config));
    }

    #[test]
    fn should_send_next_log_entry_on_okay_from_downstream() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();
        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();

        let mut core = Core::new().expect("core::new");
        let (tx, rx) = channel::channel(&core.handle()).expect("channel");

        let downstream = MyDownstreamBuilder { send: tx };

        let replica = Replicator::new(store.clone(), downstream);

        core.handle().spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
        debug!("Spawned replica task");

        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let config: HostConfig<::std::net::SocketAddr> = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let off0 = append_entry(&store,
                                &mut core,
                                LogPos::zero(),
                                LogEntry::Config(config.clone()));
        let _off1 = append_entry(&store,
                                 &mut core,
                                 off0,
                                 LogEntry::Data(b"Hello world!".to_vec()));

        let (rx, response, _assumed0, off0, _entry0) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        response.complete(ReplicaResponse::Done(off0));

        let (_rx, _response, assumed1, off1, entry1) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        assert_eq!((assumed1, entry1),
                   (off0, LogEntry::Data(b"Hello world!".to_vec())));
        assert!(off1 > assumed1);
    }

    #[test]
    fn should_restart_replication_on_badsequence_from_downstream() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();
        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();

        let mut core = Core::new().expect("core::new");
        let (tx, rx) = channel::channel(&core.handle()).expect("channel");


        let downstream = MyDownstreamBuilder { send: tx };

        let replica = Replicator::new(store.clone(), downstream);

        core.handle().spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
        debug!("Spawned replica task");

        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let config: HostConfig<::std::net::SocketAddr> = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let log_off = LogPos::zero();
        let log_off0 = append_entry(&store, &mut core, log_off, LogEntry::Config(config.clone()));
        let log_off1 = append_entry(&store,
                                    &mut core,
                                    log_off0,
                                    LogEntry::Data(b"Hello".to_vec()));
        let log_off2 = append_entry(&store,
                                    &mut core,
                                    log_off1,
                                    LogEntry::Data(b"world!".to_vec()));

        let (rx, response, _assumed0, _off0, _entry0) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        // So we expect the delivery of "world!"
        let resp = ReplicaResponse::BadSequence(log_off1);
        println!("Respond with: {:?}", resp);
        response.complete(resp);
        let (_rx, _response, assumed1, off1, entry1) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        assert_eq!((assumed1, off1, entry1),
                   (log_off1, log_off2, LogEntry::Data(b"world!".to_vec())));
    }


    fn append_entry<S: Store>(store: &S, core: &mut Core, off: LogPos, entry: LogEntry) -> LogPos {
        let next = off.next();
        debug!("append config message {:?} -> {:?}", off, next);
        core.run(store.append_entry(off, next, entry))
            .expect("append");
        next
    }

    fn take_next_entry(rx: channel::Receiver<MessageType>,
                       target: &SocketAddr,
                       core: &mut Core,
                       timer: &tokio_timer::Timer)
                       -> (channel::Receiver<MessageType>,
                           futures::Complete<ReplicaResponse>,
                           LogPos,
                           LogPos,
                           LogEntry) {
        let f = timer.timeout(rx.into_future().map_err(|(e, _)| e),
                              Duration::from_millis(1000));
        debug!("wait for stub message");
        let (data, rx) = core.run(f).expect("receive downstream");
        let ((addr, msg), response) = data.expect("Some message");
        assert_eq!(&addr, target);
        println!("Some: {:?}", (&addr, &msg));
        // Expect that the target at `head_addr` receives a set of replication messages.
        match msg {
            ReplicaRequest::AppendLogEntry { assumed_offset, entry_offset, datum } => {
                (rx, response, assumed_offset, entry_offset, datum)
            }
        }
    }
}
