use futures::{Poll, Future, Async};
use tokio_service::Service;
use store::Store;
use {LogPos, Error};
use std::net::SocketAddr;
use replica::{ReplicaRequest, ReplicaResponse, LogEntry, HostConfig};

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
        debug!("new downstream: {:?}", addr);
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
    identity: HostConfig,
}

impl<S: Store, N: NewDownstreamService> Replicator<S, N> {
    pub fn new(store: S, identity: &HostConfig, new_downstream: N) -> Self {
        Replicator {
            store: store,
            new_downstream: new_downstream,
            downstream: None,
            last_seen_seq: LogPos::zero(),
            state: ReplicatorState::Idle,
            identity: identity.clone(),
        }
    }

    fn process_config_message(&mut self, conf: &HostConfig) {
        debug!("{}: logged Config message: {:?}", self.identity, conf);
        // FIXME: Well, this is blatantly wrong.
        debug!("connecting downstream to: {:?}", conf.head);
        self.downstream = Some(self.new_downstream
            .new_downstream(conf.head));
    }

    fn try_take_next(&mut self) -> Poll<(), Error> {
        if let ReplicatorState::Idle = self.state {
            debug!("{}: Idle, fetching after: {:?}",
                   self.identity,
                   self.last_seen_seq);
            let fetch_f = self.store.fetch_next(self.last_seen_seq);
            self.state = ReplicatorState::Fetching(fetch_f);
        }
        Ok(Async::Ready(()))
    }

    fn try_process_message(&mut self) -> Poll<(), Error> {
        let (off, val) = if let &mut ReplicatorState::Fetching(ref mut fetch_f) = &mut self.state {
            debug!("{}: Fetching", self.identity);
            try_ready!(fetch_f.poll())
        } else {
            return Ok(Async::Ready(()));
        };

        debug!("Fetched: {:?}", (&off, &val));
        if let &LogEntry::Config(ref conf) = &val {
            self.process_config_message(conf);
        };

        self.state = if let Some(ref downstream) = self.downstream {
            debug!("Forward {:?} -> {:?}", self.last_seen_seq, off);
            let req = ReplicaRequest::AppendLogEntry {
                assumed_offset: self.last_seen_seq,
                // FIXME: NO. WRONG.
                entry_offset: off,
                datum: val,
            };
            ReplicatorState::Forwarding(downstream.call(req))
        } else {
            debug!("No downstream at {:?}", off);
            ReplicatorState::Idle
        };

        self.last_seen_seq = off;
        Ok(Async::Ready(()))
    }

    fn try_process_forward(&mut self) -> Poll<(), Error> {
        let resp = if let &mut ReplicatorState::Forwarding(ref mut f) = &mut self.state {
            try_ready!(f.poll())
        } else {
            return Ok(Async::Ready(()));
        };

        debug!("{}: Forward response:{:?}", self.identity, resp);

        match resp {
            ReplicaResponse::Done(pos) => {
                debug!("woo! {:?}", pos);
            }
            ReplicaResponse::BadSequence(pos) => {
                debug!("Bad sequence: resetting to {:?}", pos);
                self.last_seen_seq = pos;
            }
        };

        self.state = ReplicatorState::Idle;
        Ok(Async::Ready(()))
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
            try_ready!(self.try_take_next());
            try_ready!(self.try_process_message());
            try_ready!(self.try_process_forward());
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

    fn anidentity() -> HostConfig {
        HostConfig {
            head: "127.0.0.1:23".parse().unwrap(),
            tail: "127.0.0.1:42".parse().unwrap(),
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

        let replica = Replicator::new(store.clone(), &anidentity(), downstream);

        core.handle().spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
        debug!("Spawned replica task");

        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let config: HostConfig = HostConfig {
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

        let replica = Replicator::new(store.clone(), &anidentity(), downstream);

        core.handle().spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
        debug!("Spawned replica task");

        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let config: HostConfig = HostConfig {
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
                                 LogEntry::Data(b"Hello world!".to_vec().into()));

        let (rx, response, _assumed0, off0, _entry0) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        response.complete(ReplicaResponse::Done(off0));

        let (_rx, _response, assumed1, off1, entry1) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        assert_eq!((assumed1, entry1),
                   (off0, LogEntry::Data(b"Hello world!".to_vec().into())));
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

        let replica = Replicator::new(store.clone(), &anidentity(), downstream);

        core.handle().spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
        debug!("Spawned replica task");

        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let config: HostConfig = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let log_off = LogPos::zero();
        let log_off0 = append_entry(&store, &mut core, log_off, LogEntry::Config(config.clone()));
        let log_off1 = append_entry(&store,
                                    &mut core,
                                    log_off0,
                                    LogEntry::Data(b"Hello".to_vec().into()));
        let log_off2 = append_entry(&store,
                                    &mut core,
                                    log_off1,
                                    LogEntry::Data(b"world!".to_vec().into()));

        let (rx, response, _assumed0, _off0, _entry0) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        // So we expect the delivery of "world!"
        let resp = ReplicaResponse::BadSequence(log_off1);
        println!("Respond with: {:?}", resp);
        response.complete(resp);
        let (_rx, _response, assumed1, off1, entry1) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        assert_eq!((assumed1, off1, entry1),
                   (log_off1, log_off2, LogEntry::Data(b"world!".to_vec().into())));
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
