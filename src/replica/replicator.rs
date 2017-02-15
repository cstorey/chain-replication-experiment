use futures::{Poll, Future, Stream, Async};
use tokio_service::Service;
use store::Store;
use {LogPos, Error};
use std::net::SocketAddr;
use std::fmt;
use replica::{ReplicaRequest, ReplicaResponse, LogEntry, HostConfig, ChainView};

pub trait DownstreamService {
    type Future: Future<Item = ReplicaResponse, Error = Error>;
    fn call(&self, req: ReplicaRequest) -> Self::Future;
}

pub trait NewDownstreamService {
    type Item: DownstreamService;
    type Future: Future<Item = Self::Item, Error = Error>;
    fn new_downstream(&self, addr: SocketAddr) -> Self::Future;
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

impl<F, R, D> NewDownstreamService for F
    where F: Fn(SocketAddr) -> R,
          R: Future<Item = D, Error = Error>,
          D: DownstreamService
{
    type Item = D;
    type Future = R;
    fn new_downstream(&self, addr: SocketAddr) -> R {
        debug!("new downstream: {:?}", addr);
        self(addr)
    }
}

enum ReplicatorState<S: Store, N>
    where N: NewDownstreamService
{
    Idle,
    Fetching(S::FetchStream),
    Forwarding(<N::Item as DownstreamService>::Future),
}

impl<S: Store, N: NewDownstreamService> fmt::Debug for ReplicatorState<S, N> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ReplicatorState::Idle => fmt.debug_tuple("Idle").finish(),
            &ReplicatorState::Fetching(_) => {
                fmt.debug_tuple("Fetching").field(&format_args!("_")).finish()
            }
            &ReplicatorState::Forwarding(_) => {
                fmt.debug_tuple("Forwarding").field(&format_args!("_")).finish()
            }
        }
    }
}

enum DownstreamState<N: NewDownstreamService> {
    Disconnected,
    Connecting(N::Future),
    Ready(N::Item),
}

impl<N: NewDownstreamService> fmt::Debug for DownstreamState<N> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &DownstreamState::Disconnected => fmt.debug_tuple("Disconnected").finish(),
            &DownstreamState::Connecting(_) => {
                fmt.debug_tuple("Connecting").field(&format_args!("_")).finish()
            }
            &DownstreamState::Ready(_) => {
                fmt.debug_tuple("Ready").field(&format_args!("_")).finish()
            }
        }
    }
}

#[derive(Debug)]
pub struct ReplicaView {
    identity: HostConfig,
    view: ChainView,
}

pub struct Replicator<S: Store, N: NewDownstreamService> {
    store: S,
    new_downstream: N,
    downstream: DownstreamState<N>,
    last_seen_seq: LogPos,
    state: ReplicatorState<S, N>,
    identity: HostConfig,
    config: ReplicaView,
}

impl<S: Store, N: NewDownstreamService> Replicator<S, N> {
    pub fn new(store: S, identity: &HostConfig, new_downstream: N) -> Self {
        Replicator {
            store: store,
            new_downstream: new_downstream,
            downstream: DownstreamState::Disconnected,
            last_seen_seq: LogPos::zero(),
            state: ReplicatorState::Idle,
            identity: identity.clone(),
            config: ReplicaView::new(identity.clone()),
        }
    }

    fn process_config_message(&mut self, conf: &ChainView) {
        debug!("{}: logged Config message: {:?}", self.identity, conf);
        self.config.process(conf);
        if let Some(next) = self.config.get_downstream() {
            debug!("connecting downstream to: {:?}", next.head);
            self.downstream = DownstreamState::Connecting(self.new_downstream
                .new_downstream(next.head));
        }
    }
    fn try_ensure_downstream(&mut self) -> Poll<(), Error> {
        let ds = if let &mut DownstreamState::Connecting(ref mut f) = &mut self.downstream {
            try_ready!(f.poll())
        } else {
            return Ok(Async::Ready(()));
        };

        self.downstream = DownstreamState::Ready(ds);
        return Ok(Async::Ready(()));
    }

    fn try_take_next(&mut self) -> Poll<(), Error> {
        if let ReplicatorState::Idle = self.state {
            debug!("{}: Idle, fetching after: {:?}",
                   self.identity,
                   self.last_seen_seq);
            let fetch_f = self.store.fetch_from(self.last_seen_seq);
            self.state = ReplicatorState::Fetching(fetch_f);
        }
        Ok(Async::Ready(()))
    }

    fn try_process_message(&mut self) -> Poll<(), Error> {
        debug!("try_process_message: state: {:?}", self.state);
        let (off, val) = if let &mut ReplicatorState::Fetching(ref mut fetch_f) = &mut self.state {
            debug!("{}: Fetching", self.identity);
            try_ready!(fetch_f.poll()).expect("first item of stream!")
        } else {
            return Ok(Async::Ready(()));
        };

        debug!("Fetched: {:?}", (&off, &val));
        if let &LogEntry::ViewChange(ref conf) = &val {
            self.process_config_message(conf);
        };
        // This fails, because really, we need to be tracking our internal
        // config and what we are replicating to the downstream seperately.

        debug!("try_process_message: downstream: {:?}", self.downstream);
        self.state = match self.downstream {
            DownstreamState::Ready(ref downstream) => {
                debug!("try_process_message: Forward {:?} -> {:?}",
                       self.last_seen_seq,
                       off);
                let req = ReplicaRequest::AppendLogEntry {
                    assumed_offset: self.last_seen_seq,
                    // FIXME: NO. WRONG.
                    entry_offset: off,
                    datum: val,
                };
                ReplicatorState::Forwarding(downstream.call(req))
            }
            DownstreamState::Connecting(_) => {
                debug!("try_process_message: Connecting to {:?}", off);
                ReplicatorState::Idle
            }
            DownstreamState::Disconnected => {
                debug!("try_process_message: No downstream at {:?}", off);
                ReplicatorState::Idle
            }
        };

        debug!("New state: {:?}", self.state);

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
            try_ready!(self.try_ensure_downstream());
            try_ready!(self.try_take_next());
            try_ready!(self.try_process_message());
            try_ready!(self.try_process_forward());
        }
    }
}


impl ReplicaView {
    pub fn new(identity: HostConfig) -> Self {
        ReplicaView {
            identity: identity,
            view: ChainView::default(),
        }
    }

    pub fn process(&mut self, conf: &ChainView) {
        self.view = conf.clone();
        debug!("View changed, now: {:?}", self);
    }
    pub fn get_downstream(&self) -> Option<HostConfig> {
        debug!("ReplicaView#get_downstream");
        let index = self.view
            .members
            .iter()
            .enumerate()
            .filter(|&(_n, it)| it == &self.identity)
            .map(|(n, _)| n)
            .next();
        debug!("I am {:?}/{}", index, self.view.members.len());

        let next = index.and_then(|idx| self.view.members.get(idx + 1)).cloned();
        debug!("downstream: {:?}", next);
        next
    }
}

#[cfg(test)]
mod test {
    use futures::{self, Future, BoxFuture};
    use futures::stream::Stream;
    use service::Service;
    use tokio::channel;
    use replica::LogPos;
    use store::{RamStore, Store};
    use replica::{ReplicaRequest, ReplicaResponse, LogEntry, HostConfig, ChainView};
    use errors::Error;
    use std::net::{SocketAddr, IpAddr, Ipv4Addr};
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
        type Future = futures::future::Ok<Self::Item, Error>;
        fn new_downstream(&self, addr: SocketAddr) -> Self::Future {
            futures::future::ok(MyMagicalDownstream {
                addr: addr,
                send: self.send.clone(),
            })
        }
    }

    fn anidentity() -> HostConfig {
        HostConfig {
            head: "127.0.0.1:23".parse().unwrap(),
            tail: "127.0.0.1:42".parse().unwrap(),
        }
    }

    fn host_config(n: u8) -> HostConfig {
        HostConfig {
            head: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)), 10000),
            tail: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n + 1)), 10001),
        }
    }

    #[ignore]
    #[test]
    fn should_start_replicating_to_downstream_on_committed_config_message() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();
        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();

        let mut core = Core::new().expect("core::new");
        let (tx, rx) = channel::channel(&core.handle()).expect("channel");

        let downstream = MyDownstreamBuilder { send: tx };

        let me = anidentity();
        let replica = Replicator::new(store.clone(), &me, downstream);

        core.handle().spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
        debug!("Spawned replica task");

        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let config: HostConfig = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };
        let view = ChainView::of(vec![me.clone(), config.clone()]);
        let off = LogPos::zero();
        let _ = append_entry(&store, &mut core, off, LogEntry::ViewChange(view.clone()));

        let (_rx, _response, _assumed0, _off0, entry0) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        assert_eq!(entry0, LogEntry::ViewChange(view.clone()));
    }

    #[ignore]
    #[test]
    fn should_send_next_log_entry_on_okay_from_downstream() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();
        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();

        let mut core = Core::new().expect("core::new");
        let (tx, rx) = channel::channel(&core.handle()).expect("channel");

        let downstream = MyDownstreamBuilder { send: tx };

        let me = anidentity();
        let replica = Replicator::new(store.clone(), &me, downstream);

        core.handle().spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
        debug!("Spawned replica task");

        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let config: HostConfig = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let view = ChainView::of([&me, &config].iter().cloned().cloned());
        let log_off0 = append_entry(&store,
                                    &mut core,
                                    LogPos::zero(),
                                    LogEntry::ViewChange(view));
        let _log_off1 = append_entry(&store,
                                     &mut core,
                                     log_off0,
                                     LogEntry::Data(b"Hello world!".to_vec().into()));

        let (rx, response, _assumed0, off0, _entry0) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        response.complete(ReplicaResponse::Done(off0));

        let (_rx, _response, assumed1, off1, entry1) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        assert_eq!((assumed1, entry1),
                   (log_off0, LogEntry::Data(b"Hello world!".to_vec().into())));
        assert!(off1 > assumed1);
    }

    #[ignore]
    #[test]
    fn should_restart_replication_on_badsequence_from_downstream() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();
        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();

        let mut core = Core::new().expect("core::new");
        let (tx, rx) = channel::channel(&core.handle()).expect("channel");


        let downstream = MyDownstreamBuilder { send: tx };

        let me = anidentity();
        let replica = Replicator::new(store.clone(), &me, downstream);

        core.handle().spawn(replica.map_err(|e| panic!("Replicator failed!: {:?}", e)));
        debug!("Spawned replica task");

        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let config: HostConfig = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let log_off = LogPos::zero();
        let view = ChainView::of([&me, &config].iter().cloned().cloned());
        let log_off0 = append_entry(&store, &mut core, log_off, LogEntry::ViewChange(view));
        let log_off1 = append_entry(&store,
                                    &mut core,
                                    log_off0,
                                    LogEntry::Data(b"Hello".to_vec().into()));
        let log_off2 = append_entry(&store,
                                    &mut core,
                                    log_off1,
                                    LogEntry::Data(b"Hello".to_vec().into()));

        let (rx, response, _assumed0, _off0, _entry0) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        // So we expect the delivery of "Hello"
        let resp = ReplicaResponse::BadSequence(log_off1);
        println!("Respond with: {:?}", resp);
        response.complete(resp);
        let (_rx, _response, assumed1, off1, entry1) =
            take_next_entry(rx, &head_addr, &mut core, &timer);

        assert_eq!((assumed1, off1, entry1),
                   (log_off1, log_off2, LogEntry::Data(b"Hello".to_vec().into())));
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

    #[test]
    fn should_not_have_downstream_when_sole_member() {
        let me = anidentity();
        let mut config = ReplicaView::new(me.clone());
        config.process(&ChainView::of(vec![me]));
        assert_eq!(config.get_downstream(), None);
    }

    #[test]
    fn should_not_have_downstream_when_not_in_chain() {
        let config = ReplicaView::new(anidentity());
        assert_eq!(config.get_downstream(), None);
    }

    #[test]
    fn config_should_connect_to_next_downstream() {
        let me = anidentity();
        let mut config = ReplicaView::new(me.clone());
        let downstream = host_config(2);
        config.process(&ChainView::of(vec![me.clone(), downstream.clone()]));
        assert_eq!(config.get_downstream(), Some(downstream));
    }

    #[test]
    fn should_connect_to_2_of_3_when_1() {
        let me = anidentity();
        let mut config = ReplicaView::new(me.clone());

        config.process(&ChainView::of(vec![me, host_config(2), host_config(42)]));
        assert_eq!(config.get_downstream(), Some(host_config(2)));
    }

    #[test]
    fn should_connect_to_3_of_3_when_2() {
        let me = anidentity();
        let mut config = ReplicaView::new(me.clone());
        config.process(&ChainView::of(vec![host_config(1), me, host_config(3)]));
        assert_eq!(config.get_downstream(), Some(host_config(3)));
    }


    #[test]
    fn should_have_down_downstream_when_tail() {
        let me = anidentity();
        let mut config = ReplicaView::new(me.clone());
        config.process(&ChainView::of(vec![host_config(1), host_config(3), me]));
        assert_eq!(config.get_downstream(), None);
    }
}
