use futures::{Poll, Future, Stream, Async};
use tokio_service::Service;
use store::Store;
use {LogPos, Error};
use std::net::SocketAddr;
use std::fmt;
use replica::{ReplicaRequest, ReplicaResponse, LogEntry, HostConfig, ChainView};


struct ReplicatorDownstreams<S> {
    source: S,
    identity: HostConfig,
}

struct ReplSupervisor<S, T, F> {
    source: S,
    factory: F,
    inner: Option<T>,
}

impl<S, T, F> ReplSupervisor<S, T, F> {
    fn new(src: S, factory: F) -> Self {
        ReplSupervisor {
            source: src,
            factory: factory,
            inner: None,
        }
    }
}


impl<S> ReplicatorDownstreams<S> {
    fn new(src: S, identity: HostConfig) -> ReplicatorDownstreams<S> {
        ReplicatorDownstreams {
            source: src,
            identity: identity,
        }
    }
}

impl<S> Stream for ReplicatorDownstreams<S>
    where S: Stream<Item = (LogPos, LogEntry)>
{
    type Item = HostConfig;
    type Error = S::Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, S::Error> {
        loop {
            let it = try_ready!(self.source.poll());
            println!("Found: {:?}", it);
            match it {
                None => return Ok(Async::Ready(None)),
                Some((off, LogEntry::ViewChange(conf))) => {
                    let ds = conf.downstream_of(&self.identity);
                    return Ok(Async::Ready(ds));
                }
                Some((_, _)) => (),
            }
        }
    }
}
impl<S: Stream, R: Future<Item = (), Error = S::Error>, F: FnMut(S::Item) -> R> Future
    for ReplSupervisor<S, R, F> {
    type Item = ();
    type Error = S::Error;
    fn poll(&mut self) -> Poll<Self::Item, S::Error> {
        match try!(self.source.poll()) {
            Async::Ready(Some(it)) => {
                trace!("ReplSupervisor: new item");
                self.inner = Some((self.factory)(it));
            }
            Async::Ready(None) => {
                trace!("ReplSupervisor: done");
                return Ok(Async::Ready(()));
            }
            Async::NotReady => {}
        }

        if let &mut Some(ref mut inner) = &mut self.inner {
            trace!("ReplSupervisor: poll inner");
            try_ready!(inner.poll());
            trace!("ReplSupervisor: inner done");
        }

        Ok(Async::NotReady)
    }
}


#[cfg(test)]
mod test {
    use futures::{self, Future, BoxFuture, future, Stream, Sink, task};
    use futures::stream;
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
    use std::cell::Cell;
    use std::borrow::BorrowMut;
    use std::io;
    use tokio_timer;
    use env_logger;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct NullUnpark;

    impl task::Unpark for NullUnpark {
        fn unpark(&self) {}
    }

    fn null_parker() -> Arc<task::Unpark> {
        Arc::new(NullUnpark)
    }

    fn anidentity() -> HostConfig {
        host_config(10)
    }

    fn host_config(n: u8) -> HostConfig {
        HostConfig {
            head: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)), 10000),
            tail: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n + 1)), 10001),
        }
    }

    #[test]
    fn should_start_replicating_to_downstream_on_committed_config_message() {
        env_logger::init().unwrap_or(());
        let mut core = Core::new().expect("core::new");
        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let tail_config: HostConfig = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let me = anidentity();
        let view = ChainView::of(vec![me.clone(), tail_config.clone()]);
        let off = LogPos::zero();
        let logs: Vec<Result<(LogPos, LogEntry), ()>> =
            vec![Ok((off, LogEntry::ViewChange(view.clone())))];

        let downstreams = ReplicatorDownstreams::new(stream::iter(logs), me);

        let results = core.run(downstreams.collect()).expect("run downstreams");

        assert_eq!(results, vec![tail_config])
    }


    #[test]
    fn should_yield_no_downstream_when_not_present() {
        env_logger::init().unwrap_or(());
        let mut core = Core::new().expect("core::new");
        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let head_config: HostConfig = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let me = anidentity();
        let view = ChainView::of(vec![head_config.clone(), me.clone()]);
        let off = LogPos::zero();
        let logs: Vec<Result<(LogPos, LogEntry), ()>> =
            vec![Ok((off, LogEntry::ViewChange(view.clone())))];

        let downstreams = ReplicatorDownstreams::new(stream::iter(logs), me);

        let results = core.run(downstreams.collect()).expect("run downstreams");

        assert_eq!(results, vec![])
    }

    #[test]
    fn should_ignore_non_config_items() {
        env_logger::init().unwrap_or(());

        let mut core = Core::new().expect("core::new");
        let tail_addr = "1.2.3.4:5".parse().expect("parse");

        let tail_config: HostConfig = HostConfig {
            head: tail_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let me = anidentity();
        let view = ChainView::of(vec![me.clone(), tail_config.clone()]);
        let off = LogPos::zero();
        let logs: Vec<Result<(LogPos, LogEntry), ()>> = vec![
            Ok((off, LogEntry::Data(b"x".to_vec().into()))),
            Ok((off, LogEntry::ViewChange(view.clone())))];

        let downstreams = ReplicatorDownstreams::new(stream::iter(logs), me);

        let results = core.run(downstreams.collect()).expect("run downstreams");

        assert_eq!(results, vec![tail_config])
    }



    #[test]
    fn should_yield_empty_from_empty() {
        env_logger::init().unwrap_or(());
        let mut core = Core::new().expect("core::new");
        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let tail_config: HostConfig = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let logs: Vec<Result<(LogPos, LogEntry), ()>> = vec![];

        let me = anidentity();
        let downstreams = ReplicatorDownstreams::new(stream::iter(logs), me);

        let results = core.run(downstreams.collect()).expect("run downstreams");

        assert_eq!(results, vec![])
    }


    #[test]
    fn should_poll_returned_future() {
        env_logger::init().unwrap_or(());
        let head_addr = "1.2.3.4:5".parse().expect("parse");

        let tail_config: HostConfig = HostConfig {
            head: head_addr,
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let items: Vec<Result<HostConfig, ()>> = vec![Ok(tail_config)];

        let pollcnt = Cell::new(0);

        let me = anidentity();
        let mut sup = ReplSupervisor::new(stream::iter(items), |conf: HostConfig| {
            assert_eq!(conf.head, head_addr);
            future::poll_fn(|| -> Poll<(), ()> {
                pollcnt.set(pollcnt.get() + 1);
                Ok(Async::NotReady)
            })
        });

        assert_eq!(sup.poll().expect("poll!"), Async::NotReady);

        assert!(pollcnt.get() > 0, "inner polled {:?} > 0", pollcnt.get());
    }

    #[test]
    fn should_replace_future_when_new_thingy_available() {
        use futures::unsync::mpsc;

        env_logger::init().unwrap_or(());
        let mut core = Core::new().expect("core::new");
        let (tx, rx) = mpsc::channel(1);

        let a = host_config(1);
        let b = host_config(2);

        let pollcnt0 = Cell::new(0);
        let pollcnt1 = Cell::new(0);
        let mut futures: HashMap<HostConfig, Box<Future<Item = (), Error = ()>>> = HashMap::new();
        futures.insert(a.clone(),
                       Box::new(future::poll_fn(|| -> Poll<(), ()> {
                           pollcnt0.set(pollcnt0.get() + 1);
                           debug!("Poll for a: {:?}", pollcnt0.get());
                           Ok(Async::NotReady)
                       })));
        futures.insert(b.clone(),
                       Box::new(future::poll_fn(|| -> Poll<(), ()> {
                           pollcnt1.set(pollcnt1.get() + 1);
                           debug!("Poll for b: {:?}", pollcnt1.get());
                           Ok(Async::NotReady)
                       })));

        let me = anidentity();
        let mut sup = task::spawn(ReplSupervisor::new(rx, |conf: HostConfig| {
            debug!("Spawn: {:?}", conf);
            futures.remove(&conf).expect("next future")
        }));

        debug!("Poll on empty");
        assert_eq!(sup.poll_future(null_parker()).expect("poll!"),
                   Async::NotReady);
        assert!(pollcnt0.get() == 0,
                "inner0 polled {:?} == 0",
                pollcnt0.get());
        assert!(pollcnt1.get() == 0,
                "inner1 polled {:?} == 0",
                pollcnt1.get());

        debug!("Send first: {:?}", a);
        let tx = core.run(tx.send(a)).expect("send");

        debug!("Poll first");
        assert_eq!(sup.poll_future(null_parker()).expect("poll!"),
                   Async::NotReady);

        assert!(pollcnt0.get() > 0, "inner0 polled {:?} > 0", pollcnt0.get());
        assert!(pollcnt1.get() == 0,
                "inner1 polled {:?} == 0",
                pollcnt1.get());

        let p0 = pollcnt0.get();

        debug!("Poll first 1/2");
        assert_eq!(sup.poll_future(null_parker()).expect("poll!"),
                   Async::NotReady);

        assert!(pollcnt0.get() > p0,
                "inner0 polled {:?} > {:?}",
                pollcnt0.get(),
                p0);
        assert!(pollcnt1.get() == 0,
                "inner1 polled {:?} == 0",
                pollcnt1.get());

        debug!("Send second: {:?}", b);
        let tx = core.run(tx.send(b)).expect("send");

        debug!("Poll Second");
        assert_eq!(sup.poll_future(null_parker()).expect("poll!"),
                   Async::NotReady);
        assert!(pollcnt1.get() > 0, "inner1 polled {:?} > 0", pollcnt1.get());
    }

    #[test]
    fn should_terminate_on_non_terminating_child() {
        env_logger::init().unwrap_or(());
        let mut core = Core::new().expect("core::new");
        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();

        let items: Vec<Result<HostConfig, ()>> = vec![Ok(host_config(42))];

        let me = anidentity();
        let sup = ReplSupervisor::new(stream::iter(items),
                                      |conf: HostConfig| future::empty::<(), ()>());
        core.run(timer.timeout(sup, Duration::from_millis(100))).expect("ran okay!")
    }
}
