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

#[cfg(test)]
mod test {
    use futures::{self, Future, BoxFuture};
    use futures::stream::{self, Stream};
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
}
