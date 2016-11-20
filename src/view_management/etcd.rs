use futures::stream::Stream;
use futures::{self, Async, Poll, Future};
use futures_cpupool::{CpuPool, CpuFuture};
use etcd;
use tokio_timer::{Timer, Sleep};
use std::sync::Arc;
use std::time::Duration;
use std::fmt;
use std::collections::{BTreeMap, VecDeque};
use serde_json;

use {Error, Result, ChainErr, ChainView, HostConfig};

pub type View = ChainView;
pub struct EtcdHeartbeater {
    value: HostConfig,
    view: BTreeMap<String, HostConfig>,
    heartbeats: HeartBeater,
    watcher: Watcher,
}

const TTL: u64 = 2;

impl EtcdHeartbeater {
    pub fn new(_url: &str, dir: &str, value: HostConfig) -> Self {
        let client = Arc::new(etcd::Client::default());
        let pool = CpuPool::new(2);
        let hb = HeartBeater::new(pool.clone(), client.clone(), &dir, value.clone());
        let w = Watcher::new(pool.clone(), client, dir);
        EtcdHeartbeater {
            value: value,
            view: Default::default(),
            heartbeats: hb,
            watcher: w,
        }
    }

    fn update_view(&mut self, ev: WatchEvent) -> bool {
        trace!("update_view: {:?} -> {:?}", self.view, ev);
        let changed = match ev {
            WatchEvent::Alive(id, ver, val) => {
                if self.view.get(&id) != Some(&val) {
                    self.view.insert(id, val);
                    true
                } else {
                    false
                }
            }
            WatchEvent::Dead(id, ver) => {
                if self.view.get(&id).is_some() {
                    self.view.remove(&id);
                    true
                } else {
                    false
                }
            }
        };
        trace!("update_view changed:{:?}; post: {:?}", changed, self.view);
        changed
    }
    fn current_view(&mut self) -> ChainView {
        let members = self.view.values().cloned().collect::<Vec<HostConfig>>();
        ChainView::of(members)
    }

    fn poll_heartbeat(&mut self) -> Poll<(), Error> {
        self.heartbeats.poll()
    }

    fn poll_watcher(&mut self) -> Poll<Option<View>, Error> {
        loop {
            let event = match try!(self.watcher.poll()) {
                Async::NotReady => return Ok(Async::NotReady),
                Async::Ready(None) => return Ok(Async::Ready(None)),
                Async::Ready(Some(ret)) => ret,
            };
            trace!("WatchEvent: {:?}", event);

            if self.update_view(event) {
                let view = self.current_view();
                debug!("Current view from {:?}: {:?}", self.value, view);
                return Ok(Async::Ready(Some(view)));
            } else {
                trace!("Current view unchanged");
            }
        }
    }
}

impl Stream for EtcdHeartbeater {
    type Item = View;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        trace!("EtcdHeartbeater#poll");
        let heartbeat_result = try!(self.poll_heartbeat());
        // Has it terminated?
        if heartbeat_result.is_ready() {
            return Ok(Async::Ready(None));
        }

        Ok(try!(self.poll_watcher()))
    }
}


enum HeartBeatState {
    New,
    Creating(CpuFuture<etcd::KeySpaceInfo, Error>),
    Started(String, u64),
    Sleeping(String, u64, Sleep),
}
struct HeartBeater {
    pool: CpuPool,
    etcd: Arc<etcd::Client>,
    timer: Timer,
    state: HeartBeatState,
    value: HostConfig,
    dir: String,
}

enum WatcherState {
    Fresh(Option<u64>),
    WaitScan(CpuFuture<etcd::KeySpaceInfo, Error>),
    WaitWatch(CpuFuture<etcd::KeySpaceInfo, Error>),
}

#[derive(Debug, Clone)]
enum WatchEvent {
    Alive(String, u64, HostConfig),
    Dead(String, u64),
}

impl fmt::Debug for WatcherState {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &WatcherState::Fresh(ref var) => {
                fmt.debug_tuple("WatcherState::Fresh").field(&var).finish()
            }
            &WatcherState::WaitScan(_) => writeln!(fmt, "WatcherState::WaitScan(_)"),
            &WatcherState::WaitWatch(_) => writeln!(fmt, "WatcherState::WaitWatch(_)"),
        }
    }
}

impl fmt::Debug for HeartBeatState {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &HeartBeatState::New => fmt.debug_tuple("New").finish(),
            &HeartBeatState::Creating(_) => {
                fmt.debug_tuple("Creating").field(&format_args!("_")).finish()
            }
            &HeartBeatState::Started(ref id, ref vers) => {
                fmt.debug_tuple("Started").field(&id).field(&vers).finish()
            }
            &HeartBeatState::Sleeping(ref id, ref vers, _) => {
                fmt.debug_tuple("Sleeping")
                    .field(&id)
                    .field(&vers)
                    .field(&format_args!("_"))
                    .finish()
            }
        }
    }
}


struct Watcher {
    pool: CpuPool,
    etcd: Arc<etcd::Client>,
    timer: Timer,
    state: WatcherState,
    pending: VecDeque<WatchEvent>,
    dir: String,
}

const KEY_EXISTS: u64 = 105;

fn ensure_dir(cl: &etcd::Client, dir: &str) -> Result<()> {
    match cl.create_dir(&dir, None) {
        Ok(_) => Ok(()),
        Err(mut es) => {
            match es.pop().expect("first error") {
                etcd::Error::Api(etcd::ApiError { error_code, .. }) if error_code == KEY_EXISTS => {
                    debug!("Directory prefix {:?} already exists: {:?}", dir, es);
                    Ok(())
                }
                e => {
                    error!("Error creating {:?}: {:?}", dir, es);
                    return Err(e.into());
                }
            }
        }
    }
}

impl HeartBeater {
    fn new(cpupool: CpuPool, etcd: Arc<etcd::Client>, dir: &str, value: HostConfig) -> Self {
        HeartBeater {
            pool: cpupool,
            etcd: etcd,
            state: HeartBeatState::New,
            timer: Timer::default(),
            value: value,
            dir: dir.to_string(),
        }
    }

    fn create_if_needed(&mut self) -> Poll<(), Error> {
        if let &mut HeartBeatState::New = &mut self.state {
            // ...
        } else {
            return Ok(Async::Ready(()));
        }

        let create_fut = {
            let cl = self.etcd.clone();
            let dir = self.dir.clone();
            let val = self.value.clone();
            self.pool.spawn(futures::lazy(move || {
                trace!("Starting; create dir node {:?}", dir);
                try!(ensure_dir(&*cl, &dir));
                trace!("Starting; creating seq node");
                let res =
                    try!(cl.create_in_order(&dir, &try!(serde_json::to_string(&val)), Some(TTL))
                        .map_err(|mut es| es.pop().unwrap()));
                trace!("Created:{:?}", res);
                Ok(res)
            }))
        };

        self.state = HeartBeatState::Creating(create_fut);
        trace!("Creating");
        Ok(Async::Ready(()))
    }

    fn await_setup(&mut self) -> Poll<(), Error> {
        let res = if let &mut HeartBeatState::Creating(ref mut fut) = &mut self.state {
            try_ready!(fut.poll())
        } else {
            return Ok(Async::Ready(()));
        };

        let node = res.node.expect("node");
        let key = node.key.expect("key");
        let ver = node.modified_index.expect("key");

        trace!("Alive! {}@{}", key, ver);
        self.state = HeartBeatState::Started(key, ver);
        Ok(Async::Ready(()))
    }
    fn maybe_sleep(&mut self) -> Poll<(), Error> {
        let (key, ver) = if let &mut HeartBeatState::Started(ref key, ref ver) = &mut self.state {
            (key.clone(), ver.clone())
        } else {
            return Ok(Async::Ready(()));
        };

        let sleeper = self.timer.sleep(Duration::from_secs(TTL) / 2);

        self.state = HeartBeatState::Sleeping(key, ver, sleeper);
        Ok(Async::Ready(()))
    }
    fn maybe_send_hb(&mut self) -> Poll<(), Error> {
        let (key, ver) = if let &mut HeartBeatState::Sleeping(ref key, ref ver, ref mut fut) =
                                &mut self.state {
            try_ready!(fut.poll().chain_err(|| "sleeping error?"));
            (key.clone(), ver.clone())
        } else {
            return Ok(Async::Ready(()));
        };

        trace!("Ping?");
        let ping_fut = {
            let cl = self.etcd.clone();
            let value = self.value.clone();
            self.pool.spawn(futures::lazy(move || {
                trace!("Pinging for {:?}@{:?}", key, ver);
                let res = try!(cl.compare_and_swap(&key,
                                      &try!(serde_json::to_string(&value)),
                                      Some(TTL),
                                      None,
                                      Some(ver))
                    .map_err(|mut es| es.pop().unwrap()));
                trace!("Pinged:{:?}", res);
                Ok(res)
            }))
        };
        self.state = HeartBeatState::Creating(ping_fut);
        Ok(Async::Ready(()))
    }
}


impl Future for HeartBeater {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<(), Error> {
        trace!("HeartBeater#poll: {:?}", self.state);
        loop {
            try_ready!(self.create_if_needed());
            try_ready!(self.await_setup());
            try_ready!(self.maybe_send_hb());
            try_ready!(self.maybe_sleep());
        }
    }
}

impl Watcher {
    fn new(cpupool: CpuPool, etcd: Arc<etcd::Client>, dir: &str) -> Self {
        Watcher {
            pool: cpupool,
            etcd: etcd,
            timer: Timer::default(),
            state: WatcherState::Fresh(None),
            pending: VecDeque::new(),
            dir: dir.to_string(),
        }
    }


    fn maybe_scan_all(&mut self) -> Poll<(), Error> {
        trace!("maybe_watch? {:?}", self.state);
        if let &mut WatcherState::Fresh(None) = &mut self.state {
            trace!("Fresh: None");
        } else {
            return Ok(Async::Ready(()));
        };

        let watch_fut = {
            let cl = self.etcd.clone();
            let dir = self.dir.clone();
            self.pool.spawn(futures::lazy(move || {
                trace!("Watch; create dir node if needed");
                try!(ensure_dir(&*cl, &dir));
                trace!("Scanning");
                let res = try!(cl.get(&dir, true, false, false)
                    .map_err(|mut es| es.pop().unwrap()));
                Ok(res)
            }))
        };

        self.state = WatcherState::WaitScan(watch_fut);
        Ok(Async::Ready(()))
    }

    fn maybe_fetch_scan(&mut self) -> Poll<(), Error> {
        trace!("maybe_fetch_scan? {:?}", self.state);
        let res = if let &mut WatcherState::WaitScan(ref mut fut) = &mut self.state {
            trace!("Waiting");
            try_ready!(fut.poll())
        } else {
            return Ok(Async::Ready(()));
        };

        trace!("Response: {:#?}", res);
        let nodes: Vec<etcd::Node> = res.node
            .into_iter()
            .flat_map(|node| node.nodes.into_iter())
            .flat_map(|nodes| nodes.into_iter())
            .collect();

        let latest_version = nodes.iter().filter_map(|node| node.modified_index).max();
        let res = try!(nodes.into_iter()
            .filter_map(|node| {
                let etcd::Node { key, value, modified_index, .. } = node;
                key.and_then(move |key| {
                    modified_index.map(move |ver| match value {
                        Some(val) => {
                            trace!("Raw value: {:?}=>{:?}", key, val);
                            Ok(WatchEvent::Alive(key, ver, try!(serde_json::from_str(&val))))
                        }
                        None => Ok(WatchEvent::Dead(key, ver)),
                    })
                })
            })
            .collect::<Result<Vec<_>>>());

        trace!("Events: {:?}", res);

        self.pending.extend(res);

        // FIXME: May result in infinite scan loop?
        self.state = WatcherState::Fresh(latest_version);
        Ok(Async::Ready(()))
    }

    fn maybe_watch(&mut self) -> Poll<(), Error> {
        trace!("maybe_watch? {:?}", self.state);
        let vers = if let &mut WatcherState::Fresh(Some(vers)) = &mut self.state {
            trace!("Fresh:{:?}", vers);
            vers
        } else {
            return Ok(Async::Ready(()));
        };

        let watch_fut = {
            let cl = self.etcd.clone();
            let dir = self.dir.clone();
            self.pool.spawn(futures::lazy(move || {
                let next = vers + 1;
                trace!("Watching from {:?}", next);
                let res = try!(cl.watch(&dir, Some(next), true)
                    .map_err(|mut es| es.pop().unwrap()));
                trace!("Watch Event:{:?}", res);
                Ok(res)
            }))
        };

        self.state = WatcherState::WaitWatch(watch_fut);
        Ok(Async::Ready(()))
    }

    fn maybe_fetch_watch(&mut self) -> Poll<(), Error> {
        trace!("maybe_fetch_watch? {:?}", self.state);
        let res = if let &mut WatcherState::WaitWatch(ref mut fut) = &mut self.state {
            trace!("Waiting");
            try_ready!(fut.poll())
        } else {
            return Ok(Async::Ready(()));
        };

        trace!("Response: {:?}", res);
        let node = res.clone().node.expect("node");
        let key = node.key.expect("key");
        let ver = node.modified_index.expect("ver");
        let value = node.value;

        self.state = WatcherState::Fresh(Some(ver));
        let event = match value {
            Some(val) => {

                trace!("Raw value: {:?}=>{:?}", key, val);
                WatchEvent::Alive(key, ver, try!(serde_json::from_str(&val)))
            }
            None => WatchEvent::Dead(key, ver),
        };

        self.pending.push_back(event);
        Ok(Async::Ready(()))
    }

    fn poll_inner(&mut self) -> Poll<Option<WatchEvent>, Error> {
        loop {
            trace!("Watcher#poll: {:?}", self.state);

            try_ready!(self.maybe_scan_all());
            try_ready!(self.maybe_fetch_scan());
            try_ready!(self.maybe_watch());
            try_ready!(self.maybe_fetch_watch());
        }
    }
}


impl Stream for Watcher {
    type Item = WatchEvent;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        let res = try!(self.poll_inner());

        trace!("pending: {:?}", self.pending);
        if let Some(next) = self.pending.pop_front() {
            trace!("Next event: {:?}", next);
            Ok(Async::Ready(Some(next)))
        } else {
            Ok(res)
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::reactor::Core;
    use super::EtcdHeartbeater;
    use futures::{self, Future};
    use futures::stream::Stream;
    use tokio_timer::Timer;
    use std::time::Duration;
    use std::iter;
    use env_logger;
    use rand::{self, Rng};
    use HostConfig;

    fn rand_dir() -> String {
        let mut gen = rand::thread_rng();
        iter::once('/').chain(gen.gen_ascii_chars().take(20)).collect::<String>()
    }

    const ETCD_URL: &'static str = "http://localhost/";

    // This module is responsible for registering itself as a node in eg: etcd,
    // and sending heart-beats when we are alive, and shutting down on removal.

    // For now, we'll test this by creating a process that a) sends heartbeats b)
    // yields a stream of config changes.

    #[test]
    fn should_yield_single_node_for_single_item() {
        env_logger::init().unwrap_or(());
        let prefix = rand_dir();
        let mut core = Core::new().expect("core::new");
        let t = Timer::default();
        let timeout = Duration::from_millis(1000);
        let self_config = HostConfig {
            head: "10.0.0.23:1".parse().unwrap(),
            tail: "10.0.0.23:2".parse().unwrap(),
        };
        let me = EtcdHeartbeater::new(ETCD_URL, &prefix, self_config.clone());
        let (next, me) = core.run(t.timeout(me.into_future().map_err(|(e, _)| e), timeout))
            .expect("run one");
        let config = next.expect("next value");

        assert_eq!(config.members, vec![self_config]);
    }


    #[test]
    fn should_add_new_members_to_tail() {
        env_logger::init().unwrap_or(());
        let prefix = rand_dir();
        let mut core = Core::new().expect("core::new");
        let t = Timer::default();
        let timeout = Duration::from_millis(2000);
        let first_config = HostConfig {
            head: "10.0.0.23:1".parse().unwrap(),
            tail: "10.0.0.23:2".parse().unwrap(),
        };
        let second_config = HostConfig {
            head: "10.0.0.42:1".parse().unwrap(),
            tail: "10.0.0.42:2".parse().unwrap(),
        };
        let first = EtcdHeartbeater::new(ETCD_URL, &prefix, first_config.clone());
        let second = EtcdHeartbeater::new(ETCD_URL, &prefix, second_config.clone());
        let (next, first) = core.run(first.into_future().map_err(|(e, _)| e)).expect("run one");
        // let config = next.expect("next value");
        core.handle().spawn(first.for_each(|e|
                    Ok(println!("should_add_new_members_to_tail::first: {:?}", e)))
                    .map_err(|e| panic!("first: {:?}", e)));

        trace!("Await configuration values");
        let (next, me) = core.run(t.timeout(second.filter(|r| r.members.len() > 1)
                               .into_future()
                               .map_err(|(e, _)| e),
                           timeout))
            .expect("run one");
        let config = next.expect("next value");
        assert_eq!(config.members, vec![first_config, second_config]);
    }

    #[test]
    fn should_remove_dead_members() {
        env_logger::init().unwrap_or(());
        let prefix = rand_dir();
        let mut core = Core::new().expect("core::new");
        let t = Timer::default();
        let timeout = Duration::from_millis(5000);
        let first_config = HostConfig {
            head: "10.0.0.23:1".parse().unwrap(),
            tail: "10.0.0.23:2".parse().unwrap(),
        };
        let second_config = HostConfig {
            head: "10.0.0.42:1".parse().unwrap(),
            tail: "10.0.0.42:2".parse().unwrap(),
        };
        let first = EtcdHeartbeater::new(ETCD_URL, &prefix, first_config.clone());
        let mut second = EtcdHeartbeater::new(ETCD_URL, &prefix, second_config.clone());

        let (c, p) = futures::oneshot();

        core.handle().spawn(first.for_each(|e|
                    Ok(println!("should_add_new_members_to_tail::first: {:?}", e)))
                    .map_err(|e| panic!("first: {:?}", e))
                    .select(p)
                    .map(|((), _)| println!("First exiting"))
                    .map_err(|(e, _)| panic!("cancellation: {:?}", e))
                    );

        println!("Await config from second");
        // Run both until quiescent
        loop {
            let (next, second2) =
                core.run(t.timeout(second.into_future().map_err(|(e, _)| e), timeout))
                    .expect("run one");
            second = second2;
            println!("config from second: {:?}", next);
            if next.expect("next").members.len() > 1 {
                break;
            }
        }

        // forcibly remove one instance.
        println!("terminating first future");
        c.complete(());

        let (next, second2) = core.run(t.timeout(second.into_future().map_err(|(e, _)| e), timeout))
            .expect("run one");
        second = second2;
        println!("config from second: {:?}", next);
        let config = next.expect("next value");

        assert_eq!(config.members, vec![second_config]);
    }
}
