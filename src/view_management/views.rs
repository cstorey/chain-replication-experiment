use std::fmt;
use futures::{Future, Poll, Async};
use futures::stream::Stream;
use store::Store;
use replica::{HostConfig, ChainView, LogEntry, LogPos};
use errors::*;

pub struct ViewManager<S: Store, V> {
    store: S,
    views: V,
    identity: HostConfig,
    state: ViewManagerState<S::AppendFut>,
    last_view: ChainView,
    last_seen_change: LogPos,
}

enum ViewManagerState<StoreFuture> {
    Waiting,
    HaveViewChange,
    AwaitStore(StoreFuture),
    // The underlying stream is empty
    Terminated,
}

impl<F> fmt::Debug for ViewManagerState<F> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ViewManagerState::Waiting => fmt.debug_tuple("Waiting").finish(),
            &ViewManagerState::HaveViewChange => fmt.debug_tuple("HaveViewChange").finish(),
            &ViewManagerState::AwaitStore(_) => fmt.debug_tuple("AwaitStore").finish(),
            &ViewManagerState::Terminated => fmt.debug_tuple("Terminated").finish(),
        }
    }
}

// STATES:
//
// Our job here, is to watch for updates from the external view manger, and write any changes to the store.
//
// start: waiting
//
// waiting -> hasChange;
// hasChange -> submittedChange;
// submittedChange -> casFailed;
// casFailed -> submittedChange;
// submittedChange -> waiting;
//
//


impl<S: Store, V: Stream<Item = ChainView, Error = Error>> ViewManager<S, V> {
    pub fn new(store: S, identity: &HostConfig, source: V) -> Self {
        ViewManager {
            store: store,
            views: source,
            identity: identity.clone(),
            state: ViewManagerState::Waiting,
            last_view: ChainView::default(),
            last_seen_change: LogPos::zero(),
        }
    }

    fn check_for_view_change(&mut self) -> Poll<(), Error> {
        debug!("check_for_view_change: {:?}", self.state);
        if let &mut ViewManagerState::Waiting = &mut self.state {
            ()
        } else {
            return Ok(Async::Ready(()));
        };

        if let Some(change) = try_ready!(self.views.poll()) {
            self.last_view = change;
            self.state = ViewManagerState::HaveViewChange;
            debug!("New view change");
        } else {
            self.state = ViewManagerState::Terminated;
            debug!("Views source terminated");
        }
        Ok(Async::Ready(()))
    }
    fn maybe_submit_new_change(&mut self) -> Poll<(), Error> {
        debug!("maybe_submit_new_change: {:?}", self.state);
        if let &mut ViewManagerState::HaveViewChange = &mut self.state {
            ()
        } else {
            return Ok(Async::Ready(()));
        };
        let curr = self.last_seen_change;
        let next = curr.next();
        let store_fut = self.store
            .append_entry(curr, next, LogEntry::ViewChange(self.last_view.clone()));
        self.last_seen_change = next;

        self.state = ViewManagerState::AwaitStore(store_fut);
        debug!("Awaiting store");

        Ok(Async::Ready(()))
    }

    fn maybe_await_store(&mut self) -> Poll<(), Error> {
        debug!("maybe_await_store: {:?}", self.state);
        let store_result = if let &mut ViewManagerState::AwaitStore(ref mut f) = &mut self.state {
            match f.poll() {
                Ok(Async::Ready(r)) => Ok(r),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => Err(e),
            }
        } else {
            return Ok(Async::Ready(()));
        };

        self.state = match store_result {
            Ok(pos) => {
                debug!("Stored as: {:?}", self.last_seen_change);
                ViewManagerState::Waiting
            }
            Err(e) => {
                match e.kind() {
                    &ErrorKind::BadSequence(pos) => {
                        debug!("Head at: {:?}", pos);
                        panic!("I haven't tested this case");
                        self.last_seen_change = pos;
                        ViewManagerState::HaveViewChange
                    }
                    _ => return Err(e),
                }
            }
        };

        Ok(Async::NotReady)
    }
}

impl<S: Store, V: Stream<Item = ChainView, Error = Error>> Future for ViewManager<S, V> {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<(), Error> {
        loop {
            try_ready!(self.check_for_view_change());
            try_ready!(self.maybe_submit_new_change());
            try_ready!(self.maybe_await_store());

            if let &ViewManagerState::Terminated = &self.state {
                return Ok(Async::Ready(()));
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
        HostConfig {
            head: "127.0.0.1:23".parse().unwrap(),
            tail: "127.0.0.1:42".parse().unwrap(),
        }
    }

    #[test]
    fn should_write_to_store_when_given_update() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();

        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();
        let timeout = Duration::from_millis(100);

        let mut core = Core::new().expect("core::new");
        let (tx, rx) = channel::channel::<ChainView>(&core.handle()).expect("channel");
        let vm = ViewManager::new(store.clone(), &anidentity(), rx.map_err(|e| e.into()));

        core.handle().spawn(vm.map_err(|e| panic!("Replicator failed!: {:?}", e)));

        let aview: ChainView = ChainView::of(vec![anidentity()]);
        tx.send(aview.clone()).expect("send");

        let (pos, entry) = core.run(timer.timeout(store.fetch_next(LogPos::zero()), timeout))
            .expect("next change");

        assert_eq!(entry, LogEntry::ViewChange(aview));
    }
}
