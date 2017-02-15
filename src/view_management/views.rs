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
        trace!("check_for_view_change: {:?}", self.state);
        if let &mut ViewManagerState::Waiting = &mut self.state {
            ()
        } else {
            return Ok(Async::Ready(()));
        };

        match try_ready!(self.views.poll()) {
            Some(change) => {
                if change.is_head(&self.identity) {
                    info!("New view change: {:?}", change);
                    self.last_view = change;
                    self.state = ViewManagerState::HaveViewChange;
                } else {
                    info!("Observed view change for someone else:{:?}", change);
                }
            }
            _ => {
                self.state = ViewManagerState::Terminated;
                trace!("Views source terminated");
            }
        }
        Ok(Async::Ready(()))
    }

    fn maybe_submit_new_change(&mut self) -> Poll<(), Error> {
        trace!("maybe_submit_new_change: {:?}", self.state);
        if let &mut ViewManagerState::HaveViewChange = &mut self.state {
            ()
        } else {
            return Ok(Async::Ready(()));
        };
        let curr = self.last_seen_change;
        let next = curr.next();
        info!("Storing view change:{:?}->{:?}: {:?}",
              curr,
              next,
              self.last_view);
        let store_fut = self.store
            .append_entry(curr, next, LogEntry::ViewChange(self.last_view.clone()));
        self.last_seen_change = next;

        self.state = ViewManagerState::AwaitStore(store_fut);
        trace!("Awaiting store");

        Ok(Async::Ready(()))
    }

    fn maybe_await_store(&mut self) -> Poll<(), Error> {
        trace!("maybe_await_store: {:?}", self.state);
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
            Ok(_) => {
                info!("Stored view as: {:?}", self.last_seen_change);
                ViewManagerState::Waiting
            }
            Err(e) => {
                match e.kind() {
                    &ErrorKind::BadSequence(pos) => {
                        trace!("Head at: {:?}", pos);
                        self.last_seen_change = pos;
                        ViewManagerState::HaveViewChange
                    }
                    _ => return Err(e),
                }
            }
        };

        Ok(Async::Ready(()))
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
    use futures::{Future, Sink};
    use futures::stream::Stream;
    use futures::sync::mpsc;
    use replica::LogPos;
    use store::{RamStore, Store};
    use replica::{LogEntry, HostConfig, ChainView};
    use errors::*;
    use super::*;
    use tokio::reactor::Core;
    use std::time::Duration;
    use tokio_timer;
    use env_logger;


    fn anidentity() -> HostConfig {
        HostConfig {
            head: "127.0.0.1:23".parse().unwrap(),
            tail: "127.0.0.1:42".parse().unwrap(),
        }
    }

    fn anidentity2() -> HostConfig {
        HostConfig {
            head: "10.0.0.1:23".parse().unwrap(),
            tail: "10.0.0.1:42".parse().unwrap(),
        }
    }



    fn append_entry<S: Store>(store: &S, core: &mut Core, off: LogPos, entry: LogEntry) -> LogPos {
        let next = off.next();
        debug!("append config message {:?} -> {:?}", off, next);
        core.run(store.append_entry(off, next, entry))
            .expect("append");
        next
    }


    #[test]
    fn should_write_to_store_when_given_update() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();

        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();
        let timeout = Duration::from_millis(100);

        let mut core = Core::new().expect("core::new");
        let (tx, rx) = mpsc::channel::<ChainView>(10);
        let vm = ViewManager::new(store.clone(),
                                  &anidentity(),
                                  rx.map_err(|_| "reciver error".into()));

        core.handle().spawn(vm.map_err(|e| panic!("Replicator failed!: {:?}", e)));

        let aview: ChainView = ChainView::of(vec![anidentity()]);
        core.run(tx.send(aview.clone())).expect("send");

        let entries = core.run(timer.timeout_stream(store.fetch_from(LogPos::zero()), timeout)
                .take(1)
                .map(|(_pos, entry)| entry)
                .collect())
            .expect("next change");

        assert_eq!(entries, vec![LogEntry::ViewChange(aview)]);
    }

    #[test]
    fn should_find_current_offset() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();

        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();
        let timeout = Duration::from_millis(100);

        let mut core = Core::new().expect("core::new");

        let log_off0 = append_entry(&store,
                                    &mut core,
                                    LogPos::zero(),
                                    LogEntry::Data(b"Hello world!".to_vec().into()));

        let (tx, rx) = mpsc::channel::<ChainView>(10);
        let vm = ViewManager::new(store.clone(),
                                  &anidentity(),
                                  rx.map_err(|_| "reciever error".into()));

        core.handle().spawn(vm.map_err(|e| panic!("Replicator failed!: {:?}", e)));

        let aview: ChainView = ChainView::of(vec![anidentity()]);
        core.run(tx.send(aview.clone())).expect("send");

        let (_pos, entry) = core.run(timer.timeout(store.fetch_from(log_off0)
                                   .into_future()
                                   .map(|(it, _)| it)
                                   .map_err(|(e, _)| e),
                               timeout))
            .expect("next change")
            .expect("next change");

        assert_eq!(entry, LogEntry::ViewChange(aview));
    }

    #[test]
    fn should_only_update_view_id_when_current_head() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();

        let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();
        let timeout = Duration::from_millis(100);

        let mut core = Core::new().expect("core::new");
        let (tx, rx) = mpsc::channel::<ChainView>(10);
        let vm = ViewManager::new(store.clone(),
                                  &anidentity(),
                                  rx.map_err(|_| "reciever error".into()));

        core.handle().spawn(vm.map_err(|e| panic!("Replicator failed!: {:?}", e)));

        let aview: ChainView = ChainView::of(vec![anidentity2(), anidentity()]);
        core.run(tx.send(aview.clone())).expect("send");

        let result = core.run(timer.timeout_stream(store.fetch_from(LogPos::zero()), timeout)
            .take(1)
            .map(|(_pos, e)| e)
            .collect());

        assert!(result.is_err(), "Result should be error:{:?}", result);

        let is_timeout = match result.as_ref().unwrap_err().kind() {
            &ErrorKind::Timeout => true,
            _ => false,
        };
        assert!(is_timeout, "Is Timeout error:{:?}", result);

    }
}
