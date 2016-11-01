use service::Service;
use futures::{BoxFuture, Async, Future, Poll};
use super::{TailRequest, TailResponse};
use store::Store;
use replica::{LogEntry, LogPos};
use std::mem;
use std::sync::Arc;

use Error;

/// The main interface to the outside world.
#[derive(Clone,Debug)]
pub struct TailService<S> {
    store: Arc<S>,
}

impl<S: Store> TailService<S> {
    pub fn new(store: S) -> Self {
        TailService { store: Arc::new(store) }
    }
}

// self.store
// .fetch_next(pos)
// .map(|(pos, val)| match val {
// LogEntry::Data(val) => TailResponse::NextItem(pos, val),
// LogEntry::ViewChange(_) => unimplemented!(),
// })
// .map_err(|e| e.into())
// .then(|r| {
// debug!("Response: {:?}", r);
// r
// })
//
//
enum TailFuture<S: Store> {
    Start(Arc<S>, LogPos),
    Fetching(Arc<S>, S::FetchFut),
    Dead,
}

impl<S: Store> Future for TailFuture<S> {
    type Item = TailResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match mem::replace(self, TailFuture::Dead) {
                TailFuture::Start(store, pos) => {
                    debug!("TailFuture::Start:{:?}", pos);
                    let f = store.fetch_next(pos);
                    *self = TailFuture::Fetching(store, f)
                }
                TailFuture::Fetching(store, mut fut) => {
                    debug!("TailFuture::Fetching");
                    match try!(fut.poll()) {
                        Async::Ready((pos, LogEntry::Data(val))) => {
                            debug!("poll => Data@{:?}", pos);
                            let resp = TailResponse::NextItem(pos, val);
                            return Ok(Async::Ready(resp));
                        }
                        Async::Ready((pos, LogEntry::ViewChange(_))) => {
                            debug!("poll => Config@{:?}", pos);
                            *self = TailFuture::Start(store, pos);
                        }
                        Async::NotReady => {
                            *self = TailFuture::Fetching(store, fut);
                            return Ok(Async::NotReady);
                        }
                    }
                }
                TailFuture::Dead => unreachable!(),
            };
        }
    }
}

impl<S: Store + Send + Sync + 'static> Service for TailService<S>
    where S::FetchFut: Send + 'static
{
    // The type of the input requests we get.
    type Request = TailRequest;
    type Response = TailResponse;
    type Error = Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("TailService#call: {:?}", req);
        match req {
            TailRequest::FetchNextAfter(pos) => TailFuture::Start(self.store.clone(), pos).boxed(),
        }
        // futures::finished(TailResponse::NextItem(LogPos::zero(), vec![]))
    }
}

#[cfg(test)]
mod test {
    use futures::{Async, task, Future};
    use service::Service;
    use replica::{LogPos, LogEntry, HostConfig};
    use store::{Store, RamStore};
    use tokio::reactor::Core;
    use tail::messages::*;
    use super::*;
    use std::sync::Arc;
    use env_logger;

    struct NullUnpark;

    impl task::Unpark for NullUnpark {
        fn unpark(&self) {}
    }

    fn null_parker() -> Arc<task::Unpark> {
        Arc::new(NullUnpark)
    }

    #[test]
    fn should_defer_read_from_empty_log() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();
        let tail = TailService::new(store.clone());

        let mut resp = task::spawn(tail.call(TailRequest::FetchNextAfter(LogPos::zero())));

        assert_eq!(resp.poll_future(null_parker()).expect("fut"),
                   Async::NotReady)
    }

    #[test]
    fn should_yield_first_item_after_write() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();
        let tail = TailService::new(store.clone());

        let mut resp = task::spawn(tail.call(TailRequest::FetchNextAfter(LogPos::zero())));
        let next = LogPos::zero().next();
        let entry = LogEntry::Data(b"foo".to_vec().into());
        task::spawn(store.append_entry(LogPos::zero(), next, entry))
            .wait_future()
            .expect("append_entry 1");

        assert_eq!(resp.poll_future(null_parker()).expect("fut"),
                   Async::Ready(TailResponse::NextItem(next, b"foo".to_vec().into())));
    }

    #[test]
    fn should_skip_config_records() {
        env_logger::init().unwrap_or(());
        let store = RamStore::new();
        let tail = TailService::new(store.clone());
        let mut core = Core::new().unwrap();

        let zero = LogPos::zero();
        let mut resp = task::spawn(tail.call(TailRequest::FetchNextAfter(zero)));
        let one = zero.next();
        let two = one.next();

        let config = HostConfig {
            head: "1.2.3.4:5".parse().expect("parse"),
            tail: "1.2.3.6:7".parse().expect("parse"),
        };

        let config_entry = LogEntry::ViewChange(config);
        let data = LogEntry::Data(b"foo".to_vec().into());
        core.run(store.append_entry(zero, one, config_entry)
                .and_then(|()| store.append_entry(one, two, data))).expect("append_entry");

        assert_eq!(resp.poll_future(null_parker()).expect("fut"),
                   Async::Ready(TailResponse::NextItem(two, b"foo".to_vec().into())));
    }


}
