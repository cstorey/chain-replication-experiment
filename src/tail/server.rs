use service::Service;
use futures::{BoxFuture, Async, Future};
use super::{TailRequest, TailResponse};
use store::Store;
use replica::LogEntry;

use Error;

/// The main interface to the outside world.
#[derive(Clone,Debug)]
pub struct TailService<S> {
    store: S,
}

impl<S: Store> TailService<S> {
    pub fn new(store: S) -> Self {
        TailService { store: store }
    }
}

impl<S: Store + Send> Service for TailService<S>
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
            TailRequest::FetchNextAfter(pos) => {
                self.store
                    .fetch_next(pos)
                    .map(|(pos, _key, val)| match val {
                        LogEntry::Data(val) => TailResponse::NextItem(pos, val),
                        LogEntry::Config(_) => unimplemented!(),
                    })
                    .map_err(|e| e.into())
                    .then(|r| {
                        debug!("Response: {:?}", r);
                        r
                    })
                    .boxed()
            }
        }
        // futures::finished(TailResponse::NextItem(LogPos::zero(), vec![]))
    }
}

#[cfg(test)]
mod test {
    use futures::{Async, task};
    use service::Service;
    use replica::{LogPos, LogEntry};
    use store::{Store, RamStore, StoreKey};
    use tail::messages::*;
    use super::*;
    use std::sync::Arc;

    struct NullUnpark;

    impl task::Unpark for NullUnpark {
        fn unpark(&self) {}
    }

    fn null_parker() -> Arc<task::Unpark> {
        Arc::new(NullUnpark)
    }

    #[test]
    fn should_defer_read_from_empty_log() {
        let store = RamStore::new();
        let tail = TailService::new(store.clone());

        let mut resp = task::spawn(tail.call(TailRequest::FetchNextAfter(LogPos::zero())));

        assert_eq!(resp.poll_future(null_parker()).expect("fut"),
                   Async::NotReady)
    }

    #[test]
    fn shuold_yield_first_item_after_write() {
        let store = RamStore::new();
        let tail = TailService::new(store.clone());

        let mut resp = task::spawn(tail.call(TailRequest::FetchNextAfter(LogPos::zero())));
        let next = LogPos::zero().next();
        let entry = LogEntry::Data(b"foo".to_vec());
        task::spawn(store.append_entry(LogPos::zero(), next, StoreKey::Data, entry))
            .wait_future()
            .expect("append_entry 1");

        assert_eq!(resp.poll_future(null_parker()).expect("fut"),
                   Async::Ready(TailResponse::NextItem(next, b"foo".to_vec())));
    }

}
