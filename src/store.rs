use {LogPos, Error, ErrorKind};
use futures::{self, Future, BoxFuture, Poll, Async};
use std::sync::{Arc, Mutex};
use stable_bst::map::TreeMap;

pub trait Store {
    type AppendFut : Future<Item=(), Error=Error>;
    type FetchFut : Future<Item=(LogPos, Vec<u8>), Error=Error>;
    fn append_entry(&self, current: LogPos, next: LogPos, value: Vec<u8>) -> Self::AppendFut;
    fn fetch_next(&self, current: LogPos) -> Self::FetchFut;
}

#[derive(Debug)]
struct RamInner {
    log: TreeMap<LogPos, Vec<u8>>,
}

#[derive(Debug,Clone)]
pub struct RamStore {
    inner: Arc<Mutex<RamInner>>,
}

pub struct FetchFut(Arc<Mutex<RamInner>>, LogPos);

impl RamStore {
    pub fn new() -> Self {
        let inner = RamInner { log: TreeMap::new() };
        RamStore { inner: Arc::new(Mutex::new(inner)) }
    }
}

impl Store for RamStore {
    type AppendFut = BoxFuture<(), Error>;
    type FetchFut = FetchFut;

    fn append_entry(&self, current: LogPos, next: LogPos, val: Vec<u8>) -> Self::AppendFut {
        let inner = self.inner.clone();
        futures::lazy(move || {
            let mut inner = inner.lock().expect("lock");
            use stable_bst::Bound::*;
            let current_head = inner.log
                                    .range(Unbounded, Unbounded)
                                    .rev()
                                    .map(|(off, _)| off)
                                    .cloned()
                                    .next()
                                    .unwrap_or_else(LogPos::zero);

            debug!("append_entry: if at:{:?}; next:{:?}; current head:{:?}",
                   current,
                   next,
                   current_head);
            if current_head >= next {
                return futures::failed(ErrorKind::BadSequence(current_head).into()).boxed();
            }

            inner.log.insert(next, val);
            debug!("Wrote to {:?}", next);
            futures::finished(()).boxed()
        })
            .boxed()
    }

    fn fetch_next(&self, current: LogPos) -> Self::FetchFut {
        FetchFut(self.inner.clone(), current.next())
    }
}

impl Future for FetchFut {
    type Item =(LogPos, Vec<u8>);
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let inner = self.0.lock().expect("lock");
        let next = self.1;
        trace!("Polling: {:?} for {:?}", next, *inner);
        if let Some(r) = inner.log.get(&next) {
            trace!("Found: {:?}b", r.len());
            Ok(Async::Ready((next, r.clone())))
        } else {
            trace!("NotReady");
            Ok(Async::NotReady)
        }
    }
}
