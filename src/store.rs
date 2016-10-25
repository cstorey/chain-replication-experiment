use {LogPos, Error, ErrorKind};
use futures::{self, Future, BoxFuture, Poll, Async, task};
use std::sync::{Arc, Mutex};
use stable_bst::map::TreeMap;
use std::collections::VecDeque;
use std::fmt;
use replica::LogEntry;
use spki_sexp as sexp;

#[derive(Debug,Clone,Eq,PartialEq,Ord,PartialOrd,Hash)]
pub enum StoreKey {
    Data,
    Meta,
}
pub trait Store {
    type AppendFut: Future<Item = (), Error = Error>;
    type FetchFut: Future<Item = (LogPos, StoreKey, LogEntry), Error = Error>;
    fn append_entry(&self,
                    current: LogPos,
                    next: LogPos,
                    key: StoreKey,
                    value: LogEntry)
                    -> Self::AppendFut;
    fn fetch_next(&self, current: LogPos) -> Self::FetchFut;
}

struct RamInner {
    log: TreeMap<LogPos, (StoreKey, LogEntry)>,
    waiters: VecDeque<task::Task>,
}

impl fmt::Debug for RamInner {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("RamInner")
            .field("log/len", &self.log.len())
            .field("waiters/len", &self.waiters.len())
            .finish()
    }
}

#[derive(Debug,Clone)]
pub struct RamStore {
    inner: Arc<Mutex<RamInner>>,
}

pub struct FetchFut(Arc<Mutex<RamInner>>, LogPos);

impl RamStore {
    pub fn new() -> Self {
        let inner = RamInner {
            log: TreeMap::new(),
            waiters: VecDeque::new(),
        };
        RamStore { inner: Arc::new(Mutex::new(inner)) }
    }
}

impl Store for RamStore {
    type AppendFut = BoxFuture<(), Error>;
    type FetchFut = FetchFut;

    fn append_entry(&self,
                    current: LogPos,
                    next: LogPos,
                    key: StoreKey,
                    val: LogEntry)
                    -> Self::AppendFut {
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

                inner.log.insert(next, (key, val));
                debug!("Wrote to {:?}", next);
                trace!("Notify {:?} waiters", inner.waiters.len());
                for waiter in inner.waiters.drain(..) {
                    waiter.unpark()
                }
                futures::finished(()).boxed()
            })
            .boxed()
    }

    fn fetch_next(&self, current: LogPos) -> Self::FetchFut {
        FetchFut(self.inner.clone(), current.next())
    }
}

impl Future for FetchFut {
    type Item = (LogPos, StoreKey, LogEntry);
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut inner = self.0.lock().expect("lock");
        let next = self.1;
        trace!("Polling: {:?} for {:?}", next, *inner);
        if let Some(&(ref key, ref val)) = inner.log.get(&next) {
            trace!("Found: {:?}:{:?}", key, val);
            return Ok(Async::Ready((next, key.clone(), val.clone())));
        }

        inner.waiters.push_back(task::park());
        trace!("NotReady: {:?}", *inner);
        Ok(Async::NotReady)
    }
}
