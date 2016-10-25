use service::Service;
use futures::{Async, Poll, Future};
use super::{ReplicaRequest, ReplicaResponse, LogPos};
use store::{Store, StoreKey};

use errors::{Error, ErrorKind};

/// The main interface to the outside world.
#[derive(Clone,Debug)]
pub struct ServerService<S> {
    store: S,
}

#[derive(Debug)]
pub enum ReplicaFut<F> {
    Append(F, LogPos),
}

impl<S> ServerService<S> {
    pub fn new(store: S) -> Self {
        ServerService { store: store }
    }
}

impl<S: Store> Service for ServerService<S> {
    // The type of the input requests we get.
    type Request = ReplicaRequest;
    type Response = ReplicaResponse;
    type Error = Error;
    type Future = ReplicaFut<S::AppendFut>;

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("ReplicaResponse#service: {:?}", req);
        match req {
            ReplicaRequest::AppendLogEntry { assumed_offset, entry_offset, datum } => {
                let append_fut = self.store
                    .append_entry(assumed_offset, entry_offset, StoreKey::Data, datum);
                ReplicaFut::Append(append_fut, entry_offset)
            }
        }
    }
}

impl<F: Future<Item = (), Error = Error>> Future for ReplicaFut<F> {
    type Item = ReplicaResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            &mut ReplicaFut::Append(ref mut f, offset) => {
                trace!("ReplicaFut::Append");
                match f.poll() {
                    Ok(Async::Ready(())) => {
                        trace!("ReplicaFut::Append: => Done");
                        Ok(Async::Ready(ReplicaResponse::Done(offset)))
                    }
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(e) => {
                        if let &ErrorKind::BadSequence(head) = e.kind() {
                            debug!("Bad Sequence; head at: {:?}", head);
                            Ok(Async::Ready(ReplicaResponse::BadSequence(head)))
                        } else {
                            Err(e)
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use futures::Future;
    use service::Service;
    use replica::LogPos;
    use store::RamStore;
    use replica::{ReplicaRequest, ReplicaResponse};
    use super::*;

    #[test]
    fn should_write_to_empty_log() {
        let store = RamStore::new();
        let replica = ServerService::new(store.clone());

        let zero = LogPos::zero();
        let resp = replica.call(ReplicaRequest::AppendLogEntry {
            assumed_offset: zero,
            entry_offset: zero.next(),
            datum: b"foobar".to_vec(),
        });

        assert_eq!(resp.wait().expect("append"),
                   ReplicaResponse::Done(zero.next()))
    }

    #[test]
    fn should_pass_through_bad_sequence_error() {
        let store = RamStore::new();
        let replica = ServerService::new(store.clone());

        let zero = LogPos::zero();
        let resp = replica.call(ReplicaRequest::AppendLogEntry {
            assumed_offset: zero.next(),
            entry_offset: zero,
            datum: b"foobar".to_vec(),
        });

        assert_eq!(resp.wait().expect("append"),
                   ReplicaResponse::BadSequence(zero))
    }
}
