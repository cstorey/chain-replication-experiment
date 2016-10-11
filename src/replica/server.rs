use service::Service;
use futures::{Async, Poll, Future};
use super::{ServerRequest, ServerResponse, LogPos};
use store::Store;

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
    type Request = ServerRequest;
    type Response = ServerResponse;
    type Error = Error;
    type Future = ReplicaFut<S::AppendFut>;

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("ServerResponse#service: {:?}", req);
        match req {
            ServerRequest::AppendLogEntry { assumed_offset, entry_offset, datum } => {
                ReplicaFut::Append(self.store.append_entry(assumed_offset, entry_offset, datum),
                                   entry_offset)
            }
        }
    }
}

impl<F: Future<Item = (), Error = Error>> Future for ReplicaFut<F> {
    type Item = ServerResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            &mut ReplicaFut::Append(ref mut f, offset) => {
                trace!("ReplicaFut::Append");
                match f.poll() {
                    Ok(Async::Ready(())) => {
                        trace!("ReplicaFut::Append: => Done");
                        Ok(Async::Ready(ServerResponse::Done(offset)))
                    }
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(e) => {
                        if let &ErrorKind::BadSequence(head) = e.kind() {
                            debug!("Bad Sequence; head at: {:?}", head);
                            Ok(Async::Ready(ServerResponse::BadSequence(head)))
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
    use futures::{self, Future, Async};
    use service::Service;
    use replica::LogPos;
    use store::{Store, RamStore};
    use tail::messages::*;
    use replica::{ServerRequest, ServerResponse};
    use super::*;

    #[test]
    fn should_write_to_empty_log() {
        let store = RamStore::new();
        let replica = ServerService::new(store.clone());

        let zero = LogPos::zero();
        let mut resp = replica.call(ServerRequest::AppendLogEntry {
            assumed_offset: zero,
            entry_offset: zero.next(),
            datum: b"foobar".to_vec(),
        });

        assert_eq!(resp.wait().expect("append"),
                   ServerResponse::Done(zero.next()))
    }

    #[test]
    fn should_pass_through_bad_sequence_error() {
        let store = RamStore::new();
        let replica = ServerService::new(store.clone());

        let zero = LogPos::zero();
        let mut resp = replica.call(ServerRequest::AppendLogEntry {
            assumed_offset: zero.next(),
            entry_offset: zero,
            datum: b"foobar".to_vec(),
        });

        assert_eq!(resp.wait().expect("append"),
                   ServerResponse::BadSequence(zero))
    }
}
