use tokio::reactor::Handle;
use futures::{Future, Poll, Async};
use std::net::SocketAddr;
use replica::client::{ReplicaClient, Error};
use messages::{LogPos, ServerResponse};
use tokio_service::Service;
use std::sync::{Arc, Mutex};
use std::mem;

#[derive(Debug)]
pub struct FatClient {
    head: Arc<ReplicaClient>,
    last_known_head: Arc<Mutex<LogPos>>,
}

// States:
// ```dot
// new -> request_sent;
// request_sent -> done_okay;
// request_sent -> failed_badver;
// failed_badver -> request_sent;
// ```
//
type ReplicaFut = <ReplicaClient as Service>::Future;

#[derive(Debug)]
pub struct LogItemFut(ReplicaFut);

pub struct AwaitCommitFut {
    client: Arc<ReplicaClient>,
    offset: LogPos,
}

impl FatClient {
    pub fn new(handle: Handle, target: &SocketAddr) -> Self {
        let repl = ReplicaClient::new(handle, target);

        FatClient {
            head: Arc::new(repl),
            last_known_head: Arc::new(Mutex::new(LogPos::zero())),
        }
    }

    pub fn log_item(&mut self, body: Vec<u8>) -> LogItemFut {
        let current = *self.last_known_head.lock().expect("lock current");
        let f = self.head.append_entry(current, current.next(), body.clone());
        LogItemFut(f)
    }
    pub fn await_commit(&mut self, offset: LogPos) -> AwaitCommitFut {
        AwaitCommitFut {
            client: self.head.clone(),
            offset: offset,
        }
    }
}

impl Future for LogItemFut {
    type Item = LogPos;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try!(self.0.poll()) {
            Async::Ready(ServerResponse::Done(offset)) => {
                debug!("Done =>{:?}", offset);
                return Ok(Async::Ready(offset));
            }
            Async::Ready(other) => {
                panic!("Unhandled response: {:?}", other);
            }
            Async::NotReady => {
                return Ok(Async::NotReady);
            }
        }
    }
}

impl Future for AwaitCommitFut {
    type Item = LogPos;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(self.offset))
    }
}
