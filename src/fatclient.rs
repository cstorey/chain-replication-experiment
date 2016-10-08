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
    client: Arc<ReplicaClient>,
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
pub enum LogItemFut {
    Start(Arc<ReplicaClient>, Arc<Mutex<LogPos>>, Vec<u8>),
    Sent(Arc<ReplicaClient>, Arc<Mutex<LogPos>>, Vec<u8>, ReplicaFut),
    Invalid,
}

pub struct AwaitCommitFut {
    client: Arc<ReplicaClient>,
    offset: LogPos,
}

impl FatClient {
    pub fn new(handle: Handle, target: &SocketAddr) -> Self {
        let repl = ReplicaClient::new(handle, target);

        FatClient {
            client: Arc::new(repl),
            last_known_head: Arc::new(Mutex::new(LogPos::zero())),
        }
    }

    pub fn log_item(&mut self, body: Vec<u8>) -> LogItemFut {
        LogItemFut::Start(self.client.clone(), self.last_known_head.clone(), body)
    }
    pub fn await_commit(&mut self, offset: LogPos) -> AwaitCommitFut {
        AwaitCommitFut {
            client: self.client.clone(),
            offset: offset,
        }
    }
}

impl Future for LogItemFut {
    type Item = LogPos;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            debug!("LogItemFut::poll@{}:{}: {:?}", file!(), line!(), self);
            match mem::replace(self, LogItemFut::Invalid) {
                LogItemFut::Start(client, pos, body) => {
                    let current = *pos.lock().expect("lock current");
                    let f = client.append_entry(current, current.next(), body.clone());

                    debug!("=> LogItemFut::Sent");
                    *self = LogItemFut::Sent(client.clone(), pos.clone(), body, f);
                }
                LogItemFut::Sent(client, pos, body, mut fut) => {
                    debug!("LogItemFut::Sent");
                    match try!(fut.poll()) {
                        Async::Ready(ServerResponse::Done(offset)) => {
                            debug!("Done =>{:?}", offset);
                            return Ok(Async::Ready(offset));
                        }
                        Async::Ready(ServerResponse::BadSequence(new_offset)) => {
                            debug!("BadSequence =>{:?}", new_offset);
                            // FIXME: Can be pathological if we have already observed a more recent value?
                            *pos.lock().expect("lock current") = new_offset;

                            *self = LogItemFut::Start(client, pos, body);
                            debug!("=> LogItemFut::Start");
                        }
                        Async::NotReady => {
                            *self = LogItemFut::Sent(client, pos, body, fut);
                            return Ok(Async::NotReady);
                        }
                    }
                }
                LogItemFut::Invalid => {
                    panic!("Invalid state in LogItemFut");
                }
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
