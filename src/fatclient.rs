use tokio::reactor::Handle;
use futures::{Future, Poll, Async};
use std::net::SocketAddr;
use replica::client::ReplicaClient;
use tail::client::TailClient;
use tail::{TailRequest, TailResponse};
use replica::{LogPos, ReplicaRequest, ReplicaResponse};
use tokio_service::Service;
use std::sync::{Arc, Mutex};
use {Error, ErrorKind};

#[derive(Debug)]
pub struct FatClient {
    head: Arc<ReplicaClient>,
    tail: Arc<TailClient>,
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
type TailFut = <TailClient as Service>::Future;

pub struct LogItemFut(ReplicaFut);
pub struct FetchNextFut(TailFut);

//

impl FatClient {
    pub fn new(handle: Handle, head: &SocketAddr, tail: &SocketAddr) -> Self {
        let repl = ReplicaClient::new(handle.clone(), head);
        let tail = TailClient::new(handle, tail);

        FatClient {
            head: Arc::new(repl),
            tail: Arc::new(tail),
            last_known_head: Arc::new(Mutex::new(LogPos::zero())),
        }
    }

    pub fn log_item(&self, body: Vec<u8>) -> LogItemFut {
        let current = *self.last_known_head.lock().expect("lock current");
        let req = ReplicaRequest::AppendLogEntry {
            assumed_offset: current,
            entry_offset: current.next(),
            datum: body.clone(),
        };
        LogItemFut(self.head.call(req))
    }

    pub fn fetch_next(&self, after: LogPos) -> FetchNextFut {
        let req = TailRequest::FetchNextAfter(after);
        let f = self.tail.call(req);
        FetchNextFut(f)
    }
}

impl Future for LogItemFut {
    type Item = LogPos;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.0.poll()) {
            ReplicaResponse::Done(offset) => {
                debug!("Done =>{:?}", offset);
                return Ok(Async::Ready(offset));
            }
            ReplicaResponse::BadSequence(head) => {
                debug!("BadSequence =>{:?}", head);
                return Err(ErrorKind::BadSequence(head).into());
            }
        }
    }
}
impl Future for FetchNextFut {
    type Item = (LogPos, Vec<u8>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.0.poll()) {
            TailResponse::NextItem(offset, value) => {
                debug!("Done =>{:?}", offset);
                return Ok(Async::Ready((offset, value)));
            }
        }

    }
}
