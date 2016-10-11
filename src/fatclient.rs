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
pub struct FatClient<H, T> {
    head: H,
    tail: T,
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
pub struct LogItemFut<F>(F);
pub struct FetchNextFut<F>(F);

//

impl FatClient<ReplicaClient, TailClient> {
    pub fn new(handle: Handle, head: &SocketAddr, tail: &SocketAddr) -> Self {
        let repl = ReplicaClient::new(handle.clone(), head);
        let tail = TailClient::new(handle, tail);

        FatClient {
            head: repl,
            tail: tail,
            last_known_head: Arc::new(Mutex::new(LogPos::zero())),
        }
    }
}

impl<H, T> FatClient<H, T>
    where H: Service<Request = ReplicaRequest, Response = ReplicaResponse>,
          T: Service<Request = TailRequest, Response = TailResponse>
{
    pub fn log_item(&self, body: Vec<u8>) -> LogItemFut<H::Future> {
        let current = *self.last_known_head.lock().expect("lock current");
        let req = ReplicaRequest::AppendLogEntry {
            assumed_offset: current,
            entry_offset: current.next(),
            datum: body.clone(),
        };
        LogItemFut(self.head.call(req))
    }

    pub fn fetch_next(&self, after: LogPos) -> FetchNextFut<T::Future> {
        let req = TailRequest::FetchNextAfter(after);
        let f = self.tail.call(req);
        FetchNextFut(f)
    }
}

impl<F: Future<Item = ReplicaResponse, Error = Error>> Future for LogItemFut<F> {
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

impl<F: Future<Item = TailResponse, Error = Error>> Future for FetchNextFut<F> {
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
