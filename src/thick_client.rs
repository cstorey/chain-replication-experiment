use tokio::reactor::Handle;
use futures::{Future, Poll, Async};
use std::net::SocketAddr;
use {ReplicaClient, TailClient};
use {TailRequest, TailResponse};
use tokio_service::Service;
use std::sync::{Arc, Mutex};
use replica::{LogPos, ReplicaRequest, ReplicaResponse, LogEntry, HostConfig, ChainView};
use Error;

#[derive(Debug)]
pub struct ThickClient<H, T> {
    head: Arc<H>,
    tail: Arc<T>,
    state: Arc<Mutex<ClientState>>,
}
#[derive(Debug)]
struct ClientState {
    last_known_head: LogPos,
    view: ChainView,
}

pub type ThickClientTcp = ThickClient<ReplicaClient, TailClient>;

// States:
// ```dot
// new -> request_sent;
// request_sent -> done_okay;
// request_sent -> failed_badver;
// failed_badver -> request_sent;
// ```
//
pub struct LogItemFut<H: Service> {
    head: Arc<H>,
    req: ReplicaRequest,
    future: H::Future,
}
pub struct FetchNextFut<F>(F);

impl ThickClient<ReplicaClient, TailClient> {
    pub fn new(handle: Handle, head: &SocketAddr, tail: &SocketAddr) -> Self {
        Self::build(ReplicaClient::connect(handle.clone(), head),
                    TailClient::connect(handle, tail))
    }
}

impl<H, T> ThickClient<H, T>
    where H: Service<Request = ReplicaRequest, Response = ReplicaResponse>,
          T: Service<Request = TailRequest, Response = TailResponse>
{
    pub fn build(head: H, tail: T) -> Self {
        let state = ClientState {
            last_known_head: LogPos::zero(),
            view: ChainView::default(),
        };
        ThickClient {
            head: Arc::new(head),
            tail: Arc::new(tail),
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub fn log_item(&self, body: Vec<u8>) -> LogItemFut<H> {
        let current = self.state.lock().expect("lock current").last_known_head;
        let req = ReplicaRequest::AppendLogEntry {
            assumed_offset: current,
            entry_offset: current.next(),
            datum: LogEntry::Data(body.into()),
        };
        trace!("Sending assuming: {:?}", current);
        self.do_log_item(req)
    }

    // TODO: Move into seperate service widget, that takes updates from the
    // ViewManager, and pushes them into the replication stream.
    pub fn add_peer(&self, peer: HostConfig) -> LogItemFut<H> {
        let mut state = self.state.lock().expect("lock current");
        let current = state.last_known_head;
        state.view.members.push(peer);
        let req = ReplicaRequest::AppendLogEntry {
            assumed_offset: current,
            entry_offset: current.next(),
            datum: LogEntry::ViewChange(state.view.clone()),
        };
        trace!("Sending assuming: {:?}", current);
        self.do_log_item(req)
    }

    fn do_log_item(&self, req: ReplicaRequest) -> LogItemFut<H> {
        let fut = self.head.call(req.clone());
        LogItemFut {
            head: self.head.clone(),
            future: fut,
            req: req,
        }
    }


    pub fn fetch_next(&self, after: LogPos) -> FetchNextFut<T::Future> {
        let req = TailRequest::FetchNextAfter(after);
        let f = self.tail.call(req);
        FetchNextFut(f)
    }
}

impl<S: Service<Request = ReplicaRequest, Response = ReplicaResponse, Error = Error>> Future for LogItemFut<S> {
    type Item = LogPos;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let &mut LogItemFut { ref head, ref mut req, ref mut future } = self;
            match try_ready!(future.poll()) {
                ReplicaResponse::Done(offset) => {
                    debug!("Done =>{:?}", offset);
                    return Ok(Async::Ready(offset));
                }
                ReplicaResponse::BadSequence(new_offset) => {
                    debug!("BadSequence =>{:?}", new_offset);
                    match req {
                        &mut ReplicaRequest::AppendLogEntry { ref mut assumed_offset, ref mut entry_offset, .. } => {
                            *assumed_offset = new_offset;
                            *entry_offset = new_offset.next();
                        }
                    };
                    debug!("Sending assuming: {:?}", new_offset);
                    *future = head.call(req.clone());
                }
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
                return Ok(Async::Ready((offset, value.into())));
            }
        }

    }
}

#[cfg(test)]
mod test {
    use futures::{self, Future, BoxFuture};
    use service::simple_service;
    use tail::{TailRequest, TailResponse};
    use replica::{LogPos, LogEntry, ReplicaRequest, ReplicaResponse};
    use std::sync::{Arc, Mutex};
    use super::*;
    use Error;
    use std::collections::VecDeque;
    use env_logger;

    #[test]
    fn sends_initial_request() {
        env_logger::init().unwrap_or(());
        let head_reqs = Arc::new(Mutex::new(Vec::new()));
        let head = {
            let reqs = head_reqs.clone();
            simple_service(move |req: ReplicaRequest| -> BoxFuture<ReplicaResponse, Error> {
                reqs.lock().expect("lock").push(req);
                futures::finished(ReplicaResponse::Done(LogPos::zero())).boxed()
            })
        };
        let tail = simple_service(|_: TailRequest| -> BoxFuture<TailResponse, Error> { unimplemented!() });
        let client = ThickClient::build(head, tail);

        client.log_item(b"Hello".to_vec()).wait().unwrap();

        let reqs = &*head_reqs.lock().unwrap();
        let appends = reqs.iter()
            .filter_map(|r| {
                let &ReplicaRequest::AppendLogEntry { ref datum, .. } = r;
                Some((datum.clone()))
            })
            .collect::<Vec<_>>();
        assert_eq!(&*appends.iter().collect::<Vec<_>>(),
                   &[&LogEntry::Data(b"Hello".to_vec().into())]);
    }


    #[test]
    fn resends_with_new_sequence_no_on_cas_failure() {
        env_logger::init().unwrap_or(());
        let head_reqs = Arc::new(Mutex::new(VecDeque::new()));
        let mut head_resps = VecDeque::new();
        head_resps.push_back(ReplicaResponse::BadSequence(LogPos::new(42)));
        head_resps.push_back(ReplicaResponse::Done(LogPos::new(43)));
        let head_resps = Arc::new(Mutex::new(head_resps));

        let head = {
            let reqs = head_reqs.clone();
            let resps = head_resps.clone();
            simple_service(move |req: ReplicaRequest| -> BoxFuture<ReplicaResponse, Error> {
                reqs.lock().expect("lock").push_back(req);
                let resp = resps.lock().unwrap().pop_front().expect("response");
                futures::finished(resp).boxed()
            })
        };

        let tail = simple_service(|_: TailRequest| -> BoxFuture<TailResponse, Error> { unimplemented!() });
        let client = ThickClient::build(head, tail);

        client.log_item(b"Hello".to_vec()).wait().unwrap();

        let ref reqs = *head_reqs.lock().unwrap();
        let mut appends = reqs.iter()
            .filter_map(|r| {
                let &ReplicaRequest::AppendLogEntry { assumed_offset, entry_offset, ref datum } = r;
                Some((assumed_offset, entry_offset, datum.clone()))
            })
            .collect::<VecDeque<_>>();
        let r0 = appends.pop_front().unwrap();
        let r1 = appends.pop_front().unwrap();

        assert_eq!((r1.0, &r1.2),
                   (LogPos::new(42), &LogEntry::Data(b"Hello".to_vec().into())));
        assert_eq!(r0.2, r1.2);
        assert!(r1.0 < r1.1);

    }
}
