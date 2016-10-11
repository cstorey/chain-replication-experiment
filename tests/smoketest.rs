#[macro_use]
extern crate log;
extern crate futures;
extern crate tokio_core as tokio;
extern crate tokio_service as service;
extern crate tokio_proto as proto;
extern crate env_logger;
extern crate vastatrix;

use proto::pipeline::{self, Frame};
use tokio::reactor::{Core, Handle};
use service::Service;
use futures::{Poll, Async, Future};
use futures::stream::{self, Stream};
use std::sync::{Mutex,Arc};
use vastatrix::{RamStore,LogPos};

use vastatrix::sexp_proto;

#[test]
#[ignore]
fn smoketest() {
    env_logger::init().unwrap_or(());

    let mut core = Core::new().unwrap();

    let store = RamStore::new();

    let head_host = sexp_proto::server::serve(&core.handle(), "127.0.0.1:0".parse().unwrap(),
            vastatrix::ServerService::new(store.clone()))
        .expect("start-head");
    let tail_host = sexp_proto::server::serve(&core.handle(), "127.0.0.1:0".parse().unwrap(),
            vastatrix::TailService::new(store.clone()))
        .expect("start-tail");

    info!("Head at: {:?}, tail at: {:?}", head_host.local_addr(), tail_host.local_addr());
    let client = vastatrix::FatClient::new(core.handle(), head_host.local_addr(), tail_host.local_addr());

    let f = client.log_item(b"hello".to_vec())
        .and_then(|pos0| { client.log_item(b"world".to_vec()).map(move |pos1| (pos0, pos1)) });
    let (wpos0, wpos1) = core.run(f).expect("run write");

    info!("Wrote to offset:{:?}", (wpos0, wpos1));

    let item_f = client.fetch_next(LogPos::zero())
            .and_then(|first| client.fetch_next(LogPos::zero()).map(|second| vec![first, second]));

    let read = core.run(item_f).expect("run read");
    info!("Got: {:?}", read);

    assert_eq!(read, vec![(wpos0, b"hello".to_vec()), (wpos1, b"world".to_vec())]);
}
