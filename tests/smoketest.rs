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
use vastatrix::RamStore;

use vastatrix::sexp_proto;

#[test]
fn stuff() {
    env_logger::init().unwrap_or(());

    let mut core = Core::new().unwrap();

    let store = RamStore::new();

    let head_host = sexp_proto::server::serve(&core.handle(), "127.0.0.1:0".parse().unwrap(),
            vastatrix::ServerService::new(store.clone()))
        .expect("start-head");
    let tail_host = sexp_proto::server::serve(&core.handle(), "127.0.0.1:0".parse().unwrap(),
            vastatrix::TailService::new(store.clone()))
        .expect("start-tail");
    let mut client = vastatrix::FatClient::new(core.handle(), head_host.local_addr(), tail_host.local_addr());

    let f = client.log_item(b"hello world".to_vec());
    let wpos = core.run(f).expect("run write");

    println!("Wrote to offset:{:?}", wpos);

    // let item_f = client.consume();

    // let (rpos, item) = core.run(f).expect("run read");
}
