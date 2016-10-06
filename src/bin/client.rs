extern crate futures;
extern crate tokio_core as tokio;
extern crate tokio_service as service;
extern crate tokio_proto as proto;
extern crate sexp_proto_tokio as spki_proto;
extern crate env_logger;
use std::env::args;
use std::io;

use proto::pipeline::{self, Frame};
use tokio::reactor::{Core, Handle};
use service::Service;
use futures::{Poll, Async};
use futures::stream::{self, Stream};

enum Empty {}

pub fn main() {
    env_logger::init().unwrap();

    let mut core = Core::new().unwrap();

    let addr = "127.0.0.1:12345".parse().unwrap();

    let client: spki_proto::client::Client<String, String> =
        spki_proto::client::connect(core.handle(), &addr);
    // - one that returns a future that we can 'await' on.
    let task = stream::iter(args().map(Ok).skip(1))
                   .and_then(|s| client.call(s.to_string()))
                   .for_each(|r| Ok(println!("RESPONSE: {:?}", r)));

    let res = core.run(task);
    println!("Done:{:?}", res);
}
