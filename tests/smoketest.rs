extern crate futures;
extern crate tokio_core as tokio;
extern crate tokio_service as service;
extern crate tokio_proto as proto;
extern crate env_logger;
extern crate vastatrix;

use proto::pipeline::{self, Frame};
use tokio::reactor::{Core, Handle};
use service::Service;
use futures::{Poll, Async};
use futures::stream::{self, Stream};
use std::sync::Mutex;

use vastatrix::sexp_proto;

#[test]
fn stuff() {
    env_logger::init().unwrap_or(());

    let mut core = Core::new().unwrap();

    let head = vastatrix::ServerService::new();
    let head_host = sexp_proto::server::serve(&core.handle(), "127.0.0.1:0".parse().unwrap(), head);


    
}
