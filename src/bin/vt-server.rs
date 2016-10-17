#[macro_use]
extern crate log;
extern crate futures;
extern crate tokio_core as tokio;
extern crate tokio_service as service;
extern crate tokio_proto as proto;
extern crate env_logger;
extern crate vastatrix;
#[macro_use]
extern crate clap;

use tokio::reactor::Core;
use futures::Future;
use vastatrix::{RamStore, LogPos};

use std::net::SocketAddr;

use clap::{App, Arg};
use vastatrix::sexp_proto;

fn main() {
    env_logger::init().unwrap_or(());
    let mut core = Core::new().unwrap();

    let matches = App::new("chain-repl-test")
                      .arg(Arg::with_name("head").short("h").takes_value(true))
                      .arg(Arg::with_name("tail").short("t").takes_value(true))
                      .get_matches();



    let store = RamStore::new();

    if let Some(head_addr) = value_t!(matches, "head", SocketAddr)
                                 .map(Some)
                                 .unwrap_or_else(|e| {
                                     if e.kind == clap::ErrorKind::ArgumentNotFound {
                                         None
                                     } else {
                                         e.exit()
                                     }
                                 }) {
        let head_host = sexp_proto::server::serve(&core.handle(),
                                                  head_addr,
                                                  vastatrix::ServerService::new(store.clone()))
                            .expect("start-head");
        println!("Head at: {:?}", head_host.local_addr());
    }
    if let Some(tail_addr) = value_t!(matches, "tail", SocketAddr)
                                 .map(Some)
                                 .unwrap_or_else(|e| {
                                     if e.kind == clap::ErrorKind::ArgumentNotFound {
                                         None
                                     } else {
                                         e.exit()
                                     }
                                 }) {
        let tail_host = sexp_proto::server::serve(&core.handle(),
                                                  tail_addr,
                                                  vastatrix::TailService::new(store.clone()))
                            .expect("start-tail");

        println!("Tail at: {:?}", tail_host.local_addr());
    }
    println!("running");
    core.run(futures::empty::<(), ()>()).expect("run read");
}
