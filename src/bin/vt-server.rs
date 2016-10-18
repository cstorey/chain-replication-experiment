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

use std::net::SocketAddr;

use clap::{App, Arg};
use vastatrix::hosting::*;

fn main() {
    env_logger::init().unwrap_or(());
    let mut core = Core::new().unwrap();

    let matches = App::new("chain-repl-test")
                      .arg(Arg::with_name("head").short("h").takes_value(true).required(true))
                      .arg(Arg::with_name("tail").short("t").takes_value(true).required(true))
                      .get_matches();



    let head_addr = value_t!(matches, "head", SocketAddr).unwrap_or_else(|e| e.exit());
    let tail_addr = value_t!(matches, "tail", SocketAddr).unwrap_or_else(|e| e.exit());

    let service = CoreService::new();

    let server = SexpHost::build_server(service, &core.handle(), head_addr, tail_addr)
                     .expect("start server");
    println!("running: {:?}", server);
    core.run(futures::empty::<(), ()>()).expect("run read");
}
