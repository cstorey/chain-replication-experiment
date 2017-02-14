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
use std::io::{self, BufRead};

fn main() {
    env_logger::init().unwrap_or(());

    let mut core = Core::new().unwrap();

    let matches = App::new("chain-repl-test")
        .arg(Arg::with_name("head").short("h").takes_value(true))
        .arg(Arg::with_name("tail").short("t").takes_value(true))
        .get_matches();

    let head_addr = value_t!(matches, "head", SocketAddr).unwrap_or_else(|e| e.exit());
    let tail_addr = value_t!(matches, "tail", SocketAddr).unwrap_or_else(|e| e.exit());

    let client = {
            let f = vastatrix::ThickClient::new(core.handle(), &head_addr, &tail_addr);
            core.run(f)
        }
        .expect("connect");

    let stdin = io::stdin();
    for l in stdin.lock().lines().map(|l| l.expect("read-line")) {
        let f = client.log_item(l.into_bytes());
        let wpos = core.run(f).expect("run write");
        println!("Produce: {:?}", wpos);
    }
}
