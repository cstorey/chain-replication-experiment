#[macro_use]
extern crate clap;
extern crate crexp_client;
extern crate time;
extern crate env_logger;
use std::net::SocketAddr;
use std::io::{self, Read, BufReader, BufRead};
use std::fs::File;
use time::{Duration, PreciseTime};

use clap::{Arg, App, SubCommand};

use crexp_client::Producer;

struct Void;

fn main() {
    env_logger::init().expect("env-logger init");

    let matches = App::new("producer")
        .arg(Arg::with_name("head").index(1))
        .arg(Arg::with_name("file").short("f").takes_value(true))
        .get_matches();

    let head = value_t_or_exit!(matches.value_of("head"), SocketAddr);
    let source = matches.value_of("file");

    println!("Top level start from {}", source.as_ref().unwrap_or(&"stdin"));
    let src = source.map(|f| Box::new(File::open(f).expect("open source file")) as Box<Read>)
            .unwrap_or_else(|| Box::new(io::stdin()) as Box<Read>);
    let src = BufReader::new(src);

    let mut producer = Producer::new(head).expect("connect");
    println!("Got producer");

    for msg in src.lines() {
        let msg = msg.expect("input message");
        let start = PreciseTime::now();
        println!("Producing: {:?}", msg.trim());
        let res = producer.publish(msg.trim()).expect("produce");
        println!("Produce: {:?} -> {:?} in {}", msg, res, start.to(PreciseTime::now()));
    }
}
