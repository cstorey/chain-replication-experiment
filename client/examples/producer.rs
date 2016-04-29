#[macro_use]
extern crate clap;
extern crate crexp_client;
extern crate time;
use std::net::SocketAddr;
use time::{Duration, PreciseTime};

use clap::{Arg, App, SubCommand};

use crexp_client::Producer;

struct Void;

fn main() {
    let matches = App::new("producer")
        .arg(Arg::with_name("head").index(1))
        .arg(Arg::with_name("messages").index(2).multiple(true))
        .get_matches();

    let head = value_t_or_exit!(matches.value_of("head"), SocketAddr);
    let messages = values_t_or_exit!(matches.values_of("messages"), String);

    println!("Top level start");
    let mut producer = Producer::new(head).expect("connect");
    println!("Got producer");

    for msg in messages {
        let start = PreciseTime::now();
        println!("Producing: {:?}", msg);
        let res = producer.publish(&msg).expect("produce");
        println!("Produce: {:?} -> {:?} in {}", msg, res, start.to(PreciseTime::now()));
    }
}
