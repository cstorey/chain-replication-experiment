#[macro_use]
extern crate clap;
extern crate crexp_client;
extern crate time;
extern crate env_logger;
extern crate eventual;
use std::net::SocketAddr;
use std::io::{self, Read, BufReader, BufRead};
use std::fs::File;
use time::{Duration, PreciseTime};
use eventual::Async;
use std::collections::VecDeque;

use clap::{Arg, App, SubCommand};

use crexp_client::Consumer;

fn main() {
    env_logger::init().expect("env-logger init");

    let matches = App::new("consumer")
        .arg(Arg::with_name("head").index(1))
        .get_matches();

    let head = value_t_or_exit!(matches.value_of("head"), SocketAddr);

    let mut consumer = Consumer::new(head).expect("connect");
    println!("Got consumer");

    let subs = consumer.subscribe();
    subs.each(|(seq, msg)| println!("{:?}: {:?}", seq, msg)).await().expect("consume each");
}
