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

use crexp_client::Producer;

fn main() {
    env_logger::init().expect("env-logger init");

    let matches = App::new("producer")
        .arg(Arg::with_name("head").index(1))
        .arg(Arg::with_name("file").short("f").takes_value(true))
        .arg(Arg::with_name("max_in_flight").short("m").takes_value(true))
        .get_matches();

    let head = value_t_or_exit!(matches.value_of("head"), SocketAddr);
    let max_in_flight = value_t!(matches.value_of("max_in_flight"), usize).unwrap_or(32);
    let source = matches.value_of("file");

    println!("Top level start from {}", source.as_ref().unwrap_or(&"stdin"));
    let src = source.map(|f| Box::new(File::open(f).expect("open source file")) as Box<Read>)
            .unwrap_or_else(|| Box::new(io::stdin()) as Box<Read>);
    let src = BufReader::new(src);

    let mut producer = Producer::new(head).expect("connect");
    println!("Got producer");

    let pending = src.lines()
        .map(|msg| msg.expect("input message"))
        .enumerate()
        .map(|(i, msg)| { 
                println!("Producing {}: {:?}", i, msg.trim());
                let started = PreciseTime::now();
                producer.publish(msg.trim())
                    .map(move |res| {
                        let ended = PreciseTime::now();
                        println!("Produce {}: {:?} -> {:?} in {}", i+1, msg, res, started.to(ended));
                    })
            });

    let mut buf = VecDeque::new();
    for (i, p) in pending.into_iter().enumerate() {
        buf.push_back((i, p));
        while buf.len() > max_in_flight {
            if let Some((i, p)) = buf.pop_front() {
                println!("Await: {:?}", i+1);
                let () = p.await().expect("producer");
            }
        };
    }

    while let Some((i, p)) = buf.pop_front() {
        println!("Await: {:?}", i+1);
        let () = p.await().expect("producer");
    }
}
