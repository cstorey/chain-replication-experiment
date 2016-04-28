#[macro_use]
extern crate clap;
extern crate crexp_client;
extern crate gj;
extern crate gjio;
use gj::{EventLoop, Promise};
use gjio::{AsyncRead, AsyncWrite, BufferPrefix, SocketStream, EventPort};
use std::net::SocketAddr;

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

    let mut event_port = ::gjio::EventPort::new().expect("Event port");
    EventLoop::top_level(|wait_scope| -> Result<(), ::std::io::Error> {
            println!("Top level start");
            let done = Producer::new(head, &mut event_port)
            .then(move |mut producer| {
                println!("Got producer");
                    let futs = messages.into_iter().map(|msg| {
                            let msg = msg.clone();
                            println!("Producing: {:?}", msg);
                            producer.publish(&msg).map(move |res| {
                                    println!("Produce: {:?} -> {:?}", msg, res);
                                    Ok(())
                                })
                            }).collect::<Vec<_>>();
                        Promise::all(futs.into_iter())
                    });
            println!("Await");
            try!(done.wait(wait_scope, &mut event_port));
            println!("Done");
            Ok(())
        }).expect("top-level")
}
