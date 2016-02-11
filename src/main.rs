extern crate mio;
extern crate log4rs;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate chain_repl_test;

use std::net::SocketAddr;
use std::time::Duration;
use clap::{Arg, App};

use chain_repl_test::{ChainRepl,Role, ConfigClient};


const LOG_FILE: &'static str = "log.toml";

fn main() {
    if let Err(e) = log4rs::init_file(LOG_FILE, Default::default()) {
        panic!("Could not init logger from file {}: {}", LOG_FILE, e);
    }
    let matches = App::new("chain-repl-test")
        .arg(Arg::with_name("bind").short("l").takes_value(true))
        .arg(Arg::with_name("peer").short("p").takes_value(true))
        .arg(Arg::with_name("next").short("n").takes_value(true))
        .arg(Arg::with_name("etcd").short("e").takes_value(true))
        .get_matches();

    let mut event_loop = mio::EventLoop::new().expect("Create event loop");
    let mut service = ChainRepl::new();

    if let Some(listen_addr) = matches.value_of("bind") {
        let listen_addr = listen_addr.parse::<std::net::SocketAddr>().expect("parse bind address");
        service.listen(&mut event_loop, listen_addr, Role::Client);
    }

    if let Some(listen_addr) = matches.value_of("peer") {
        let listen_addr = listen_addr.parse::<std::net::SocketAddr>().expect("peer listen address");
        service.listen(&mut event_loop, listen_addr, Role::Upstream);
    }

    if let Some(next_addr) = matches.value_of("next") {
        let next_addr = next_addr.parse::<std::net::SocketAddr>().expect("parse next address");
        info!("Forwarding to address {:?}", next_addr);
        service.set_downstream(&mut event_loop, next_addr);
    }

    let conf = if let Some(etcd) = matches.value_of("etcd") {
        info!("Etcd at: {:?}", etcd);
        let notifier = service.get_notifier(&mut event_loop);
        let config = ConfigClient::new(etcd, &format!("data: {:p}", &service), Duration::new(5, 0),
            move |view| notifier.notify(view)).expect("Etcd");
        Some(config)
    } else {
        info!("No etcd");
        None
    };

    info!("running chain-repl-test listener");
    event_loop.run(&mut service).expect("Run loop");
}
