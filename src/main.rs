extern crate mio;
extern crate time;
extern crate log4rs;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate vastatrix;

use time::Duration;
use std::net::ToSocketAddrs;
use std::collections::HashSet;
use clap::{Arg, App};

use vastatrix::{ChainRepl, Role, ConfigClient, ReplModel, ReplProxy, RocksdbLog};

const LOG_FILE: &'static str = "log.toml";

fn main() {
    if let Err(e) = log4rs::init_file(LOG_FILE, Default::default()) {
        panic!("Could not init logger from file {}: {}", LOG_FILE, e);
    }
    let matches = App::new("chain-repl-test")
                      .arg(Arg::with_name("bind").short("l").takes_value(true))
                      .arg(Arg::with_name("consumer").short("c").takes_value(true))
                      .arg(Arg::with_name("peer").short("p").takes_value(true))
                      .arg(Arg::with_name("etcd").short("e").takes_value(true))
                      .get_matches();

    let mut event_loop = mio::EventLoop::new().expect("Create event loop");

    let repl_notifier = ChainRepl::get_notifier(&mut event_loop);
    let log = RocksdbLog::new();
    let replication = ReplModel::new(log);

    let mut service = ChainRepl::new(
            ReplProxy::build(replication, ChainRepl::get_notifier(&mut event_loop)));

    if let Some(listen_addr) = matches.value_of("bind") {
        let listen_addrs = listen_addr.to_socket_addrs()
                                      .expect("parse bind address")
                                      .collect::<HashSet<_>>();
        info!("Client addresses: {:?}", listen_addrs);
        for listen_addr in listen_addrs {
            service.listen(&mut event_loop, listen_addr, Role::ProducerClient);
        }
    }
    if let Some(listen_addr) = matches.value_of("consumer") {
        let listen_addrs = listen_addr.to_socket_addrs()
                                      .expect("parse bind address")
                                      .collect::<HashSet<_>>();
        info!("Client addresses: {:?}", listen_addrs);
        for listen_addr in listen_addrs {
            service.listen(&mut event_loop, listen_addr, Role::ConsumerClient);
        }
    }

    if let Some(listen_addr) = matches.value_of("peer") {
        let listen_addrs = listen_addr.to_socket_addrs()
                                      .expect("peer listen address")
                                      .collect::<HashSet<_>>();
        info!("Peer addresses: {:?}", listen_addrs);
        for listen_addr in listen_addrs {
            service.listen(&mut event_loop, listen_addr, Role::Upstream);
        }
    }

    if let Some(etcd) = matches.value_of("etcd") {
        info!("Etcd at: {:?}", etcd);
        let view_cb = {
            let notifier = ChainRepl::get_notifier(&mut event_loop);
            move |view| notifier.view_changed(view)
        };
        let shutdown_cb = {
            let notifier2 = ChainRepl::get_notifier(&mut event_loop);
            move || notifier2.shutdown()
        };
        let _ = ConfigClient::new(etcd,
                                  service.node_config(),
                                  Duration::seconds(5),
                                  view_cb,
                                  shutdown_cb)
                    .expect("Etcd");
    } else {
        info!("No etcd");
    };

    info!("running chain-repl-test listener");
    event_loop.run(&mut service).expect("Run loop");
}
