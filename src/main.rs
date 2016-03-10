extern crate mio;
extern crate time;
extern crate log4rs;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate chain_repl_test;

use time::Duration;
use std::net::ToSocketAddrs;
use std::collections::HashSet;
use clap::{Arg, App};

use chain_repl_test::{ChainRepl, Role, ConfigClient, ReplModel, RocksdbLog};

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

    let repl_notifier = ChainRepl::get_notifier(&mut event_loop);
    let log = RocksdbLog::new(move |seqno| repl_notifier.committed_to(seqno));
    let replication = ReplModel::new(log);
    let mut service = ChainRepl::new(replication);

    if let Some(listen_addr) = matches.value_of("bind") {
        let listen_addrs = listen_addr.to_socket_addrs()
                                      .expect("parse bind address")
                                      .collect::<HashSet<_>>();
        info!("Client addresses: {:?}", listen_addrs);
        for listen_addr in listen_addrs {
            service.listen(&mut event_loop, listen_addr, Role::Client);
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

    if let Some(next_addr) = matches.value_of("next") {
        let next_addr = next_addr.to_socket_addrs()
                                 .expect("address lookup")
                                 .next()
                                 .expect("parse next address");
        info!("Forwarding to address {:?}", next_addr);
        service.set_downstream(&mut event_loop, Some(next_addr));
    } else {
        service.set_downstream(&mut event_loop, None);
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
