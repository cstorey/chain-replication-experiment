extern crate mio;
extern crate bytes;
extern crate log4rs;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate rustc_serialize;

use mio::tcp::*;
use mio::EventLoop;
use mio::util::Slab;
use std::collections::{VecDeque,BTreeMap};
use std::net::SocketAddr;
use clap::{Arg, App};

mod client_conn;
mod upstream_conn;
mod downstream_conn;
mod listener;
mod event_handler;
use client_conn::ClientConn;
use upstream_conn::UpstreamConn;
use downstream_conn::Downstream;
use listener::Listener;
use event_handler::EventHandler;

#[derive(Clone, Debug, RustcEncodable, RustcDecodable)]
enum Operation {
    Set(String),
    Get
}

#[derive(Debug, RustcEncodable, RustcDecodable)]
enum OpResp {
    Ok(u64, Option<String>),
    HelloIHave(u64),
}

#[derive(Clone, Debug, RustcEncodable, RustcDecodable)]
pub enum Role {
    Client,
    Upstream,
}

#[derive(Debug)]
enum ChainReplMsg {
    Operation(mio::Token, Option<u64>, Operation),
    DownstreamResponse(OpResp),
    NewClientConn(Role, TcpStream),
}

struct ChainRepl {
    connections: Slab<EventHandler>,
    downstream_slot: Option<mio::Token>,
    // "Model" fields
    downstream_seqno: Option<u64>,
    pending_operations: BTreeMap<u64, mio::Token>,
    log: BTreeMap<u64, Operation>,
    state: String,
}

impl ChainRepl {
    fn new() -> ChainRepl {
        ChainRepl {
            connections: Slab::new(1024),
            downstream_slot: None,
            downstream_seqno: None,
            log: BTreeMap::new(),
            pending_operations: BTreeMap::new(),
            state: String::new(),
        }
    }

    fn listen(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>, addr: SocketAddr, role: Role) {
        info!("Listen on {:?} for {:?}", addr, role);
        let l = EventHandler::Listener(Listener::new(addr, role));
        let token = self.connections.insert(l).expect("insert listener");
        &self.connections[token].initialize(event_loop, token);
    }

    fn set_downstream(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>, target: SocketAddr) {
        if let Some(d) = self.downstream_slot {
            panic!("Already have downstream: {:?}/{:?}", d, self.connections[d]);
        }

        let token = self.connections.insert_with(|token| EventHandler::Downstream(Downstream::new(target, token)))
            .expect("insert downstream");
        &self.connections[token].initialize(event_loop, token);
        self.downstream_slot = Some(token)
    }

    fn seqno(&self) -> u64 {
        self.log.len() as u64
    }

    fn next_seqno(&self) -> u64 {
        self.seqno()
    }

    fn process_action(&mut self, msg: ChainReplMsg, event_loop: &mut mio::EventLoop<ChainRepl>) {
        trace!("{:p}; got {:?}", self, msg);
        match msg {
            ChainReplMsg::Operation(source, seqno, s) => {
                let seqno = seqno.unwrap_or_else(|| self.next_seqno());
                assert!(seqno == 0 || self.log.get(&(seqno-1)).is_some());
                let prev = self.log.insert(seqno, s.clone());
                assert!(prev.is_none());
                debug!("Log entry {:?}: {:?}", seqno, s);

                if let Some(_) = self.downstream_slot {
                    self.pending_operations.insert(seqno, source);
                    // Replication mechanism should handle the push to downstream.
                } else {
                    info!("Terminus! {:?}/{:?}", seqno, s);
                    let resp = match s {
                        Operation::Set(s) => {
                            self.state = s;
                            OpResp::Ok(seqno, None)
                        },
                        Operation::Get => OpResp::Ok(seqno, Some(self.state.clone()))
                    };
                    self.connections[source].response(resp)
                }
            },

            ChainReplMsg::DownstreamResponse(reply) => {
                info!("Downstream response: {:?}", reply);
                match reply {
                    OpResp::Ok(seqno, _) => {
                        if let Some(token) = self.pending_operations.remove(&seqno) {
                            info!("Found in-flight op {:?} for client token {:?}", seqno, token);
                            if let Some(ref mut c)  = self.connections.get_mut(token) {
                                c.response(reply)
                            }
                        } else {
                            warn!("Unexpected response for seqno: {:?}", seqno);
                        }
                    },
                    OpResp::HelloIHave(downstream_seqno) => {
                        info!("Downstream has {:?}", downstream_seqno);
                        assert!(downstream_seqno <= self.seqno());
                        self.downstream_seqno = Some(downstream_seqno);
                    },
                };

            },
            ChainReplMsg::NewClientConn(role, socket) => {
                let peer = socket.peer_addr().expect("peer address");
                let seqno = self.seqno();
                let token = self.connections
                    .insert_with(|token| match role {
                            Role::Client => EventHandler::Conn(ClientConn::new(socket, token)),
                            Role::Upstream => {
                                let mut conn = UpstreamConn::new(socket, token);
                                info!("Inform upstream about our current version, {:?}!", seqno);
                                conn.response(OpResp::HelloIHave(seqno));
                                EventHandler::Upstream(conn)
                            },
                    })
                    .expect("token insert");
                debug!("Client connection of {:?}/{:?} from {:?}", role, token, peer);
                &self.connections[token].initialize(event_loop, token);
            }
        }
    }

    fn process_rules(&mut self) -> bool {
        info!("Repl: Ours: {:?}; downstream: {:?}", self.seqno(), self.downstream_seqno);
        let mut changed = false;
        if let Some(send_next) = self.downstream_seqno {
            debug!("Log: {:?}", self.log);
            if send_next < self.seqno() {
                info!("Need to push {:?}-{:?}", send_next, self.seqno());
                for i in send_next..self.seqno() {
                    debug!("Log item: {:?}: {:?}", i, self.log.get(&i));
                    let op = self.log[&i].clone();
                    self.downstream_seqno = Some(i+1);
                    debug!("Pushed {:?}/{:?}; ds/seqno: {:?}", i, op, self.downstream_seqno);
                    self.downstream().expect("Downstream").send_to_downstream(i, op);
                }
                changed = true
            }
        }
        changed
    }

    fn downstream<'a>(&'a mut self) -> Option<&'a mut Downstream> {
        self.downstream_slot.map(move |slot| match &mut self.connections[slot] {
            &mut EventHandler::Downstream(ref mut d) => d,
            other => panic!("Downstream slot not populated with a downstream instance: {:?}", other),
        })
    }

    fn converge_state(&mut self, event_loop: &mut mio::EventLoop<Self>, token: mio::Token) {
        let mut parent_actions = VecDeque::new();
        let mut changed = true;
        while changed {
            changed = false;
            for conn in self.connections.iter_mut() {
                changed |= conn.process_rules(event_loop, &mut |item| parent_actions.push_back(item));
            }

            changed |= self.process_rules();

            for action in parent_actions.drain(..) {
                self.process_action(action, event_loop);
            }
        }

        if self.connections[token].is_closed() {
            let it = self.connections.remove(token);
            debug!("Removing; {:?}; {:?}", token, it);
        }
    }
}

impl mio::Handler for ChainRepl {
    // This is a bit wierd; we can pass a parent action back to enqueue some action.

    type Timeout = mio::Token;
    type Message = ();
    fn ready(&mut self, event_loop: &mut mio::EventLoop<Self>, token: mio::Token, events: mio::EventSet) {
        trace!("{:?}: {:?}", token, events);
        self.connections[token].handle_event(event_loop, events);
        self.converge_state(event_loop, token);
    }


    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, token: mio::Token) {
        debug!("Timeout: {:?}", token);
        self.connections[token].handle_timeout();
        self.converge_state(event_loop, token);
    }

}

const LOG_FILE: &'static str = "log.toml";

fn main() {
    if let Err(e) = log4rs::init_file(LOG_FILE, Default::default()) {
        panic!("Could not init logger from file {}: {}", LOG_FILE, e);
    }
    let matches = App::new("chain-repl-test")
        .arg(Arg::with_name("bind").short("l").takes_value(true))
        .arg(Arg::with_name("peer").short("p").takes_value(true))
        .arg(Arg::with_name("next").short("n").takes_value(true))
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

    info!("running chain-repl-test listener");
    event_loop.run(&mut service).expect("Run loop");
}
