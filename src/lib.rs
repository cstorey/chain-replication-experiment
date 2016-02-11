extern crate mio;
extern crate bytes;
#[macro_use]
extern crate log;
#[macro_use]
extern crate rustc_serialize;

extern crate etcd;

use mio::tcp::*;
use mio::EventLoop;
use mio::util::Slab;
use std::collections::{VecDeque,BTreeMap};
use std::net::SocketAddr;

mod line_conn;
mod downstream_conn;
mod listener;
mod config;
mod event_handler;
use line_conn::LineConn;
use downstream_conn::Downstream;
use listener::Listener;
use event_handler::EventHandler;

pub use config::*;

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

pub struct ChainRepl {
    connections: Slab<EventHandler>,
    downstream_slot: Option<mio::Token>,
    // "Model" fields
    downstream_seqno: Option<u64>,
    pending_operations: BTreeMap<u64, mio::Token>,
    log: BTreeMap<u64, Operation>,
    state: String,
}

impl ChainRepl {
    pub fn new() -> ChainRepl {
        ChainRepl {
            connections: Slab::new(1024),
            downstream_slot: None,
            downstream_seqno: None,
            log: BTreeMap::new(),
            pending_operations: BTreeMap::new(),
            state: String::new(),
        }
    }

    pub fn listen(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>, addr: SocketAddr, role: Role) {
        info!("Listen on {:?} for {:?}", addr, role);
        let l = EventHandler::Listener(Listener::new(addr, role));
        let token = self.connections.insert(l).expect("insert listener");
        &self.connections[token].initialize(event_loop, token);
    }

    pub fn set_downstream(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>, target: SocketAddr) {
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
                            Role::Client => EventHandler::Conn(LineConn::client(socket, token)),
                            Role::Upstream => {
                                let mut conn = LineConn::upstream(socket, token);
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
            trace!("Log: {:?}", self.log);
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
        let mut iterations = 0;
        trace!("Converge begin");
        while changed {
            trace!("Iter: {:?}", iterations);
            changed = false;
            for conn in self.connections.iter_mut() {
                let changedp = conn.process_rules(event_loop, &mut |item| parent_actions.push_back(item));
                if changedp { trace!("Changed: {:?}", conn); };
                changed |= changedp;
            }

            let changedp = self.process_rules();
            if changedp { trace!("Changed: {:?}", "Model"); }
            changed |= changedp;

            for action in parent_actions.drain(..) {
                trace!("Action: {:?}", action);
                self.process_action(action, event_loop);
            }
            iterations += 1;
        }
        trace!("Converged after {:?} iterations", iterations);

        if self.connections[token].is_closed() && self.pending_operations.values().all(|tok| tok != &token) {
            debug!("Close candidate: {:?}: pending: {:?}",
                token, self.pending_operations.values().filter(|tok| **tok == token).count());
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
