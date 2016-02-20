extern crate mio;
extern crate bytes;
#[macro_use]
extern crate log;
extern crate serde;
extern crate serde_json;
extern crate spki_sexp;

extern crate etcd;

use mio::tcp::*;
use mio::EventLoop;
use mio::util::Slab;
use std::collections::{VecDeque,BTreeMap};
use std::net::SocketAddr;
use std::cmp;

mod line_conn;
mod downstream_conn;
mod listener;
mod config;
mod event_handler;
mod data;

use line_conn::{SexpPeer,LineConn};
use downstream_conn::Downstream;
use listener::Listener;
use event_handler::EventHandler;

pub use data::*;
pub use config::*;

const REPLICATION_CREDIT : u64 = 1024;

pub type PeerMsg = (u64, u64, Operation);


#[derive(Debug)]
enum ChainReplMsg {
    Operation { source: mio::Token, epoch: Option<u64>, seqno: Option<u64>, op: Operation },
    DownstreamResponse(OpResp),
    NewClientConn(Role, TcpStream),
}

#[derive(Debug)]
pub struct ChainRepl {
    connections: Slab<EventHandler>,
    downstream_slot: Option<mio::Token>,
    // "Model" fields
    last_sent_downstream: Option<u64>,
    last_acked_downstream: Option<u64>,
    pending_operations: BTreeMap<u64, mio::Token>,
    log: BTreeMap<u64, Operation>,
    state: String,
    new_view: Option<ConfigurationView<NodeViewConfig>>,
    node_config: NodeViewConfig,
    current_epoch: u64,
}

impl ChainRepl {
    pub fn new() -> ChainRepl {
        ChainRepl {
            connections: Slab::new(1024),
            downstream_slot: None,
            last_sent_downstream: None,
            last_acked_downstream: None,
            log: BTreeMap::new(),
            pending_operations: BTreeMap::new(),
            state: String::new(),
            new_view: None,
            node_config: Default::default(),
            current_epoch: Default::default(),
        }
    }

    pub fn listen(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>, addr: SocketAddr, role: Role) {
        info!("Listen on {:?} for {:?}", addr, role);
        let l = Listener::new(addr, role.clone());
        match role {
            Role::Upstream => self.node_config.peer_addr = Some(format!("{}", l.listen_addr())),
            Role::Client => self.node_config.client_addr = Some(format!("{}", l.listen_addr())),
        }
        let token = self.connections.insert(EventHandler::Listener(l)).expect("insert listener");
        &self.connections[token].initialize(event_loop, token);
    }

    pub fn set_downstream(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>, target: Option<SocketAddr>) {
        match (self.downstream_slot, target) {
            (Some(_), Some(target)) => self.downstream().expect("downstream").reconnect_to(target),
            (None, Some(target)) => {
                let token = self.connections.insert_with(|token| EventHandler::Downstream(Downstream::new(Some(target), token)))
                    .expect("insert downstream");
                &self.connections[token].initialize(event_loop, token);
                self.downstream_slot = Some(token)
            },
            (Some(_), None) => self.downstream().expect("downstream").disconnect(),
            (None, None) => (),
        }
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
            ChainReplMsg::Operation { source, seqno, epoch, op } => {
                let seqno = seqno.unwrap_or_else(|| self.next_seqno());
                // assert!(seqno == 0 || self.log.get(&(seqno-1)).is_some());

                let epoch = epoch.unwrap_or_else(|| self.current_epoch);

               if epoch != self.current_epoch {
                    warn!("Operation epoch ({}) differers from our last observed configuration: ({})",
                        epoch, self.current_epoch);
                    let resp = OpResp::Err(epoch, seqno, format!("BadEpoch: {}; last seen config: {}", epoch, self.current_epoch));
                    self.connections[source].response(resp);
                    return;
                }

                if !(seqno == 0 || self.log.get(&(seqno-1)).is_some()) {
                    warn!("Hole in history: saw {}/{}; have: {:?}", epoch, seqno, self.log.keys().collect::<Vec<_>>());
                    let resp = OpResp::Err(epoch, seqno, format!("Bad sequence number; previous: {:?}, saw: {:?}",
                        self.log.keys().rev().next(), seqno));
                    self.connections[source].response(resp);
                    return;
                }


                let prev = self.log.insert(seqno, op.clone());
                debug!("Log entry {:?}: {:?}", seqno, op);
                if let Some(prev) = prev {
                    panic!("Unexpectedly existing log entry for item {}: {:?}", seqno, prev);
                }

                if let Some(_) = self.downstream_slot {
                    self.pending_operations.insert(seqno, source);
                    // Replication mechanism should handle the push to downstream.
                } else {
                    info!("Terminus! {:?}/{:?}", seqno, op);
                    let resp = match op {
                        Operation::Set(s) => {
                            self.state = s;
                            OpResp::Ok(epoch, seqno, None)
                        },
                        Operation::Get => OpResp::Ok(epoch, seqno, Some(self.state.clone()))
                    };
                    self.connections[source].response(resp)
                }
            },

            ChainReplMsg::DownstreamResponse(reply) => {
                debug!("Downstream response: {:?}", reply);
                match reply {
                    OpResp::Ok(epoch, seqno, _) | OpResp::Err(epoch, seqno, _) => {
                        if let Some(token) = self.pending_operations.remove(&seqno) {
                            info!("Found in-flight op {:?} for client token {:?}", seqno, token);
                            if let Some(ref mut c)  = self.connections.get_mut(token) {
                                c.response(reply)
                            }
                            self.last_acked_downstream = Some(seqno)
                        } else {
                            warn!("Unexpected response for seqno: {:?}", seqno);
                        }
                    },
                    OpResp::HelloIWant(last_sent_downstream) => {
                        info!("Downstream has {:?}", last_sent_downstream);
                        assert!(last_sent_downstream <= self.seqno());
                        self.last_sent_downstream = Some(last_sent_downstream);
                    },
                };
            },

            ChainReplMsg::NewClientConn(role, socket) => {
                let peer = socket.peer_addr().expect("peer address");
                let seqno = self.seqno();
                assert!(self.log.get(&seqno).is_none());
                let token = self.connections
                    .insert_with(|token| match role {
                            Role::Client => EventHandler::Conn(LineConn::client(socket, token)),
                            Role::Upstream => {
                                let mut conn = LineConn::upstream(socket, token);
                                let msg = OpResp::HelloIWant(seqno);
                                info!("Inform upstream about our current version, {:?}!", msg);
                                conn.response(msg);
                                EventHandler::Upstream(conn)
                            },
                    })
                    .expect("token insert");
                debug!("Client connection of {:?}/{:?} from {:?}", role, token, peer);
                &self.connections[token].initialize(event_loop, token);
            }
        }
    }

    fn process_rules(&mut self, event_loop: &mut mio::EventLoop<Self>) -> bool {
        debug!("pre  Repl: Ours: {:?}; downstream sent: {:?}; acked: {:?}",
            self.seqno(), self.last_sent_downstream, self.last_acked_downstream);
        let mut changed = false;
        if let Some(send_next) = self.last_sent_downstream {
            if send_next < self.seqno() {
                let max_to_push_now = cmp::min(self.last_acked_downstream.unwrap_or(0) + REPLICATION_CREDIT, self.seqno());
                debug!("Window push {:?} - {:?}; waiting: {:?}", send_next, max_to_push_now, self.seqno() - max_to_push_now);
                let epoch = self.current_epoch;
                for i in send_next..max_to_push_now {
                    debug!("Log item: {:?}: {:?}", i, self.log.get(&i));
                    let op = self.log[&i].clone();
                    debug!("Queueing epoch:{:?}; seq:{:?}/{:?}; ds/seqno: {:?}", epoch, i, op, self.last_sent_downstream);
                    self.downstream().expect("Downstream").send_to_downstream(epoch, i, op);
                    self.last_sent_downstream = Some(i+1);
                    changed = true
                }
            }
            debug!("post Repl: Ours: {:?}; downstream sent: {:?}; acked: {:?}",
                self.seqno(), self.last_sent_downstream, self.last_acked_downstream);
        }

        // Cases:
        // Head(nextNode)
        // Middle(nextNode)
        // Tail
        if let Some(view) = self.new_view.take() {
            info!("Reconfigure according to: {:?}", view);
            if !view.in_configuration() {
                warn!("I am not in this configuration; shutting down: {:?}", view);
                event_loop.shutdown();
            }

            let listen_for_clients = view.should_listen_for_clients();
            info!("Listen for clients: {:?}", listen_for_clients);
            for p in self.listeners(Role::Client) {
                p.set_active(listen_for_clients);
            }
            let listen_for_upstreamp = view.should_listen_for_upstream();
            info!("Listen for upstreams: {:?}", listen_for_upstreamp);
            for p in self.listeners(Role::Upstream) {
                p.set_active(listen_for_upstreamp);
            }
            if let Some(ds) = view.should_connect_downstream() {
                info!("Push to downstream on {:?}", ds);
                if let Some(peer_addr) = ds.peer_addr {
                    self.set_downstream(event_loop, Some(peer_addr.parse().expect("peer address")));
                } else {
                    panic!("Cannot reconnect to downstream with no peer listener: {:?}", ds);
                }
            } else {
                info!("Tail node!");
                self.set_downstream(event_loop, None);
            }

            self.current_epoch = view.epoch;

            changed = true;
        }

        changed
    }

    fn downstream<'a>(&'a mut self) -> Option<&'a mut Downstream<SexpPeer>> {
        self.downstream_slot.map(move |slot| match &mut self.connections[slot] {
            &mut EventHandler::Downstream(ref mut d) => d,
            other => panic!("Downstream slot not populated with a downstream instance: {:?}", other),
        })
    }

    fn listeners<'a>(&'a mut self, role: Role) -> Vec<&'a mut Listener> {
        self.connections.iter_mut().filter_map(|it| match it {
            &mut EventHandler::Listener(ref mut l) if l.role == role => Some(l),
            _ => None
        }).collect()
    }

    fn converge_state(&mut self, event_loop: &mut mio::EventLoop<Self>) {
        let mut parent_actions = VecDeque::new();
        let mut changed = true;
        let mut iterations = 0;
        debug!("Converge begin");
        while changed {
            trace!("Iter: {:?}", iterations);
            changed = false;
            for conn in self.connections.iter_mut() {
                let changedp = conn.process_rules(event_loop, &mut |item| parent_actions.push_back(item));
                if changedp { debug!("Changed: {:?}", conn); };
                changed |= changedp;
            }

            let changedp = self.process_rules(event_loop);
            if changedp { debug!("Changed: {:?}", "Model"); }
            changed |= changedp;

            for action in parent_actions.drain(..) {
                debug!("Action: {:?}", action);
                self.process_action(action, event_loop);
            }
            iterations += 1;
        }
        debug!("Converged after {:?} iterations", iterations);
    }

    fn io_ready(&mut self, event_loop: &mut mio::EventLoop<Self>, token: mio::Token) {
        self.converge_state(event_loop);
        if self.connections[token].is_closed() && self.pending_operations.values().all(|tok| tok != &token) {
            debug!("Close candidate: {:?}: pending: {:?}",
                token, self.pending_operations.values().filter(|tok| **tok == token).count());
            let it = self.connections.remove(token);
            debug!("Removing; {:?}; {:?}", token, it);
            if Some(token) == self.downstream_slot {
                self.downstream_slot = None;
                self.last_sent_downstream = None
            }
        }
    }

    fn handle_view_changed(&mut self, view: ConfigurationView<NodeViewConfig>) {
        self.new_view = Some(view)
    }

    pub fn get_notifier(&self, event_loop: &mut mio::EventLoop<Self>) -> Notifier {
        Notifier(event_loop.channel())
    }

    pub fn node_config(&self) -> NodeViewConfig {
        self.node_config.clone()
    }
}

pub struct Notifier(mio::Sender<ConfigurationView<NodeViewConfig>>);

impl Notifier {
    pub fn notify(&self, view: ConfigurationView<NodeViewConfig>) {
        use mio::NotifyError::*;
        let mut item = view;
        let mut backoff_ms = 1;
        loop {
            item = match self.0.send(item) {
                Ok(_) => return,
                Err(Full(it)) => it,
                Err(e) => panic!("{:?}", e),
            };
            info!("Backoff: {}ms", backoff_ms);
            std::thread::sleep_ms(backoff_ms);
            backoff_ms *= 2;
        }
    }
}

impl mio::Handler for ChainRepl {
    // This is a bit wierd; we can pass a parent action back to enqueue some action.
    type Timeout = mio::Token;
    type Message = ConfigurationView<NodeViewConfig>;
    fn ready(&mut self, event_loop: &mut mio::EventLoop<Self>, token: mio::Token, events: mio::EventSet) {
        trace!("{:?}: {:?}", token, events);
        self.connections[token].handle_event(event_loop, events);
        self.io_ready(event_loop, token);
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, token: mio::Token) {
        debug!("Timeout: {:?}", token);
        self.connections[token].handle_timeout();
        self.io_ready(event_loop, token);
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: ConfigurationView<NodeViewConfig>) {
        debug!("Notified: {:?}", msg);
        self.handle_view_changed(msg);
        self.converge_state(event_loop);
    }
}
