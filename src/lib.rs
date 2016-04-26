#![cfg_attr(feature = "serde_macros", feature(custom_derive, plugin))]
#![cfg_attr(feature = "serde_macros", plugin(serde_macros))]
#![cfg_attr(feature = "benches", feature(test))]

extern crate mio;
extern crate bytes;
#[macro_use]
extern crate log;
extern crate serde;
extern crate serde_json;
extern crate spki_sexp;
extern crate time;

extern crate etcd;
extern crate tempdir;
extern crate rocksdb;
extern crate byteorder;

#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate env_logger;

#[cfg(feature = "benches")]
extern crate test;

use mio::tcp::*;
use mio::EventLoop;
use mio::util::Slab;
use std::collections::VecDeque;
use std::net::SocketAddr;

mod line_conn;
mod downstream_conn;
mod listener;
mod config;
mod event_handler;
mod data;
mod replication_log;
mod replica;

use line_conn::{SexpPeer, LineConn, UpstreamEvents, DownstreamEvents, LineConnEvents};
use downstream_conn::Downstream;
use listener::{Listener,ListenerEvents};
use event_handler::EventHandler;

use data::{Seqno,Operation,OpResp, PeerMsg, NodeViewConfig, Buf};
use config::{ConfigurationView, Epoch};

pub use config::ConfigClient;
pub use replica::ReplModel;
pub use replication_log::RocksdbLog;
pub use data::Role;


#[derive(Debug)]
pub enum ChainReplMsg {
    Operation {
        source: mio::Token,
        epoch: Epoch,
        seqno: Seqno,
        op: Vec<u8>,
    },
    ClientOperation {
        source: mio::Token,
        op: Vec<u8>,
    },
    Commit {
        source: mio::Token,
        epoch: Epoch,
        seqno: Seqno,
    },
    DownstreamResponse(OpResp),
    NewClientConn(Role, TcpStream),
}

struct ChainReplEvents { changes: VecDeque<ChainReplMsg> }

impl ListenerEvents for ChainReplEvents {
    fn new_connection(&mut self, role: Role, socket: TcpStream) {
        self.changes.push_back(ChainReplMsg::NewClientConn(role, socket))
    }
}

impl UpstreamEvents for ChainReplEvents {
    fn operation(&mut self, source: mio::Token, epoch: Epoch, seqno: Seqno, op: Vec<u8>) {
        self.changes.push_back(ChainReplMsg::Operation { source:source, epoch:epoch, seqno:seqno, op:op })
    }
    fn client_request(&mut self, source: mio::Token, op: Vec<u8>) {
        self.changes.push_back(ChainReplMsg::ClientOperation { source: source, op: op })
    }
    fn commit(&mut self, source: mio::Token, epoch: Epoch, seqno: Seqno) {
        self.changes.push_back(ChainReplMsg::Commit { source: source, epoch: epoch, seqno: seqno })
    }
}

impl DownstreamEvents for ChainReplEvents {
    fn okay(&mut self, epoch: Epoch, seqno: Seqno, data: Option<Buf>) {
        self.changes.push_back(ChainReplMsg::DownstreamResponse(OpResp::Ok(epoch, seqno, data)))
    }
    fn hello_i_want(&mut self, seqno: Seqno) {
        self.changes.push_back(ChainReplMsg::DownstreamResponse(OpResp::HelloIWant(seqno)))
    }
    fn error(&mut self, epoch: Epoch, seqno: Seqno, data: String) {
        self.changes.push_back(ChainReplMsg::DownstreamResponse(OpResp::Err(epoch, seqno, data)))
    }
}

impl LineConnEvents for ChainReplEvents { }

#[derive(Debug)]
pub struct ChainRepl {
    connections: Slab<EventHandler>,
    downstream_slot: Option<mio::Token>,
    node_config: NodeViewConfig,
    new_view: Option<ConfigurationView<NodeViewConfig>>,
    model: ReplModel<RocksdbLog>,
}

impl ChainRepl {
    pub fn new(model: ReplModel<RocksdbLog>) -> ChainRepl {
        ChainRepl {
            connections: Slab::new(1024),
            downstream_slot: None,
            node_config: Default::default(),
            new_view: None,
            model: model,
        }
    }

    pub fn listen(&mut self,
                  event_loop: &mut mio::EventLoop<ChainRepl>,
                  addr: SocketAddr,
                  role: Role) {
        info!("Listen on {:?} for {:?}", addr, role);
        let &mut ChainRepl { ref mut connections, ref mut node_config, .. } = self;
        let token = connections.insert_with(|token| {
                                   let l = Listener::new(addr, token, role.clone());
                                   match role {
                                       Role::Upstream => {
                                           node_config.peer_addr = Some(format!("{}",
                                                                                l.listen_addr()))
                                       }
                                       Role::Client => {
                                           node_config.client_addr = Some(format!("{}",
                                                                                  l.listen_addr()))
                                       }
                                   }

                                   trace!("Listener: {:?}", l);
                                   EventHandler::Listener(l)
                               })
                               .expect("insert listener");
        connections[token].initialize(event_loop, token);
    }

    pub fn set_downstream(&mut self,
                          event_loop: &mut mio::EventLoop<ChainRepl>,
                          target: Option<SocketAddr>) {
        match (self.downstream_slot, target) {
            (Some(_), Some(target)) => self.downstream().expect("downstream").reconnect_to(target),
            (None, Some(target)) => {
                let token = self.connections
                                .insert_with(|token| {
                                    EventHandler::Downstream(Downstream::new(Some(target), token))
                                })
                                .expect("insert downstream");
                &self.connections[token].initialize(event_loop, token);
                self.downstream_slot = Some(token)
            }
            (Some(_), None) => self.downstream().expect("downstream").disconnect(),
            (None, None) => (),
        }
    }

    fn process_action(&mut self, msg: ChainReplMsg, event_loop: &mut mio::EventLoop<ChainRepl>) {
        trace!("{:p}; got {:?}", self, msg);
        match msg {
            ChainReplMsg::Operation { source, seqno, epoch, op } => {
                let &mut ChainRepl { ref mut model, ref mut connections, .. } = self;
                let mut out = OutPorts(self.downstream_slot, connections);
                model.process_operation(&mut out, source, seqno, epoch, &op)
            }
            ChainReplMsg::ClientOperation { source, op } => {
                let &mut ChainRepl { ref mut model, ref mut connections, .. } = self;
                let mut out = OutPorts(self.downstream_slot, connections);
                model.process_client(&mut out, source, &op)
            }

            ChainReplMsg::Commit { seqno, .. } => {
                self.model.commit_observed(seqno);
            }

            ChainReplMsg::DownstreamResponse(reply) => self.process_downstream_response(reply),

            ChainReplMsg::NewClientConn(role, socket) => {
                self.process_new_client_conn(event_loop, role, socket)
            }
        }
    }


    fn process_downstream_response(&mut self, reply: OpResp) {
        trace!("Downstream response: {:?}", reply);
        let &mut ChainRepl { ref mut model, ref mut connections, .. } = self;
        let mut out = OutPorts(self.downstream_slot, connections);

        model.process_downstream_response(&mut out, reply)
    }

    fn process_new_client_conn(&mut self,
                               event_loop: &mut mio::EventLoop<ChainRepl>,
                               role: Role,
                               socket: TcpStream) {
        let peer = socket.peer_addr().expect("peer address");
        let seqno = self.model.seqno();
        trace!("process_new_client_conn: {:?}@{:?}", role, peer);

        let token = self.connections
                        .insert_with(|token| {
                            match role {
                                Role::Client => EventHandler::Conn(LineConn::client(socket, token)),
                                Role::Upstream => {
                                    let mut conn = LineConn::upstream(socket, token);
                                    let msg = OpResp::HelloIWant(seqno);
                                    info!("Inform upstream about our current version, {:?}!", msg);
                                    conn.response(msg);
                                    EventHandler::Upstream(conn)
                                }
                            }
                        })
                        .expect("token insert");
        trace!("Client connection of {:?}/{:?} from {:?}",
               role,
               token,
               peer);
        &self.connections[token].initialize(event_loop, token);
    }


    fn reconfigure(&mut self,
                   event_loop: &mut mio::EventLoop<Self>,
                   view: ConfigurationView<NodeViewConfig>) {
        info!("Reconfigure according to: {:?}", view);

        let listen_for_clients = view.should_listen_for_clients();
        info!("Listen for clients: {:?}", listen_for_clients);
        for p in self.listeners(Role::Client) {
            trace!("Active: {:?} -> {:?}", p, listen_for_clients);
            p.set_active(listen_for_clients);
        }

        let listen_for_upstreamp = view.should_listen_for_upstream();
        info!("Listen for upstreams: {:?}", listen_for_upstreamp);
        for p in self.listeners(Role::Upstream) {
            trace!("Active: {:?} -> {:?}", p, listen_for_clients);
            p.set_active(listen_for_upstreamp);
        }

        if let Some(ds) = view.should_connect_downstream() {
            info!("Push to downstream on {:?}", ds);
            let peer_addr = ds.peer_addr.as_ref().expect("Cannot reconnect to downstream with no peer listener");
            self.set_downstream(event_loop, Some(peer_addr.parse().expect("peer address")));
        } else {
            info!("Tail node!");
            self.set_downstream(event_loop, None);
        }

        info!("Reconfigure model: {:?}", view);
        self.model.reconfigure(view)
    }

    fn downstream<'a>(&'a mut self) -> Option<&'a mut Downstream<SexpPeer>> {
        self.downstream_slot.map(move |slot| {
            match &mut self.connections[slot] {
                &mut EventHandler::Downstream(ref mut d) => d,
                other => {
                    panic!("Downstream slot not populated with a downstream instance: {:?}",
                           other)
                }
            }
        })
    }

    fn listeners<'a>(&'a mut self, role: Role) -> Vec<&'a mut Listener> {
        self.connections
            .iter_mut()
            .filter_map(|it| {
                match it {
                    &mut EventHandler::Listener(ref mut l) if l.role == role => Some(l),
                    _ => None,
                }
            })
            .collect()
    }

    fn process_rules(&mut self, event_loop: &mut mio::EventLoop<Self>)
                                             -> bool {
        let mut changed = false;

        if let Some(view) = self.new_view.take() {
            self.reconfigure(event_loop, view);
            changed |= true;
        }

        changed
    }

    fn converge_state(&mut self, event_loop: &mut mio::EventLoop<Self>) {
        let mut parent_actions = VecDeque::new();
        let mut changed = true;
        let mut iterations = 0;
        trace!("Converge begin");

        while changed {
            trace!("Iter: {:?}", iterations);
            let mut events = ChainReplEvents { changes: VecDeque::new() };
            changed = false;
            let mut events = ChainReplEvents { changes: VecDeque::new() };
            {
                let &mut ChainRepl { ref mut model, ref mut connections, .. } = self;
                for conn in connections.iter_mut() {
                    changed |= conn.process_rules(event_loop,
                                                      &mut |item| parent_actions.push_back(item),
                                                      &mut events);
                    trace!("changed:{:?}; conn:{:?} ", changed, conn);
                }
            }

            changed |= self.process_rules(event_loop);
            trace!("changed:{:?}; self rules", changed);

            {

                let &mut ChainRepl { ref mut model, ref mut connections, .. } = self;
                let mut out = OutPorts(self.downstream_slot, connections);
                changed |= model.process_replication(&mut out);
                trace!("changed:{:?}; replicate", changed);

                changed |= model.flush();
                trace!("changed:{:?}; replicate flush", changed);
            }

            trace!("Changed:{:?}; actions:{:?}+{:?}", changed, parent_actions.len(), events.changes.len());
            for action in parent_actions.drain(..).chain(events.changes.drain(..)) {
                trace!("Action: {:?}", action);
                self.process_action(action, event_loop);
            }
            iterations += 1;
        }
        trace!("Converged after {:?} iterations", iterations);
    }

    fn io_ready(&mut self, event_loop: &mut mio::EventLoop<Self>, token: mio::Token) {
        self.converge_state(event_loop);
        if self.connections[token].should_close() && self.model.has_pending(token) {
            trace!("Close candidate: {:?}", token);
            let it = self.connections.remove(token);
            trace!("Removing; {:?}; {:?}", token, it);
            if Some(token) == self.downstream_slot {
                self.downstream_slot = None;
                self.model.reset();
            }
        }
    }

    fn handle_view_changed(&mut self, view: ConfigurationView<NodeViewConfig>) {
        self.new_view = Some(view)
    }

    fn committed_to(&mut self, seqno: Seqno) {
        info!("Committed upto: {:?}", seqno);
    }
    pub fn get_notifier(event_loop: &mut mio::EventLoop<Self>) -> Notifier {
        Notifier(event_loop.channel())
    }

    pub fn node_config(&self) -> NodeViewConfig {
        self.node_config.clone()
    }
}

#[derive(Debug)]
pub enum Notification {
    ViewChange(ConfigurationView<NodeViewConfig>),
    Shutdown,
    CommittedTo(Seqno),
}
pub struct Notifier(mio::Sender<Notification>);

impl Notifier {
    fn notify(&self, mut item: Notification) {
        use mio::NotifyError::*;
        use std::time::Duration;
        let mut backoff = Duration::from_millis(1);
        loop {
            item = match self.0.send(item) {
                Ok(_) => return,
                Err(Full(it)) => it,
                Err(e) => panic!("{:?}", e),
            };
            info!("Backoff: {:?}", backoff);
            std::thread::sleep(backoff);
            backoff = backoff * 2;
        }
    }

    pub fn view_changed(&self, view: ConfigurationView<NodeViewConfig>) {
        self.notify(Notification::ViewChange(view))
    }
    pub fn shutdown(&self) {
        self.notify(Notification::Shutdown)
    }
    pub fn committed_to(&self, seqno: Seqno) {
        self.notify(Notification::CommittedTo(seqno))
    }
}

struct OutPorts<'a>(Option<mio::Token>, &'a mut Slab<event_handler::EventHandler>);

impl<'a> replica::Outputs for OutPorts<'a> {
    fn respond_to(&mut self, token: mio::Token, resp: OpResp) {
        let &mut OutPorts(downstream, ref mut conns) = self;
        trace!("respond_to: {:?} -> {:?}", token, resp);
        conns[token].response(resp);
    }

    fn forward_downstream(&mut self, epoch: Epoch, msg: PeerMsg) {
        let &mut OutPorts(downstream, ref mut conns) = self;
        trace!("forward_downstream: @{:?} -> {:?}", epoch, msg);

        let slot = downstream.expect("downstream_slot");
        let ds = match &mut conns[slot] {
            &mut EventHandler::Downstream(ref mut d) => d,
            other => {
                panic!("Downstream slot not populated with a downstream instance: {:?}",
                        other)
            }
        };
        ds.send_to_downstream(epoch, msg)
    }
}

impl mio::Handler for ChainRepl {
    // This is a bit wierd; we can pass a parent action back to enqueue some action.
    type Timeout = mio::Token;
    type Message = Notification;
    fn ready(&mut self,
             event_loop: &mut mio::EventLoop<Self>,
             token: mio::Token,
             events: mio::EventSet) {
        trace!("{:?}: {:?}", token, events);
        self.connections[token].handle_event(event_loop, events);
        self.io_ready(event_loop, token);
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, token: mio::Token) {
        trace!("Timeout: {:?}", token);
        self.connections[token].handle_timeout();
        self.io_ready(event_loop, token);
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {
        trace!("Notified: {:?}", msg);
        match msg {
            Notification::ViewChange(view) => self.handle_view_changed(view),
            Notification::CommittedTo(seqno) => self.committed_to(seqno),
            Notification::Shutdown => {
                info!("Shutting down");
                event_loop.shutdown();
            }
        }
        self.converge_state(event_loop);
    }
}
