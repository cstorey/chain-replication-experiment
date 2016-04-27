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
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::mem;

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

use data::{Seqno, OpResp, PeerMsg, NodeViewConfig, Buf};
use config::{ConfigurationView, Epoch};

pub use config::ConfigClient;
pub use replica::{ReplModel,ReplCommand};
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
    HelloDownstream(mio::Token, Epoch),
}

struct ChainReplEvents<'a> { changes: &'a mut VecDeque<ChainReplMsg> }

impl<'a> ListenerEvents for ChainReplEvents<'a> {
    fn new_connection(&mut self, role: Role, socket: TcpStream) {
        self.changes.push_back(ChainReplMsg::NewClientConn(role, socket))
    }
}

impl<'a> UpstreamEvents for ChainReplEvents<'a> {
    fn hello_downstream(&mut self, source: mio::Token, epoch: Epoch) {
        self.changes.push_back(ChainReplMsg::HelloDownstream(source, epoch))
    }
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

impl<'a> DownstreamEvents for ChainReplEvents<'a> {
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

impl<'a> LineConnEvents for ChainReplEvents<'a> { }

pub struct ChainRepl {
    connections: Slab<EventHandler>,
    downstream_slot: Option<mio::Token>,
    node_config: NodeViewConfig,
    new_view: Option<ConfigurationView<NodeViewConfig>>,
    queue: VecDeque<ChainReplMsg>,
    model_thread: thread::JoinHandle<()>,
    model: Sender<ReplCommand>,
}

impl ChainRepl {
    pub fn new(mut model: ReplModel<RocksdbLog>, event_loop: &mut EventLoop<ChainRepl>) -> ChainRepl {
        let (tx, rx) = channel();
        let notifications = Self::get_notifier(event_loop);
        let model_thread = thread::Builder::new().name("replmodel".to_string()).spawn(move || {
            model.run_from(rx, notifications);
        }).expect("spawn model");
        ChainRepl {
            connections: Slab::new(1024),
            downstream_slot: None,
            node_config: Default::default(),
            new_view: None,
            queue: VecDeque::new(),
            model_thread: model_thread,
            model: tx,
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
                          epoch: Epoch,
                          target: Option<SocketAddr>) {
        match (self.downstream_slot, target) {
            (Some(_), Some(target)) => self.downstream().expect("downstream").reconnect_to(target, epoch),
            (None, Some(target)) => {
                let token = self.connections
                                .insert_with(|token| {
                                    EventHandler::Downstream(Downstream::new(Some(target), token, epoch))
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
                self.model.send(ReplCommand::Operation(source, epoch, seqno, op)).expect("model send");
            }
            ChainReplMsg::ClientOperation { source, op } => {
                self.model.send(ReplCommand::ClientOperation(source, op)).expect("model send");
            }

            ChainReplMsg::Commit { epoch, seqno, .. } => {
                self.model.send(ReplCommand::CommitObserved(epoch, seqno)).expect("model send");
            }

            ChainReplMsg::DownstreamResponse(reply) => {
                trace!("Downstream response: {:?}", reply);
                self.model.send(ReplCommand::ResponseObserved(reply)).expect("model send");
            },
            ChainReplMsg::NewClientConn(role, socket) => {
                self.process_new_client_conn(event_loop, role, socket)
            }
            ChainReplMsg::HelloDownstream (source, epoch) => {
                self.model.send(ReplCommand::HelloDownstream(source, epoch)).expect("model send")
            }
        }
    }

    fn process_new_client_conn(&mut self,
                               event_loop: &mut mio::EventLoop<ChainRepl>,
                               role: Role,
                               socket: TcpStream) {
        let peer = socket.peer_addr().expect("peer address");
        trace!("process_new_client_conn: {:?}@{:?}", role, peer);

        let token = self.connections
                        .insert_with(|token| {
                            match role {
                                Role::Client => EventHandler::Conn(LineConn::client(socket, token)),
                                Role::Upstream => EventHandler::Upstream(LineConn::upstream(socket, token))
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
            self.set_downstream(event_loop, view.epoch, Some(peer_addr.parse().expect("peer address")));
        } else {
            info!("Tail node!");
            self.set_downstream(event_loop, view.epoch, None);
        }

        info!("Reconfigure model: {:?}", view);
        self.model.send(ReplCommand::NewConfiguration(view)).expect("model send")
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
        let mut changed = true;
        let mut iterations = 0;
        trace!("Converge begin");

        while changed {
            trace!("Iter: {:?}", iterations);
            changed = false;
            {
                let &mut ChainRepl { ref mut queue, ref mut connections, .. } = self;
                let mut events = ChainReplEvents { changes: queue };
                for conn in connections.iter_mut() {
                    changed |= conn.process_rules(event_loop, &mut events);
                    trace!("changed:{:?}; conn:{:?} ", changed, conn);
                }
            }

            changed |= self.process_rules(event_loop);
            trace!("changed:{:?}; self rules", changed);

            trace!("Changed:{:?}; actions:{:?}", changed, self.queue.len());
            for action in mem::replace(&mut self.queue, VecDeque::new()) {
                trace!("Action: {:?}", action);
                self.process_action(action, event_loop);
            }
            iterations += 1;
        }
        trace!("Converged after {:?} iterations", iterations);
    }

    fn io_ready(&mut self, event_loop: &mut mio::EventLoop<Self>, token: mio::Token) {
        self.converge_state(event_loop);
        if self.connections[token].should_close() {
            trace!("Close candidate: {:?}", token);
            let it = self.connections.remove(token);
            trace!("Removing; {:?}; {:?}", token, it);
            if Some(token) == self.downstream_slot {
                self.downstream_slot = None;
                self.model.send(ReplCommand::Reset).expect("model send");
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

    fn respond_to(&mut self, token: mio::Token, resp: OpResp) {
        trace!("respond_to: {:?} -> {:?}", token, resp);
        if let Some(conn) = self.connections.get_mut(token) {
            conn.response(resp);
        } else {
            warn!("Response for disconnected client {:?}: {:?}", token, resp);
        }
    }

    fn forward_downstream(&mut self, epoch: Epoch, msg: PeerMsg) {
        trace!("forward_downstream: @{:?} -> {:?}", epoch, msg);

        self.downstream().expect("downstream").send_to_downstream(epoch, msg)
    }
}

#[derive(Debug)]
pub enum Notification {
    ViewChange(ConfigurationView<NodeViewConfig>),
    Shutdown,
    CommittedTo(Seqno),
    RespondTo(mio::Token, OpResp),
    Forward(Epoch, PeerMsg),
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

impl replica::Outputs for Notifier {
    fn respond_to(&mut self, token: mio::Token, resp: OpResp) {
        trace!("respond_to: {:?} -> {:?}", token, resp);
        self.notify(Notification::RespondTo(token, resp))
    }

    fn forward_downstream(&mut self, epoch: Epoch, msg: PeerMsg) {
        trace!("forward_downstream: @{:?} -> {:?}", epoch, msg);
        self.notify(Notification::Forward(epoch, msg))
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
            },
            Notification::RespondTo(token, resp) => self.respond_to(token, resp),
            Notification::Forward(epoch, msg) => self.forward_downstream(epoch, msg),
        }
        self.converge_state(event_loop);
    }
}
