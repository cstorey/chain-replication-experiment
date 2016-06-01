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
extern crate crexp_client_proto;
extern crate time;

extern crate etcd;
extern crate tempdir;
extern crate rocksdb;
extern crate byteorder;
extern crate hex_slice;
extern crate hybrid_clocks;

#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate env_logger;
#[cfg(test)]
extern crate slab;
#[macro_use]
extern crate quick_error;

#[cfg(feature = "benches")]
extern crate test;

use mio::tcp::*;
use mio::EventLoop;
use mio::util::Slab;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::mem;
use hybrid_clocks::{Clock,Wall, Timestamp, WallT};

mod line_conn;
mod downstream_conn;
mod listener;
mod config;
mod event_handler;
mod data;
mod replication_log;
mod replica;
mod consumer;

use line_conn::{SexpPeer, LineConn, LineConnEvents};
use downstream_conn::Downstream;
use listener::{Listener,ListenerEvents};
use event_handler::EventHandler;

use data::{Seqno, OpResp, PeerMsg, NodeViewConfig, Buf};
use crexp_client_proto::messages as client;
use config::{ConfigurationView, Epoch};

pub use config::ConfigClient;
pub use replica::{ReplModel,ReplProxy};
pub use replication_log::RocksdbLog;
pub use data::Role;


#[derive(Debug)]
pub enum ChainReplMsg {
   NewClientConn(Role, TcpStream),
}

struct ChainReplEvents<'a> {
    changes: &'a mut VecDeque<ChainReplMsg>,
    clock: &'a mut Clock<Wall>,
    model: &'a mut ReplProxy<RocksdbLog>,
}

impl<'a> ListenerEvents for ChainReplEvents<'a> {
    fn new_connection(&mut self, role: Role, socket: TcpStream) {
        self.changes.push_back(ChainReplMsg::NewClientConn(role, socket))
    }
}

impl<'a> LineConnEvents for ChainReplEvents<'a> {
    fn hello_downstream(&mut self, source: mio::Token, at: Timestamp<WallT>, epoch: Epoch) {
        self.clock.observe(&at).expect("observe clock");
        let now = self.clock.now();
        debug!("recv hello_downstream: {} -> {}", at, now);
        self.model.hello_downstream(source, now, epoch)
    }
    fn operation(&mut self, source: mio::Token, epoch: Epoch, seqno: Seqno, op: Buf) {
        self.model.process_operation(source, epoch, seqno, op)
    }
    fn client_request(&mut self, source: mio::Token, op: Buf) {
        self.model.client_operation(source, op)
    }

    fn consume_requested(&mut self, source: mio::Token, mark: Seqno) {
        debug!("recv: consume_requested: {:?} from {:?}", source, mark);
        self.model.consume_requested(source, mark)
    }

    fn commit(&mut self, _source: mio::Token, epoch: Epoch, seqno: Seqno) {
        self.model.commit_observed(epoch, seqno)
    }

    fn okay(&mut self, epoch: Epoch, seqno: Seqno, data: Option<Buf>) {
        self.model.response_observed(OpResp::Ok(epoch, seqno, data))
    }

    fn hello_i_want(&mut self, at: Timestamp<WallT>, seqno: Seqno) {
        self.clock.observe(&at).expect("observe clock");
        let now = self.clock.now();
        debug!("recv hello_i_want: {} -> {}", at, now);
        self.model.response_observed(OpResp::HelloIWant(now, seqno))
    }
    fn error(&mut self, epoch: Epoch, seqno: Seqno, data: String) {
        self.model.response_observed(OpResp::Err(epoch, seqno, data))
    }
}

pub struct ChainRepl {
    listeners: Slab<Listener>,
    connections: Slab<EventHandler>,
    downstream_slot: Option<mio::Token>,
    node_config: NodeViewConfig,
    new_view: Option<ConfigurationView<NodeViewConfig>>,
    queue: VecDeque<ChainReplMsg>,
    model: ReplProxy<RocksdbLog>,
    clock: Clock<Wall>,
}

const MAX_LISTENERS: usize = 4;
const MAX_CONNS: usize = 1024;

impl ChainRepl {
    pub fn new(model: ReplProxy<RocksdbLog>) -> ChainRepl {
        ChainRepl {
            listeners: Slab::new(MAX_LISTENERS),
            connections: Slab::new_starting_at(mio::Token(MAX_LISTENERS), MAX_CONNS),
            downstream_slot: None,
            node_config: Default::default(),
            new_view: None,
            queue: VecDeque::new(),
            model: model,
            clock: Clock::wall(),
        }
    }

    pub fn listen(&mut self,
                  event_loop: &mut mio::EventLoop<ChainRepl>,
                  addr: SocketAddr,
                  role: Role) {
        info!("Listen on {:?} for {:?}", addr, role);
        let &mut ChainRepl { ref mut listeners, ref mut node_config, .. } = self;
        let token = listeners.insert_with(|token| {
                                   let l = Listener::new(addr, token, role.clone());
                                   match role {
                                       Role::Upstream => {
                                           node_config.peer_addr = Some(format!("{}",
                                                                                l.listen_addr()))
                                       }
                                       Role::ProducerClient => {
                                           node_config.producer_addr = Some(format!("{}",
                                                                                  l.listen_addr()))
                                       }
                                       Role::ConsumerClient => {
                                           node_config.consumer_addr = Some(format!("{}",
                                                                                  l.listen_addr()))
                                       }
                                   }

                                   trace!("Listener: {:?}", l);
                                   l
                               })
                               .expect("insert listener");
        listeners[token].initialize(event_loop, token);
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
            ChainReplMsg::NewClientConn(role, socket) => {
                self.process_new_client_conn(event_loop, role, socket)
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
                                Role::ProducerClient => EventHandler::Client(LineConn::peer(socket, token)),
                                Role::ConsumerClient => EventHandler::Consumer(LineConn::peer(socket, token)),
                                Role::Upstream => EventHandler::Upstream(LineConn::peer(socket, token))
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

        for p in self.listeners.iter_mut() {
            let should_listen = view.should_listen_for(&p.role);
            trace!("Active: {:?} -> {:?}", p, should_listen);
            p.set_active(should_listen);
        }

        let ds = view.should_connect_downstream().map(|ds| ds.peer_addr
                    .as_ref().expect("Cannot reconnect to downstream with no peer listener")
                    .parse().expect("peer address"));
        info!("Push to downstream on {:?}", ds);
        self.set_downstream(event_loop, view.epoch, ds);

        info!("Reconfigure model: {:?}", view);
        self.model.new_configuration(view)
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
                let &mut ChainRepl { ref mut queue, ref mut clock, ref mut model,
                    ref mut connections, ref mut listeners, .. } = self;
                let mut events = ChainReplEvents {
                    changes: queue, clock: clock, model: model,
                };
                for conn in listeners.iter_mut() {
                    changed |= conn.process_rules(event_loop, &mut events);
                    trace!("changed:{:?}; listener:{:?} ", changed, conn);
                }
                for conn in connections.iter_mut() {
                    let now = events.clock.now();
                    changed |= conn.process_rules(event_loop, &now, &mut events);
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
        if !self.token_is_listener(token) && self.connections[token].should_close() {
            trace!("Close candidate: {:?}", token);
            let it = self.connections.remove(token);
            trace!("Removing; {:?}; {:?}", token, it);
            if Some(token) == self.downstream_slot {
                self.downstream_slot = None;
                self.model.reset()
            }
        }
    }

    fn handle_view_changed(&mut self, view: ConfigurationView<NodeViewConfig>) {
        self.new_view = Some(view)
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
        let now = self.clock.now();

        self.downstream().expect("downstream").send_to_downstream(now, epoch, msg)
    }

    fn consumer_message(&mut self, token: mio::Token, seqno: Seqno, msg: Buf) {
        trace!("consumer_message: {:?} -> {:?}; {:?}", token, seqno, msg);
        if let Some(conn) = self.connections.get_mut(token) {
            conn.consumer_message(client::ConsumerResp::Ok(seqno.into(), msg.into()));
        } else {
            warn!("Response for disconnected client {:?}: {:?}", token, msg);
        }
    }


    fn token_is_listener(&self, token: mio::Token) -> bool {
        token.as_usize() < MAX_LISTENERS
    }
}

#[derive(Debug)]
pub enum Notification {
    ViewChange(ConfigurationView<NodeViewConfig>),
    Shutdown,
    RespondTo(mio::Token, OpResp),
    Forward(Epoch, PeerMsg),
    ConsumerMessage(mio::Token, Seqno, Buf),
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
            trace!("Backoff: {:?}", backoff);
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
}

impl replica::Outputs for Notifier {
    fn respond_to(&mut self, token: mio::Token, resp: &OpResp) {
        trace!("respond_to: {:?} -> {:?}", token, resp);
        self.notify(Notification::RespondTo(token, resp.clone()))
    }

    fn forward_downstream(&mut self, now: Timestamp<WallT>, epoch: Epoch, msg: PeerMsg) {
        trace!("forward_downstream: @{}, {:?}:{:?}", now, epoch, msg);
        self.notify(Notification::Forward(epoch, msg))
    }

    fn consumer_message(&mut self, token: mio::Token, seqno: Seqno, msg: Buf) {
        self.notify(Notification::ConsumerMessage(token, seqno, msg))
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
        if self.token_is_listener(token) {
            self.listeners[token].handle_event(event_loop, events);
        } else {
            self.connections[token].handle_event(event_loop, events);
        }
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
            Notification::Shutdown => {
                info!("Shutting down");
                event_loop.shutdown();
            },
            Notification::RespondTo(token, resp) => self.respond_to(token, resp),
            Notification::Forward(epoch, msg) => self.forward_downstream(epoch, msg),
            Notification::ConsumerMessage(token, seq, msg) => self.consumer_message(token, seq, msg),
        }
        self.converge_state(event_loop);
    }
}
