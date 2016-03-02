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

use mio::tcp::*;
use mio::EventLoop;
use mio::util::Slab;
use std::collections::{VecDeque, BTreeMap};
use std::net::SocketAddr;
use std::cmp;

mod line_conn;
mod downstream_conn;
mod listener;
mod config;
mod event_handler;
mod data;
mod replication_log;
mod replica;

use line_conn::{SexpPeer, LineConn};
use downstream_conn::Downstream;
use listener::Listener;
use event_handler::EventHandler;

pub use data::*;
pub use config::*;

pub use replica::ReplModel;
pub use replication_log::Log;


#[derive(Debug)]
enum ChainReplMsg {
    Operation {
        source: mio::Token,
        epoch: Option<u64>,
        seqno: Option<u64>,
        op: Operation,
    },
    Commit {
        source: mio::Token,
        epoch: u64,
        seqno: u64,
    },
    DownstreamResponse(OpResp),
    NewClientConn(Role, TcpStream),
    ForwardDownstream(u64, PeerMsg),
}

#[derive(Debug)]
pub struct ChainRepl {
    connections: Slab<EventHandler>,
    downstream_slot: Option<mio::Token>,
    node_config: NodeViewConfig,
    new_view: Option<ConfigurationView<NodeViewConfig>>,
    model: ReplModel,
}

impl ChainRepl {
    pub fn new(model: ReplModel) -> ChainRepl {
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
                self.model.process_operation(&mut self.connections[source], seqno, epoch, op);
            }

            ChainReplMsg::Commit { source, seqno, epoch } => {
                self.model.commit_observed(seqno);
            }

            ChainReplMsg::DownstreamResponse(reply) => self.process_downstream_response(reply),

            ChainReplMsg::NewClientConn(role, socket) => {
                self.process_new_client_conn(event_loop, role, socket)
            }
            ChainReplMsg::ForwardDownstream(epoch, msg) => {
                self.downstream().expect("Downstream").send_to_downstream(epoch, msg)
            }
        }
    }


    fn process_downstream_response(&mut self, reply: OpResp) {
        debug!("Downstream response: {:?}", reply);
        if let Some(waiter) = self.model.process_downstream_response(&reply) {
            if let Some(ref mut c) = self.connections.get_mut(waiter) {
                c.response(reply)
            }
        }
    }

    fn process_new_client_conn(&mut self,
                               event_loop: &mut mio::EventLoop<ChainRepl>,
                               role: Role,
                               socket: TcpStream) {
        let peer = socket.peer_addr().expect("peer address");
        let seqno = self.model.seqno();
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
        debug!("Client connection of {:?}/{:?} from {:?}",
               role,
               token,
               peer);
        &self.connections[token].initialize(event_loop, token);
    }

    fn process_rules<F: FnMut(ChainReplMsg)>(&mut self,
                                             event_loop: &mut mio::EventLoop<Self>,
                                             mut action: F)
                                             -> bool {
        let mut changed = false;
        changed |= self.model.process_replication(|epoch, msg| {
            action(ChainReplMsg::ForwardDownstream(epoch, msg))
        });

        changed |= self.model.flush();

        if let Some(view) = self.new_view.take() {
            self.reconfigure(event_loop, view);
            changed = true;
        }

        changed
    }


    fn reconfigure(&mut self,
                   event_loop: &mut mio::EventLoop<Self>,
                   view: ConfigurationView<NodeViewConfig>) {
        info!("Reconfigure according to: {:?}", view);

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

        self.model.set_should_auto_commit(view.is_head());

        if let Some(ds) = view.should_connect_downstream() {
            info!("Push to downstream on {:?}", ds);
            if let Some(ref peer_addr) = ds.peer_addr {
                self.set_downstream(event_loop, Some(peer_addr.parse().expect("peer address")));
            } else {
                panic!("Cannot reconnect to downstream with no peer listener: {:?}",
                       ds);
            }

            self.model.set_has_downstream(true);
        } else {
            info!("Tail node!");
            self.model.set_has_downstream(false);
            self.set_downstream(event_loop, None);
        }

        self.model.epoch_changed(view.epoch);
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

    fn converge_state(&mut self, event_loop: &mut mio::EventLoop<Self>) {
        let mut parent_actions = VecDeque::new();
        let mut changed = true;
        let mut iterations = 0;
        debug!("Converge begin");
        while changed {
            trace!("Iter: {:?}", iterations);
            changed = false;
            for conn in self.connections.iter_mut() {
                let changedp = conn.process_rules(event_loop,
                                                  &mut |item| parent_actions.push_back(item));
                if changedp {
                    trace!("Changed: {:?}", conn);
                };
                changed |= changedp;
            }

            let changedp = self.process_rules(event_loop,
                                              &mut |item| parent_actions.push_back(item));
            if changedp {
                trace!("Changed: {:?}", "Model");
            }
            changed |= changedp;

            debug!("Actions pending: {:?}", parent_actions.len());
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
        if self.connections[token].is_closed() && self.model.has_pending(token) {
            debug!("Close candidate: {:?}", token);
            let it = self.connections.remove(token);
            debug!("Removing; {:?}; {:?}", token, it);
            if Some(token) == self.downstream_slot {
                self.downstream_slot = None;
                self.model.reset();
            }
        }
    }

    fn handle_view_changed(&mut self, view: ConfigurationView<NodeViewConfig>) {
        self.new_view = Some(view)
    }

    fn committed_to(&mut self, seqno: u64) {
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
    CommittedTo(u64),
}
pub struct Notifier(mio::Sender<Notification>);

impl Notifier {
    fn notify(&self, mut item: Notification) {
        use mio::NotifyError::*;
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

    pub fn view_changed(&self, view: ConfigurationView<NodeViewConfig>) {
        self.notify(Notification::ViewChange(view))
    }
    pub fn shutdown(&self) {
        self.notify(Notification::Shutdown)
    }
    pub fn committed_to(&self, seqno: u64) {
        self.notify(Notification::CommittedTo(seqno))
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
        debug!("Timeout: {:?}", token);
        self.connections[token].handle_timeout();
        self.io_ready(event_loop, token);
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {
        debug!("Notified: {:?}", msg);
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
