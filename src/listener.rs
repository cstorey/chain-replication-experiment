use mio;
use mio::tcp::*;
use std::net::SocketAddr;

use super::{Role,ChainRepl, ChainReplMsg};

#[derive(Debug)]
pub struct Listener {
    listener: TcpListener,
    token: mio::Token,
    sock_status: mio::EventSet,
    pub role: Role,
    active: bool,
}

impl Listener {
    pub fn new(listen_addr: SocketAddr, token: mio::Token, role: Role) -> Listener {
        let listener = TcpListener::bind(&listen_addr).expect("bind");
        Listener {
            listener: listener,
            token: token,
            sock_status: mio::EventSet::none(),
            role: role,
            active: false,
        }
    }

    pub fn token(&self) -> mio::Token {
        self.token
    }

    pub fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        event_loop.register(&self.listener, token).expect("Register listener");
    }

    pub fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl>, events: mio::EventSet) {
        assert!(events.is_readable());
        self.sock_status.insert(events);
        trace!("Listener::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.listener.local_addr(), events, self.sock_status);
    }

    pub fn process_rules<F: FnMut(ChainReplMsg)>(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
            to_parent: &mut F) -> bool {

        let mut changed = false;
        if self.sock_status.is_readable() {
            trace!("the listener socket is ready to accept a connection");
            match self.listener.accept() {
                Ok(Some(socket)) => {
                    let cmd = ChainReplMsg::NewClientConn(self.role.clone(), socket);
                    if self.active {
                        to_parent(cmd);
                    } else {
                        debug!("Ignoring events on inactive listener: {:?}; {:?}", self.listener.local_addr(), self.sock_status);
                    }

                    changed = true;
                }
                Ok(None) => {
                    trace!("the listener socket wasn't actually ready");
                }
                Err(e) => {
                    trace!("listener.accept() errored: {}", e);
                    event_loop.shutdown();
                }
            }
            self.sock_status.remove(mio::EventSet::readable());
        }
        changed
    }

    pub fn is_closed(&self) -> bool {
        false
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listener.local_addr().expect("local_addr")
    }

    pub fn set_active(&mut self, state: bool) {
        self.active = state;
        debug!("Now: {:?}", self);
    }
}
