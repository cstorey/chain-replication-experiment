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
use mio::{TryRead,TryWrite};
use mio::util::Slab;
use std::collections::VecDeque;
use std::net::SocketAddr;
use clap::{Arg, App};
use rustc_serialize::json;

#[derive(Debug, RustcEncodable, RustcDecodable)]
enum Operation {
    Set(String),
    Get
}

#[derive(Debug)]
enum ChainReplMsg {
    Operation(Operation),
    NewClientConn(TcpStream),
}

struct ChainRepl {
    connections: Slab<EventHandler>,
    downstream_slot: Option<mio::Token>,
}

impl ChainRepl {
    fn new() -> ChainRepl {
        ChainRepl {
            connections: Slab::new(1024),
            downstream_slot: None,
        }
    }

    fn listen(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>, listener: TcpListener) {
        info!("Listen on {:?}", listener);
        let l = EventHandler::Listener(Listener::new(listener));
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

    fn process_action(&mut self, msg: ChainReplMsg, event_loop: &mut mio::EventLoop<ChainRepl>) {
        trace!("{:p}; got {:?}", self, msg);
        match msg {
            ChainReplMsg::Operation(s) => {
                self.downstream().send_to_downstream(s);
            },

            ChainReplMsg::NewClientConn(socket) => {
                let token = self.connections
                    .insert_with(|token| EventHandler::Conn(ClientConn::new(socket, token)))
                    .expect("token insert");
                &self.connections[token].initialize(event_loop, token);
            }
        }
    }

    fn downstream<'a>(&'a mut self) -> &'a mut Downstream {
        match &mut self.connections[self.downstream_slot.expect("Some downstream connection")] {
            &mut EventHandler::Downstream(ref mut d) => d,
            other => panic!("Downstream slot not populated with a downstream instance: {:?}", other),
        }
    }
}

#[derive(Debug)]
struct ClientConn {
    socket: TcpStream,
    sock_status: mio::EventSet,
    token: mio::Token,
    read_buf: Vec<u8>,
    read_eof: bool,
    failed: bool,
    write_buf: Vec<u8>,
}

impl ClientConn {
    fn new(socket: TcpStream, token: mio::Token) -> ClientConn {
        info!("New client connection {:?} from {:?}", token, socket.local_addr());
        ClientConn {
            socket: socket,
            sock_status: mio::EventSet::none(),
            token: token,
            read_buf: Vec::with_capacity(1024),
            write_buf: Vec::new(),
            read_eof: false,
            failed: false,
        }
    }

    fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        event_loop.register_opt(
                &self.socket,
                token,
                mio::EventSet::readable(),
                mio::PollOpt::edge() | mio::PollOpt::oneshot())
            .expect("event loop initialize");
    }

    // Event updates arrive here
    fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl>, events: mio::EventSet) {
        self.sock_status.insert(events);
        info!("ClientConn::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.socket.peer_addr(), events, self.sock_status);
    }

    // actions are processed here on down.

    fn process_rules(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
        to_parent: &mut VecDeque<ChainReplMsg>) {
        if self.sock_status.is_readable() {
            self.read();
            self.sock_status.remove(mio::EventSet::readable());
        }

        self.process_buffer(to_parent);

        if self.sock_status.is_writable() {
            self.write();
            self.sock_status.remove(mio::EventSet::writable());
        }

        if !self.is_closed() {
            self.reinitialize(event_loop)
        }
    }

    fn process_buffer(&mut self, to_parent: &mut VecDeque<ChainReplMsg>) {
        let mut prev = 0;
        info!("{:?}: Read buffer: {:?}", self.socket.peer_addr(), self.read_buf);
        for n in self.read_buf.iter().enumerate()
                .filter_map(|(i, e)| if *e == '\n' as u8 { Some(i) } else { None } ) {
            info!("{:?}: Pos: {:?}; chunk: {:?}", self.socket.peer_addr(), n, &self.read_buf[prev..n]);
            let slice = &self.read_buf[prev..n];
            let op = if slice.is_empty() {
                Operation::Get
            } else {
                Operation::Set(String::from_utf8_lossy(slice).to_string())
            };
            let cmd = ChainReplMsg::Operation(op);
            info!("Send! {:?}", cmd);
            to_parent.push_back(cmd);
            prev = n+1;
        }
        let remainder = self.read_buf[prev..].to_vec();
        info!("{:?}: read Remainder: {}", self.socket.peer_addr(), remainder.len());
        self.read_buf = remainder;
    }

    fn read(&mut self) {
        let mut abuf = Vec::new();
        match self.socket.try_read_buf(&mut abuf) {
            Ok(Some(0)) => {
                info!("{:?}: EOF!", self.socket.peer_addr());
                self.read_eof = true
            },
            Ok(Some(n)) => {
                info!("{:?}: Read {}bytes", self.socket.peer_addr(), n);
                self.read_buf.extend(abuf);
            },
            Ok(None) => {
                info!("{:?}: Noop!", self.socket.peer_addr());
            },
            Err(e) => {
                error!("got an error trying to read; err={:?}", e);
                self.failed =true;
            }
        }
    }

    fn write(&mut self) {
        match self.socket.try_write(&mut self.write_buf) {
            Ok(Some(n)) => {
                info!("{:?}: Wrote {} of {} in buffer", self.socket.peer_addr(), n,
                    self.write_buf.len());
                self.write_buf = self.write_buf[n..].to_vec();
                info!("{:?}: Now {:?}b", self.socket.peer_addr(), self.write_buf.len());
            },
            Ok(None) => {
                info!("Write unready");
            },
            Err(e) => {
                error!("got an error trying to write; err={:?}", e);
                self.failed = true;
            }
        }
    }

    fn reinitialize(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>) {
        let mut flags = mio::EventSet::readable();
        if !self.write_buf.is_empty() {
            flags.insert(mio::EventSet::writable());
        }
        info!("Registering {:?} with {:?}", self, flags);

        event_loop.reregister(
                &self.socket,
                self.token,
                flags,
                mio::PollOpt::oneshot()).expect("EventLoop#reinitialize")
    }



    fn is_closed(&self) -> bool {
        self.failed || (self.read_eof && self.write_buf.is_empty())
    }

    fn enqueue(&mut self, s: &str) {
        self.write_buf.extend(s.as_bytes());
        self.write_buf.push('\n' as u8);
    }
}

#[derive(Debug)]
struct Listener {
    listener: TcpListener,
    sock_status: mio::EventSet,
}

impl Listener {
    fn new(listener: TcpListener) -> Listener {
        Listener {
            listener: listener,
            sock_status: mio::EventSet::none(),
        }
    }

    fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        event_loop.register(&self.listener, token).expect("Register listener");
    }

    fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl>, events: mio::EventSet) {
        assert!(events.is_readable());
        self.sock_status.insert(events);
        info!("Listener::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.listener.local_addr(), events, self.sock_status);
    }

    fn process_rules(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
            to_parent: &mut VecDeque<ChainReplMsg>) {
        if self.sock_status.is_readable() {
            info!("the listener socket is ready to accept a connection");
            match self.listener.accept() {
                Ok(Some(socket)) => {
                    let cmd = ChainReplMsg::NewClientConn(socket);
                    to_parent.push_back(cmd);
                }
                Ok(None) => {
                    info!("the listener socket wasn't actually ready");
                }
                Err(e) => {
                    info!("listener.accept() errored: {}", e);
                    event_loop.shutdown();
                }
            }
            self.sock_status.remove(mio::EventSet::readable());
        }
    }

    fn is_closed(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct Downstream {
    token: mio::Token,
    peer: SocketAddr,
    socket: Option<TcpStream>,
    sock_status: mio::EventSet,
    pending: VecDeque<Operation>,
    write_buf: Vec<u8>,
    read_buf: Vec<u8>,
    read_eof: bool,
}

impl Downstream {
    fn new(target: SocketAddr, token: mio::Token) -> Self {
        info!("Connecting to {:?}", target);
        let mut conn = Downstream {
            token: token,
            peer: target,
            socket: None,
            sock_status: mio::EventSet::none(),
            pending: VecDeque::new(),
            write_buf: Vec::new(),
            read_buf: Vec::new(),
            read_eof: false,
        };
        conn.attempt_connect();
        conn
    }

    fn send_to_downstream(&mut self, s: Operation) {
        // self.write_buf.extend(s.as_bytes());
        // self.write_buf.push('\n' as u8);
        self.pending.push_front(s);
        debug!("Sending to downstream: {:?}", self);
    }

    fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        info!("Register Downstream conn to {:?} as {:?}", self.peer, token);
        if let Some(ref sock) = self.socket {
            event_loop.register_opt(
                    sock,
                    token,
                    mio::EventSet::readable(),
                    mio::PollOpt::edge() | mio::PollOpt::oneshot())
                .expect("register downstream");
        } else {
            warn!("Registering disconnected downstream?!? {:?}", self);
        }
    }

    fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl>, events: mio::EventSet) {
        self.sock_status.insert(events);
        info!("Listener::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.socket.as_ref().map(|s| s.local_addr()), events, self.sock_status);
    }

    fn process_rules(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
            to_parent: &mut VecDeque<ChainReplMsg>) {
        info!("the downstream socket is {:?}", self.sock_status);

        if self.sock_status.is_readable() {
            self.read();
            warn!("Read from downstream!");
            self.sock_status.remove(mio::EventSet::readable());
        }

        self.process_buffer(to_parent);


        if self.sock_status.is_writable() {
            self.prep_write_buffer();
            self.write();
            self.sock_status.remove(mio::EventSet::writable());
        }

        if self.sock_status.is_hup() || self.sock_status.is_error() {
            if let Some(sock) = self.socket.take() {
                info!("Deregistering socket! {:?}", sock);
                event_loop.deregister(&sock).expect("deregister downstream");
                self.attempt_connect();
                self.initialize(event_loop, self.token);
            }
        }

        if !self.is_closed() {
            self.reinitialize(event_loop)
        }
    }

    fn attempt_connect(&mut self) {
        assert!(self.socket.is_none());
        let conn = TcpStream::connect(&self.peer).expect("Connect downstream");
        info!("New downstream for {:?}! {:?}", self.token, self.peer);
        self.socket = Some(conn);
        self.sock_status.remove(mio::EventSet::all());
    }


    fn read(&mut self) {
        let mut abuf = Vec::new();
        if let &mut Downstream { socket: Some(ref mut sock), ref mut read_buf, token, .. } = self {
            match sock.try_read_buf(&mut abuf) {
                Ok(Some(0)) => {
                    info!("{:?}: EOF!", token);
                    self.read_eof = true
                },
                Ok(Some(n)) => {
                    info!("{:?}: Read {}bytes", token, n);
                    read_buf.extend(abuf);
                },
                Ok(None) => {
                    info!("{:?}: Noop!", token);
                },
                Err(e) => {
                    error!("got an error trying to read; err={:?}", e);
                }
            }
        }
    }

    fn prep_write_buffer(&mut self) {
        if let Some(it) = self.pending.pop_front() {
            debug!("Preparing to send downstream: {:?}", it);
            let out = json::encode(&it).expect("json encode");
            self.write_buf.extend(out.as_bytes());
            self.write_buf.push('\n' as u8);
        }
    }

    fn write(&mut self) {
        if let &mut Downstream { socket: Some(ref mut sock), ref mut write_buf, token, .. } = self {
            match sock.try_write(write_buf) {
                Ok(Some(n)) => {
                    info!("Downstream: {:?}: Wrote {} of {} in buffer", token, n,
                            write_buf.len());
                    *write_buf = write_buf[n..].to_vec();
                    info!("Downstream: {:?}: Now {:?}b", sock, write_buf.len());
                },
                Ok(None) => {
                    info!("Write unready");
                },
                Err(e) => {
                    error!("got an error trying to write; err={:?}", e);
                    // self.failed = true;
                }
            }
        }
    }

    fn process_buffer(&mut self, to_parent: &mut VecDeque<ChainReplMsg>) {
        let mut prev = 0;
        info!("Downstream: {:?}: Read buffer: {:?}", self.token, self.read_buf);
        for n in self.read_buf.iter().enumerate()
                .filter_map(|(i, e)| if *e == '\n' as u8 { Some(i) } else { None } ) {
            let line = &self.read_buf[prev..n];
            info!("{:?}: Pos: {:?}; chunk: {:?}", self.token, n, String::from_utf8_lossy(line));
        }
        let remainder = self.read_buf[prev..].to_vec();
        info!("{:?}: read Remainder: {}", self.token, remainder.len());
        self.read_buf = remainder;
    }
    fn is_closed(&self) -> bool {
        self.socket.is_none()
    }

    fn reinitialize(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>) {
        if let Some(ref sock) = self.socket {
            let mut flags = mio::EventSet::readable();
            if !(self.write_buf.is_empty() && self.pending.is_empty()) {
                flags.insert(mio::EventSet::writable());
            }
            info!("Re-register {:?} with {:?}", self, flags);

            event_loop.reregister(
                    sock,
                    self.token,
                    flags,
                    mio::PollOpt::oneshot()).expect("EventLoop#reinitialize")
        }
    }
}

#[derive(Debug)]
enum EventHandler {
    Listener (Listener),
    Conn (ClientConn),
    Downstream (Downstream),
}

impl EventHandler {
    fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl>, events: mio::EventSet) {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Downstream(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Listener(ref mut listener) => listener.handle_event(_event_loop, events)
        }
    }

    fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        match self {
            &EventHandler::Conn(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Downstream(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Listener(ref listener) => listener.initialize(event_loop, token)
        }
    }


    fn process_rules(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
        to_parent: &mut VecDeque<ChainReplMsg>) {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.process_rules(event_loop, to_parent),
            &mut EventHandler::Downstream(ref mut conn) => conn.process_rules(event_loop, to_parent),
            &mut EventHandler::Listener(ref mut listener) => listener.process_rules(event_loop, to_parent)
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            &EventHandler::Conn(ref conn) => conn.is_closed(),
            &EventHandler::Downstream(ref conn) => conn.is_closed(),
            &EventHandler::Listener(ref listener) => listener.is_closed()
        }
    }
}

impl mio::Handler for ChainRepl {
    type Timeout = ();
    type Message = ();
    fn ready(&mut self, event_loop: &mut mio::EventLoop<Self>, token: mio::Token, events: mio::EventSet) {
        info!("{:?}: {:?}", token, events);
        self.connections[token].handle_event(event_loop, events);

        let mut parent_actions = VecDeque::new();
        loop {
            for conn in self.connections.iter_mut() {
                conn.process_rules(event_loop, &mut parent_actions);
            }
            // Anything left to process?
            if parent_actions.is_empty() { break; }

            for action in parent_actions.drain(..) {
                self.process_action(action, event_loop);
            }
        }

        if self.connections[token].is_closed() {
            let it = self.connections.remove(token);
            info!("Removing; {:?}; {:?}", token, it);
        }
    }
}

const LOG_FILE: &'static str = "log.toml";

fn main() {
    if let Err(e) = log4rs::init_file(LOG_FILE, Default::default()) {
        panic!("Could not init logger from file {}: {}", LOG_FILE, e);
    }
    let matches = App::new("chain-repl-test")
        .arg(Arg::with_name("bind").short("l").takes_value(true).required(true))
        .arg(Arg::with_name("next").short("n").takes_value(true).required(true))
        .get_matches();

    let listen_addr = value_t_or_exit!(matches.value_of("bind"), std::net::SocketAddr);
    let listener = TcpListener::bind(&listen_addr).expect("bind");
    let next_addr = value_t_or_exit!(matches.value_of("next"), std::net::SocketAddr);

    let mut event_loop = mio::EventLoop::new().expect("Create event loop");
    let mut service = ChainRepl::new();

    service.set_downstream(&mut event_loop, next_addr);
    service.listen(&mut event_loop, listener);
    info!("running chain-repl-test listener at: {:?}; next: {:?}", listen_addr, next_addr);
    event_loop.run(&mut service).expect("Run loop");
}
