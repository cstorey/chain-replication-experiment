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
use std::collections::{VecDeque,HashMap};
use std::net::SocketAddr;
use clap::{Arg, App};
use rustc_serialize::json;

#[derive(Debug, RustcEncodable, RustcDecodable)]
enum Operation {
    Set(String),
    Get
}

#[derive(Debug, RustcEncodable, RustcDecodable)]
enum OpResp {
    OkVal(u64, String),
    Ok(u64),
}

#[derive(Clone, Debug, RustcEncodable, RustcDecodable)]
enum Role {
    Client,
    Peer,
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
    seq: u64,
    pending_operations: HashMap<u64, mio::Token>,
    state: String,
}

impl ChainRepl {
    fn new() -> ChainRepl {
        ChainRepl {
            connections: Slab::new(1024),
            downstream_slot: None,
            seq: 0,
            pending_operations: HashMap::new(),
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

    fn next_seqno(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn process_action(&mut self, msg: ChainReplMsg, event_loop: &mut mio::EventLoop<ChainRepl>) {
        trace!("{:p}; got {:?}", self, msg);
        match msg {
            ChainReplMsg::Operation(source, seqno, s) => {
                let seqno = seqno.unwrap_or_else(|| self.next_seqno());
                if let Some(_) = self.downstream_slot {
                    self.pending_operations.insert(seqno, source);
                    self.downstream().expect("Downstream").send_to_downstream(seqno, s);
                    // Wait for acks.
                } else {
                    info!("Terminus! {:?}/{:?}", seqno, s);
                    let resp = match s {
                        Operation::Set(s) => {
                            self.state = s;
                            OpResp::Ok(seqno)
                        },
                        Operation::Get => OpResp::OkVal(seqno, self.state.clone())
                    };
                    self.connections[source].response(resp)
                }
            },

            ChainReplMsg::DownstreamResponse(reply) => {
                info!("Downstream response: {:?}", reply);
                let seqno = match reply {
                    OpResp::Ok(seq) => seq,
                    OpResp::OkVal(seq, _) => seq,
                };
                if let Some(token) = self.pending_operations.remove(&seqno) {
                    info!("Found in-flight op {:?} for client token {:?}", seqno, token);
                    self.connections[token].response(reply)
                } else {
                    warn!("Unexpected response for seqno: {:?}", seqno);
                }
            },
            ChainReplMsg::NewClientConn(role, socket) => {
                debug!("Client connection of {:?} for {:?}", role, socket);
                let token = self.connections
                    .insert_with(|token| match role {
                            Role::Client => EventHandler::Conn(LineConn::new(socket, token, Client)),
                            Role::Peer => EventHandler::Peer(LineConn::new(socket, token, Peer)),
                    })
                    .expect("token insert");
                &self.connections[token].initialize(event_loop, token);
            }
        }
    }

    fn downstream<'a>(&'a mut self) -> Option<&'a mut Downstream> {
        self.downstream_slot.map(move |slot| match &mut self.connections[slot] {
            &mut EventHandler::Downstream(ref mut d) => d,
            other => panic!("Downstream slot not populated with a downstream instance: {:?}", other),
        })
    }

    fn converge_state(&mut self, event_loop: &mut mio::EventLoop<Self>, token: mio::Token) {
        let mut parent_actions = VecDeque::new();
        loop {
            for conn in self.connections.iter_mut() {
                conn.process_rules(event_loop, &mut |item| parent_actions.push_back(item))
            }

            // Anything left to process?
            if parent_actions.is_empty() { break; }

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

trait Proto {
    fn process_operation<F: FnMut(ChainReplMsg)>(&self, token: mio::Token, slice: &[u8], to_parent: &mut F);
    fn encode_response(&mut self, OpResp) -> Vec<u8>;
}

#[derive(Debug)]
struct LineConn<T> {
    socket: TcpStream,
    sock_status: mio::EventSet,
    token: mio::Token,
    read_buf: Vec<u8>,
    read_eof: bool,
    failed: bool,
    write_buf: Vec<u8>,
    protocol: T
}

impl<T: Proto + std::fmt::Debug> LineConn<T> {
    fn new(socket: TcpStream, token: mio::Token, protocol: T) -> LineConn<T> {
        trace!("New client connection {:?} from {:?}", token, socket.local_addr());
        LineConn {
            socket: socket,
            sock_status: mio::EventSet::none(),
            token: token,
            read_buf: Vec::with_capacity(1024),
            write_buf: Vec::new(),
            read_eof: false,
            failed: false,
            protocol: protocol
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
        trace!("LineConn::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.socket.peer_addr(), events, self.sock_status);
    }

    // actions are processed here on down.

    fn process_rules<F: FnMut(ChainReplMsg)>(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
        to_parent: &mut F) {
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

    fn process_buffer<F: FnMut(ChainReplMsg)>(&mut self, to_parent: &mut F) {
        let mut prev = 0;
        trace!("{:?}: Read buffer: {:?}", self.socket.peer_addr(), self.read_buf);
        for n in self.read_buf.iter().enumerate()
                .filter_map(|(i, e)| if *e == '\n' as u8 { Some(i) } else { None } ) {
            trace!("{:?}: Pos: {:?}-{:?}; chunk: {:?}", self.socket.peer_addr(), prev, n, &self.read_buf[prev..n]);
            let slice = &self.read_buf[prev..n];
            self.protocol.process_operation(self.token, slice, to_parent);

            prev = n+1;

        }
        let remainder = self.read_buf[prev..].to_vec();
        trace!("{:?}: read Remainder: {}", self.socket.peer_addr(), remainder.len());
        self.read_buf = remainder;
    }


    fn read(&mut self) {
        let mut abuf = Vec::new();
        match self.socket.try_read_buf(&mut abuf) {
            Ok(Some(0)) => {
                trace!("{:?}: EOF!", self.socket.peer_addr());
                self.read_eof = true
            },
            Ok(Some(n)) => {
                trace!("{:?}: Read {}bytes", self.socket.peer_addr(), n);
                self.read_buf.extend(abuf);
            },
            Ok(None) => {
                trace!("{:?}: Noop!", self.socket.peer_addr());
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
                trace!("{:?}: Wrote {} of {} in buffer", self.socket.peer_addr(), n,
                    self.write_buf.len());
                self.write_buf = self.write_buf[n..].to_vec();
                trace!("{:?}: Now {:?}b", self.socket.peer_addr(), self.write_buf.len());
            },
            Ok(None) => {
                trace!("Write unready");
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
        trace!("Registering {:?} with {:?}", self, flags);

        event_loop.reregister(
                &self.socket,
                self.token,
                flags,
                mio::PollOpt::oneshot()).expect("EventLoop#reinitialize")
    }



    fn is_closed(&self) -> bool {
        self.failed || (self.read_eof && self.write_buf.is_empty())
    }

    // Per item to output
    fn response(&mut self, s: OpResp) {
        debug!("{:?}: Response: {:?}", self.token, s);
        self.write_buf.extend(self.protocol.encode_response(s));
        self.write_buf.push('\n' as u8)
    }
}

#[derive(Debug)]
struct Client;
impl Proto for Client {
    fn encode_response(&mut self, s: OpResp) -> Vec<u8> {
        format!("{:?}", s).as_bytes().to_vec()
    }

    // Per line of input
    fn process_operation<F: FnMut(ChainReplMsg)>(&self, token: mio::Token, slice: &[u8], to_parent: &mut F) {
        let op = if slice.is_empty() {
            Operation::Get
        } else {
            Operation::Set(String::from_utf8_lossy(slice).to_string())
        };

        let cmd = ChainReplMsg::Operation(token, None, op);
        debug!("Send! {:?}", cmd);
        to_parent(cmd);
    }

}

#[derive(Debug)]
struct Peer;
impl Proto for Peer {
    fn encode_response(&mut self, s: OpResp) -> Vec<u8> {
        json::encode(&s).expect("Encode json response").as_bytes().to_vec()
    }

    fn process_operation<F: FnMut(ChainReplMsg)>(&self, token: mio::Token, slice: &[u8], to_parent: &mut F) {
        let (seqno, op) = json::decode(&String::from_utf8_lossy(slice)).expect("Decode peer operation");

        let cmd = ChainReplMsg::Operation(token, Some(seqno), op);
        debug!("Send! {:?}", cmd);
        to_parent(cmd);
    }
}


#[derive(Debug)]
struct Listener {
    listener: TcpListener,
    sock_status: mio::EventSet,
    role: Role,
}

impl Listener {
    fn new(listen_addr: SocketAddr, role: Role) -> Listener {
        let listener = TcpListener::bind(&listen_addr).expect("bind");
        Listener {
            listener: listener,
            sock_status: mio::EventSet::none(),
            role: role
        }
    }

    fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        event_loop.register(&self.listener, token).expect("Register listener");
    }

    fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl>, events: mio::EventSet) {
        assert!(events.is_readable());
        self.sock_status.insert(events);
        trace!("Listener::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.listener.local_addr(), events, self.sock_status);
    }

    fn process_rules<F: FnMut(ChainReplMsg)>(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
            to_parent: &mut F) {
        if self.sock_status.is_readable() {
            trace!("the listener socket is ready to accept a connection");
            match self.listener.accept() {
                Ok(Some(socket)) => {
                    let cmd = ChainReplMsg::NewClientConn(self.role.clone(), socket);
                    to_parent(cmd);
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
    }

    fn is_closed(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct Downstream {
    token: mio::Token,
    peer: SocketAddr,
    // Rather feels like we need to factor this into per-iteration state.
    // ... LAYERS!
    socket: Option<TcpStream>,
    sock_status: mio::EventSet,
    pending: VecDeque<(u64, Operation)>,
    write_buf: Vec<u8>,
    read_buf: Vec<u8>,
    read_eof: bool,
    timeout_triggered: bool,
}

impl Downstream {
    fn new(target: SocketAddr, token: mio::Token) -> Self {
        debug!("Connecting to {:?}", target);
        let mut conn = Downstream {
            token: token,
            peer: target,
            socket: None,
            sock_status: mio::EventSet::none(),
            pending: VecDeque::new(),
            write_buf: Vec::new(),
            read_buf: Vec::new(),
            read_eof: false,
            timeout_triggered: false,
        };
        conn.attempt_connect();
        conn
    }

    fn send_to_downstream(&mut self, seqno: u64, op: Operation) {
        // self.write_buf.extend(s.as_bytes());
        // self.write_buf.push('\n' as u8);
        self.pending.push_front((seqno, op));
        debug!("Sending to downstream: {:?}", self);
    }

    fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        debug!("Register Downstream conn to {:?} as {:?}", self.peer, token);
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
        trace!("Listener::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.socket.as_ref().map(|s| s.local_addr()), events, self.sock_status);
    }

    fn handle_timeout(&mut self) {
        self.timeout_triggered = true
    }

    fn process_rules<F: FnMut(ChainReplMsg)>(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
            to_parent: &mut F) {
        trace!("the downstream socket is {:?}", self.sock_status);

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

        if self.sock_status.is_hup() || self.sock_status.is_error() || self.read_eof {
            if let Some(sock) = self.socket.take() {
                trace!("Deregistering socket! {:?}", sock);
                event_loop.deregister(&sock).expect("deregister downstream");
                event_loop.timeout_ms(self.token, 1000);
                self.read_eof = false;
                self.read_buf.clear();
                self.write_buf.clear();
                self.sock_status.remove(mio::EventSet::all());
            }
        }

        if self.timeout_triggered {
            self.attempt_connect();
            self.initialize(event_loop, self.token);
            self.timeout_triggered = false;
        }


        if !self.is_closed() {
            self.reinitialize(event_loop)
        }
    }

    fn attempt_connect(&mut self) {
        assert!(self.socket.is_none());
        let conn = TcpStream::connect(&self.peer).expect("Connect downstream");
        trace!("New downstream for {:?}! {:?}", self.token, self.peer);
        self.socket = Some(conn);
        self.sock_status.remove(mio::EventSet::all());
    }


    fn read(&mut self) {
        let mut abuf = Vec::new();
        if let &mut Downstream { socket: Some(ref mut sock), ref mut read_buf, token, .. } = self {
            match sock.try_read_buf(&mut abuf) {
                Ok(Some(0)) => {
                    trace!("{:?}: EOF!", token);
                    self.read_eof = true
                },
                Ok(Some(n)) => {
                    trace!("{:?}: Read {}bytes", token, n);
                    read_buf.extend(abuf);
                },
                Ok(None) => {
                    trace!("{:?}: Noop!", token);
                },
                Err(e) => {
                    error!("got an error trying to read; err={:?}", e);
                }
            }
        }
    }

    fn prep_write_buffer(&mut self) {
        if let Some((seqno, it)) = self.pending.pop_front() {
            debug!("Preparing to send downstream: {:?}/{:?}", seqno, it);
            let out = json::encode(&(seqno, it)).expect("json encode");
            self.write_buf.extend(out.as_bytes());
            self.write_buf.push('\n' as u8);
        }
    }

    fn write(&mut self) {
        if let &mut Downstream { socket: Some(ref mut sock), ref mut write_buf, token, .. } = self {
            match sock.try_write(write_buf) {
                Ok(Some(n)) => {
                    trace!("Downstream: {:?}: Wrote {} of {} in buffer", token, n,
                            write_buf.len());
                    *write_buf = write_buf[n..].to_vec();
                    trace!("Downstream: {:?}: Now {:?}b", sock, write_buf.len());
                },
                Ok(None) => {
                    trace!("Write unready");
                },
                Err(e) => {
                    error!("got an error trying to write; err={:?}", e);
                    // self.failed = true;
                }
            }
        }
    }

    fn process_buffer<F: FnMut(ChainReplMsg)>(&mut self, to_parent: &mut F) {
        let mut prev = 0;
        trace!("Downstream: {:?}: Read buffer: {:?}", self.token, self.read_buf);
        for n in self.read_buf.iter().enumerate()
                .filter_map(|(i, e)| if *e == '\n' as u8 { Some(i) } else { None } ) {
            let line = String::from_utf8_lossy(&self.read_buf[prev..n]);
            trace!("{:?}: Pos: {:?}-{:?}; chunk: {:?}", self.token, prev, n, line);

            let val : OpResp = json::decode(&line).expect("Decode json");
            trace!("From downstream: {:?}", val);
            to_parent(ChainReplMsg::DownstreamResponse(val));

            prev = n + 1
        }
        let remainder = self.read_buf[prev..].to_vec();
        trace!("{:?}: read Remainder: {}", self.token, remainder.len());
        self.read_buf = remainder;
    }

    fn is_closed(&self) -> bool {
        false
    }

    fn reinitialize(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>) {
        if let Some(ref sock) = self.socket {
            let mut flags = mio::EventSet::readable();
            if !(self.write_buf.is_empty() && self.pending.is_empty()) {
                flags.insert(mio::EventSet::writable());
            }
            trace!("Re-register {:?} with {:?}", self, flags);

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
    Conn (LineConn<Client>),
    Peer (LineConn<Peer>),
    Downstream (Downstream),
}

impl EventHandler {
    fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl>, events: mio::EventSet) {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Peer(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Downstream(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Listener(ref mut listener) => listener.handle_event(_event_loop, events)
        }
    }

    fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        match self {
            &EventHandler::Conn(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Peer(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Downstream(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Listener(ref listener) => listener.initialize(event_loop, token)
        }
    }


    fn process_rules<F: FnMut(ChainReplMsg)>(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
        to_parent: &mut F)  {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.process_rules(event_loop, to_parent),
            &mut EventHandler::Peer(ref mut conn) => conn.process_rules(event_loop, to_parent),
            &mut EventHandler::Downstream(ref mut conn) => conn.process_rules(event_loop, to_parent),
            &mut EventHandler::Listener(ref mut listener) => listener.process_rules(event_loop, to_parent)
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            &EventHandler::Conn(ref conn) => conn.is_closed(),
            &EventHandler::Peer(ref conn) => conn.is_closed(),
            &EventHandler::Downstream(ref conn) => conn.is_closed(),
            &EventHandler::Listener(ref listener) => listener.is_closed()
        }
    }

    fn response(&mut self, val: OpResp) {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.response(val),
            &mut EventHandler::Peer(ref mut conn) => conn.response(val),
            other => panic!("Unexpected Response to {:?}", other),
        }
    }


    fn handle_timeout(&mut self) {
        match self {
            &mut EventHandler::Downstream(ref mut conn) => conn.handle_timeout(),
            other => warn!("Unexpected timeout for {:?}", other),
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
        service.listen(&mut event_loop, listen_addr, Role::Peer);
    }

    if let Some(next_addr) = matches.value_of("next") {
        let next_addr = next_addr.parse::<std::net::SocketAddr>().expect("parse next address");
        info!("Forwarding to address {:?}", next_addr);
        service.set_downstream(&mut event_loop, next_addr);
    }

    info!("running chain-repl-test listener");
    event_loop.run(&mut service).expect("Run loop");
}
