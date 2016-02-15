use mio;
use mio::tcp::*;
use mio::{TryRead,TryWrite};
use rustc_serialize::json;
use std::collections::VecDeque;
use std::net::SocketAddr;

use super::{ChainRepl, ChainReplMsg,OpResp, Operation, PeerMsg};


#[derive(Debug)]
pub struct Downstream {
    token: mio::Token,
    peer: Option<SocketAddr>,
    // Rather feels like we need to factor this into per-iteration state.
    // ... LAYERS!
    socket: Option<TcpStream>,
    sock_status: mio::EventSet,
    pending: VecDeque<PeerMsg>,
    write_buf: Vec<u8>,
    read_buf: Vec<u8>,
    should_disconnect: bool,
    timeout_triggered: bool,
}

impl Downstream {
    pub fn new(target: SocketAddr, token: mio::Token) -> Self {
        debug!("Connecting to {:?}", target);
        let mut conn = Downstream {
            token: token,
            peer: Some(target),
            socket: None,
            sock_status: mio::EventSet::none(),
            pending: VecDeque::new(),
            write_buf: Vec::new(),
            read_buf: Vec::new(),
            should_disconnect: false,
            timeout_triggered: false,
        };
        conn.attempt_connect();
        conn
    }

    pub fn reconnect_to(&mut self, target: SocketAddr) {
        info!("New downstream: {:?}", target);
        self.peer = Some(target);
        self.should_disconnect = true;
    }

    pub fn disconnect(&mut self) {
        info!("Disconnect");
        self.peer = None;
        self.should_disconnect = true;
    }

    pub fn send_to_downstream(&mut self, epoch: u64, seqno: u64, op: Operation) {
        let msg = (epoch, seqno, op);
        debug!("Sending to downstream: {:?}: {:?}", self.peer, msg);
        self.pending.push_back(msg);
    }

    pub fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
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

    pub fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl>, events: mio::EventSet) {
        self.sock_status.insert(events);
        trace!("Listener::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.socket.as_ref().map(|s| s.local_addr()), events, self.sock_status);
    }

    pub fn handle_timeout(&mut self) {
        self.timeout_triggered = true
    }

    pub fn process_rules<F: FnMut(ChainReplMsg)>(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
            to_parent: &mut F) -> bool {
        trace!("the downstream socket is {:?}", self.sock_status);

        if self.sock_status.is_readable() {
            self.read();
            self.sock_status.remove(mio::EventSet::readable());
        }

        let changed = self.process_buffer(to_parent);

        if self.sock_status.is_writable() {
            self.prep_write_buffer();
            self.write();
            self.sock_status.remove(mio::EventSet::writable());
        }

        if self.sock_status.is_hup() || self.sock_status.is_error() || self.should_disconnect {
            if let Some(sock) = self.socket.take() {
                trace!("Deregistering socket! {:?}", sock);
                event_loop.deregister(&sock).expect("deregister downstream");
                event_loop.timeout_ms(self.token, 1000).expect("reconnect timeout");
                self.should_disconnect = false;
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
        changed
    }

    fn attempt_connect(&mut self) {
        assert!(self.socket.is_none());
        if let &mut Downstream { peer: Some(ref peer), ref token, ref mut socket, ref mut sock_status, .. } = self {
            let conn = TcpStream::connect(peer).expect("Connect downstream");
            trace!("New downstream for {:?}! {:?}", self.token, self.peer);
            *socket = Some(conn);
            sock_status.remove(mio::EventSet::all());
        } else {
            warn!("Attempting to connect Downstream with no target set");
        }
    }


    fn read(&mut self) {
        let mut abuf = Vec::new();
        if let &mut Downstream { socket: Some(ref mut sock), ref mut read_buf, token, .. } = self {
            match sock.try_read_buf(&mut abuf) {
                Ok(Some(0)) => {
                    trace!("{:?}: EOF!", token);
                    self.should_disconnect = true
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
        if let Some((epoch, seqno, it)) = self.pending.pop_front() {
            debug!("Preparing to send downstream: {:?}/{:?}", seqno, it);
            let msg : PeerMsg = (epoch, seqno, it);
            let out = json::encode(&msg).expect("json encode");
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

    fn process_buffer<F: FnMut(ChainReplMsg)>(&mut self, to_parent: &mut F) -> bool {
        let mut prev = 0;
        let mut changed = false;
        trace!("Downstream: {:?}: Read buffer: {:?}", self.token, self.read_buf);
        for n in self.read_buf.iter().enumerate()
                .filter_map(|(i, e)| if *e == '\n' as u8 { Some(i) } else { None } ) {
            let line = String::from_utf8_lossy(&self.read_buf[prev..n]);
            trace!("{:?}: Pos: {:?}-{:?}; chunk: {:?}", self.token, prev, n, line);

            let val : OpResp = json::decode(&line).expect("Decode json");
            debug!("From downstream: {:?}", val);
            to_parent(ChainReplMsg::DownstreamResponse(val));
            changed = true;
            prev = n + 1
        }

        let remainder = self.read_buf[prev..].to_vec();
        trace!("{:?}: read Remainder: {}", self.token, remainder.len());
        self.read_buf = remainder;
        changed
    }

    pub fn is_closed(&self) -> bool {
        self.peer.is_none()
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
