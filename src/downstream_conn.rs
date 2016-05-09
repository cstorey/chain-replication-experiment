use mio;
use mio::tcp::*;
use mio::{TryRead, TryWrite};

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::fmt;
use std::io::ErrorKind;

use super::ChainRepl;
use data::{OpResp, PeerMsg, ReplicationMessage};
use config::Epoch;
use line_conn::{Encoder, Reader, SexpPeer, Protocol, LineConnEvents};
use hybrid_clocks::{Clock,Wall, Timestamp, WallT};

#[derive(Debug)]
pub struct Downstream<T: fmt::Debug> {
    token: mio::Token,
    peer: Option<SocketAddr>,
    // Rather feels like we need to factor this into per-iteration state.
    // ... LAYERS!
    socket: Option<TcpStream>,
    sock_status: mio::EventSet,
    pending: VecDeque<ReplicationMessage>,
    write_buf: Vec<u8>,
    read_buf: Vec<u8>,
    should_disconnect: bool,
    timeout_triggered: bool,
    epoch: Epoch,
    codec: T,
}

#[derive(Debug)]
struct DownstreamProtocol;

impl Protocol for DownstreamProtocol {
    type Send = ReplicationMessage;
    type Recv = OpResp;
}



impl Downstream<SexpPeer> {
    pub fn new(target: Option<SocketAddr>, token: mio::Token, epoch: Epoch) -> Self {
        Self::with_codec(target, token, epoch, SexpPeer::fresh(token))
    }
}

impl<T: Reader<OpResp> + Encoder<ReplicationMessage> + fmt::Debug> Downstream<T> {
    pub fn with_codec(target: Option<SocketAddr>, token: mio::Token, epoch: Epoch, codec: T) -> Self {
        debug!("Connecting to {:?}", target);
        let conn = Downstream {
            token: token,
            peer: target,
            socket: None,
            sock_status: mio::EventSet::none(),
            pending: VecDeque::new(),
            write_buf: Vec::with_capacity(1 << 14),
            read_buf: Vec::with_capacity(1 << 14),
            should_disconnect: false,
            timeout_triggered: true,
            codec: codec,
            epoch: epoch,
        };
        conn
    }

    pub fn reconnect_to(&mut self, target: SocketAddr, epoch: Epoch) {
        self.epoch = epoch;

        if self.peer != Some(target) {
            info!("New downstream: {:?}", target);
            self.peer = Some(target);
            self.should_disconnect = true;
        } else {
            debug!("No change of downstream: {:?}", target);
        }
    }

    pub fn disconnect(&mut self) {
        info!("Disconnect");
        self.peer = None;
        self.should_disconnect = true;
    }

    pub fn send_to_downstream(&mut self, now: Timestamp<WallT>, epoch: Epoch, msg: PeerMsg) {
        debug!("Queuing for downstream (qlen: {}) at {}: {:?}: {:?}",
               self.pending.len() + 1,
               now,
               self.peer,
               msg);
        self.pending.push_back(ReplicationMessage {
            epoch: epoch,
            ts: now,
            msg: msg,
        });
    }

    pub fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        if let Some(ref sock) = self.socket {
            debug!("Initialize! {:?}", self);
            match event_loop.register_opt(sock,
                                          token,
                                          mio::EventSet::readable(),
                                          mio::PollOpt::edge() | mio::PollOpt::oneshot()) {
                Ok(()) => (),
                Err(e) => {
                    warn!("Registration failed:{}; kind:{:?}", e, e.kind());
                    // self.should_disconnect = true;
                }
            }
        } else {
            warn!("Registering disconnected downstream?!? {:?}", self);
        }
    }

    pub fn handle_event(&mut self,
                        _event_loop: &mut mio::EventLoop<ChainRepl>,
                        events: mio::EventSet) {
        self.sock_status.insert(events);
        trace!("Downstream::handle_event: {:?}; this time: {:?}; now: {:?}",
               self.socket.as_ref().map(|s| s.local_addr()),
               events,
               self.sock_status);
    }

    pub fn handle_timeout(&mut self) {
        self.timeout_triggered = true
    }

    pub fn process_rules<E: LineConnEvents>(&mut self,
                                                 event_loop: &mut mio::EventLoop<ChainRepl>,
                                                 now: &Timestamp<WallT>,
                                                 events: &mut E)
                                                 -> bool {
        trace!("the downstream socket is {:?}", self.sock_status);

        if self.sock_status.is_readable() {
            self.read();
            self.sock_status.remove(mio::EventSet::readable());
        }

        let changed = self.process_buffer(events);

        if self.sock_status.is_writable() {
            self.prep_write_buffer();
            self.write();
            self.sock_status.remove(mio::EventSet::writable());
        }

        if self.sock_status.is_hup() || self.sock_status.is_error() || self.should_disconnect {
            if let Some(sock) = self.socket.take() {
                debug!("Disconnecting socket! {:?}: {:?}", sock, self.sock_status);
                match event_loop.deregister(&sock) {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Deregister socket failed: {}", e);
                    }
                }
                event_loop.timeout_ms(self.token, 1000).expect("reconnect timeout");
                self.reset();
            }
        }

        if self.socket.is_none() && self.timeout_triggered {
            self.attempt_connect(now.clone());
            self.initialize(event_loop, self.token);
            self.timeout_triggered = false;
        }


        if !self.should_close() {
            self.reinitialize(event_loop)
        }
        changed
    }

    fn reset(&mut self) {
        let peer = self.peer.clone();
        let token = self.token.clone();
        let epoch = self.epoch;
        let _ = ::std::mem::replace(self, Self::with_codec(peer, token, epoch, T::new(token)));
    }

    fn attempt_connect(&mut self, now: Timestamp<WallT>) {
        assert!(self.socket.is_none());
        debug!("Connecting @{}: {:?}", now, self);
        if let &mut Downstream { peer: Some(ref peer), ref mut socket, ref mut sock_status, .. } =
               self {
            let conn = TcpStream::connect(peer).expect("Connect downstream");
            debug!("New downstream for {:?}! {:?}; {:?}",
                   self.token,
                   self.peer,
                   conn);
            *socket = Some(conn);
            sock_status.remove(mio::EventSet::all());
        } else {
            warn!("Attempting to connect Downstream with no target set");
        }

        self.pending.push_back(ReplicationMessage {
            epoch: self.epoch,
            ts: now,
            msg: PeerMsg::HelloDownstream,
        });
    }


    fn read(&mut self) {
        if let &mut Downstream { socket: Some(ref mut sock), ref mut codec, token, .. } = self {
            match sock.try_read_buf(&mut self.read_buf) {
                Ok(Some(0)) => {
                    trace!("{:?}: EOF!", token);
                    self.should_disconnect = true
                }
                Ok(Some(n)) => {
                    trace!("{:?}: Read {}bytes", token, n);
                    codec.feed(&self.read_buf);
                    self.read_buf.clear();
                }
                Ok(None) => {
                    trace!("{:?}: Noop!", token);
                }
                Err(e) => {
                    error!("got an error trying to read; err={:?}", e);
                    self.should_disconnect = true
                }
            }
        }
    }

    fn prep_write_buffer(&mut self) {
        debug!("Preparing to send downstream (qlen {}; bufsz {})",
               self.pending.len(),
               self.write_buf.len());
        while let Some(it) = self.pending.pop_front() {
            let out = self.codec.encode(it);
            self.write_buf.extend(&*out);
        }
        debug!("Prepared to send downstream (qlen {}; bufsz {})",
               self.pending.len(),
               self.write_buf.len());
    }

    fn write(&mut self) {
        if let &mut Downstream { socket: Some(ref mut sock), ref mut write_buf, token, .. } = self {
            match sock.try_write(write_buf) {
                Ok(Some(n)) => {
                    trace!("Downstream: {:?}: Wrote {} of {} in buffer",
                           token,
                           n,
                           write_buf.len());
                    *write_buf = write_buf[n..].to_vec();
                    trace!("Downstream: {:?}: Now {:?}b", sock, write_buf.len());
                }
                Ok(None) => {
                    trace!("Write unready");
                }
                Err(e) => {
                    error!("got an error trying to write; err={:?}", e);
                    self.should_disconnect = true
                }
            }
        }
    }

    fn process_buffer<E: LineConnEvents>(&mut self, events: &mut E) -> bool {
        self.codec.process::<DownstreamProtocol, E>(self.token, events)
    }

    pub fn should_close(&self) -> bool {
        self.peer.is_none()
    }

    fn reinitialize(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>) {
        if let Some(ref sock) = self.socket {
            let mut flags = mio::EventSet::readable();
            if !(self.write_buf.is_empty() && self.pending.is_empty()) {
                flags.insert(mio::EventSet::writable());
            }
            trace!("Re-register {:?} with {:?}", self, flags);

            match event_loop.reregister(sock, self.token, flags, mio::PollOpt::oneshot()) {
                Ok(()) => (),
                Err(ref e) if e.kind() == ErrorKind::NotFound => {
                    warn!("Re-registration failed:{}; kind:{:?}", e, e.kind());
                    self.should_disconnect = true
                }
                Err(ref e) => {
                    warn!("Re-registration failed:{}; kind:{:?}", e, e.kind());
                    self.should_disconnect = true
                }
            }
        }
    }
}
