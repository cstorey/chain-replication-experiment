use mio;
use mio::tcp::*;
use mio::{TryRead,TryWrite};
use serde::ser::Serialize;
use serde::de::Deserialize;
use spki_sexp as sexp;
use std::fmt;
use std::marker::PhantomData;
use std::collections::VecDeque;

use super::{ChainRepl, ChainReplMsg, OpResp, Operation, PeerMsg, ReplicationMessage};
use replica::AppModel;

const NL : u8 = '\n' as u8;
const DEFAULT_BUFSZ : usize = 1 << 12;

pub trait Encoder<T> {
    fn encode(&self, s: T) -> Vec<u8>;
}

pub trait Reader<T> {
    fn feed(&mut self, slice: &[u8]);
    fn take(&mut self) -> Option<T>;
    fn new(mio::Token) -> Self;
}

trait Protocol<O> : fmt::Debug{
    // What goes over the wire
    type Send;
    type Recv;

    fn as_msg(mio::Token, Self::Recv) -> ChainReplMsg<O>;
 }

#[derive(Debug)]
pub struct PeerClientProto<O>(PhantomData<O>);
impl<O: fmt::Debug> Protocol<O> for PeerClientProto<O> {
    type Send = OpResp;
    type Recv = ReplicationMessage<O>;

    fn as_msg<k>(token: mio::Token, msg: Self::Recv) -> ChainReplMsg<O> {
        match msg {
            ReplicationMessage { epoch, msg: PeerMsg::Commit(seqno, op) } =>
                ChainReplMsg::Operation { source: token, epoch: Some(epoch), seqno: Some(seqno), op: op },
        }
    }
}

#[derive(Debug)]
pub struct ManualClientProto;
impl<O> Protocol<O> for ManualClientProto {
    type Send = OpResp;
    type Recv = Operation;
    fn as_msg(token: mio::Token, msg: Self::Recv) -> ChainReplMsg<O> {
        ChainReplMsg::Operation { source: token, epoch: None, seqno: None, op: msg }
    }
}

#[derive(Debug)]
pub struct LineConn<T, P> {
    socket: TcpStream,
    sock_status: mio::EventSet,
    token: mio::Token,
    read_eof: bool,
    failed: bool,
    write_buf: Vec<u8>,
    read_buf: Vec<u8>,
    codec: T,
    proto: PhantomData<P>,
}

impl<T, P> LineConn<T, P> where P: Protocol, T: /* Reader<P::Recv> + Encoder<P::Send> +*/ fmt::Debug {
    fn new(socket: TcpStream, token: mio::Token, codec: T) -> LineConn<T, P> {
        trace!("New client connection {:?} from {:?}", token, socket.local_addr());
        LineConn {
            socket: socket,
            sock_status: mio::EventSet::none(),
            token: token,
            write_buf: Vec::with_capacity(DEFAULT_BUFSZ),
            read_buf: Vec::with_capacity(DEFAULT_BUFSZ),
            read_eof: false,
            failed: false,
            codec: codec,
        }
    }

    pub fn token(&self) -> mio::Token {
        self.token
    }

    pub fn initialize<M: AppModel>(&self, event_loop: &mut mio::EventLoop<ChainRepl<M>>, token: mio::Token) {
        event_loop.register_opt(
                &self.socket,
                token,
                mio::EventSet::readable(),
                mio::PollOpt::edge() | mio::PollOpt::oneshot())
            .expect("event loop initialize");
    }

    // Event updates arrive here
    pub fn handle_event<M: AppModel>(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl<M>>, events: mio::EventSet) {
        self.sock_status.insert(events);
        trace!("LineConn::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.socket.peer_addr(), events, self.sock_status);
    }

}

    // actions are processed here on down


impl<T, P> LineConn<T, P> where
        P: Protocol,
        T: Reader<P::Recv> + Encoder<P::Send> + fmt::Debug {
    pub fn process_rules<M: AppModel, F: FnMut(ChainReplMsg<M::Operation>)>(
        &mut self, event_loop: &mut mio::EventLoop<ChainRepl<M>>,
        to_parent: &mut F) -> bool {

        if self.sock_status.is_readable() {
            self.read();
            self.sock_status.remove(mio::EventSet::readable());
        }

        let changed = self.process_buffer(to_parent);

        if self.sock_status.is_writable() {
            self.write();
            self.sock_status.remove(mio::EventSet::writable());
        }

        if !self.is_closed() {
            self.reinitialize(event_loop)
        }
        changed
    }

    fn process_buffer<O, F: FnMut(ChainReplMsg<O>)>(&mut self, to_parent: &mut F) -> bool {
        let mut changed = false;
        // trace!("{:?}: Read buffer: {:?}", self.socket.peer_addr(), self.read_buf);
        while let Some(cmd) = self.codec.take() {
            to_parent(P::as_msg(self.token, cmd));
            changed = true;
        }

        changed
    }


    fn read(&mut self) {
        match self.socket.try_read_buf(&mut self.read_buf) {
            Ok(Some(0)) => {
                debug!("{:?}: EOF!", self.socket.peer_addr());
                self.read_eof = true
            },
            Ok(Some(n)) => {
                trace!("{:?}: Read {}bytes", self.socket.peer_addr(), n);
                self.codec.feed(&self.read_buf);
                self.read_buf.clear();
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

    fn reinitialize<M: AppModel>(&mut self, event_loop: &mut mio::EventLoop<ChainRepl<M>>) {
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



    pub fn is_closed(&self) -> bool {
        trace!("Closed? failed: {:?}; read_eof:{:?}; write_empty: {:?}", self.failed, self.read_eof, self.write_buf.is_empty());
        self.failed || (self.read_eof && self.write_buf.is_empty())
    }

    // Per item to output
    pub fn response(&mut self, s: P::Send) {
        let chunk = self.codec.encode(s);
        self.write_buf.extend(chunk);
    }

}

#[derive(Debug)]
pub struct SexpPeer {
    token: mio::Token,
    packets: sexp::Packetiser,
}

impl SexpPeer {
    pub fn fresh(token: mio::Token) -> SexpPeer {
        SexpPeer { token: token, packets: sexp::Packetiser::new() }
    }
}

impl<T: Serialize> Encoder<T> for SexpPeer {
    fn encode(&self, s: T) -> Vec<u8> {
        sexp::as_bytes(&s).expect("Encode json response")
    }
}

impl<O> Reader<ChainReplMsg<O>> for SexpPeer {
    fn new(token: mio::Token) -> SexpPeer {
        Self::fresh(token)
    }

    fn feed(&mut self, slice: &[u8]) {
        self.packets.feed(slice)
    }
    fn take(&mut self) -> Option<ChainReplMsg<O>> {
        if let Some(msg) = self.packets.take().expect("Pull packet") {
            let _ : ReplicationMessage = msg;
            debug!("Decoding PeerMsg: {:?}", msg);
            let ret = match msg {
                 ReplicationMessage { epoch, msg: PeerMsg::Commit(seqno, op) } =>
                    ChainReplMsg::Operation { source: self.token, epoch: Some(epoch), seqno: Some(seqno), op: op },
            };
            Some(ret)
        } else {
            None
        }
    }
}
impl<T: Deserialize> Reader<T> for SexpPeer {
    fn new(token: mio::Token) -> SexpPeer {
        Self::fresh(token)
    }
    fn feed(&mut self, slice: &[u8]) {
        self.packets.feed(slice)
    }
    fn take(&mut self) -> Option<T> {
        self.packets.take().expect("Pull packet")
    }
}

impl<P> LineConn<SexpPeer, P> where P: Protocol, SexpPeer: Reader<P::Recv> + Encoder<P::Send> + fmt::Debug {
    pub fn upstream(socket: TcpStream, token: mio::Token) -> LineConn<SexpPeer, P> {
        Self::new(socket, token, SexpPeer::fresh(token))
    }
}

#[derive(Debug)]
pub struct PlainClient {
    token: mio::Token,
    buf: VecDeque<u8>
}

impl<T: fmt::Debug> Encoder<T> for PlainClient {
    fn encode(&self, s: T) -> Vec<u8> {
        format!("{:?}\n", s).as_bytes().to_vec()
    }
}

impl Reader<Operation> for PlainClient {
    fn new(token: mio::Token) -> PlainClient {
        PlainClient { token: token, buf: VecDeque::new() }
    }
    fn feed(&mut self, slice: &[u8]) {
        self.buf.extend(slice);
    }
    fn take(&mut self) -> Option<Operation> {
        if let Some(idx) = self.buf.iter().enumerate().filter_map(|(i, &c)| if c == NL { Some(i+1) } else { None }).next() {
            let slice = self.buf.drain(..idx).collect::<Vec<_>>();
            let s = String::from_utf8_lossy(&slice);
            let op = if s.trim().is_empty() {
                Operation::Get
            } else {
                Operation::Set(s.trim().to_string())
            };
            Some(op)
        } else {
            None
        }
    }
}


impl<P> LineConn<PlainClient, P> where P: Protocol, PlainClient: Reader<P::Recv> + Encoder<P::Send> + fmt::Debug {
    pub fn client(socket: TcpStream, token: mio::Token) -> LineConn<PlainClient, P> {
        Self::new(socket, token, PlainClient::new(token))
    }
}
