use mio;
use mio::tcp::*;
use mio::{TryRead, TryWrite};
use serde::ser::Serialize;
use serde::de::Deserialize;
use spki_sexp as sexp;
use std::fmt;
use std::marker::PhantomData;
use std::collections::VecDeque;

use super::{ChainRepl,ChainReplMsg};
use data::{OpResp, Operation, PeerMsg, ReplicationMessage, Seqno, Buf};
use config::Epoch;
use crexp_client_proto::messages::{ClientReq,ClientResp};
use hybrid_clocks::{Timestamp, WallT};

const NL: u8 = '\n' as u8;
const DEFAULT_BUFSZ: usize = 1 << 12;

pub trait Encoder<T> {
    fn encode(&self, s: T) -> Vec<u8>;
}

pub trait Reader<T> {
    fn feed(&mut self, slice: &[u8]);
    fn new(mio::Token) -> Self;

    fn process<P: Protocol<Recv=T>, E: LineConnEvents>(&mut self, token: mio::Token, events: &mut E) -> bool ;
}

pub trait Protocol : fmt::Debug{
    type Send;
    type Recv;
 }

pub trait UpstreamEvents {
    fn hello_downstream(&mut self, source: mio::Token, at: Timestamp<WallT>, epoch: Epoch);
    fn operation(&mut self, source: mio::Token, epoch: Epoch, seqno: Seqno, op: Buf);
    fn client_request(&mut self, source: mio::Token, op: Buf);
    fn commit(&mut self, source: mio::Token, epoch: Epoch, seqno: Seqno);
}

pub trait DownstreamEvents {
    fn okay(&mut self, epoch: Epoch, seqno: Seqno, data: Option<Buf>);
    fn hello_i_want(&mut self, at: Timestamp<WallT>, seqno: Seqno);
    fn error(&mut self, epoch: Epoch, seqno: Seqno, data: String);
}

pub trait LineConnEvents : UpstreamEvents + DownstreamEvents {
}

#[derive(Debug)]
pub struct PeerClientProto;
impl Protocol for PeerClientProto {
    type Send = OpResp;
    type Recv = ReplicationMessage;
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

impl<T, P> LineConn<T, P>
    where P: Protocol,
          T: fmt::Debug
{
    fn new(socket: TcpStream, token: mio::Token, codec: T) -> LineConn<T, P> {
        trace!("New client connection {:?} from {:?}",
               token,
               socket.local_addr());
        LineConn {
            socket: socket,
            sock_status: mio::EventSet::none(),
            token: token,
            write_buf: Vec::with_capacity(DEFAULT_BUFSZ).into(),
            read_buf: Vec::with_capacity(DEFAULT_BUFSZ).into(),
            read_eof: false,
            failed: false,
            codec: codec,
            proto: PhantomData,
        }
    }

    pub fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        event_loop.register_opt(&self.socket,
                                token,
                                mio::EventSet::readable(),
                                mio::PollOpt::edge() | mio::PollOpt::oneshot())
                  .expect("event loop initialize");
    }

    // Event updates arrive here
    pub fn handle_event(&mut self,
                        _event_loop: &mut mio::EventLoop<ChainRepl>,
                        events: mio::EventSet) {
        self.sock_status.insert(events);
        trace!("LineConn::handle_event: {:?}; this time: {:?}; now: {:?}",
               self.socket.peer_addr(),
               events,
               self.sock_status);
    }
}



impl<T, P> LineConn<T, P>
    where P: Protocol,
          T: Reader<P::Recv> + Encoder<P::Send> + fmt::Debug
{
    pub fn process_rules<E: LineConnEvents>(&mut self,
                                                 event_loop: &mut mio::EventLoop<ChainRepl>,
                                                 events: &mut E)
                                                 -> bool {

        if self.sock_status.is_readable() {
            self.read();
            self.sock_status.remove(mio::EventSet::readable());
        }

        let changed = self.process_buffer(events);

        if self.sock_status.is_writable() {
            self.write();
            self.sock_status.remove(mio::EventSet::writable());
        }

        if !self.should_close() {
            self.reinitialize(event_loop)
        }
        changed
    }

    fn process_buffer<E: LineConnEvents>(&mut self, events: &mut E) -> bool {
        self.codec.process::<P, E>(self.token, events)
    }


    fn read(&mut self) {
        match self.socket.try_read_buf(&mut self.read_buf) {
            Ok(Some(0)) => {
                debug!("{:?}: EOF!", self.socket.peer_addr());
                self.read_eof = true
            }
            Ok(Some(n)) => {
                trace!("{:?}: Read {}bytes", self.socket.peer_addr(), n);
                self.codec.feed(&self.read_buf);
                self.read_buf.clear();
            }
            Ok(None) => {
                trace!("{:?}: Noop!", self.socket.peer_addr());
            }
            Err(e) => {
                error!("got an error trying to read; err={:?}", e);
                self.failed = true;
            }
        }
    }

    fn write(&mut self) {
        match self.socket.try_write(&mut self.write_buf) {
            Ok(Some(n)) => {
                trace!("{:?}: Wrote {} of {} in buffer",
                       self.socket.peer_addr(),
                       n,
                       self.write_buf.len());
                self.write_buf = self.write_buf[n..].to_vec().into();
                trace!("{:?}: Now {:?}b",
                       self.socket.peer_addr(),
                       self.write_buf.len());
            }
            Ok(None) => {
                trace!("Write unready");
            }
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

        event_loop.reregister(&self.socket, self.token, flags, mio::PollOpt::oneshot())
                  .expect("EventLoop#reinitialize")
    }



    pub fn should_close(&self) -> bool {
        trace!("Closed? failed: {:?}; read_eof:{:?}; write_empty: {:?}",
               self.failed,
               self.read_eof,
               self.write_buf.is_empty());
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
        SexpPeer {
            token: token,
            packets: sexp::Packetiser::new(),
        }
    }
}

impl<T: Serialize> Encoder<T> for SexpPeer {
    fn encode(&self, s: T) -> Vec<u8> {
        sexp::as_bytes(&s).expect("Encode json response").into()
    }
}

impl Reader<ReplicationMessage> for SexpPeer {
    fn new(token: mio::Token) -> SexpPeer {
        Self::fresh(token)
    }
    fn feed(&mut self, slice: &[u8]) {
        self.packets.feed(slice)
    }

    fn process<P: Protocol<Recv=ReplicationMessage>, E: LineConnEvents>(&mut self, token: mio::Token, events: &mut E) -> bool {
        let mut changed = false;
        // trace!("{:?}: Read buffer: {:?}", self.socket.peer_addr(), self.read_buf);
        while let Some(msg) = self.packets.take().expect("Pull packet") {
            match msg {
                ReplicationMessage { epoch, ts, msg: PeerMsg::HelloDownstream } => events.hello_downstream(token, ts, epoch),
                ReplicationMessage { epoch, ts, msg: PeerMsg::Prepare(seqno, op) } => events.operation(token, epoch, seqno, op.into()),
                ReplicationMessage { epoch, ts, msg: PeerMsg::CommitTo(seqno) } => events.commit(token, epoch, seqno),
            }
            changed = true;
        }

        changed
    }
}

impl Reader<Operation> for SexpPeer {
    fn new(token: mio::Token) -> SexpPeer {
        Self::fresh(token)
    }
    fn feed(&mut self, slice: &[u8]) {
        self.packets.feed(slice)
    }

    fn process<P: Protocol<Recv=Operation>, E: LineConnEvents>(&mut self, token: mio::Token, events: &mut E) -> bool {
        let mut changed = false;
        while let Some(msg) = self.packets.take().expect("Pull packet") {
            trace!("{:?}: Read buffer: {:?}", self.token, msg);
            match msg {
                ReplicationMessage { epoch, ts, msg: PeerMsg::HelloDownstream } => events.hello_downstream(token, ts, epoch),
                ReplicationMessage { epoch, ts, msg: PeerMsg::Prepare(seqno, op) } => events.operation(token, epoch, seqno, op.into()),
                ReplicationMessage { epoch, ts, msg: PeerMsg::CommitTo(seqno) } => events.commit(token, epoch, seqno),
            }
            changed = true;
        }

        changed
    }
}

impl Reader<OpResp> for SexpPeer {
    fn new(token: mio::Token) -> SexpPeer {
        Self::fresh(token)
    }
    fn feed(&mut self, slice: &[u8]) {
        self.packets.feed(slice)
    }

    fn process<P: Protocol<Recv=OpResp>, E: LineConnEvents>(&mut self, token: mio::Token, events: &mut E) -> bool {
        let mut changed = false;
        // trace!("{:?}: Read buffer: {:?}", self.socket.peer_addr(), self.read_buf);
        while let Some(msg) = self.packets.take().expect("Pull packet") {
            match msg {
                OpResp::Ok(epoch, seqno, data) => events.okay(epoch, seqno, data),
                OpResp::HelloIWant(ts, seqno) => events.hello_i_want(ts, seqno),
                OpResp::Err(epoch, seqno, data) => events.error(epoch, seqno, data),
            }
            changed = true;
        }

        changed
    }
}


impl Reader<ClientReq> for SexpPeer {
    fn new(token: mio::Token) -> SexpPeer {
        Self::fresh(token)
    }
    fn feed(&mut self, slice: &[u8]) {
        self.packets.feed(slice)
    }

    fn process<P: Protocol<Recv=ClientReq>, E: LineConnEvents>(&mut self, token: mio::Token, events: &mut E) -> bool {
        let mut changed = false;
        // trace!("{:?}: Read buffer: {:?}", self.socket.peer_addr(), self.read_buf);
        while let Some(msg) = self.packets.take().expect("Pull packet") {
            match msg {
                ClientReq::Publish(data)  => events.client_request(token, data.into()),
            }
            changed = true;
        }

        changed
    }
}

impl<P> LineConn<SexpPeer, P>
    where P: Protocol,
          SexpPeer: Reader<P::Recv> + Encoder<P::Send> + fmt::Debug
{
    pub fn peer(socket: TcpStream, token: mio::Token) -> LineConn<SexpPeer, P> {
        Self::new(socket, token, SexpPeer::fresh(token))
    }
}

#[derive(Debug)]
pub struct ClientProto;
impl Protocol for ClientProto {
    type Send = ClientResp;
    type Recv = ClientReq;
}
