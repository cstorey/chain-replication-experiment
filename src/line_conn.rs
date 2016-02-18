use mio;
use mio::tcp::*;
use mio::{TryRead,TryWrite};
use serde_json as json;
use std::fmt;
use std::collections::VecDeque;

use super::{ChainRepl, ChainReplMsg, OpResp, Operation, PeerMsg};

const NL : u8 = '\n' as u8;

trait Codec {
    fn encode_response(&self, s: OpResp) -> Vec<u8>;
    fn feed(&mut self, slice: &[u8]);
    fn take(&mut self) -> Option<ChainReplMsg>;
}

#[derive(Debug)]
pub struct LineConn<T> {
    socket: TcpStream,
    sock_status: mio::EventSet,
    token: mio::Token,
    read_eof: bool,
    failed: bool,
    write_buf: Vec<u8>,
    codec: T,
}

impl<T: Codec + fmt::Debug> LineConn<T> {
    fn new(socket: TcpStream, token: mio::Token, codec: T) -> LineConn<T> {
        trace!("New client connection {:?} from {:?}", token, socket.local_addr());
        LineConn {
            socket: socket,
            sock_status: mio::EventSet::none(),
            token: token,
            write_buf: Vec::new(),
            read_eof: false,
            failed: false,
            codec: codec,
        }
    }

    pub fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        event_loop.register_opt(
                &self.socket,
                token,
                mio::EventSet::readable(),
                mio::PollOpt::edge() | mio::PollOpt::oneshot())
            .expect("event loop initialize");
    }

    // Event updates arrive here
    pub fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl>, events: mio::EventSet) {
        self.sock_status.insert(events);
        trace!("LineConn::handle_event: {:?}; this time: {:?}; now: {:?}",
                self.socket.peer_addr(), events, self.sock_status);
    }

    // actions are processed here on down.

    pub fn process_rules<F: FnMut(ChainReplMsg)>(&mut self, event_loop: &mut mio::EventLoop<ChainRepl>,
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

    fn process_buffer<F: FnMut(ChainReplMsg)>(&mut self, to_parent: &mut F) -> bool {
        let mut changed = false;
        // trace!("{:?}: Read buffer: {:?}", self.socket.peer_addr(), self.read_buf);
        while let Some(cmd) = self.codec.take() {
            debug!("Read message {:?}", cmd);
            to_parent(cmd);

            changed = true;
        }

        changed
    }


    fn read(&mut self) {
        let mut abuf = Vec::new();
        match self.socket.try_read_buf(&mut abuf) {
            Ok(Some(0)) => {
                debug!("{:?}: EOF!", self.socket.peer_addr());
                self.read_eof = true
            },
            Ok(Some(n)) => {
                trace!("{:?}: Read {}bytes", self.socket.peer_addr(), n);
                self.codec.feed(&abuf);
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



    pub fn is_closed(&self) -> bool {
        trace!("Closed? failed: {:?}; read_eof:{:?}; write_empty: {:?}", self.failed, self.read_eof, self.write_buf.is_empty());
        self.failed || (self.read_eof && self.write_buf.is_empty())
    }

    // Per item to output
    pub fn response(&mut self, s: OpResp) {
        debug!("{:?}: Response: {:?}", self.token, s);
        let chunk = self.codec.encode_response(s);
        self.write_buf.extend(chunk);
        self.write_buf.push('\n' as u8)
    }

}

#[derive(Debug)]
pub struct JsonPeer {
    token: mio::Token,
    buf: VecDeque<u8>,
}

impl Codec for JsonPeer {
    fn encode_response(&self, s: OpResp) -> Vec<u8> {
        json::to_string(&s).expect("Encode json response").as_bytes().to_vec()
    }

    fn feed(&mut self, slice: &[u8]) {
        self.buf.extend(slice);
    }
    fn take(&mut self) -> Option<ChainReplMsg> {
        if let Some(idx) = self.buf.iter().enumerate()
                .filter_map(|(i, &c)| if c == NL { Some(i) } else { None })
                .next() {
            let slice = self.buf.drain(..idx).collect::<Vec<_>>();
            let (epoch, seqno, op) : PeerMsg = json::from_str(&String::from_utf8_lossy(&slice)).expect("Decode peer operation");
            let ret = ChainReplMsg::Operation { source: self.token, epoch: Some(epoch), seqno: Some(seqno), op: op };
            Some(ret)
        } else {
            None
        }
    }
}

impl LineConn<JsonPeer> {
    pub fn upstream(socket: TcpStream, token: mio::Token) -> LineConn<JsonPeer> {
        Self::new(socket, token, JsonPeer { token: token, buf: VecDeque::new() })
    }
}

#[derive(Debug)]
pub struct PlainClient {
    token: mio::Token,
    buf: VecDeque<u8>
}

impl Codec for PlainClient {
    fn encode_response(&self, s: OpResp) -> Vec<u8> {
        format!("{:?}", s).as_bytes().to_vec()
    }

    fn feed(&mut self, slice: &[u8]) {
        self.buf.extend(slice);
    }
    fn take(&mut self) -> Option<ChainReplMsg> {
        if let Some(idx) = self.buf.iter().enumerate().filter_map(|(i, &c)| if c == NL { Some(i) } else { None }).next() {
            let slice = self.buf.drain(..idx).collect::<Vec<_>>();
            let op = if slice.is_empty() {
                Operation::Get
            } else {
                Operation::Set(String::from_utf8_lossy(&slice).to_string())
            };
            let ret = ChainReplMsg::Operation { source: self.token, epoch: None, seqno: None, op: op };
            Some(ret)
        } else {
            None
        }
    }
}


impl LineConn<PlainClient> {
    pub fn client(socket: TcpStream, token: mio::Token) -> LineConn<PlainClient> {
        Self::new(socket, token, PlainClient { token: token, buf: VecDeque::new() } )
    }
}
