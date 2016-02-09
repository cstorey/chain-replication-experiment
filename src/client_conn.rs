use mio;
use mio::tcp::*;
use mio::{TryRead,TryWrite};

use super::{ChainRepl, ChainReplMsg,OpResp, Operation};

#[derive(Debug)]
pub struct ClientConn {
    socket: TcpStream,
    sock_status: mio::EventSet,
    token: mio::Token,
    read_buf: Vec<u8>,
    read_eof: bool,
    failed: bool,
    write_buf: Vec<u8>,
}

impl ClientConn {
    pub fn new(socket: TcpStream, token: mio::Token) -> ClientConn {
        trace!("New client connection {:?} from {:?}", token, socket.local_addr());
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
        let mut prev = 0;
        let mut changed = false;
        trace!("{:?}: Read buffer: {:?}", self.socket.peer_addr(), self.read_buf);
        for n in self.read_buf.iter().enumerate()
                .filter_map(|(i, e)| if *e == '\n' as u8 { Some(i) } else { None } ) {
            trace!("{:?}: Pos: {:?}-{:?}; chunk: {:?}", self.socket.peer_addr(), prev, n, &self.read_buf[prev..n]);
            let slice = &self.read_buf[prev..n];
            self.process_operation(self.token, slice, to_parent);
            changed = true;

            prev = n+1;
        }

        let remainder = self.read_buf[prev..].to_vec();
        trace!("{:?}: read Remainder: {}", self.socket.peer_addr(), remainder.len());
        self.read_buf = remainder;
        changed
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



    pub fn is_closed(&self) -> bool {
        self.failed || (self.read_eof && self.write_buf.is_empty())
    }

    // Per item to output
    pub fn response(&mut self, s: OpResp) {
        debug!("{:?}: Response: {:?}", self.token, s);
        let chunk = self.encode_response(s);
        self.write_buf.extend(chunk);
        self.write_buf.push('\n' as u8)
    }

    fn encode_response(&self, s: OpResp) -> Vec<u8> {
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
