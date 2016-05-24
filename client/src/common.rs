use std::net::{SocketAddr, TcpStream};
use std::io::{self,Read,Write};
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::thread;
use std::sync::mpsc;

use eventual::{Future,Complete, Stream, Sender, Async};
use sexp;
use crexp_client_proto::messages::Seqno;

use serde::{ser,de};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            description(err.description())
            cause(err)
        }
        Sexp(err: sexp::Error) {
            from()
            description(err.description())
            cause(err)
        }
        Server(seq: Seqno, desc: String) {
            description("server error")
        }
        ThreadDeath {
            description("I/O thread died")
        }
        Disconnected {
            description("Lost connection")
        }
    }
}

pub struct SexpChannel<S, R> {
    stream: TcpStream,
    packets: sexp::Packetiser,
    _send: PhantomData<S>,
    _recv: PhantomData<R>,
}

impl<S: ser::Serialize + 'static + fmt::Debug, R: de::Deserialize + 'static + fmt::Debug> SexpChannel<S, R> {
    pub fn new(stream: TcpStream) -> SexpChannel<S, R> {
        SexpChannel {
            stream: stream,
            packets: sexp::Packetiser::new(),
            _send: PhantomData, _recv: PhantomData,
        }
    }

    pub fn send(&mut self, data: S) -> Result<(), Error> {
        try!(sexp::to_writer(&mut self.stream, &data));
        Ok(())
    }

    pub fn recv(&mut self) -> Result<R, Error> {
        let mut buf = vec![0; 4096];
        loop {
            if let Some(msg) = try!(self.packets.take()) {
                return Ok(msg)
            }
            let nread = try!(self.stream.read(&mut buf));
            if nread > 0 {
                trace!("Read {:?} bytes", nread);
                self.packets.feed(&buf[..nread]);
            } else {
                warn!("Server closed socket EOF");
                return Err(Error::Disconnected)
            }
        }
    }
}


