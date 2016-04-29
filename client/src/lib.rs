extern crate crexp_client_proto;
extern crate spki_sexp as sexp;
extern crate serde;
#[macro_use]
extern crate quick_error;

use std::net::{SocketAddr, TcpStream};
use std::io::{self,Read,Write};
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::fmt;

use serde::{ser,de};

use crexp_client_proto::messages::{ClientReq,ClientResp, Seqno};

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
    }
}

struct SexpChannel<S, R> {
    stream: TcpStream,
    packets: sexp::Packetiser,
    _send: PhantomData<S>,
    _recv: PhantomData<R>,
}

impl<S: ser::Serialize + 'static + fmt::Debug, R: de::Deserialize + 'static + fmt::Debug> SexpChannel<S, R> {
    fn new(stream: TcpStream) -> SexpChannel<S, R> {
        SexpChannel {
            stream: stream,
            packets: sexp::Packetiser::new(),
            _send: PhantomData, _recv: PhantomData,
        }
    }

    fn send(&mut self, data: S) -> Result<(), Error> {
        try!(sexp::to_writer(&mut self.stream, &data));
        Ok(())
    }
 
    fn recv(&mut self) -> Result<R, Error> {
        let mut buf = vec![0; 4096];
        loop {
            let nread = try!(self.stream.read(&mut buf));
            self.packets.feed(&buf[..nread]);
            if let Some(msg) = try!(self.packets.take()) {
                return Ok(msg)
            }
        }
    }
}

pub struct Producer {
    chan: SexpChannel<ClientReq, ClientResp>,
}



impl Producer {
    pub fn new(host: SocketAddr) -> Result<Producer, Error> {
        let stream = try!(TcpStream::connect(host));
        Ok(Producer {
            chan: SexpChannel::new(stream),
        })
    }

    pub fn publish(&mut self, data: &str) -> Result<Seqno, Error> {
        let req = ClientReq::Publish(data.as_bytes().to_vec().into());
        try!(self.chan.send(req));
        let resp = try!(self.chan.recv());
        match resp {
            ClientResp::Ok(seq) => Ok(seq),
            ClientResp::Err(seq, msg) => Err(Error::Server(seq, msg)),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn it_works() {
    }
}
