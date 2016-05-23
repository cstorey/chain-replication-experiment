extern crate crexp_client_proto;
extern crate spki_sexp as sexp;
extern crate serde;
extern crate eventual;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;

use std::net::{SocketAddr, TcpStream};
use std::io::{self,Read,Write};
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::thread;
use std::sync::mpsc;

use eventual::{Future,Complete};

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
        ThreadDeath {
            description("I/O thread died")
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
    thread: thread::JoinHandle<()>,
    chan: mpsc::Sender<(ClientReq, Complete<Seqno, Error>)>,
}

struct ProducerInner {
    recv: mpsc::Receiver<(ClientReq, Complete<Seqno, Error>)>,
    chan: SexpChannel<ClientReq, ClientResp>,
}

impl Producer {
    pub fn new(host: SocketAddr) -> Result<Producer, Error> {
        let stream = try!(TcpStream::connect(host));
        let (tx, rx) = mpsc::channel();

        let inner = ProducerInner {
            recv: rx,
            chan: SexpChannel::new(stream),
        };

        let thread = try!(thread::Builder::new()
                .name(format!("prod:{}", host))
                .spawn(move || inner.run()));
        Ok(Producer {
            chan: tx,
            thread: thread,
        })
    }

    pub fn publish(&mut self, data: &str) -> Future<Seqno, Error> {
        let req = ClientReq::Publish(data.as_bytes().to_vec().into());

        let (completer, future) = Future::pair();
        if let Err(mpsc::SendError((_req, completer))) = self.chan.send((req, completer)) {
            completer.fail(Error::ThreadDeath)
        }
        future
    }
}

impl ProducerInner {
    fn run(mut self) {
        debug!("Running producer inner loop");
        for (req, completer) in self.recv {
            debug!("Sending: {:?}", req);
            match self.chan.send(req) {
                Ok(()) => (),
                Err(err) => {
                    error!("Failed to send with {:?}", err);
                    completer.fail(err);
                    break;
                }
            };

            let resp = match self.chan.recv() {
                Ok(resp) => resp,
                Err(err) => {
                    error!("Failed to receive with {:?}", err);
                    completer.fail(err);
                    break;
                }
            };
            debug!("Response: {:?}", resp);
            match resp {
                ClientResp::Ok(seq) => completer.complete(seq),
                ClientResp::Err(seq, msg) => completer.fail(Error::Server(seq, msg)),
            };
        };
        debug!("Producer inner done");
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn it_works() {
    }
}
