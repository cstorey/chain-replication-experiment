extern crate gj;
extern crate gjio;
extern crate crexp_client_proto;
extern crate spki_sexp as sexp;
extern crate serde;
#[macro_use]
extern crate quick_error;

use gj::{EventLoop, Promise};
use gjio::{AsyncRead, AsyncWrite, BufferPrefix, SocketStream, EventPort};
use std::net::SocketAddr;
use std::io;
use std::io::Write;
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
    stream: Rc<RefCell<SocketStream>>,
    packets: Rc<RefCell<sexp::Packetiser>>,
    _send: PhantomData<S>,
    _recv: PhantomData<R>,
}

impl<S: ser::Serialize + 'static + fmt::Debug, R: de::Deserialize + 'static + fmt::Debug> SexpChannel<S, R> {
    fn new(stream: SocketStream) -> SexpChannel<S, R> {
        SexpChannel {
            stream: Rc::new(RefCell::new(stream)),
            packets: Rc::new(RefCell::new(sexp::Packetiser::new())),
            _send: PhantomData, _recv: PhantomData,
        }
    }

    fn send(&self, data: S) -> Promise<(), Error> {
        let stream = self.stream.clone();
        Promise::ok(data)
            .map(move |req| Ok(try!(sexp::as_bytes(&req))))
            .then(move |bytes| {
                let mut borr = stream.borrow_mut();
                borr.write(bytes).map(|_| Ok(())).map_err(From::from) 
            })
    }
    fn read_a_packet(stream: Rc<RefCell<SocketStream>>, packets: Rc<RefCell<sexp::Packetiser>>) -> Promise<R, Error> {
        println!("Read a packet: {:?}", packets);
        let buf = vec![0; 11];
        let read = {
            let mut borrowed = stream.borrow_mut();
            borrowed.try_read(buf, 1)
        };
        read.map_err(From::from) 
            .then(move |(buf, nbytes)| {
                println!("Read bytes: {:?}", &buf[..nbytes]);
                if nbytes == 0 {
                    return Promise::err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF").into())
                }
                let read = {
                    let mut borrowed = packets.borrow_mut();
                    borrowed.feed(&buf[..nbytes]);
                    borrowed.take()
                };
                println!("Take a packet: {:?}", read);
                match read {
                    Ok(Some(p)) => Promise::ok(p),
                    Ok(None) => Self::read_a_packet(stream, packets),
                    Err(e) => Promise::err(e.into()),
                }
            })
    }
    fn recv(&self) -> Promise<R, Error> {
        let stream = self.stream.clone();
        let packets = self.packets.clone();
        Promise::ok(())
            .then(move |()| {
                Self::read_a_packet(stream, packets)
            })
    }
}

pub struct Producer {
    chan: Rc<RefCell<SexpChannel<ClientReq, ClientResp>>>,
}



impl Producer {
    pub fn new(host: SocketAddr, event_port: &mut EventPort) -> Promise<Producer, Error> {
        let network = event_port.get_network();
        let mut address = network.get_tcp_address(host);
        address.connect().map(move |mut stream|
            Ok(Producer {
                chan: Rc::new(RefCell::new(SexpChannel::new(stream))),
            })
        ).map_err(From::from)
    }

    pub fn publish(&mut self, data: &str) -> Promise<Seqno, Error> {
        let chan = self.chan.clone();
        Promise::ok(ClientReq::Publish(data.as_bytes().to_vec().into()))
            .then(move |req| {
                    let p = {
                        let mut borr = chan.borrow_mut();
                        borr.send(req).map_err(From::from) 
                    };
                    p.map(|()| Ok(chan))
                })
            .then(move |chan| {
                let mut borr = chan.borrow_mut();
                borr.recv()
            }).map(move |val| {
                match val {
                    ClientResp::Ok(seq, _) => Ok(seq),
                    ClientResp::Err(seq, msg) => Err(Error::Server(seq, msg)),
                }
            })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn it_works() {
    }
}
