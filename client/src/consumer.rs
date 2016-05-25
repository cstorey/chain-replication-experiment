
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

use crexp_client_proto::messages::{ConsumerReq, ConsumerResp, Seqno};
use common::{Error,SexpChannel};

pub struct Consumer {
    tx: mpsc::Sender<Sender<(Seqno, Vec<u8>), Error>>,
    chan: SexpChannel<ConsumerReq, ConsumerResp>,
}

struct ConsumerInner {
    rx: mpsc::Receiver<Sender<(Seqno, Vec<u8>), Error>>,
    chan: SexpChannel<ConsumerReq, ConsumerResp>,
}

impl ConsumerInner {
    fn run(mut self) { 
        let mut sender = self.rx.recv().expect("sender");
        loop {
            debug!("Wait for message from server");
            let msg = match self.chan.recv() {
                Ok(resp) => resp,
                Err(err) => {
                    error!("Failed to receive with {:?}", err);
                    sender.fail(err);
                    break;
                }
            };
            debug!("Response: {:?}", msg);
            let next = match msg {
                ConsumerResp::Ok(seq, val) => sender.send((seq, val.into())),
                // ConsumerResp::Err(seq, msg) => sender.fail(Error::Server(seq, msg)),
            };
            debug!("Get next sender");
            sender = next.await().expect("sender");
        }
    }
}

impl Consumer {
    pub fn new(host: SocketAddr) -> Result<Consumer, Error> {
        let stream = try!(TcpStream::connect(host));

        let (tx, rx) = mpsc::channel();

        let inner = ConsumerInner {
            rx: rx,
            chan: SexpChannel::new(try!(stream.try_clone())),
        };

        let thread = try!(thread::Builder::new()
                .name(format!("cons:{}", host))
                .spawn(move || inner.run()));

        Ok(Consumer {
            tx: tx,
            chan: SexpChannel::new(stream),
        })
    }

    pub fn subscribe(&mut self) -> Stream<(Seqno, Vec<u8>), Error> {
        let (sender, stream) = Stream::pair();
        let req = ConsumerReq::ConsumeFrom(Seqno::zero());
        match self.chan.send(req) {
            Ok(()) => (),
            Err(err) => {
                error!("Failed to send with {:?}", err);
                sender.fail(err);
                return stream;
            }
        }
        if let Err(mpsc::SendError(sender)) = self.tx.send(sender) {
            sender.fail(Error::ThreadDeath);
        }

        stream
    }
}
