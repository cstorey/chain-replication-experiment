
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

use crexp_client_proto::messages::{ProducerReq,ProducerResp, Seqno};
use common::{Error,SexpChannel};

pub struct Producer {
    thread: thread::JoinHandle<()>,
    tx: mpsc::Sender<Complete<Seqno, Error>>,
    chan: SexpChannel<ProducerReq, ProducerResp>,
}

struct ProducerInner {
    rx: mpsc::Receiver<Complete<Seqno, Error>>,
    chan: SexpChannel<ProducerReq, ProducerResp>,
}

impl Producer {
    pub fn new(host: SocketAddr) -> Result<Producer, Error> {
        let stream = try!(TcpStream::connect(host));
        let (tx, rx) = mpsc::channel();

        let inner = ProducerInner {
            rx: rx,
            chan: SexpChannel::new(try!(stream.try_clone())),
        };

        let thread = try!(thread::Builder::new()
                .name(format!("prod:{}", host))
                .spawn(move || inner.run()));
        Ok(Producer {
            chan: SexpChannel::new(stream),
            tx: tx,
            thread: thread,

        })
    }

    pub fn publish(&mut self, data: &str) -> Future<Seqno, Error> {
        let req = ProducerReq::Publish(data.as_bytes().to_vec().into());
        let (completer, future) = Future::pair();
        debug!("Sending: {:?}", req);
        match self.chan.send(req) {
            Ok(()) => (),
            Err(err) => {
                error!("Failed to send with {:?}", err);
                completer.fail(err);
                return future;
            }
        }
        debug!("Sending completer to reader: {:?}", completer);
        if let Err(mpsc::SendError(completer)) = self.tx.send(completer) {
            completer.fail(Error::ThreadDeath)
        }
        future
    }
}

impl ProducerInner {
    fn run(mut self) {
        debug!("Running producer inner loop");
        for completer in self.rx {
            debug!("waiting for completer: {:?}", completer);
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
                ProducerResp::Ok(seq) => completer.complete(seq),
                ProducerResp::Err(seq, msg) => completer.fail(Error::Server(seq, msg)),
            };
        };
        debug!("Producer inner done");
    }
}


