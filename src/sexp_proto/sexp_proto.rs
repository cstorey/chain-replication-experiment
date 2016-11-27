use bytes::{self, BufMut};
use proto::{pipeline};
use tokio::io::{EasyBuf,Io, Codec, Framed};
use spki_sexp as sexp;
use serde::{ser, de};
use std::fmt::{self,Write};
use std::error;
use std::io;
use void::Void;

use std::marker::PhantomData;

#[derive(Debug)]
pub struct SexpCodec<S, D, E> {
    _x: PhantomData<(S, D, E)>,
    packets: sexp::Packetiser,
}

pub type Frame<T, E> = pipeline::Frame<T, Void, E>;

impl<S, D, E> SexpCodec<S, D, E> {
    pub fn new() -> Self {
        SexpCodec {
            _x: PhantomData,
            packets: sexp::Packetiser::new(),
        }
    }
}

// impl< E: error::Error> Serialize for SexpSerializer<T, E> {

impl<S: ser::Serialize, D: de::Deserialize, E: From<sexp::Error> + fmt::Debug> Codec for SexpCodec<S, D, E> {
    type Out = Frame<S, E>;
    type In = Frame<D, E>;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        use proto::pipeline::Frame;

        self.packets.feed(&buf.as_slice());
        let len = buf.len();
        buf.drain_to(len);
        debug!("Buffer now:{:?}", buf.len());

        debug!("Packets now:{:?}", self.packets);
        match self.packets.take() {
            Ok(Some(msg)) => Ok(Some(Frame::Message { message: msg, body: false })),
            Ok(None) => Ok(None),
            Err(e) => {
                error!("Transport error:{:?}", e);
                Ok(Some(Frame::Error { error: e.into() }))
            }
        }
    }

    fn encode(&mut self, frame: Self::Out, buf: &mut Vec<u8>) -> Result<(), io::Error> {
        use proto::pipeline::Frame;
        match frame {
            Frame::Message { message: val, body: false } => buf.extend_from_slice(&sexp::as_bytes(&val).expect("serialize")),
            Frame::Error { error } => {
                warn!("Error handling in serializer:{:?}", error);
            }
            Frame::Done => {}
            Frame::Message { message: _, body: true } | Frame::Body { .. } => unreachable!(),
        };
        Ok(())
    }
}

pub type FramedSexpTransport<T, X, Y, E> = Framed<T, SexpCodec<Y, X, E>>;

pub fn sexp_proto_new<T: Io,
                      X: de::Deserialize,
                      Y: ser::Serialize,
                      E: From<sexp::Error> + error::Error>
    (inner: T)
     -> FramedSexpTransport<T, X, Y, E> {
    inner.framed(SexpCodec::new())

}
