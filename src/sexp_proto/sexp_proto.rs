use bytes::{self, MutBuf};
use bytes::buf::BlockBuf;
use proto::{pipeline, Parse, Serialize, Framed};
use tokio::io::Io;
use spki_sexp as sexp;
use serde::{ser, de};
use std::fmt::Write;
use std::error;
use void::Void;

use std::marker::PhantomData;

#[derive(Debug)]
pub struct SexpParser<T, E> {
    _x: PhantomData<fn() -> Result<T, E>>,
    packets: sexp::Packetiser,
}

pub type Frame<T, E> = pipeline::Frame<T, Void, E>;

impl<T, E> SexpParser<T, E> {
    pub fn new() -> Self {
        SexpParser {
            _x: PhantomData,
            packets: sexp::Packetiser::new(),
        }
    }
}

impl<T: de::Deserialize, E: From<sexp::Error>> Parse for SexpParser<T, E> {
    type Out = Frame<T, E>;

    fn parse(&mut self, buf: &mut BlockBuf) -> Option<Self::Out> {
        use proto::pipeline::Frame;

        if !buf.is_compact() {
            buf.compact();
        }
        self.packets.feed(&buf.bytes().expect("compacted buffer"));
        let len = buf.len();
        buf.shift(len);
        debug!("Buffer now:{:?}", buf.len());

        debug!("Packets now:{:?}", self.packets);
        match self.packets.take() {
            Ok(Some(msg)) => Some(Frame::Message(msg)),
            Ok(None) => None,
            Err(e) => {
                error!("Transport error:{:?}", e);
                Some(Frame::Error(e.into()))
            }
        }
    }
}


pub struct SexpSerializer<T, E>(PhantomData<(T, E)>);

impl<T: ser::Serialize, E: error::Error> Serialize for SexpSerializer<T, E> {
    type In = Frame<T, E>;
    fn serialize(&mut self, frame: Self::In, buf: &mut BlockBuf) {
        use proto::pipeline::Frame;
        match frame {
            Frame::Message(val) => buf.write_slice(&sexp::as_bytes(&val).expect("serialize")),
            Frame::Error(e) => {
                warn!("Error handling in serializer:{:?}", e);
                let _ = write!(bytes::buf::Fmt(buf), "[ERROR] {}\n", e);
            }
            Frame::Done => {}
            Frame::MessageWithBody(_, _) |
            Frame::Body(_) => unreachable!(),
        }
    }
}

pub type FramedSexpTransport<T, X, Y, E> = Framed<T, SexpParser<X, E>, SexpSerializer<Y, E>>;

pub fn sexp_proto_new<T: Io,
                      X: de::Deserialize,
                      Y: ser::Serialize,
                      E: From<sexp::Error> + error::Error>
    (inner: T)
     -> FramedSexpTransport<T, X, Y, E> {
    Framed::new(inner,
                SexpParser::new(),
                SexpSerializer(PhantomData),
                BlockBuf::default(),
                BlockBuf::default())

}
