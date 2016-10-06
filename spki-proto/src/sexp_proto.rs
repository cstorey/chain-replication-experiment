use bytes::{self, MutBuf};
use bytes::buf::BlockBuf;
use std::io;
use proto::{pipeline, Parse, Serialize, Framed};
use tokio::io::Io;
use spki_sexp as sexp;
use serde::{ser, de};
use std::fmt::Write;

use std::marker::PhantomData;

use errors::Error;
use super::Empty;

#[derive(Debug)]
pub struct SexpParser<T> {
    _x: PhantomData<fn() -> (T)>,
    packets: sexp::Packetiser,
}

pub type Frame<T> = pipeline::Frame<T, Empty, Error>;

impl<T> SexpParser<T> {
    pub fn new() -> Self {
        SexpParser {
            _x: PhantomData,
            packets: sexp::Packetiser::new(),
        }
    }
}

impl<T: de::Deserialize> Parse for SexpParser<T> {
    type Out = Frame<T>;

    fn parse(&mut self, buf: &mut BlockBuf) -> Option<Frame<T>> {
        use proto::pipeline::Frame;

        if !buf.is_compact() {
            buf.compact();
        }
        self.packets.feed(&buf.bytes().expect("compacted buffer"));
        let len = buf.len();
        buf.shift(len);
        debug!("Buffer now:{:?}", buf.len());

        match self.packets.take() {
            Ok(Some(msg)) => Some(Frame::Message(msg)),
            Ok(None) => None,
            Err(e) => {
                error!("Transport error:{:?}", e);
                Some(Frame::Error(e.into()))
            },
        }
    }
}


pub struct SexpSerializer<T>(PhantomData<T>);

impl<T: ser::Serialize> Serialize for SexpSerializer<T> {
    type In = Frame<T>;
    fn serialize(&mut self, frame: Frame<T>, buf: &mut BlockBuf) {
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

pub type FramedSexpTransport<T, X, Y> = Framed<T, SexpParser<X>, SexpSerializer<Y>>;

pub fn sexp_proto_new<T: Io, X: de::Deserialize, Y: ser::Serialize>
    (inner: T)
     -> FramedSexpTransport<T, X, Y> {
    Framed::new(inner,
                SexpParser::new(),
                SexpSerializer(PhantomData),
                BlockBuf::default(),
                BlockBuf::default())

}
