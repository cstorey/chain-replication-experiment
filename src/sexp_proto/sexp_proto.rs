use tokio::io::{EasyBuf, Io, Codec, Framed};
use proto::pipeline::{ServerProto, ClientProto};
use spki_sexp as sexp;
use serde::{ser, de};
use std::io;

use std::marker::PhantomData;

#[derive(Debug)]
pub struct SexpCodec<S, D> {
    _x: PhantomData<(S, D)>,
    packets: sexp::Packetiser,
}

pub struct SexpProto<S: ser::Serialize + 'static, D: de::Deserialize + 'static>(PhantomData<(S,
                                                                                             D)>);
impl<S, D> SexpCodec<S, D> {
    pub fn new() -> Self {
        SexpCodec {
            _x: PhantomData,
            packets: sexp::Packetiser::new(),
        }
    }
}

// impl< E: error::Error> Serialize for SexpSerializer<T, E> {

impl<S: ser::Serialize, D: de::Deserialize> Codec for SexpCodec<S, D> {
    type In = D;
    type Out = S;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        self.packets.feed(&buf.as_slice());
        let len = buf.len();
        buf.drain_to(len);
        debug!("Buffer now:{:?}", buf.len());

        debug!("Packets now:{:?}", self.packets);
        match self.packets.take() {
            Ok(Some(msg)) => Ok(Some(msg)),
            Ok(None) => Ok(None),
            Err(e) => {
                error!("Transport error:{:?}", e);
                Err(io::Error::new(io::ErrorKind::Other, e.to_string()))
            }
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> Result<(), io::Error> {
        buf.extend_from_slice(&sexp::as_bytes(&msg).expect("as_bytes"));
        Ok(())
    }
}

impl<S: ser::Serialize + 'static, D: de::Deserialize + 'static> SexpProto<S, D> {
    pub fn new() -> Self {
        SexpProto(PhantomData)
    }
}
impl<T: Io + 'static, S: ser::Serialize + 'static, D: de::Deserialize + 'static> ClientProto<T> for SexpProto<S, D> {
    type Request = S;
    type Response = D;
    type Transport = Framed<T, SexpCodec<S, D>>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(SexpCodec::new()))
    }
}

impl<T: Io + 'static, S: ser::Serialize + 'static, D: de::Deserialize + 'static> ServerProto<T> for SexpProto<S, D> {
    type Request = D;
    type Response = S;
    type Transport = Framed<T, SexpCodec<S, D>>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(SexpCodec::new()))
    }
}
