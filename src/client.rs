use futures::{self, Async, Future, Poll};
use std::io;
use std::net::SocketAddr;
use service::Service;
use proto::{self, pipeline};
use tokio::reactor::Handle;
use tokio::net::TcpStream;
use tokio::io::FramedIo;
use futures::stream;
use sexp_proto::sexp_proto_new;
use serde;
use errors::Error;

use super::Empty;

use std::marker::PhantomData;

pub struct Client<T, R> {
    inner: proto::Client<T, R, stream::Empty<Empty, Error>, Error>,
}
impl<T: Send + 'static, R: Send + 'static> Service for Client<T, R> {
    type Request = T;
    type Response = R;
    type Error = Error;
    type Future = Box<Future<Item = Self::Response, Error = Error> + Send>;

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
    fn call(&self, req: T) -> Self::Future {
        self.inner.call(proto::Message::WithoutBody(req))
    }
}

pub fn connect<T, R>(handle: Handle, addr: &SocketAddr) -> Client<T, R>
    where T: Send + 'static + serde::ser::Serialize,
          R: Send + 'static + serde::de::Deserialize
{
    let addr = addr.clone();
    let h = handle.clone();

    let builder = move || TcpStream::connect(&addr, &h).map(|s| sexp_proto_new(s));

    let client = pipeline::connect(builder, &handle);
    Client { inner: client }
}
