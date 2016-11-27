use futures::{Async, IntoFuture, Future};
use std::net::SocketAddr;
use service::Service;
use proto::{self, pipeline, Body};
use proto::pipeline::Frame;
use tokio::reactor::Handle;
use tokio::io::FramedIo;
use tokio::net::TcpStream;
use futures::stream;
use serde;

use super::sexp_proto::sexp_proto_new;
use super::errors::Error;
use void::Void;

use std::fmt;
use std::io;

pub struct Client<Req, Res> {
    inner: proto::Client<Req, Res, stream::Empty<Void, Error>, Body<Void, Error>, Error>,
}

impl<Req: Send + 'static, Res: Send + 'static> Service for Client<Req, Res> {
    type Request = Req;
    type Response = Res;
    type Error = Error;
    type Future = Box<Future<Item = Self::Response, Error = Error> + Send>;

    fn call(&self, req: Req) -> Self::Future {
        self.inner.call(proto::Message::WithoutBody(req))
    }
}

impl<Req, Res> fmt::Debug for Client<Req, Res> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("Client").finish()
    }
}

pub fn connect<Req, Res>(handle: Handle, addr: &SocketAddr) -> Client<Req, Res>
    where Req: Send + 'static + serde::ser::Serialize,
          Res: Send + 'static + serde::de::Deserialize
{
    let addr = addr.clone();
    let h = handle.clone();

    let builder = move || TcpStream::connect(&addr, &h).map(|s| sexp_proto_new(s));
    connect_with(builder, handle)
}

pub fn connect_with<B, F, T, Req, Res>(builder: B, handle: Handle) -> Client<Req, Res>
    where B: Fn() -> F,
          F: IntoFuture<Item = T, Error = io::Error>,
          F::Future: Send + 'static,
          T: FramedIo<In = Frame<Req, Void, Error>, Out = Frame<Res, Void, Error>> + Send + 'static,
          Req: Send + 'static,
          Res: Send + 'static
{
    let client = pipeline::connect(builder, &handle);
    Client { inner: client }
}
