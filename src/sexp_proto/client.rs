use futures::{Async, Future};
use std::net::SocketAddr;
use service::Service;
use proto::{self, pipeline};
use tokio::reactor::Handle;
use tokio::net::TcpStream;
use futures::stream;
use serde;

use super::sexp_proto::sexp_proto_new;
use super::errors::Error;
use void::Void;

use std::fmt;

pub struct Client<T, R> {
    inner: proto::Client<T, R, stream::Empty<Void, Error>, Error>,
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

impl<T, R> fmt::Debug for Client<T, R> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("Client").finish()
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
