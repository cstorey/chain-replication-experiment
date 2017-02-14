use futures::Future;
use std::net::SocketAddr;
use service::Service;
use proto::TcpClient;
use proto::pipeline::ClientService;
use tokio::reactor::Handle;
use tokio::net::TcpStream;
use serde;
use serde::{ser, de};

use super::sexp_proto::SexpProto;

use std::fmt;
use std::io;

pub struct Client<Req: ser::Serialize + 'static, Res: de::Deserialize + 'static> {
    inner: ClientService<TcpStream, SexpProto<Req, Res>>,
}

impl<Req: ser::Serialize + Send + 'static, Res: de::Deserialize + Send + 'static> Service
    for Client<Req, Res> {
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Future = Box<Future<Item = Self::Response, Error = io::Error> + Send>;

    fn call(&self, req: Req) -> Self::Future {
        self.inner.call(req).boxed()
    }
}

impl<Req: ser::Serialize + 'static, Res: de::Deserialize + 'static> fmt::Debug
    for Client<Req, Res> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("Client").finish()
    }
}

pub fn connect<Req, Res>(handle: Handle,
                         addr: &SocketAddr)
                         -> Box<Future<Item = Client<Req, Res>, Error = io::Error>>
    where Req: Send + 'static + serde::ser::Serialize,
          Res: Send + 'static + serde::de::Deserialize
{
    let addr = addr.clone();
    let h = handle.clone();

    let client = TcpClient::new(SexpProto::new())
        .connect(&addr, &h)
        .map(|client_service| Client { inner: client_service });
    Box::new(client)
}

// pub fn connect_with<B, F, T, Req: ser::Serialize + 'static, Res: de::Deserialize + 'static>(builder: B, handle: Handle) -> Client<Req, Res>
// where B: Fn() -> F,
// F: IntoFuture<Item = T, Error = io::Error>,
// F::Future: Send + 'static,
// T: FramedIo<In = Req, Out = Res> + Send + 'static,
// Req: Send + 'static,
// Res: Send + 'static
// {
// let client = builder();
// Client { inner: client }
// }
//
