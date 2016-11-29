use proto::{self, pipeline, server, Body};
use proto::server::ServerHandle;
use service::{Service, NewService};
use tokio::reactor::Handle;
use tokio::io::FramedIo;
use futures::{Async, Poll, Future};
use futures::stream;
use spki_sexp as sexp;
use std::io;
use std::error;
use std::net::SocketAddr;

use super::sexp_proto::sexp_proto_new;
use void::Void;
use serde;
use Error;

struct SexpService<T> {
    inner: T,
}

struct SexpFuture<F>(F);

impl<T> Service for SexpService<T>
    where T: Service,
          T::Future: Send + 'static
{
    type Request = proto::Message<T::Request, Body<Void, T::Error>>;
    type Response = proto::Message<T::Response, stream::Empty<Void, T::Error>>;
    type Error = T::Error;
    // type Future = Box<Future<Item = Self::Response, Error = Error> + Send + 'static>;
    type Future = SexpFuture<T::Future>;

    fn call(&self, req: Self::Request) -> Self::Future {
        SexpFuture(self.inner.call(req.into_inner()))
    }
}

impl<F: Future> Future for SexpFuture<F> {
    type Item = proto::Message<F::Item, stream::Empty<Void, F::Error>>;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let ret = try_ready!(self.0.poll());
        Ok(Async::Ready(proto::Message::WithoutBody(ret)))
    }
}

pub fn serve<T>(handle: &Handle, addr: SocketAddr, new_service: T) -> io::Result<ServerHandle>
    where T: NewService + Send + 'static,
          <<T as NewService>::Instance as Service>::Future: Send,
          <T as NewService>::Error: Send + error::Error + From<proto::Error<T::Error>> + From<sexp::Error>,
          T::Request: serde::Deserialize + serde::Serialize,
          T::Response: serde::Deserialize + serde::Serialize
{
    let handle = try!(server::listen(handle, addr, move |stream| {
        // Initialize the pipeline dispatch with the service and the line
        // transport
        let transport = sexp_proto_new(stream);
        let _ : &FramedIo<In=pipeline::Frame<T::Response, Void, Error>, Out=pipeline::Frame<T::Request, Void, Error>> = &transport;
        let _ : &pipeline::Transport<In = T::Response, Out=T::Request, BodyIn=Void, BodyOut=Void, Error=Error> = &transport;
        debug!("Accept connection: {:?}", stream);
        let service = SexpService { inner: try!(new_service.new_service()) };
        pipeline::Server::new(service, transport)
    }));
    info!("Listening on {}", handle.local_addr());
    Ok(handle)
}
