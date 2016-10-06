use proto::{self, pipeline, server};
use proto::server::ServerHandle;
use service::{Service, NewService};
use tokio::reactor::Handle;
use futures::{Async, Future};
use futures::stream;
use std::io;
use std::net::SocketAddr;
use sexp_proto::sexp_proto_new;

use errors::Error;
use super::Empty;

struct SexpService<T> {
    inner: T,
}

impl<T> Service for SexpService<T>
    where T: Service<Request = String, Response = String, Error = Error>,
          T::Future: Send + 'static
{
    type Request = String;
    type Response = proto::Message<String, stream::Empty<Empty, Error>>;
    type Error = Error;
    type Future = Box<Future<Item = Self::Response, Error = Error> + Send>;

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("Got request:{:?}", req);
        self.inner
            .call(req)
            .map(|r| {
                debug!("Respond with:{:?}", r);
                r
            })
            .map(proto::Message::WithoutBody)
            .boxed()
    }
}

pub fn serve<T>(handle: &Handle, addr: SocketAddr, new_service: T) -> io::Result<ServerHandle>
    where T: NewService<Request = String, Response = String, Error = Error> + Send + 'static,
          <<T as NewService>::Item as Service>::Future: Send
{
    let handle = try!(server::listen(handle, addr, move |stream| {
        // Initialize the pipeline dispatch with the service and the line
        // transport
        debug!("Accept connection: {:?}", stream);
        let service = SexpService { inner: try!(new_service.new_service()) };
        pipeline::Server::new(service, sexp_proto_new(stream))
    }));
    info!("Listening on {}", handle.local_addr());
    Ok(handle)
}
