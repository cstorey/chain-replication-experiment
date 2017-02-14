use service::{Service, NewService};
use tokio::reactor::Handle;
use tokio::net::TcpListener;
use proto::BindServer;
use futures::{Future, Stream};
use spki_sexp as sexp;
use std::io;
use std::error;
use std::net::SocketAddr;
use super::sexp_proto::SexpProto;

use serde;

struct SexpService<T> {
    inner: T,
}

// struct SexpFuture<F>(F);

impl<T> Service for SexpService<T>
    where T: Service,
          T::Future: Send + 'static,
          T::Error: Send + error::Error + 'static
{
    type Request = T::Request;
    type Response = T::Response;
    type Error = io::Error;
    type Future = Box<Future<Item = Self::Response, Error = io::Error> + Send + 'static>;
    // type Future = T::Future;

    fn call(&self, req: Self::Request) -> Self::Future {
        self.inner
            .call(req)
            .map_err(|e| {
                warn!("Caught error in service: {:?}", e);
                io::Error::new(io::ErrorKind::Other, "Service error")
            })
            .boxed()
    }
}

// impl<F: Future> Future for SexpFuture<F> {
// type Item = F::Item;
// type Error = F::Error;
//
// fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
// let ret = try_ready!(self.0.poll());
// Ok(Async::Ready(proto::Message::WithoutBody(ret)))
// }
// }

pub fn serve<T>(handle: &Handle, addr: SocketAddr, new_service: T) -> io::Result<SocketAddr>
    where T: NewService + Send + 'static,
          <<T as NewService>::Instance as Service>::Future: Send,
          <T as NewService>::Error: Send + error::Error + From<sexp::Error>,
          T::Request: serde::Deserialize + serde::Serialize,
          T::Response: serde::Deserialize + serde::Serialize
{
    let socket = try!(TcpListener::bind(&addr, handle));
    let addr = try!(socket.local_addr());
    info!("Listening on {}", addr);


    let done = {
        let handle = handle.clone();
        socket.incoming()
            .for_each(move |(stream, _addr)| {
                // Initialize the pipeline dispatch with the service and the line
                // transport

                let service = SexpService { inner: try!(new_service.new_service()) };
                // binder
                SexpProto::new().bind_server(&handle, stream, service);
                Ok(())
            })
            .map_err(|e| warn!("Listener: {}", e))
    };
    handle.spawn(done);
    Ok(addr)
}
