extern crate futures;
extern crate tokio_core as tokio;
extern crate tokio_service as service;
extern crate sexp_proto_tokio as spki_proto;
extern crate env_logger;

use tokio::reactor::Core;

pub fn main() {
    env_logger::init().unwrap();

    let mut core = Core::new().unwrap();

    // This brings up our server.
    let addr = "127.0.0.1:12345".parse().unwrap();

    let _service = spki_proto::server::serve(&core.handle(),
                                          addr,
                                          service::simple_service(|msg| {
                                              println!("GOT: {:?}", msg);
                                              Ok(msg)
                                          }));

    core.run(futures::empty::<(), ()>()).expect("run loop");
}
