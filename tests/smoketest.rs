#[macro_use]
extern crate log;
extern crate futures;
extern crate tokio_core as tokio;
extern crate tokio_service as service;
extern crate tokio_proto as proto;
extern crate env_logger;
extern crate vastatrix;

use std::net::SocketAddr;

use tokio::reactor::Core;
use futures::Future;
use vastatrix::LogPos;

use vastatrix::hosting::*;

#[test]
fn smoketest() {
    env_logger::init().unwrap_or(());

    let mut core = Core::new().unwrap();

    let service = CoreService::new();

    let local_anon_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let server = SexpHost.build_server(service,
                                        &core.handle(),
                                        local_anon_addr.clone(),
                                        local_anon_addr.clone())
                     .expect("start server");
    println!("running: {:?}", server);

    info!("Head at: {:?}, tail at: {:?}",
          server.head,
          server.tail);
    let client = vastatrix::ThickClient::new(core.handle(),
                                             &server.head,
                                             &server.tail);

    let f = client.log_item(b"hello".to_vec())
                  .and_then(|pos0| {
                      client.log_item(b"world".to_vec()).map(move |pos1| (pos0, pos1))
                  });
    let (wpos0, wpos1) = core.run(f).expect("run write");

    info!("Wrote to offset:{:?}", (wpos0, wpos1));

    let item_f = client.fetch_next(LogPos::zero())
                       .and_then(|(pos0, val0)| {
                           client.fetch_next(pos0).map(move |second| vec![(pos0, val0), second])
                       });

    let read = core.run(item_f).expect("run read");
    info!("Got: {:?}", read);

    assert_eq!(read,
               vec![(wpos0, b"hello".to_vec()), (wpos1, b"world".to_vec())]);
}
