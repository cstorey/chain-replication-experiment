extern crate sexp_proto_tokio;
extern crate futures;
extern crate tokio_core as tokio;
extern crate tokio_service as service;
extern crate tokio_proto as proto;
extern crate sexp_proto_tokio as spki_proto;
extern crate env_logger;

use proto::pipeline::{self, Frame};
use tokio::reactor::{Core, Handle};
use service::Service;
use futures::{Poll, Async};
use futures::stream::{self, Stream};

use std::sync::Mutex;


#[test]
fn ping_pong() {
    env_logger::init().unwrap_or(());

    let mut core = Core::new().unwrap();

    // This brings up our server.

    let handle = spki_proto::server::serve(&core.handle(),
                                          "127.0.0.1:0".parse().unwrap(),
                                          service::simple_service(|msg| {
                                              println!("GOT: {:?}", msg);
                                              Ok(msg)
                                          })).expect("serve");

    let addr = handle.local_addr();
    println!("Server:{}", addr);

    let client: spki_proto::client::Client<String, String> =
        spki_proto::client::connect(core.handle(), &addr);
    // - one that returns a future that we can 'await' on.
    let task = stream::iter(vec![Ok("one"), Ok("two"), Ok("three")].into_iter())
                   .and_then(|s| client.call(s.to_string()))
                   .collect();

    let res = core.run(task).expect("run task");
    println!("Result:{:?}", res);

    assert_eq!(res, vec!["one", "two", "three"]);
}


#[test]
fn chain() {
    env_logger::init().unwrap_or(());

    let mut core = Core::new().unwrap();

    // This brings up our server.

    let handle0 = spki_proto::server::serve(&core.handle(),
                                          "127.0.0.1:0".parse().unwrap(),
                                          service::simple_service(|msg| {
                                              println!("GOT0: {:?}", msg);
                                              Ok(msg)
                                          })).expect("serve");

    let client0: spki_proto::client::Client<String, String> =
        spki_proto::client::connect(core.handle(), &handle0.local_addr());
    let client0 = Mutex::new(client0);

    let serv1 = service::simple_service(move |msg| {
            println!("GOT1: {:?}", msg);
            client0.lock().expect("lock").call(msg)
            });

    let handle1 = spki_proto::server::serve(&core.handle(),
                                          "127.0.0.1:0".parse().unwrap(),
                                          serv1).expect("serve");

    let client1: spki_proto::client::Client<String, String> =
        spki_proto::client::connect(core.handle(), &handle1.local_addr());
    let client1 = Mutex::new(client1);

    let serv2 : service::SimpleService<_, String> = service::simple_service(move |msg| {
            println!("GOT2: {:?}", msg);
            client1.lock().expect("lock").call(msg)
            });
    let handle2 = spki_proto::server::serve(&core.handle(),
                                          "127.0.0.1:0".parse().unwrap(),
                                          serv2).expect("serve");

    let client2: spki_proto::client::Client<String, String> =
        spki_proto::client::connect(core.handle(), &handle2.local_addr());

    // - one that returns a future that we can 'await' on.
    let task = stream::iter(vec![Ok("one"), Ok("two"), Ok("three")].into_iter())
                   .and_then(|s| client2.call(s.to_string()))
                   .collect();

    let res = core.run(task).expect("run task");
    println!("Result:{:?}", res);

    assert_eq!(res, vec!["one", "two", "three"]);
}
