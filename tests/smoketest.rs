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
use vastatrix::bovine;
use vastatrix::{TailRequest, TailResponse, ReplicaRequest, ReplicaResponse};
use vastatrix::{TailClient, ReplicaClient};

#[test]
fn smoketest_single_node() {
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

#[test]
#[ignore]
fn smoketest_two_member_chain() {
    env_logger::init().unwrap_or(());

    let mut core = Core::new().unwrap();

    let local_anon_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let server0 = SexpHost.build_server(CoreService::new(),
                      &core.handle(),
                      local_anon_addr.clone(),
                      local_anon_addr.clone())
        .expect("start server");
    println!("running: {:?}", server0);

    let server1 = SexpHost.build_server(CoreService::new(),
                      &core.handle(),
                      local_anon_addr.clone(),
                      local_anon_addr.clone())
        .expect("start server");

    println!("running: {:?}", server1);

    let client = vastatrix::ThickClient::new(core.handle(), &server0.head, &server1.tail);

    let f = client.add_peer(server1.clone())
        .and_then(|_| client.log_item(b"hello".to_vec()))
        .and_then(|pos0| client.log_item(b"world".to_vec()).map(move |pos1| (pos0, pos1)));
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

#[cfg(never)]
#[test]
fn smoketest_bovine() {
    env_logger::init().unwrap_or(());

    let mut net = bovine::SphericalBovine::new();

    let scheduler = bovine::Scheduler::new();

    let service = CoreService::new();
    let server = net.build_server(service,
                      &scheduler,
                      bovine::BovineAddr(0),
                      bovine::BovineAddr(1))
        .expect("start server");
    println!("running: {:?}", server);

    info!("Head at: {:?}, tail at: {:?}", server.head, server.tail);

    let head_client = ReplicaClient::build(&scheduler, &server.head);
    let tail_client = TailClient::build(&scheduler, &server.tail);
    let client = vastatrix::ThickClient::build(head_client, tail_client);

    let f = client.log_item(b"hello".to_vec())
        .and_then(|pos0| client.log_item(b"world".to_vec()).map(move |pos1| (pos0, pos1)));
    let (wpos0, wpos1) = scheduler.run(f).expect("run write");

    info!("Wrote to offset:{:?}", (wpos0, wpos1));

    let item_f = client.fetch_next(LogPos::zero())
        .and_then(|(pos0, val0)| {
            client.fetch_next(pos0).map(move |second| vec![(pos0, val0), second])
        });

    let read = scheduler.run(item_f).expect("run read");
    info!("Got: {:?}", read);

    assert_eq!(read,
               vec![(wpos0, b"hello".to_vec()), (wpos1, b"world".to_vec())]);
}
