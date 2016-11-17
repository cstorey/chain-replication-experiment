#[macro_use]
extern crate log;
extern crate futures;
extern crate tokio_core as tokio;
extern crate tokio_service as service;
extern crate tokio_proto as proto;
extern crate tokio_timer;
extern crate env_logger;
extern crate vastatrix;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::reactor::Core;
use futures::Future;
use vastatrix::LogPos;

use vastatrix::hosting::*;

/// Remove ThickClient#add_peer calls, replace with Zarniwhoop^Wetcd config.
#[test]
fn smoketest_single_node() {
    env_logger::init().unwrap_or(());

    let mut core = Core::new().unwrap();
    let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();
    let timeout = Duration::from_millis(1000);

    let local_anon_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let server = build_server(
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
    let (wpos0, wpos1) = core.run(timer.timeout(f, timeout)).expect("run write");

    info!("Wrote to offset:{:?}", (wpos0, wpos1));

    let item_f = client.fetch_next(LogPos::zero())
                       .and_then(|(pos0, val0)| {
                           client.fetch_next(pos0).map(move |second| vec![(pos0, val0), second])
                       });

    let read = core.run(timer.timeout(item_f, timeout)).expect("run read");
    info!("Got: {:?}", read);

    assert_eq!(read,
               vec![(wpos0, b"hello".to_vec()), (wpos1, b"world".to_vec())]);
}

#[test]
fn smoketest_two_member_chain() {
    env_logger::init().unwrap_or(());
    let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();
    let timeout = Duration::from_millis(1000);
    let mut core = Core::new().unwrap();

    let local_anon_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut head_addr = local_anon_addr.clone();
    let mut tail_addr = local_anon_addr.clone();

    head_addr.set_port(11000);
    tail_addr.set_port(11001);
    let server0 = build_server(
                      &core.handle(),
                      head_addr.clone(),
                      tail_addr.clone())
        .expect("start server");
    println!("running: {:?}", server0);

    head_addr.set_port(11010);
    tail_addr.set_port(11011);
    let server1 = build_server(
                      &core.handle(),
                      head_addr.clone(),
                      tail_addr.clone())
        .expect("start server");

    println!("running: {:?}", server1);

    let client = vastatrix::ThickClient::new(core.handle(), &server0.head, &server1.tail);

    let f = client.add_peer(server0.clone())
        .and_then(|_| client.add_peer(server1.clone()))
        .and_then(|_| client.log_item(b"hello".to_vec()))
        .and_then(|pos0| client.log_item(b"world".to_vec()).map(move |pos1| (pos0, pos1)));
    let (wpos0, wpos1) = core.run(timer.timeout(f, timeout)).expect("run write");

    info!("Wrote to offset:{:?}", (wpos0, wpos1));

    let item_f = client.fetch_next(LogPos::zero())
        .and_then(|(pos0, val0)| {
            client.fetch_next(pos0).map(move |second| vec![(pos0, val0), second])
        });

    let read = core.run(timer.timeout(item_f, timeout)).expect("run read");
    info!("Got: {:?}", read);

    assert_eq!(read,
               vec![(wpos0, b"hello".to_vec()), (wpos1, b"world".to_vec())]);
}

#[test]
fn smoketest_three_member_chain() {
    env_logger::init().unwrap_or(());
    let timer = tokio_timer::wheel().tick_duration(Duration::from_millis(1)).build();
    let timeout = Duration::from_millis(1000);
    let mut core = Core::new().unwrap();

    let local_anon_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut head_addr = local_anon_addr.clone();
    let mut tail_addr = local_anon_addr.clone();

    head_addr.set_port(11300);
    tail_addr.set_port(11301);
    let server0 = build_server(
                      &core.handle(),
                      head_addr.clone(),
                      tail_addr.clone())
        .expect("start server");
    println!("running: {:?}", server0);

    head_addr.set_port(11310);
    tail_addr.set_port(11311);
    let server1 = build_server(
                      &core.handle(),
                      head_addr.clone(),
                      tail_addr.clone())
        .expect("start server");

    println!("running: {:?}", server1);

    head_addr.set_port(11320);
    tail_addr.set_port(11321);
    let server2 = build_server(
                      &core.handle(),
                      head_addr.clone(),
                      tail_addr.clone())
        .expect("start server");
    println!("running: {:?}", server2);

    let client = vastatrix::ThickClient::new(core.handle(), &server0.head, &server2.tail);

    let f = client.add_peer(server0.clone())
        .and_then(|_| client.add_peer(server1.clone()))
        .and_then(|_| client.log_item(b"hello".to_vec()))
        .and_then(|pos0| client.add_peer(server2.clone()).map(move |_| pos0))
        .and_then(|pos0| client.log_item(b"world".to_vec()).map(move |pos1| (pos0, pos1)));
    let (wpos0, wpos1) = core.run(timer.timeout(f, timeout)).expect("run write");

    info!("Wrote to offset:{:?}", (wpos0, wpos1));

    let (pos0, val0) = core.run(timer.timeout(client.fetch_next(LogPos::zero()), timeout)).expect("fetch first");
    info!("Got: {:?}", (&pos0, &val0));
    let (pos1, val1) = core.run(timer.timeout(client.fetch_next(pos0), timeout)).expect("fetch second");
    info!("Got: {:?}", (&pos1, &val1));

    assert_eq!(vec![(pos0, &*String::from_utf8_lossy(&val0)), (wpos1, &*String::from_utf8_lossy(&val1))],
               vec![(wpos0, "hello"), (wpos1, "world")]);
}
