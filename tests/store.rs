extern crate vastatrix;
extern crate futures;
extern crate env_logger;
use futures::{Future, Stream, Async, task};
use vastatrix::{RamStore, Store};
use vastatrix::{LogPos, LogEntry, HostConfig, ChainView};
use vastatrix::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

struct NullUnpark;

impl task::Unpark for NullUnpark {
    fn unpark(&self) {}
}

fn null_parker() -> Arc<task::Unpark> {
    Arc::new(NullUnpark)
}

#[test]
fn can_append_one() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    task::spawn(store.append_entry(current, next, LogEntry::Data(b"foobar".to_vec().into())))
        .wait_future()
        .expect("append_entry");
    let (off, val) = task::spawn(store.fetch_from(current))
        .wait_stream()
        .expect("an entry")
        .expect("fetch");

    assert_eq!((off, val),
               (next, LogEntry::Data(b"foobar".to_vec().into())))
}

// FIXME: Remove keys for now?
#[test]
fn should_store_config() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    let config = HostConfig {
        head: "1.2.3.4:5".parse().expect("parse ip"),
        tail: "6.7.8.9:10".parse().expect("parse ip"),
    };
    let view = ChainView::of(vec![config]);
    task::spawn(store.append_entry(current, next, LogEntry::ViewChange(view.clone())))
        .wait_future()
        .expect("append_entry");
    let (off, val) = task::spawn(store.fetch_from(current))
        .wait_stream()
        .expect("an entry")
        .expect("fetch");

    assert_eq!((off, val), (next, LogEntry::ViewChange(view)))
}

#[test]
fn fetching_one_is_lazy() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    let mut fetch_f = task::spawn(store.fetch_from(current));
    assert_eq!(fetch_f.poll_stream(null_parker()).expect("poll-notready"),
               Async::NotReady);

    task::spawn(store.append_entry(current, next, LogEntry::Data(b"foobar".to_vec().into())))
        .wait_future()
        .expect("append_entry");

    let (off, val) = fetch_f.wait_stream().expect("an-entry").expect("fetch");
    assert_eq!((off, val),
               (next, LogEntry::Data(b"foobar".to_vec().into())))
}

#[test]
fn writes_are_lazy() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    let mut store_f =
        task::spawn(store.append_entry(current, next, LogEntry::Data(b"foobar".to_vec().into())));
    let mut fetch_f = task::spawn(store.fetch_from(current));
    assert_eq!(fetch_f.poll_stream(null_parker()).expect("poll-notready"),
               Async::NotReady);

    store_f.wait_future().expect("append_entry");

    let (off, val) = fetch_f.wait_stream().expect("first item").expect("fetch");
    assert_eq!((off, val),
               (next, LogEntry::Data(b"foobar".to_vec().into())))
}


#[test]
fn can_write_many() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    task::spawn(store.append_entry(current, next, LogEntry::Data(b"foo".to_vec().into())))
        .wait_future()
        .expect("append_entry 1");
    let current = next;
    let next = next.next();
    task::spawn(store.append_entry(current, next, LogEntry::Data(b"bar".to_vec().into())))
        .wait_future()
        .expect("append_entry 2");

    let fetch_stream = store.fetch_from(LogPos::zero());
    let items = fetch_stream.map(|(_pos, it)| it).take(2).collect();
    let res = task::spawn(items).wait_future().expect("fetch");
    assert_eq!(res,
               vec![LogEntry::Data(b"foo".to_vec().into()), LogEntry::Data(b"bar".to_vec().into())])
}

#[test]
fn cannot_overwrite_item() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    task::spawn(store.append_entry(current, next, LogEntry::Data(b"foo".to_vec().into())))
        .wait_future()
        .expect("append_entry 1");
    let overwrite_res =
        task::spawn(store.append_entry(current, next, LogEntry::Data(b"bar".to_vec().into())))
            .wait_future();

    let expected_seq = head_from_error(overwrite_res.unwrap_err());
    assert_eq!(expected_seq, Some(next));
}

#[test]
fn cannot_overwrite_history() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    task::spawn(store.append_entry(current, next, LogEntry::Data(b"foo".to_vec().into())))
        .wait_future()
        .expect("append_entry 1");
    let current = next;
    let next = next.next();
    task::spawn(store.append_entry(current, next, LogEntry::Data(b"bar".to_vec().into())))
        .wait_future()
        .expect("append_entry 1");
    let overwrite_res = task::spawn(store.append_entry(LogPos::zero(),
                                                       LogPos::zero().next(),
                                                       LogEntry::Data(b"moo".to_vec().into())))
        .wait_future();

    let expected_seq = head_from_error(overwrite_res.unwrap_err());
    assert_eq!(expected_seq, Some(next));
}

#[test]
fn cannot_start_in_future() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero().next().next().next();
    let next = current.next();

    let res =
        task::spawn(store.append_entry(current, next, LogEntry::Data(b"foo".to_vec().into())))
            .wait_future();

    let expected_seq = head_from_error(res.unwrap_err());
    assert_eq!(expected_seq, Some(LogPos::zero()));
}

#[test]
fn cannot_append_in_future() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();

    let current = LogPos::zero();
    let next = current.next();

    task::spawn(store.append_entry(current, next, LogEntry::Data(b"foo".to_vec().into())))
        .wait_future()
        .expect("append_entry 1");


    let current1 = next.next().next();
    let next1 = current.next();

    let res =
        task::spawn(store.append_entry(current1, next1, LogEntry::Data(b"foo".to_vec().into())))
            .wait_future();

    let expected_seq = head_from_error(res.unwrap_err());
    assert_eq!(expected_seq, Some(next));
}



#[test]
fn error_feedback_should_indicate_new_head() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    task::spawn(store.append_entry(current, next, LogEntry::Data(b"foo".to_vec().into())))
        .wait_future()
        .expect("append_entry 1");
    let overwrite_res = task::spawn(store.append_entry(LogPos::zero(),
                                                       LogPos::zero().next(),
                                                       LogEntry::Data(b"moo".to_vec().into())))
        .wait_future();

    let new_head = head_from_error(overwrite_res.unwrap_err()).expect("current sequence number");

    let final_write = task::spawn(store.append_entry(new_head,
                                                     new_head.next(),
                                                     LogEntry::Data(b"moo".to_vec().into())))
        .wait_future();
    assert!(final_write.is_ok());
}

#[test]
fn concurrent_writes_should_be_able_to_recover() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    // Request a write from a slow client
    let mut write_1 =
        task::spawn(store.append_entry(current, next, LogEntry::Data(b"foo".to_vec().into())));
    // A second client gets there first
    task::spawn(store.append_entry(current, next, LogEntry::Data(b"foo".to_vec().into())))
        .wait_future()
        .expect("append_entry 1");

    // Wait for the first write to be completed
    let overwrite_res = write_1.wait_future();

    // Retry the first write
    let new_head = head_from_error(overwrite_res.unwrap_err()).expect("current sequence number");
    let final_write = task::spawn(store.append_entry(new_head,
                                                     new_head.next(),
                                                     LogEntry::Data(b"foo".to_vec().into())))
        .wait_future();
    assert!(final_write.is_ok());
}


fn head_from_error(error: Error) -> Option<LogPos> {
    if let &ErrorKind::BadSequence(n) = error.kind() {
        Some(n)
    } else {
        None
    }
}

#[derive(Debug,Clone)]
struct Unparker(usize, Arc<Mutex<VecDeque<usize>>>);

impl task::Unpark for Unparker {
    fn unpark(&self) {
        let &Unparker(ref n, ref q) = self;
        println!("Unpark! {:?}", n);
        q.lock().expect("lock").push_back(*n);
    }
}


#[test]
fn writes_notify_waiters() {
    env_logger::init().unwrap_or(());
    let sched_q = Arc::new(Mutex::new(VecDeque::new()));

    let waiter_task_id = 0;
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    println!("spawn fetcher");
    let mut fetch_t = task::spawn(store.fetch_from(current));
    let fetch_unparker = Arc::new(Unparker(waiter_task_id, sched_q.clone()));

    println!("spawn appender");
    let mut appender =
        task::spawn(store.append_entry(current, next, LogEntry::Data(b"foobar".to_vec().into())));

    println!("Poll fetcher");
    assert_eq!(fetch_t.poll_stream(fetch_unparker.clone()).expect("poll-notready"),
               Async::NotReady);

    appender.wait_future().expect("append_entry");

    println!("Pending: {:?}", sched_q);

    assert_eq!(sched_q.lock().expect("lock").pop_front(),
               Some(waiter_task_id));
}

#[test]
fn can_stream_data() {
    env_logger::init().unwrap_or(());
    let sched_q = Arc::new(Mutex::new(VecDeque::new()));
    let logentry = LogEntry::Data(b"foobar".to_vec().into());

    let waiter_task_id = 0;
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    println!("spawn fetcher");

    let mut fetch_t = task::spawn(store.fetch_from(current));
    let fetch_unparker = Arc::new(Unparker(waiter_task_id, sched_q.clone()));

    println!("spawn appender");
    let mut appender = task::spawn(store.append_entry(current, next, logentry.clone()));

    println!("Poll fetcher");
    assert_eq!(fetch_t.poll_stream(fetch_unparker.clone()).expect("poll-notready"),
               Async::NotReady);

    appender.wait_future().expect("append_entry");

    println!("Pending: {:?}", sched_q);

    assert_eq!(sched_q.lock().expect("lock").pop_front(),
               Some(waiter_task_id));

    let (pos, it) = match fetch_t.poll_stream(fetch_unparker.clone()).expect("poll-notready") {
        Async::Ready(x) => x.expect("an item"),
        Async::NotReady => panic!("Stream future should be ready!"),
    };
    assert_eq!(it, logentry);
}
