extern crate vastatrix;
extern crate futures;
extern crate env_logger;
use futures::{Future,Async, task};
use vastatrix::{RamStore,Store};
use vastatrix::LogPos;
use vastatrix::{Error,ErrorKind};
use std::sync::Arc;

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
    task::spawn(store.append_entry(current, next, b"foobar".to_vec())).wait_future().expect("append_entry");
    let (off, val) = task::spawn(store.fetch_next(current)).wait_future().expect("fetch");

    assert_eq!((off, val), (next, b"foobar".to_vec()))
}

#[test]
fn fetching_one_is_lazy() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    let mut fetch_f = task::spawn(store.fetch_next(current));
    assert_eq!(fetch_f.poll_future(null_parker()).expect("poll-notready"), Async::NotReady);

    task::spawn(store.append_entry(current, next, b"foobar".to_vec())).wait_future().expect("append_entry");

    let (off, val) = fetch_f.wait_future().expect("fetch");
    assert_eq!((off, val), (next, b"foobar".to_vec()))
}

#[test]
fn writes_are_lazy() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    let mut store_f = task::spawn(store.append_entry(current, next, b"foobar".to_vec()));
    let mut fetch_f = task::spawn(store.fetch_next(current));
    assert_eq!(fetch_f.poll_future(null_parker()).expect("poll-notready"), Async::NotReady);

    store_f.wait_future().expect("append_entry");

    let (off, val) = fetch_f.wait_future().expect("fetch");
    assert_eq!((off, val), (next, b"foobar".to_vec()))
}


#[test]
fn can_write_many() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    task::spawn(store.append_entry(current, next, b"foo".to_vec())).wait_future().expect("append_entry 1");
    let current = next; let next = next.next();
    task::spawn(store.append_entry(current, next, b"bar".to_vec())).wait_future().expect("append_entry 2");

    let fetch_f = store.fetch_next(LogPos::zero())
                  .and_then(|(curr, first)| {
                          store.fetch_next(curr).map(|(_, second)| vec![first, second]) 
                          });
    let res = task::spawn(fetch_f).wait_future().expect("fetch");
    assert_eq!(res, vec![b"foo", b"bar"])
}

#[test]
fn cannot_overwrite_item() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    task::spawn(store.append_entry(current, next, b"foo".to_vec())).wait_future().expect("append_entry 1");
    let overwrite_res = task::spawn(store.append_entry(current, next, b"bar".to_vec())).wait_future();

    let expected_seq = head_from_error(overwrite_res.unwrap_err());
    assert_eq!(expected_seq, Some(next));
}

#[test]
fn cannot_overwrite_history() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    task::spawn(store.append_entry(current, next, b"foo".to_vec())).wait_future().expect("append_entry 1");
    let current = next; let next = next.next();
    task::spawn(store.append_entry(current, next, b"bar".to_vec())).wait_future().expect("append_entry 1");
    let overwrite_res = task::spawn(store.append_entry(LogPos::zero(), LogPos::zero().next(), b"moo".to_vec())).wait_future();

    let expected_seq = head_from_error(overwrite_res.unwrap_err());
    assert_eq!(expected_seq, Some(next));
}

#[test]
fn error_feedback_should_indicate_new_head() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    task::spawn(store.append_entry(current, next, b"foo".to_vec())).wait_future().expect("append_entry 1");
    let overwrite_res = task::spawn(store.append_entry(LogPos::zero(), LogPos::zero().next(), b"moo".to_vec())).wait_future();

    let new_head = head_from_error(overwrite_res.unwrap_err()).expect("current sequence number");

    let final_write = task::spawn(store.append_entry(new_head, new_head.next(), b"moo".to_vec())).wait_future();
    assert!(final_write.is_ok());
}

#[test]
fn concurrent_writes_should_be_able_to_recover() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    // Request a write from a slow client
    let mut write_1 = task::spawn(store.append_entry(current, next, b"foo".to_vec()));
    // A second client gets there first
    task::spawn(store.append_entry(current, next, b"foo".to_vec())).wait_future().expect("append_entry 1");

    // Wait for the first write to be completed
    let overwrite_res = write_1.wait_future();

    // Retry the first write
    let new_head = head_from_error(overwrite_res.unwrap_err()).expect("current sequence number");
    let final_write = task::spawn(store.append_entry(new_head, new_head.next(), b"foo".to_vec())).wait_future();
    assert!(final_write.is_ok());
}


fn head_from_error(error: Error) -> Option<LogPos> {
    if let &ErrorKind::BadSequence(n) = error.kind() {
        Some(n)
    } else {
        None
    }
}
