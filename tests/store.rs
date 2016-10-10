extern crate vastatrix;
extern crate futures;
extern crate env_logger;
use futures::{Future,Async};
use vastatrix::{RamStore,Store};
use vastatrix::LogPos;
use vastatrix::{Error,ErrorKind};

#[test]
fn can_append_one() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    store.append_entry(current, next, b"foobar".to_vec()).wait().expect("append_entry");
    let (off, val) = store.fetch_next(current).wait().expect("fetch");

    assert_eq!((off, val), (next, b"foobar".to_vec()))
}

#[test]
fn fetching_one_is_lazy() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    let mut fetch_f = store.fetch_next(current);
    assert_eq!(fetch_f.poll().expect("poll-notready"), Async::NotReady);

    store.append_entry(current, next, b"foobar".to_vec()).wait().expect("append_entry");

    let (off, val) = fetch_f.wait().expect("fetch");
    assert_eq!((off, val), (next, b"foobar".to_vec()))
}

#[test]
fn writes_are_lazy() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();

    let store_f = store.append_entry(current, next, b"foobar".to_vec());
    let mut fetch_f = store.fetch_next(current);
    assert_eq!(fetch_f.poll().expect("poll-notready"), Async::NotReady);

    store_f.wait().expect("append_entry");

    let (off, val) = fetch_f.wait().expect("fetch");
    assert_eq!((off, val), (next, b"foobar".to_vec()))
}


#[test]
fn can_write_many() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    store.append_entry(current, next, b"foo".to_vec()).wait().expect("append_entry 1");
    let current = next; let next = next.next();
    store.append_entry(current, next, b"bar".to_vec()).wait().expect("append_entry 2");

    let fetch_f = store.fetch_next(LogPos::zero())
                  .and_then(|(curr, first)| {
                          store.fetch_next(curr).map(|(_, second)| vec![first, second]) 
                          });
    let res = fetch_f.wait().expect("fetch");
    assert_eq!(res, vec![b"foo", b"bar"])
}

#[test]
fn cannot_overwrite_item() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    store.append_entry(current, next, b"foo".to_vec()).wait().expect("append_entry 1");
    let overwrite_res = store.append_entry(current, next, b"bar".to_vec()).wait();

    let expected_seq = if let &ErrorKind::BadSequence(n) = overwrite_res.unwrap_err().kind() {
        Some(n) } else { None };
    assert_eq!(expected_seq, Some(next));
}

#[test]
fn cannot_overwrite_history() {
    env_logger::init().unwrap_or(());
    let store = RamStore::new();
    let current = LogPos::zero();
    let next = current.next();
    store.append_entry(current, next, b"foo".to_vec()).wait().expect("append_entry 1");
    let current = next; let next = next.next();
    store.append_entry(current, next, b"bar".to_vec()).wait().expect("append_entry 1");
    let overwrite_res = store.append_entry(LogPos::zero(), LogPos::zero().next(), b"moo".to_vec()).wait();

    let expected_seq = if let &ErrorKind::BadSequence(n) = overwrite_res.unwrap_err().kind() {
        Some(n) } else { None };
    assert_eq!(expected_seq, Some(next));
}
