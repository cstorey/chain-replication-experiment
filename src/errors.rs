use sexp_proto;
use LogPos;
use std::io;
use spki_sexp;
use tokio_timer::{self, TimeoutError};
use etcd;
use serde_json;

error_chain!{
    links {
        sexp_proto::Error, sexp_proto::ErrorKind, SexpP;
    }

    foreign_links {
        io::Error, Io;
        etcd::Error, Etcd;
        serde_json::Error, Json;
        tokio_timer::TimerError, Timer;
    }

    errors {
        Timeout {
            description("Timeout")
            display("Timeout")
        }
        BadSequence(current_head: LogPos) {
            description("Invalid sequence number")
            display("Invalid sequence number: current head {:?}", current_head)
        }
    }
}

// This shouldn't belong here, really.
impl From<spki_sexp::Error> for Error {
    fn from(x: spki_sexp::Error) -> Self {
        let e: sexp_proto::Error = x.into();
        e.into()
    }
}
impl<F> From<tokio_timer::TimeoutError<F>> for Error {
    fn from(x: tokio_timer::TimeoutError<F>) -> Self {
        match x {
            TimeoutError::Timer(_f, e) => e.into(),
            TimeoutError::TimedOut(_fe) => ErrorKind::Timeout.into(),
        }
        // let e: Box<::std::errors::Error> = Box::new(x);
        // e.into()
    }
}

// impl From<proto::Error<Error>> for Error {
// fn from(x: proto::Error<Error>) -> Self {
// match x {
// proto::Error::Transport(e) => e,
// proto::Error::Io(e) => e.into(),
// }
// }
// }
