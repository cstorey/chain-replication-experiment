use proto;
use sexp_proto;
use LogPos;
use std::io;
use spki_sexp;
use tokio_timer;
use etcd;
use serde_json;

error_chain!{
    links {
        sexp_proto::Error, sexp_proto::ErrorKind, SexpP;
    }

    foreign_links {
        io::Error, Io;
        tokio_timer::TimeoutError, Timeout;
        etcd::Error, Etcd;
        serde_json::Error, Json;
    }

    errors {
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
impl From<proto::Error<Error>> for Error {
    fn from(x: proto::Error<Error>) -> Self {
        match x {
            proto::Error::Transport(e) => e,
            proto::Error::Io(e) => e.into(),
        }
    }
}
