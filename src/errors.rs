use sexp_proto;
use LogPos;

error_chain!{
    links {
        sexp_proto::Error, sexp_proto::ErrorKind, SexpP;
    }

    errors {
        BadSequence(current_head: LogPos) {
            description("Invalid sequence number")
            display("Invalid sequence number: current head {:?}", current_head)
        }
    }
}
