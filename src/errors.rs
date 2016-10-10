use sexp_proto;

error_chain!{
    links {
        sexp_proto::Error, sexp_proto::ErrorKind, SexpP;
    }
}
