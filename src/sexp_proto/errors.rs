use std::io;
use spki_sexp;

error_chain! {
    foreign_links {
        io::Error, Io;
        spki_sexp::Error, Sexp;
    }
}
