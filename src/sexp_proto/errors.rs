use proto;
use std::io;
use spki_sexp;
error_chain! { 
    foreign_links {
        io::Error, Io;
        spki_sexp::Error, Sexp;
    }
}

impl<T> From<proto::Error<T>> for Error
    where Error: From<T>
{
    fn from(x: proto::Error<T>) -> Self {
        match x {
            proto::Error::Transport(e) => e.into(),
            proto::Error::Io(e) => e.into(),
        }
    }
}
