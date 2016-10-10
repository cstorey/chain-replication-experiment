#[macro_use]
extern crate futures;
extern crate tokio_core as tokio;
extern crate tokio_service as service;
extern crate tokio_proto as proto;
extern crate serde;
extern crate tokio_service;
extern crate spki_sexp;
#[macro_use]
extern crate log;
extern crate bytes;
#[macro_use]
extern crate error_chain;
extern crate void;

pub mod sexp_proto;
mod replica;
mod proxy;
mod fatclient;

pub use replica::server::ServerService;
pub use proxy::server::ProxyService;
pub use proxy::client::ProxyClient;
pub use fatclient::FatClient;
