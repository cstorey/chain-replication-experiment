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
extern crate stable_bst;
#[cfg(test)]
extern crate env_logger;

pub mod sexp_proto;
mod errors;
mod replica;
mod tail;
mod thick_client;
mod store;

pub use replica::server::ServerService;
pub use replica::LogPos;
pub use tail::server::TailService;
pub use tail::client::TailClient;
pub use thick_client::{ThickClient, ThickClientTcp};
pub use errors::*;
pub use store::{RamStore, Store};
