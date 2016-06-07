
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate vastatrix;
extern crate mio;
extern crate time;
extern crate quickcheck;
extern crate serde;
extern crate serde_json;
extern crate spki_sexp;
extern crate hybrid_clocks;
extern crate petgraph;

mod replica;
mod replication_log;
mod consumer;
