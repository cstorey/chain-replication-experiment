use messages::{ServerRequest, ServerResponse};
use sexp_proto::client as sclient;
use tokio::reactor::Handle;
use std::sync::Mutex;
use std::net::SocketAddr;

#[derive(Debug)]
pub struct Client(Mutex<sclient::Client<ServerRequest, ServerResponse>>);

impl Client {
    pub fn new(handle: Handle, target: &SocketAddr) -> Self {
        let client0: sclient::Client<ServerRequest, ServerResponse> = sclient::connect(handle,
                                                                                       target);
        Client(Mutex::new(client0))
    }
}
