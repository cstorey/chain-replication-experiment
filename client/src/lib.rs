extern crate gj;
extern crate gjio;
use gj::{EventLoop, Promise};
use gjio::{AsyncRead, AsyncWrite, BufferPrefix, SocketStream, EventPort};
use std::net::SocketAddr;
use std::io;
use std::io::Write;

pub struct Producer {
    stream: SocketStream,
}

impl Producer {
    pub fn new(host: SocketAddr, event_port: &mut EventPort) -> Promise<Producer, io::Error> {
        let network = event_port.get_network();
        let mut address = network.get_tcp_address(host);
        address.connect().map(move |mut stream|
            Ok(Producer {
                stream: stream,
            })
        )
    }

    pub fn publish(&mut self, data: &str) -> Promise<(), io::Error> {
        let mut req = Vec::new();
        if let Err(e) = writeln!(req, "{}", data) {
            return Promise::err(e);
        }
        self.stream.write(req).then(move |val| {
            println!("Write returned {:?}", val);
            Promise::ok(())
        })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn it_works() {
    }
}
