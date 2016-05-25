use mio;

use super::ChainRepl;
use data::OpResp;
use crexp_client_proto::messages as client;

use line_conn::{SexpPeer, LineConn, ClientProto, ConsumerProto, PeerClientProto, LineConnEvents};
use downstream_conn::Downstream;
use hybrid_clocks::{Timestamp, WallT};

#[derive(Debug)]
pub enum EventHandler {
    Client(LineConn<SexpPeer, ClientProto>),
    Consumer(LineConn<SexpPeer, ConsumerProto>),
    Upstream(LineConn<SexpPeer, PeerClientProto>),
    Downstream(Downstream<SexpPeer /* , ServerProto */>),
}

impl EventHandler {
    pub fn handle_event(&mut self,
                        _event_loop: &mut mio::EventLoop<ChainRepl>,
                        events: mio::EventSet) {
        match self {
            &mut EventHandler::Client(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Consumer(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Upstream(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Downstream(ref mut conn) => conn.handle_event(_event_loop, events),
        }
    }

    pub fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        match self {
            &EventHandler::Client(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Consumer(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Upstream(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Downstream(ref conn) => conn.initialize(event_loop, token),
        }
    }

    pub fn process_rules<E: LineConnEvents>(
        &mut self,
                                                 event_loop: &mut mio::EventLoop<ChainRepl>,
                                                 now: &Timestamp<WallT>,
                                                 events: &mut E)
                                                 -> bool {
        match self {
            &mut EventHandler::Client(ref mut conn) => conn.process_rules(event_loop, events),
            &mut EventHandler::Consumer(ref mut conn) => conn.process_rules(event_loop, events),
            &mut EventHandler::Upstream(ref mut conn) => conn.process_rules(event_loop, events),
            &mut EventHandler::Downstream(ref mut conn) => conn.process_rules(event_loop, now, events),
        }
    }

    pub fn should_close(&self) -> bool {
        match self {
            &EventHandler::Client(ref conn) => conn.should_close(),
            &EventHandler::Consumer(ref conn) => conn.should_close(),
            &EventHandler::Upstream(ref conn) => conn.should_close(),
            &EventHandler::Downstream(ref conn) => conn.should_close(),
        }
    }

    pub fn response(&mut self, val: OpResp) {
        match self {
            &mut EventHandler::Client(ref mut conn) => conn.response(val.into()),
            &mut EventHandler::Upstream(ref mut conn) => conn.response(val),
            other => panic!("Unexpected Response to {:?}", other),
        }
    }

    pub fn consumer_message(&mut self, msg: client::ConsumerResp) {
        match self {
            &mut EventHandler::Consumer(ref mut conn) => conn.response(msg),
            other => panic!("Unexpected consumer message to {:?}", other),
        }
    }

    pub fn handle_timeout(&mut self) {
        match self {
            &mut EventHandler::Downstream(ref mut conn) => conn.handle_timeout(),
            other => warn!("Unexpected timeout for {:?}", other),
        }
    }
}
