use mio;

use super::{ChainRepl, ChainReplMsg};
use data::OpResp;

use line_conn::{SexpPeer, PlainClient, LineConn, ManualClientProto, PeerClientProto, LineConnEvents};
use downstream_conn::Downstream;
use listener::{Listener,ListenerEvents};

#[derive(Debug)]
pub enum EventHandler {
    Listener(Listener),
    Conn(LineConn<PlainClient, ManualClientProto>),
    Upstream(LineConn<SexpPeer, PeerClientProto>),
    Downstream(Downstream<SexpPeer /* , ServerProto */>),
}

impl EventHandler {
    pub fn handle_event(&mut self,
                        _event_loop: &mut mio::EventLoop<ChainRepl>,
                        events: mio::EventSet) {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Upstream(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Downstream(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Listener(ref mut listener) => {
                listener.handle_event(_event_loop, events)
            }
        }
    }

    pub fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl>, token: mio::Token) {
        match self {
            &EventHandler::Conn(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Upstream(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Downstream(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Listener(ref listener) => listener.initialize(event_loop, token),
        }
    }

    pub fn process_rules<F: FnMut(ChainReplMsg), E: ListenerEvents + LineConnEvents>(
        &mut self,
                                                 event_loop: &mut mio::EventLoop<ChainRepl>,
                                                 to_parent: &mut F,
                                                 events: &mut E)
                                                 -> bool {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.process_rules(event_loop, events),
            &mut EventHandler::Upstream(ref mut conn) => conn.process_rules(event_loop, events),
            &mut EventHandler::Downstream(ref mut conn) => conn.process_rules(event_loop, to_parent),
            &mut EventHandler::Listener(ref mut listener) => {
                listener.process_rules(event_loop, events)
            }
        }
    }

    pub fn should_close(&self) -> bool {
        match self {
            &EventHandler::Conn(ref conn) => conn.should_close(),
            &EventHandler::Upstream(ref conn) => conn.should_close(),
            &EventHandler::Downstream(ref conn) => conn.should_close(),
            &EventHandler::Listener(ref listener) => listener.should_close(),
        }
    }

    pub fn response(&mut self, val: OpResp) {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.response(val),
            &mut EventHandler::Upstream(ref mut conn) => conn.response(val),
            other => panic!("Unexpected Response to {:?}", other),
        }
    }


    pub fn handle_timeout(&mut self) {
        match self {
            &mut EventHandler::Downstream(ref mut conn) => conn.handle_timeout(),
            other => warn!("Unexpected timeout for {:?}", other),
        }
    }
}
