use mio;

use super::{ChainRepl, ChainReplMsg,OpResp};

use line_conn::{SexpPeer, PlainClient, LineConn, ManualClientProto, PeerClientProto};
use downstream_conn::Downstream;
use listener::Listener;
use replica::AppModel;

#[derive(Debug)]
pub enum EventHandler<M: AppModel> {
    Listener (Listener),
    Conn (LineConn<PlainClient, ManualClientProto>),
    Upstream (LineConn<SexpPeer, PeerClientProto<M::Operation>>),
    Downstream (Downstream<SexpPeer, M::Operation>),
}

impl<M: AppModel> EventHandler<M> {
    pub fn handle_event(&mut self, _event_loop: &mut mio::EventLoop<ChainRepl<M>>, events: mio::EventSet) 
        {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Upstream(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Downstream(ref mut conn) => conn.handle_event(_event_loop, events),
            &mut EventHandler::Listener(ref mut listener) => listener.handle_event(_event_loop, events)
        }
    }

    pub fn initialize(&self, event_loop: &mut mio::EventLoop<ChainRepl<M>>, token: mio::Token) {
        match self {
            &EventHandler::Conn(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Upstream(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Downstream(ref conn) => conn.initialize(event_loop, token),
            &EventHandler::Listener(ref listener) => listener.initialize(event_loop, token)
        }
    }

    pub fn token(&self) -> mio::Token {
        match self {
            &EventHandler::Conn(ref conn) => conn.token(),
            &EventHandler::Upstream(ref conn) => conn.token(),
            &EventHandler::Downstream(ref conn) => conn.token(),
            &EventHandler::Listener(ref listener) => listener.token(),
        }
    }

    pub fn process_rules<F: FnMut(ChainReplMsg<M::Operation>)>(&mut self, event_loop: &mut mio::EventLoop<ChainRepl<M>>,
        to_parent: &mut F) -> bool {
        match self {
            &mut EventHandler::Conn(ref mut conn) => conn.process_rules(event_loop, to_parent),
            &mut EventHandler::Upstream(ref mut conn) => conn.process_rules(event_loop, to_parent),
            &mut EventHandler::Downstream(ref mut conn) => conn.process_rules(event_loop, to_parent),
            &mut EventHandler::Listener(ref mut listener) => listener.process_rules(event_loop, to_parent)
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            &EventHandler::Conn(ref conn) => conn.is_closed(),
            &EventHandler::Upstream(ref conn) => conn.is_closed(),
            &EventHandler::Downstream(ref conn) => conn.is_closed(),
            &EventHandler::Listener(ref listener) => listener.is_closed()
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
