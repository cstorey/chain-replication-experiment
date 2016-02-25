use std::cmp;
use std::mem;
use std::fmt;
use std::default::Default;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use mio;

use super::{Operation,OpResp, PeerMsg};
use event_handler::EventHandler;
use replication_log::Log;
use serde::{Serialize,Deserialize};

const REPLICATION_CREDIT : u64 = 1024;

#[derive(Debug)]
struct Forwarder<M> {
    last_sent_downstream: Option<u64>,
    last_acked_downstream: Option<u64>,
    pending_operations: BTreeMap<u64, mio::Token>,
    phantom: PhantomData<M>,
}

trait AppModel : fmt::Debug {
    type Operation : Serialize + Deserialize + fmt::Debug;
    fn new() -> Self;
    fn apply_op(&mut self, epoch: u64, seqno: u64, op: Self::Operation) -> OpResp;
}

#[derive(Debug)]
struct Terminus<M> {
    state: M,
}

#[derive(Debug)]
enum Role<M> {
    Forwarder(Forwarder<M>),
    Terminus(Terminus<M>),
}

#[derive(Debug)]
pub struct ReplModel<M> {
    next: Role<M>,
    log: Log,
    current_epoch: u64,
}

#[derive(Debug)]
struct StringRegister {
    value: String,
}

impl AppModel for StringRegister {
    type Operation = super::Operation;
    fn new() -> Self {
        StringRegister { value: "".to_string() }
    }

    fn apply_op(&mut self, epoch: u64, seqno: u64, op: Self::Operation) -> OpResp {
        match op {
            Operation::Set(s) => {
                self.value = s;
                OpResp::Ok(epoch, seqno, None)
            },
            Operation::Get => OpResp::Ok(epoch, seqno, Some(self.value.clone()))
        }
    }
}

impl<M: AppModel> Role<M> {
    fn process_operation(&mut self, channel: &mut EventHandler<M>, epoch: u64, seqno: u64, op: M::Operation) {
        match self {
            &mut Role::Forwarder(ref mut f) => f.process_operation(channel, epoch, seqno, op),
            &mut Role::Terminus(ref mut t) => t.process_operation(channel, epoch, seqno, op),
        }
    }
    pub fn process_downstream_response(&mut self, reply: &OpResp) -> Option<mio::Token> {
        match self {
            &mut Role::Forwarder(ref mut f) => f.process_downstream_response(reply),
            _ => None,
        }
    }

    pub fn process_replication<F: FnMut(PeerMsg<M::Operation>)>(&mut self, log: &Log, forward: F) -> bool {
        match self {
            &mut Role::Forwarder(ref mut f) => f.process_replication(log, forward),
            _ => false,
        }
    }
    fn has_pending(&self, token: mio::Token) -> bool {
        match self {
            &Role::Forwarder(ref f) => f.has_pending(token),
            _ => false,
        }
    }
    fn reset(&mut self) {
        match self {
            &mut Role::Forwarder(ref mut f) => f.reset(),
            _ => (),
        }
    }
}

impl<M: AppModel> Forwarder<M> {
    fn new() -> Forwarder<M> {
        Forwarder {
            last_sent_downstream: None,
            last_acked_downstream: None,
            pending_operations: BTreeMap::new(),
            phantom: PhantomData,
        }
    }

    fn process_operation(&mut self, channel: &mut EventHandler<M>, epoch: u64, seqno: u64, op: M::Operation) {
        self.pending_operations.insert(seqno, channel.token());
    }

    fn process_downstream_response(&mut self, reply: &OpResp) -> Option<mio::Token> {
        match reply {
            &OpResp::Ok(epoch, seqno, _) | &OpResp::Err(epoch, seqno, _) => {
                self.last_acked_downstream = Some(seqno);
                if let Some(token) = self.pending_operations.remove(&seqno) {
                    trace!("Found in-flight op {:?} for client token {:?}", seqno, token);
                    Some(token)
                } else {
                    warn!("Unexpected response for seqno: {:?}", seqno);
                    None
                }
            },
            &OpResp::HelloIWant(last_sent_downstream) => {
                info!("Downstream has {:?}", last_sent_downstream);
                // assert!(last_sent_downstream <= self.seqno());
                self.last_acked_downstream = Some(last_sent_downstream);
                self.last_sent_downstream = Some(last_sent_downstream);
                None
            },
        }
    }

    pub fn process_replication<F: FnMut(PeerMsg<M::Operation>)>(&mut self, log: &Log, mut forward: F) -> bool {
        let mut changed = false;
        debug!("pre  Repl: Ours: {:?}; downstream sent: {:?}; acked: {:?}",
            log.seqno(), self.last_sent_downstream, self.last_acked_downstream);

        let mut changed = false;
        if let Some(send_next) = self.last_sent_downstream {
            if send_next < log.seqno() {
                let max_to_push_now = cmp::min(self.last_acked_downstream.unwrap_or(0) + REPLICATION_CREDIT, log.seqno());
                debug!("Window push {:?} - {:?}; waiting: {:?}", send_next, max_to_push_now, log.seqno() - max_to_push_now);
                for i in send_next..max_to_push_now {
                    if let Some(op) = log.read(i) {
                        debug!("Queueing seq:{:?}/{:?}; ds/seqno: {:?}", i, op, self.last_sent_downstream);
                        forward(PeerMsg::Commit(i, op));
                        self.last_sent_downstream = Some(i+1);
                        changed = true
                    } else {
                        panic!("Attempted to replicate item not in log: {:?}", self);
                    }
                }
            }
            debug!("post Repl: Ours: {:?}; downstream sent: {:?}; acked: {:?}",
                log.seqno(), self.last_sent_downstream, self.last_acked_downstream);
        }
        changed
    }

    fn has_pending(&self, token: mio::Token) -> bool {
        self.pending_operations.values().all(|tok| tok != &token)
    }

    fn reset(&mut self) {
        self.last_sent_downstream = None
    }
}


impl<M: AppModel> Terminus<M> {
    fn new() -> Terminus<M> {
        Terminus {
            state: M::new(),
        }
    }

    fn process_operation(&mut self, channel: &mut EventHandler<M>, epoch: u64, seqno: u64, op: M::Operation) {
        info!("Terminus! {:?}/{:?}", seqno, op);
        let resp = self.state.apply_op(epoch, seqno, op);
        channel.response(resp)
    }
}

impl<M: AppModel> ReplModel<M> {
    pub fn new() -> ReplModel<M> {
        ReplModel {
            log: Log::new(),
            current_epoch: Default::default(),
            next: Role::Terminus(Terminus::new()),
        }
    }

    pub fn seqno(&self) -> u64 {
        self.log.seqno()
    }

    fn next_seqno(&self) -> u64 {
        self.log.seqno()
    }

    pub fn process_operation(&mut self, channel: &mut EventHandler<M>, seqno: Option<u64>, epoch: Option<u64>, op: M::Operation) {
        let seqno = seqno.unwrap_or_else(|| self.next_seqno());
        let epoch = epoch.unwrap_or_else(|| self.current_epoch);

       if epoch != self.current_epoch {
            warn!("Operation epoch ({}) differs from our last observed configuration: ({})",
                epoch, self.current_epoch);
            let resp = OpResp::Err(epoch, seqno, format!("BadEpoch: {}; last seen config: {}", epoch, self.current_epoch));
            channel.response(resp);
            return;
        }

        if !self.log.verify_sequential(seqno) {
            let resp = OpResp::Err(epoch, seqno, format!("Bad sequence number; saw: {:?}", seqno));
            channel.response(resp);
            return;
        }

        self.log.prepare(seqno, &op);
        self.next.process_operation(channel, epoch, seqno, op)
    }

    pub fn process_downstream_response(&mut self, reply: &OpResp) -> Option<mio::Token> {
        self.next.process_downstream_response(reply)
    }

    pub fn process_replication<F: FnMut(u64, PeerMsg<M::Operation>)>(&mut self, mut forward: F) -> bool {
        let epoch = self.current_epoch;
        self.next.process_replication(&self.log, |msg| forward(epoch, msg))
    }

    pub fn epoch_changed(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }

    pub fn has_pending(&self, token: mio::Token) -> bool {
        self.next.has_pending(token)
    }

    pub fn reset(&mut self) {
        self.next.reset()
    }

    pub fn flush(&mut self) -> bool {
        self.log.flush()
    }

    pub fn set_has_downstream(&mut self, is_forwarder: bool) {
        // XXX: Replays?
        match (is_forwarder, &mut self.next) {
            (true, role @ &mut Role::Terminus(_)) => {
                let prev = mem::replace(role, Role::Forwarder(Forwarder::new()));
                info!("Switched to forwarding from {:?}", prev);
            },

            (false, role @ &mut Role::Forwarder(_)) => {
                let prev = mem::replace(role, Role::Terminus(Terminus::new()));
                info!("Switched to terminating from {:?}", prev);
            },
            _ => { info!("No change of config"); }
        }
    }
}
