use std::cmp;
use std::collections::BTreeMap;
use mio;

use super::{Operation,OpResp};
use event_handler::EventHandler;

const REPLICATION_CREDIT : u64 = 1024;

#[derive(Debug)]
struct Forwarder {
    last_sent_downstream: Option<u64>,
    last_acked_downstream: Option<u64>,
    pending_operations: BTreeMap<u64, mio::Token>,
}

#[derive(Debug)]
struct Terminus {
    state: String,
}

#[derive(Debug)]
enum Role {
    Forwarder(Forwarder),
    Terminus(Terminus),
}

#[derive(Debug)]
struct Log (BTreeMap<u64, Operation>);

impl Log {
    pub fn seqno(&self) -> u64 {
        self.0.len() as u64
    }

    fn read(&self, idx: u64) -> Option<Operation> {
        self.0.get(&idx).map(|x| x.clone())
    }


    fn verify_sequential(&self, seqno: u64) -> bool {
        if !(seqno == 0 || self.0.contains_key(&(seqno-1))) {
            warn!("Hole in history: saw {}; have: {:?}", seqno, self.0.keys().collect::<Vec<_>>());
            false
        } else {
            true
        }
    }

    fn insert_at(&mut self, seqno: u64, op: &Operation) {
        let prev = self.0.insert(seqno, op.clone());
        debug!("Log entry {:?}: {:?}", seqno, op);
        if let Some(prev) = prev {
            panic!("Unexpectedly existing log entry for item {}: {:?}", seqno, prev);
        }
    }
}

#[derive(Debug)]
pub struct ReplModel {
    next: Role,
    log: Log,
    current_epoch: u64,
}


impl Role {
    fn process_operation(&mut self, channel: &mut EventHandler, epoch: u64, seqno: u64, op: Operation) {
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

    pub fn process_replication<F: FnMut(u64, Operation)>(&mut self, log: &Log, forward: F) -> bool {
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

impl Forwarder {
    fn new() -> Forwarder {
        Forwarder {
            last_sent_downstream: None,
            last_acked_downstream: None,
            pending_operations: BTreeMap::new(),
        }
    }

    fn process_operation(&mut self, channel: &mut EventHandler, epoch: u64, seqno: u64, op: Operation) {
        self.pending_operations.insert(seqno, channel.token());
    }

    fn process_downstream_response(&mut self, reply: &OpResp) -> Option<mio::Token> {
        match reply {
            &OpResp::Ok(epoch, seqno, _) | &OpResp::Err(epoch, seqno, _) => {
                self.last_acked_downstream = Some(seqno);
                if let Some(token) = self.pending_operations.remove(&seqno) {
                    info!("Found in-flight op {:?} for client token {:?}", seqno, token);
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

    pub fn process_replication<F: FnMut(u64, Operation)>(&mut self, log: &Log, mut forward: F) -> bool {
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
                        forward(i, op);
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


impl Terminus {
    fn new() -> Terminus {
        Terminus {
            state: "".to_string()
        }
    }

    fn process_operation(&mut self, channel: &mut EventHandler, epoch: u64, seqno: u64, op: Operation) {
        info!("Terminus! {:?}/{:?}", seqno, op);
        let resp = match op {
            Operation::Set(s) => {
                self.state = s;
                OpResp::Ok(epoch, seqno, None)
            },
                Operation::Get => OpResp::Ok(epoch, seqno, Some(self.state.clone()))
        };
        channel.response(resp)
    }
}

impl ReplModel {
    pub fn new() -> ReplModel {
        ReplModel {
            log: Log(BTreeMap::new()),
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

    pub fn process_operation(&mut self, channel: &mut EventHandler, seqno: Option<u64>, epoch: Option<u64>, op: Operation) {
        let seqno = seqno.unwrap_or_else(|| self.next_seqno());
        let epoch = epoch.unwrap_or_else(|| self.current_epoch);

       if epoch != self.current_epoch {
            warn!("Operation epoch ({}) differers from our last observed configuration: ({})",
                epoch, self.current_epoch);
            let resp = OpResp::Err(epoch, seqno, format!("BadEpoch: {}; last seen config: {}", epoch, self.current_epoch));
            channel.response(resp);
            return;
        }

        if self.log.verify_sequential(seqno) {
            let resp = OpResp::Err(epoch, seqno, format!("Bad sequence number; saw: {:?}", seqno));
            channel.response(resp);
            return;
        }


        self.log.insert_at(seqno, &op);
        self.next.process_operation(channel, epoch, seqno, op)
    }

    pub fn process_downstream_response(&mut self, reply: &OpResp) -> Option<mio::Token> {
        self.next.process_downstream_response(reply)
    }

    pub fn process_replication<F: FnMut(u64, u64, Operation)>(&mut self, mut forward: F) -> bool {
        let epoch = self.current_epoch;
        self.next.process_replication(&self.log, |i, op| forward(epoch, i, op))
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
}
