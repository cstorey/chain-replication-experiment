use std::cmp;
use std::collections::BTreeMap;
use mio;

use super::{Operation,OpResp};
use event_handler::EventHandler;

const REPLICATION_CREDIT : u64 = 1024;


#[derive(Debug)]
pub struct ReplModel {
    last_sent_downstream: Option<u64>,
    last_acked_downstream: Option<u64>,
    is_terminus: bool,
    pending_operations: BTreeMap<u64, mio::Token>,
    log: BTreeMap<u64, Operation>,
    current_epoch: u64,
    state: String,
}

impl ReplModel {
    pub fn new() -> ReplModel {
        ReplModel {
            last_sent_downstream: None,
            last_acked_downstream: None,
            is_terminus: false,
            log: BTreeMap::new(),
            pending_operations: BTreeMap::new(),
            state: String::new(),
            current_epoch: Default::default(),
        }
    }

    pub fn seqno(&self) -> u64 {
        self.log.len() as u64
    }

    fn next_seqno(&self) -> u64 {
        self.seqno()
    }


    pub fn process_operation(&mut self, channel: &mut EventHandler, seqno: Option<u64>, epoch: Option<u64>, op: Operation) {
        let seqno = seqno.unwrap_or_else(|| self.next_seqno());
        // assert!(seqno == 0 || self.log.get(&(seqno-1)).is_some());

        let epoch = epoch.unwrap_or_else(|| self.current_epoch);

       if epoch != self.current_epoch {
            warn!("Operation epoch ({}) differers from our last observed configuration: ({})",
                epoch, self.current_epoch);
            let resp = OpResp::Err(epoch, seqno, format!("BadEpoch: {}; last seen config: {}", epoch, self.current_epoch));
            channel.response(resp);
            return;
        }

        if !(seqno == 0 || self.log.get(&(seqno-1)).is_some()) {
            warn!("Hole in history: saw {}/{}; have: {:?}", epoch, seqno, self.log.keys().collect::<Vec<_>>());
            let resp = OpResp::Err(epoch, seqno, format!("Bad sequence number; previous: {:?}, saw: {:?}",
                self.log.keys().rev().next(), seqno));
            channel.response(resp);
            return;
        }


        let prev = self.log.insert(seqno, op.clone());
        debug!("Log entry {:?}: {:?}", seqno, op);
        if let Some(prev) = prev {
            panic!("Unexpectedly existing log entry for item {}: {:?}", seqno, prev);
        }

        if self.is_terminus {
            self.pending_operations.insert(seqno, channel.token());
            // Replication mechanism should handle the push to downstream.
        } else {
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

    pub fn process_downstream_response(&mut self, reply: &OpResp) -> Option<mio::Token> {
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
                assert!(last_sent_downstream <= self.seqno());
                self.last_acked_downstream = Some(last_sent_downstream);
                self.last_sent_downstream = Some(last_sent_downstream);
                None
            },
        }
    }

    pub fn process_replication<F: FnMut(u64, u64, Operation)>(&mut self, mut forward: F) -> bool {
        let mut changed = false;
        debug!("pre  Repl: Ours: {:?}; downstream sent: {:?}; acked: {:?}",
            self.seqno(), self.last_sent_downstream, self.last_acked_downstream);
        let mut changed = false;
        if let Some(send_next) = self.last_sent_downstream {
            if send_next < self.seqno() {
                let max_to_push_now = cmp::min(self.last_acked_downstream.unwrap_or(0) + REPLICATION_CREDIT, self.seqno());
                debug!("Window push {:?} - {:?}; waiting: {:?}", send_next, max_to_push_now, self.seqno() - max_to_push_now);
                let epoch = self.current_epoch;
                for i in send_next..max_to_push_now {
                    debug!("Log item: {:?}: {:?}", i, self.log.get(&i));
                    let op = self.log[&i].clone();
                    debug!("Queueing epoch:{:?}; seq:{:?}/{:?}; ds/seqno: {:?}", epoch, i, op, self.last_sent_downstream);
                    forward(epoch, i, op);
                    self.last_sent_downstream = Some(i+1);
                    changed = true
                }
            }
            debug!("post Repl: Ours: {:?}; downstream sent: {:?}; acked: {:?}",
                self.seqno(), self.last_sent_downstream, self.last_acked_downstream);
        }
        changed
    }

    pub fn epoch_changed(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }

    pub fn has_pending(&self, token: mio::Token) -> bool {
        self.pending_operations.values().all(|tok| tok != &token)
    }

    pub fn reset(&mut self) {
        self.last_sent_downstream = None
    }
}


