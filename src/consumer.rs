/*
Each client talks to a single consumer object, which references the shared log.

Consumer:
Client LWM -------------- Sent point -------------- Client HWM
*/

use std::cmp::max;
use data::Seqno;
use replica::{Log, Outputs};
use mio;

#[derive(Debug)]
pub struct Consumer {
    consume_next: Seqno,
}

impl Consumer {
    pub fn new() -> Consumer {
        Consumer { consume_next: Seqno::zero() }
    }

    pub fn consume_requested(&mut self, mark: Seqno) {
        self.consume_next = mark;
    }

    pub fn process<L: Log, O: Outputs>(&mut self,
            out: &mut O, token: mio::Token, log: &L) -> bool {
        let mut changed = false;
        for i in self.consume_next.upto(&log.seqno()) {
            if let Some(op) = log.read(i) {
                debug!("Consume seq:{:?}/{:?}; ds/seqno: {:?}",
                        i, op, self.consume_next);
                out.consumer_message(token, i, op.into());
                self.consume_next = i.succ();
                changed = true
            } else {
                panic!("Attempted to replicate item not in log: {:?}", self);
            }
        }
        changed
    }
}
