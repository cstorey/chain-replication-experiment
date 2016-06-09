// Each client talks to a single consumer object, which references the shared log.
//
// Consumer:
// Client LWM -------------- Sent point -------------- Client HWM
//

use data::Seqno;
use replica::{Log, Outputs};

error_chain! {
    types {
        Error, ErrorKind, ChainErr, Result;
    }
    links {}
    foreign_links {}
    errors {
        InvalidRange(lwm: Option<Seqno>, sent: Option<Seqno>, mark: Seqno)
    }
}

#[derive(Debug)]
pub struct Consumer {
    low_water_mark: Option<Seqno>,
    sent: Option<Seqno>,
}

impl Consumer {
    pub fn new() -> Consumer {
        Consumer {
            low_water_mark: None,
            sent: None,
        }
    }

    pub fn consume_requested(&mut self, mark: Seqno) -> Result<()> {
        // It would be odd for a client to request an item they have not seen
        // in the current session.
        let lwm = match (self.low_water_mark, self.sent) {
            (None, None) => Some(mark),
            (Some(m), None) if mark >= m => Some(m),
            (Some(m), Some(s)) if mark >= m && mark <= s => Some(mark),

            (None, Some(_)) => unreachable!(),
            (m, s) => return Err(ErrorKind::InvalidRange(m, s, mark).into()),
        };
        debug!("consume_requested: {:?}; mark:{:?} => {:?}",
               self,
               mark,
               lwm);
        self.low_water_mark = lwm;
        Ok(())
    }

    pub fn process<L: Log, O: Outputs<Dest = D>, D: Clone>(&mut self,
                                                           out: &mut O,
                                                           token: O::Dest,
                                                           log: &L)
                                                           -> bool {
        let mut changed = false;
        let next = self.low_water_mark;
        let committed = log.read_committed().expect("read_committed");
        trace!("Consumer#process: committed: {:?}; next: {:?}",
               committed,
               next);

        if let (Some(next), Some(committed)) = (next, committed) {
            for (i, op) in log.read_from(next)
                              .expect("read_from")
                              .take_while(|&(i, _)| i <= committed) {
                debug!("Consume seq:{:?}/{:?}; ds/seqno: {:?}",
                       i,
                       op,
                       self.low_water_mark);
                out.consumer_message(&token, i, op.into());
                self.sent = Some(i);
                self.low_water_mark = Some(i.succ());
                changed = true
            }
        }
        changed
    }
}
