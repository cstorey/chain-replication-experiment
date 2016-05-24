/*
Each client talks to a single consumer object, which references the shared log.

Consumer:
Client LWM -------------- Sent point -------------- Client HWM
*/

use data::Seqno;

#[derive(Debug)]
pub struct Consumer;

impl Consumer {
    pub fn new() -> Consumer {
        Consumer
    }

    pub fn update_consumer(&mut self, mark: Option<Seqno>) {
        unimplemented!()
    }
}
