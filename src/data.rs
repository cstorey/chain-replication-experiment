use config::Epoch;
use std::ops::Range;
use byteorder::{ByteOrder, BigEndian};

include!(concat!(env!("OUT_DIR"), "/data.rs"));

pub struct SeqIter(u64, u64);

impl Iterator for SeqIter {
    type Item = Seqno;
    fn next(&mut self) -> Option<Self::Item> {
        let &mut SeqIter(ref mut curr, ref mut end) = self;
        if curr < end {
            let ret = Some(Seqno(curr.clone()));
            *curr += 1;
            ret
        } else {
            None
        }
    }
}

impl Seqno {
    pub fn none() -> Seqno {
        Seqno(0)
    }
    pub fn is_none(&self) -> bool {
        let &Seqno(ref n) = self;
        n == &0
    }
    pub fn succ(&self) -> Seqno {
        let &Seqno(ref n) = self;
        Seqno(n+1)
    }

    pub fn upto(&self, other: &Seqno) -> SeqIter {
        SeqIter(self.0, other.0)
    }

    pub fn tokey(&self) -> [u8; 8] {
        let mut buf: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
        BigEndian::write_u64(&mut buf, self.0);
        buf
    }

    pub fn fromkey(key: &[u8]) -> Seqno {
        Seqno(BigEndian::read_u64(&key))
    }
}
