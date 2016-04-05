use config::Epoch;
use byteorder::{ByteOrder, BigEndian};
use serde::bytes::ByteBuf;
use serde::{de,ser};

#[cfg(feature = "serde_macros")]
include!("data.rs.in");

#[cfg(not(feature = "serde_macros"))]
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
    pub fn zero() -> Seqno {
        Seqno(0)
    }

    pub fn succ(&self) -> Seqno {
        let &Seqno(ref n) = self;
        Seqno(n + 1)
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

    pub fn new(seq: u64) -> Seqno {
        Seqno(seq)
    }
    pub fn offset(&self) -> u64 {
        self.0 as u64
    }
}


impl Into<Vec<u8>> for Buf {
    fn into(self) -> Vec<u8> {
        self.0.into()
    }
}

impl From<Vec<u8>> for Buf {
    fn from(bytes: Vec<u8>) -> Self {
        Buf(From::from(bytes))
    }
}

impl ser::Serialize for Buf {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error>
        where S: ser::Serializer
    {
        self.0.serialize(serializer)
    }
}

impl de::Deserialize for Buf {
    #[inline]
    fn deserialize<D>(deserializer: &mut D) -> Result<Buf, D::Error>
        where D: de::Deserializer
    {
        de::Deserialize::deserialize(deserializer).map(Buf)
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::{Arbitrary, Gen};
    use super::Seqno;
    impl Arbitrary for Seqno {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            Seqno(Arbitrary::arbitrary(g))
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            Box::new(self.0.shrink().map(Seqno))
        }
    }
}
