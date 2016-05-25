use config::Epoch;
use byteorder::{ByteOrder, BigEndian};
use serde::bytes::ByteBuf;
use serde::{de,ser};
use hybrid_clocks::{Timestamp,WallT};

use crexp_client_proto::messages as client;
use std::fmt;
use std::ops::{Deref,DerefMut};
use hex_slice::AsHex;

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone,Copy,Default, Serialize, Deserialize)]
pub struct Seqno(u64);
#[derive(PartialEq,Eq,PartialOrd,Ord,Hash,Clone)]
pub struct Buf(ByteBuf);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    Set(String),
    Get
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpResp {
    Ok(Epoch, Seqno, Option<Buf>),
    HelloIWant(Timestamp<WallT>, Seqno),
    Err(Epoch, Seqno, String),
}

impl Into<client::ProducerResp> for OpResp {
    fn into(self) -> client::ProducerResp {
        match self {
	    OpResp::Ok(_, seqno, None) => client::ProducerResp::Ok(seqno.into()),
	    OpResp::Ok(_, seqno, Some(val)) => {
		warn!("Ignored response val @{:?}: {:?}", seqno, val);
		client::ProducerResp::Ok(seqno.into())
	    },
	    OpResp::HelloIWant(_, _) => unreachable!(),
	    OpResp::Err(_, seqno, message) => client::ProducerResp::Err(seqno.into(), message),
	}
    }
}

impl Into<client::Buf> for Buf {
    fn into(self) -> client::Buf {
	let vec : Vec<u8> = self.0.into();
	vec.into()
    }
}

impl From<client::Buf> for Buf {
    fn from(item: client::Buf) -> Buf {
	let vec : Vec<u8> = item.into();
	From::from(vec)
    }
}

impl Deref for Buf {
    type Target = <ByteBuf as Deref>::Target;
    fn deref(&self) -> &Self::Target {
	&self.0
    }
}

impl DerefMut for Buf {
    // type Target = ByteBuf;
    fn deref_mut(&mut self) -> &mut Self::Target {
	&mut self.0
    }
}

impl fmt::Debug for Buf {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
	write!(fmt, "{:x}", (&*self.0).as_hex())
    }
}

impl Into<client::Seqno> for Seqno {
    fn into(self) -> client::Seqno {
	self.0.into()
    }
}

impl From<client::Seqno> for Seqno {
    fn from(s: client::Seqno) -> Seqno {
	Seqno(s.into())
    }
}


#[derive(Eq,PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum Role {
    ProducerClient,
    ConsumerClient,
    Upstream,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NodeViewConfig {
    pub peer_addr: Option<String>,
    pub producer_addr: Option<String>,
    pub consumer_addr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerMsg {
    HelloDownstream,
    Prepare (Seqno, Buf),
    CommitTo (Seqno),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationMessage {
    pub epoch: Epoch,
    pub ts: Timestamp<WallT>,
    pub msg: PeerMsg,
}

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

    #[cfg(test)]
    pub fn new(seq: u64) -> Seqno {
        Seqno(seq)
    }

    #[cfg(test)]
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
    use quickcheck::{self, Arbitrary, Gen};
    use super::Seqno;
    use data::Operation;

    impl Arbitrary for Seqno {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            Seqno(Arbitrary::arbitrary(g))
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            Box::new(self.0.shrink().map(Seqno))
        }
    }

    impl Arbitrary for Operation {
        fn arbitrary<G: Gen>(g: &mut G) -> Operation {
            match u64::arbitrary(g) % 2 {
                0 => Operation::Set(format!("{:x}", u64::arbitrary(g))),
                1 => Operation::Get,
                _ => unimplemented!(),
            }
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            match self {
                &Operation::Set(ref s) => Box::new(s.shrink().map(Operation::Set)),
                &Operation::Get => quickcheck::empty_shrinker(),
            }
        }
    }
}
