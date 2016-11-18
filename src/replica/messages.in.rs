use std::net::SocketAddr;
use serde::bytes::ByteBuf;
use std::fmt;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LogPos(usize);

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct HostConfig {
    pub head: SocketAddr,
    pub tail: SocketAddr,
}

impl fmt::Display for HostConfig {
    fn fmt(&self, fmt:&mut fmt::Formatter) ->fmt::Result {
        write!(fmt, "{}/{}", self.head, self.tail)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Default)]
pub struct ChainView {
    pub members: Vec<HostConfig>,
}

impl ChainView {
    pub fn of<I:IntoIterator<Item=HostConfig>>(members:I) ->ChainView{
        let members = members.into_iter().collect();
        ChainView { members: members }
    }

    pub fn is_head(&self, entry: &HostConfig) -> bool {
        self.members.get(0) == Some(entry)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum LogEntry {
    Data(ByteBuf),
    ViewChange(ChainView),
}
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ReplicaRequest {
    AppendLogEntry {
        assumed_offset: LogPos,
        entry_offset: LogPos,
        datum: LogEntry,
    },
}
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ReplicaResponse {
    Done(LogPos),
    BadSequence(LogPos)
}

impl LogPos {
    pub fn zero() -> Self {
        LogPos(0)
    }
    pub fn next(&self) -> Self {
        let &LogPos(off) = self;
        LogPos(off+1)
    }

    pub fn new(val: usize) -> Self {
        LogPos(val)
    }
}
