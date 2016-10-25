use std::net::SocketAddr;
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LogPos(usize);

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct HostConfig<A> {
    pub head: A,
    pub tail: A,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum LogEntry {
    Data(Vec<u8>),
    Config(HostConfig<SocketAddr>),
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
