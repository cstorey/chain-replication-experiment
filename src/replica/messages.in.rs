use std::net::SocketAddr;
use serde::bytes::ByteBuf;
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LogPos(usize);

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct HostConfig {
    pub head: SocketAddr,
    pub tail: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum LogEntry {
    Data(ByteBuf),
    Config(HostConfig),
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
