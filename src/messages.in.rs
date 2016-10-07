#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LogPos(usize);

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ServerRequest {
    AppendLogEntry {
        assumed_offset: LogPos,
        entry_offset: LogPos,
        datum: Vec<u8>
    },
    CommitEntriesUpto {
        offset: LogPos,
    }
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ServerResponse { }

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ClientRequest { 
    LogItem(Vec<u8>),
    AwaitCommit(LogPos),
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ClientResponse { }
impl ClientResponse {
    pub fn position(&self) -> LogPos {
        unimplemented!()
    }
}
