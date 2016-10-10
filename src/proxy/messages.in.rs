use replica::LogPos;

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
