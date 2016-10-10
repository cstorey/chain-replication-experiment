use replica::LogPos;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum TailRequest {
    AwaitCommit(LogPos),
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum TailResponse {
    Done(LogPos),
}
impl TailResponse {
    pub fn position(&self) -> LogPos {
        unimplemented!()
    }
}
