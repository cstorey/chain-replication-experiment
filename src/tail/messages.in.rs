use replica::LogPos;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum TailRequest {
    FetchNextAfter(LogPos),
}
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum TailResponse {
    NextItem(LogPos, Vec<u8>),
}
impl TailResponse {
    pub fn position(&self) -> LogPos {
        unimplemented!()
    }
}
