use replica::LogPos;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum TailRequest {
    FetchNextAfter(LogPos),
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum TailResponse {
    NextItem(LogPos, Vec<u8>),
}
impl TailResponse {
    pub fn position(&self) -> LogPos {
        unimplemented!()
    }
}
