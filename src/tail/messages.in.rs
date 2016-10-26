use replica::LogPos;
use serde::bytes::ByteBuf;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum TailRequest {
    FetchNextAfter(LogPos),
}
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum TailResponse {
    NextItem(LogPos, ByteBuf),
}
impl TailResponse {
    pub fn position(&self) -> LogPos {
        unimplemented!()
    }
}
